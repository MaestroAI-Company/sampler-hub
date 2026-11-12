mod mcp;
pub use mcp::*;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
mod lan;
mod profile;
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
mod proxy;
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod web;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::{Connection, ConnectionError, RecvStream, SendStream, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey};
use sampler_hub_proto::HubSettings as ProtoSettings;
use sampler_hub_proto::invite::{self, INVITE_ALPN, InviteRequest, InviteResponse};
use sampler_hub_proto::{
    ADMIN_ALPN, AdminRequest, AdminResponse, ForwardHead, HUB_ALPN, HubRequest, HubResponse,
    LOGIN_ALPN, LoginRequest, LoginResponse, MODELS_ALPN, ModelsRequest, ModelsResponse, PAIR_ALPN, PROFILES_ALPN, PROVIDERS_ALPN,
    PairRejection, PairRequest, PairResponse, PairingLink, ProfileInfo, ProfileRequest,
    ProfileResponse, ProfileRole, ProvidersRequest, ProvidersResponse, Record, SECRET_COLLECTIONS,
    SECRETS_ALPN, SYNC_ALPN, SecretsRequest, SecretsResponse, SyncRequest, SyncResponse,
    UNAUTHORIZED, auth_hash, hub_addr, machine_tag, read_frame, request, write_frame,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::{Mutex, OnceCell};

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
uniffi::setup_scaffolding!();

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
//long enough for every hub nearby to answer
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
const LAN_BROWSE: Duration = Duration::from_secs(2);
//codes are typed after the hub published them
const CODE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
//slowest relay upload still allowed to finish
const MIN_UPLOAD_BYTES_PER_SEC: usize = 32 * 1024;

#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Error),
    uniffi(flat_error)
)]
pub enum HubError {
    #[error("invalid pairing link: {0}")]
    InvalidLink(String),
    #[error("pairing code not recognized")]
    PairingRejected,
    #[error("pairing code expired")]
    PairingExpired,
    #[error("this device is not paired with the hub")]
    Unauthorized,
    #[error("hub unreachable: {0}")]
    Unreachable(String),
    #[error("the hub is too old for this app, update it")]
    Outdated,
    #[error("no profile is unlocked on this device")]
    Locked,
    #[error("{0}")]
    Internal(String),
}

//desktop and web read the code like android
impl Serialize for HubError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let code = match self {
            HubError::InvalidLink(_) => "InvalidLink",
            HubError::PairingRejected => "PairingRejected",
            HubError::PairingExpired => "PairingExpired",
            HubError::Unauthorized => "Unauthorized",
            HubError::Unreachable(_) => "Unreachable",
            HubError::Outdated => "Outdated",
            HubError::Locked => "Locked",
            HubError::Internal(_) => "Internal",
        };
        let mut error = serializer.serialize_struct("HubError", 2)?;
        error.serialize_field("code", code)?;
        error.serialize_field("message", &self.to_string())?;
        error.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct PairedHub {
    pub id: String,
    pub name: String,
    pub relays: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubInvite {
    pub code: String,
    //shown as a qr code
    pub link: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubSettings {
    pub lan_discovery: bool,
    pub share_providers: bool,
    pub mcp_servers: bool,
    pub models: bool,
    pub models_nightly: bool,
    pub ctx_size: u32,
    pub keep_alive: u32,
    pub parallel: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct NearbyHub {
    pub id: String,
    pub name: String,
    pub addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    pub repo: String,
    pub file: String,
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ModelRepo {
    pub repo: String,
    pub downloads: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ModelFile {
    pub file: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct InstalledModel {
    pub file: String,
    pub size: u64,
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ModelDownload {
    pub repo: String,
    pub file: String,
    pub received: u64,
    pub total: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct LlamaStatus {
    pub state: String,
    pub backend: Option<String>,
    pub build: Option<String>,
    pub latest: Option<String>,
    pub channel: Option<String>,
    pub message: Option<String>,
    pub installed: Vec<InstalledModel>,
    pub downloads: Vec<ModelDownload>,
    pub cpu: Option<f32>,
    pub memory: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubStatus {
    pub name: String,
    pub version: String,
    //false while relayed
    pub direct: bool,
    //a newer release the hub can install
    pub update: Option<String>,
    //share of the hub machine, with everything it runs
    pub cpu: Option<f32>,
    pub memory: Option<u64>,
    //llama.cpp options set by an admin, read only here
    pub ctx_size: Option<u32>,
    pub keep_alive: Option<u32>,
    pub parallel: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubDevice {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub paired_at: u64,
    pub paired_by: Option<String>,
    pub last_seen: Option<u64>,
    pub profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct SyncRecord {
    pub id: String,
    pub updated_at: u64,
    pub deleted: bool,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct PullPage {
    pub records: Vec<SyncRecord>,
    pub cursor: u64,
    pub more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubProvider {
    pub id: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct HubProfile {
    pub id: String,
    pub name: String,
    //admin or member
    pub role: String,
    pub created_at: u64,
    pub butterfly: Option<String>,
}

//the app keeps the key securely
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct UnlockedProfile {
    pub profile: HubProfile,
    pub key: String,
    //none after a device link, the hub has it
    pub lock: Option<ProfileLock>,
}

//pair and unlock in one step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct SignedIn {
    pub hub: PairedHub,
    pub profile: UnlockedProfile,
}

//what a cloud backup needs to reopen the key
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ProfileLock {
    pub salt: String,
    pub memory_kib: u32,
    pub passes: u32,
    pub wrapped: String,
}

//compare these digits before approving
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct LinkJoining {
    pub device: String,
    pub digits: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct LinkProgress {
    //shown once the other device answered
    pub digits: Option<String>,
    pub profile: Option<UnlockedProfile>,
    pub denied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct ProxyInfo {
    pub port: u16,
    pub token: String,
}

#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Object)
)]
pub struct HubClient {
    secret_key: SecretKey,
    //binding needs the tokio runtime
    endpoint: OnceCell<Endpoint>,
    //kept open so paths can turn direct
    connections: Mutex<HashMap<(String, &'static [u8]), Connection>>,
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    proxy: Mutex<Option<ProxyInfo>>,
    //addresses found on the local network
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    lan: std::sync::Mutex<HashMap<String, Vec<std::net::SocketAddr>>>,
    //seals what this device syncs
    profile: std::sync::Mutex<Option<Unlocked>>,
    //one device link at a time
    link: Mutex<Option<LinkSession>>,
}

struct LinkSession {
    keys: profile::LinkKeys,
    //joiner key or approver commitment
    other: Option<String>,
}

#[derive(Clone)]
struct Unlocked {
    id: String,
    key: profile::Key,
}

#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export(async_runtime = "tokio")
)]
impl HubClient {
    //the app creates and stores it
    #[cfg_attr(
        not(all(target_family = "wasm", target_os = "unknown")),
        uniffi::constructor
    )]
    pub fn new(secret_key: Vec<u8>) -> Result<Arc<Self>, HubError> {
        let bytes: [u8; 32] = secret_key
            .try_into()
            .map_err(|_| HubError::Internal("device key must be 32 bytes".into()))?;
        Ok(Arc::new(Self {
            secret_key: SecretKey::from_bytes(&bytes),
            endpoint: OnceCell::new(),
            connections: Mutex::new(HashMap::new()),
            #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
            proxy: Mutex::new(None),
            #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
            lan: Default::default(),
            profile: Default::default(),
            link: Mutex::new(None),
        }))
    }

    //key comes from the secure store
    pub fn set_profile(&self, profile_id: String, key: String) -> Result<(), HubError> {
        let key = profile::decode_key(&key).map_err(internal)?;
        *self.profile.lock().unwrap() = Some(Unlocked {
            id: profile_id,
            key,
        });
        Ok(())
    }

    pub fn clear_profile(&self) {
        *self.profile.lock().unwrap() = None;
    }

    pub async fn profiles(&self, hub: PairedHub) -> Result<Vec<HubProfile>, HubError> {
        match self.profile_request(&hub, ProfileRequest::List).await? {
            ProfileResponse::Profiles { profiles } => {
                Ok(profiles.into_iter().map(hub_profile).collect())
            }
            other => Err(unexpected_profile(other)),
        }
    }

    //the key never leaves unsealed
    pub async fn create_profile(
        &self,
        hub: PairedHub,
        name: String,
        password: String,
    ) -> Result<UnlockedProfile, HubError> {
        let key = profile::new_key().map_err(internal)?;
        let (locked, auth_key) =
            blocking(move || profile::lock(&key, &password, profile::MEMORY_KIB, profile::PASSES))
                .await?;
        let request = ProfileRequest::Create {
            name,
            locked: locked.clone(),
            auth_hash: auth_hash(&auth_key),
        };
        match self.profile_request(&hub, request).await? {
            ProfileResponse::Profile { profile } => self.unlocked_with(profile, key, Some(locked)),
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn unlock_profile(
        &self,
        hub: PairedHub,
        profile_id: String,
        password: String,
    ) -> Result<UnlockedProfile, HubError> {
        let derived = self.derive(&hub, &profile_id, password).await?;
        let request = ProfileRequest::Unlock {
            profile: profile_id,
            auth_key: derived.auth_key.clone(),
        };
        match self.profile_request(&hub, request).await? {
            ProfileResponse::Unlocked { profile, locked } => {
                let key = profile::unlock(&derived, &locked).map_err(internal)?;
                self.unlocked_with(profile, key, Some(locked))
            }
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn change_password(
        &self,
        hub: PairedHub,
        current: String,
        new: String,
    ) -> Result<(), HubError> {
        let unlocked = self.unlocked()?;
        let derived = self.derive(&hub, &unlocked.id, current).await?;
        let (locked, auth_key) = blocking(move || {
            profile::lock(&unlocked.key, &new, profile::MEMORY_KIB, profile::PASSES)
        })
        .await?;
        let request = ProfileRequest::ChangePassword {
            auth_key: derived.auth_key,
            locked,
            auth_hash: auth_hash(&auth_key),
        };
        match self.profile_request(&hub, request).await? {
            ProfileResponse::Done => Ok(()),
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn rename_profile(
        &self,
        hub: PairedHub,
        name: String,
    ) -> Result<HubProfile, HubError> {
        match self
            .profile_request(&hub, ProfileRequest::Rename { name })
            .await?
        {
            ProfileResponse::Profile { profile } => Ok(hub_profile(profile)),
            other => Err(unexpected_profile(other)),
        }
    }

    //the card look other devices see
    pub async fn set_profile_butterfly(
        &self,
        hub: PairedHub,
        butterfly: String,
    ) -> Result<HubProfile, HubError> {
        match self
            .profile_request(&hub, ProfileRequest::SetButterfly { butterfly })
            .await?
        {
            ProfileResponse::Profile { profile } => Ok(hub_profile(profile)),
            other => Err(unexpected_profile(other)),
        }
    }

    //the hub copy, for the cloud backup header
    pub async fn locked_key(&self, hub: PairedHub) -> Result<ProfileLock, HubError> {
        match self
            .profile_request(&hub, ProfileRequest::LockedKey)
            .await?
        {
            ProfileResponse::LockedKey { locked } => Ok(ProfileLock {
                salt: locked.salt,
                memory_kib: locked.memory_kib,
                passes: locked.passes,
                wrapped: locked.wrapped,
            }),
            other => Err(unexpected_profile(other)),
        }
    }

    //a code for another device of this profile
    pub async fn link_start(&self, hub: PairedHub) -> Result<HubInvite, HubError> {
        self.unlocked()?;
        let keys = profile::LinkKeys::new().map_err(internal)?;
        let request = ProfileRequest::LinkStart {
            commitment: keys.commitment(),
        };
        match self.profile_request(&hub, request).await? {
            ProfileResponse::Code { code, link } => {
                *self.link.lock().await = Some(LinkSession { keys, other: None });
                Ok(HubInvite { code, link })
            }
            other => Err(unexpected_profile(other)),
        }
    }

    //none until a device typed the code
    pub async fn link_watch(&self, hub: PairedHub) -> Result<Option<LinkJoining>, HubError> {
        let mut link = self.link.lock().await;
        let session = link
            .as_mut()
            .ok_or_else(|| HubError::Internal("no link in progress".into()))?;
        match self
            .profile_request(&hub, ProfileRequest::LinkWatch)
            .await?
        {
            ProfileResponse::Waiting => Ok(None),
            ProfileResponse::Joining { device, public } => {
                let request = ProfileRequest::LinkReveal {
                    public: session.keys.public.clone(),
                };
                match self.profile_request(&hub, request).await? {
                    ProfileResponse::Done => {}
                    other => return Err(unexpected_profile(other)),
                }
                let digits = profile::link_digits(&session.keys.public, &public);
                session.other = Some(public);
                Ok(Some(LinkJoining { device, digits }))
            }
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn link_approve(&self, hub: PairedHub) -> Result<(), HubError> {
        let key = self.unlocked()?.key;
        let session = self
            .link
            .lock()
            .await
            .take()
            .ok_or_else(|| HubError::Internal("no link in progress".into()))?;
        let joiner = session
            .other
            .ok_or_else(|| HubError::Internal("no device is waiting".into()))?;
        let sealed = session.keys.seal_for(&joiner, &key).map_err(internal)?;
        match self
            .profile_request(&hub, ProfileRequest::LinkApprove { sealed })
            .await?
        {
            ProfileResponse::Done => Ok(()),
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn link_deny(&self, hub: PairedHub) -> Result<(), HubError> {
        self.link.lock().await.take();
        match self.profile_request(&hub, ProfileRequest::LinkDeny).await? {
            ProfileResponse::Done => Ok(()),
            other => Err(unexpected_profile(other)),
        }
    }

    //right after pairing with a link code
    pub async fn link_join(&self, hub: PairedHub) -> Result<(), HubError> {
        let keys = profile::LinkKeys::new().map_err(internal)?;
        let request = ProfileRequest::LinkJoin {
            public: keys.public.clone(),
        };
        match self.profile_request(&hub, request).await? {
            ProfileResponse::Joined { commitment } => {
                *self.link.lock().await = Some(LinkSession {
                    keys,
                    other: Some(commitment),
                });
                Ok(())
            }
            other => Err(unexpected_profile(other)),
        }
    }

    pub async fn link_fetch(&self, hub: PairedHub) -> Result<LinkProgress, HubError> {
        let mut link = self.link.lock().await;
        let session = link
            .as_ref()
            .ok_or_else(|| HubError::Internal("no link in progress".into()))?;
        let ProfileResponse::Link {
            public,
            sealed,
            profile: info,
            denied,
        } = self
            .profile_request(&hub, ProfileRequest::LinkFetch)
            .await?
        else {
            return Err(HubError::Internal("unexpected answer to a link".into()));
        };
        let digits = match &public {
            //a relay swapping keys breaks the commitment
            Some(public) if session.other.as_deref() != Some(&profile::commitment(public)) => {
                link.take();
                return Err(HubError::Internal("the link was tampered with".into()));
            }
            Some(public) => Some(profile::link_digits(public, &session.keys.public)),
            None => None,
        };
        let unlocked = match (public, sealed, info) {
            (Some(public), Some(sealed), Some(info)) => {
                let key = session.keys.open_from(&public, &sealed).map_err(internal)?;
                link.take();
                Some(self.unlocked_with(info, key, None)?)
            }
            _ => None,
        };
        if denied {
            link.take();
        }
        Ok(LinkProgress {
            digits,
            profile: unlocked,
            denied,
        })
    }

    //admin only, removes data and devices
    pub async fn remove_profile(&self, hub: PairedHub, id: String) -> Result<(), HubError> {
        self.admin_profiles(&hub, AdminRequest::RemoveProfile { id })
            .await
    }

    pub async fn set_profile_role(
        &self,
        hub: PairedHub,
        id: String,
        role: String,
    ) -> Result<(), HubError> {
        let role = match role.as_str() {
            "admin" => ProfileRole::Admin,
            "member" => ProfileRole::Member,
            other => return Err(HubError::Internal(format!("unknown role {other}"))),
        };
        self.admin_profiles(&hub, AdminRequest::SetRole { id, role })
            .await
    }

    //the id the hub lists for this device
    pub fn device_id(&self) -> String {
        self.secret_key.public().to_string()
    }

    //machine is a stable os id, hashed before it leaves
    pub async fn pair(
        &self,
        link: String,
        device_name: String,
        kind: String,
        machine: Option<String>,
    ) -> Result<PairedHub, HubError> {
        let link: PairingLink = link
            .parse()
            .map_err(|e| HubError::InvalidLink(format!("{e:#}")))?;
        let conn = self
            .connect(hub_addr(link.hub, &link.relays), PAIR_ALPN)
            .await?;
        let (send, recv) = conn.open_bi().await.map_err(unreachable)?;
        let mut stream = Stream {
            conn: conn.clone(),
            send,
            recv,
        };
        let response = stream
            .request(&PairRequest {
                secret: link.secret,
                name: device_name,
                kind,
                machine: machine.map(|raw| machine_tag(&link.hub, &raw)),
            })
            .await?;
        stream.finish();
        conn.close(0u32.into(), b"done");
        match response {
            PairResponse::Paired { hub_name } => Ok(PairedHub {
                id: link.hub.to_string(),
                name: hub_name,
                relays: link.relays.iter().map(|url| url.to_string()).collect(),
            }),
            PairResponse::Rejected {
                reason: PairRejection::Invalid,
            } => Err(HubError::PairingRejected),
            PairResponse::Rejected {
                reason: PairRejection::Expired,
            } => Err(HubError::PairingExpired),
        }
    }

    //the code a hub or paired device shows
    pub async fn pair_with_code(
        &self,
        code: String,
        device_name: String,
        kind: String,
        machine: Option<String>,
    ) -> Result<PairedHub, HubError> {
        let keys = derive_code(code).await?;
        //a wrong code has no address to find
        let conn = within(
            CODE_CONNECT_TIMEOUT,
            self.connect(EndpointAddr::new(keys.id()), INVITE_ALPN),
        )
        .await
        .map_err(|_| HubError::PairingRejected)??;
        let (send, recv) = conn.open_bi().await.map_err(unreachable)?;
        let mut stream = Stream {
            conn: conn.clone(),
            send,
            recv,
        };
        let me = self.secret_key.public();
        let response = stream
            .request(&InviteRequest {
                proof: keys.proof(&me),
            })
            .await?;
        stream.finish();
        conn.close(0u32.into(), b"done");
        match response {
            InviteResponse::Link { link } => self.pair(link, device_name, kind, machine).await,
            InviteResponse::Rejected => Err(HubError::PairingRejected),
        }
    }

    //a code and qr link for a new device
    pub async fn invite(&self, hub: PairedHub) -> Result<HubInvite, HubError> {
        let mut stream = self.stream(&hub, ADMIN_ALPN).await?;
        let response = stream.request(&AdminRequest::Invite).await?;
        stream.finish();
        match response {
            AdminResponse::Invite { code, link } => Ok(HubInvite { code, link }),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //true once a device used the code
    pub async fn invite_watch(&self, hub: PairedHub) -> Result<bool, HubError> {
        let mut stream = self.stream(&hub, ADMIN_ALPN).await?;
        let response = stream.request(&AdminRequest::InviteWatch).await?;
        stream.finish();
        match response {
            AdminResponse::InviteUsed => Ok(true),
            AdminResponse::Waiting => Ok(false),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //any device renames itself, admins rename others
    pub async fn rename_device(
        &self,
        hub: PairedHub,
        id: String,
        name: String,
    ) -> Result<(), HubError> {
        if id == self.device_id() {
            let mut stream = self.stream(&hub, HUB_ALPN).await?;
            let response = stream.request(&HubRequest::Rename { name }).await?;
            stream.finish();
            return match response {
                HubResponse::Renamed => Ok(()),
                HubResponse::Error { message } => Err(HubError::Internal(message)),
                other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
            };
        }
        self.admin_devices(&hub, AdminRequest::RenameDevice { id, name })
            .await
    }

    //admins only, the device loses access at once
    pub async fn remove_device(&self, hub: PairedHub, id: String) -> Result<(), HubError> {
        self.admin_devices(&hub, AdminRequest::RemoveDevice { id })
            .await
    }

    pub async fn models_catalog(&self, hub: PairedHub) -> Result<Vec<CatalogModel>, HubError> {
        match self.models(&hub, ModelsRequest::Catalog).await? {
            ModelsResponse::Catalog { models } => Ok(models
                .into_iter()
                .map(|m| CatalogModel {
                    repo: m.repo,
                    file: m.file,
                    name: m.name,
                    size: m.size,
                })
                .collect()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //gguf repositories on hugging face
    pub async fn models_search(
        &self,
        hub: PairedHub,
        query: String,
    ) -> Result<Vec<ModelRepo>, HubError> {
        match self.models(&hub, ModelsRequest::Search { query }).await? {
            ModelsResponse::Repos { repos } => Ok(repos
                .into_iter()
                .map(|r| ModelRepo {
                    repo: r.repo,
                    downloads: r.downloads,
                })
                .collect()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn models_files(
        &self,
        hub: PairedHub,
        repo: String,
    ) -> Result<Vec<ModelFile>, HubError> {
        match self.models(&hub, ModelsRequest::Files { repo }).await? {
            ModelsResponse::Files { files } => Ok(files.into_iter().map(model_file).collect()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn models_download(
        &self,
        hub: PairedHub,
        repo: String,
        file: String,
    ) -> Result<LlamaStatus, HubError> {
        self.models_status_of(&hub, ModelsRequest::Download { repo, file })
            .await
    }

    pub async fn models_cancel(
        &self,
        hub: PairedHub,
        file: String,
    ) -> Result<LlamaStatus, HubError> {
        self.models_status_of(&hub, ModelsRequest::Cancel { file })
            .await
    }

    pub async fn models_delete(
        &self,
        hub: PairedHub,
        file: String,
    ) -> Result<LlamaStatus, HubError> {
        self.models_status_of(&hub, ModelsRequest::Delete { file })
            .await
    }

    pub async fn models_status(&self, hub: PairedHub) -> Result<LlamaStatus, HubError> {
        self.models_status_of(&hub, ModelsRequest::Status).await
    }

    //the hub installs the release and restarts
    pub async fn update_hub(&self, hub: PairedHub) -> Result<String, HubError> {
        let mut stream = self.stream(&hub, ADMIN_ALPN).await?;
        let response = stream.request(&AdminRequest::Update).await?;
        stream.finish();
        match response {
            AdminResponse::Updating { version } => Ok(version),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn hub_settings(&self, hub: PairedHub) -> Result<HubSettings, HubError> {
        self.admin(&hub, AdminRequest::Settings).await
    }

    pub async fn set_hub_settings(
        &self,
        hub: PairedHub,
        settings: HubSettings,
    ) -> Result<HubSettings, HubError> {
        let settings = ProtoSettings {
            lan_discovery: settings.lan_discovery,
            share_providers: settings.share_providers,
            mcp_servers: settings.mcp_servers,
            models: settings.models,
            models_nightly: settings.models_nightly,
            ctx_size: settings.ctx_size,
            keep_alive: settings.keep_alive,
            parallel: settings.parallel,
        };
        self.admin(&hub, AdminRequest::SetSettings { settings })
            .await
    }

    //hubs announcing themselves on this network
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub async fn nearby_hubs(&self, wait_ms: u64) -> Result<Vec<NearbyHub>, HubError> {
        let found = self.browse_lan(Duration::from_millis(wait_ms)).await?;
        Ok(found
            .into_iter()
            .map(|hub| NearbyHub {
                id: hub.id,
                name: hub.name,
                addrs: hub.addrs.iter().map(|a| a.to_string()).collect(),
            })
            .collect())
    }

    //profiles a nearby hub offers before pairing
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub async fn nearby_profiles(&self, hub: NearbyHub) -> Result<Vec<HubProfile>, HubError> {
        match self.login_request(&nearby(&hub), LoginRequest::List).await? {
            LoginResponse::Profiles { profiles } => {
                Ok(profiles.into_iter().map(hub_profile).collect())
            }
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //the password pairs this device, no code needed
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub async fn sign_in(
        &self,
        hub: NearbyHub,
        profile_id: String,
        password: String,
        device_name: String,
        kind: String,
        machine: Option<String>,
    ) -> Result<SignedIn, HubError> {
        let paired = nearby(&hub);
        let request = LoginRequest::Cost {
            profile: profile_id.clone(),
        };
        let LoginResponse::Cost {
            salt,
            memory_kib,
            passes,
        } = self.login_request(&paired, request).await?
        else {
            return Err(HubError::Internal("unexpected answer to a cost".into()));
        };
        let derived =
            blocking(move || profile::derive(&password, &salt, memory_kib, passes)).await?;
        let hub_id: EndpointId = paired
            .id
            .parse()
            .map_err(|_| HubError::Internal("invalid hub id".into()))?;
        let request = LoginRequest::Login {
            profile: profile_id,
            auth_key: derived.auth_key.clone(),
            name: device_name,
            kind,
            machine: machine.map(|raw| machine_tag(&hub_id, &raw)),
        };
        match self.login_request(&paired, request).await? {
            LoginResponse::Unlocked {
                hub_name,
                profile,
                locked,
            } => {
                let key = profile::unlock(&derived, &locked).map_err(internal)?;
                Ok(SignedIn {
                    hub: PairedHub {
                        name: hub_name,
                        ..paired
                    },
                    profile: self.unlocked_with(profile, key, Some(locked))?,
                })
            }
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn status(&self, hub: PairedHub) -> Result<HubStatus, HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let response = stream.request(&HubRequest::Hello).await?;
        let direct = stream
            .conn
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip());
        stream.finish();
        match response {
            HubResponse::Hello {
                name,
                version,
                update,
                usage,
                model_options,
            } => Ok(HubStatus {
                name,
                version,
                direct,
                update,
                cpu: usage.map(|u| u.cpu),
                memory: usage.map(|u| u.memory),
                ctx_size: model_options.map(|o| o.ctx_size),
                keep_alive: model_options.map(|o| o.keep_alive),
                parallel: model_options.map(|o| o.parallel),
            }),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn unpair(&self, hub: PairedHub) -> Result<(), HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let _: HubResponse = stream.request(&HubRequest::Unpair).await?;
        stream.finish();
        //cached sessions would now be refused
        let mut connections = self.connections.lock().await;
        connections.retain(|(id, _), conn| {
            if *id == hub.id {
                conn.close(0u32.into(), b"unpaired");
            }
            *id != hub.id
        });
        Ok(())
    }

    pub async fn devices(&self, hub: PairedHub) -> Result<Vec<HubDevice>, HubError> {
        let mut stream = self.stream(&hub, HUB_ALPN).await?;
        let response = stream.request(&HubRequest::Devices).await?;
        stream.finish();
        let HubResponse::Devices { devices } = response else {
            return Err(HubError::Internal(format!(
                "unexpected answer {response:?}"
            )));
        };
        Ok(devices
            .into_iter()
            .map(|d| HubDevice {
                id: d.id,
                name: d.name,
                kind: d.kind,
                scopes: d.scopes,
                paired_at: d.paired_at,
                paired_by: d.paired_by,
                last_seen: d.last_seen,
                profile: d.profile,
            })
            .collect())
    }

    pub async fn push(
        &self,
        hub: PairedHub,
        collection: String,
        records: Vec<SyncRecord>,
    ) -> Result<(), HubError> {
        let unlocked = self.unlocked()?;
        let records = records
            .into_iter()
            .map(|r| {
                let record = Record {
                    id: r.id,
                    updated_at: r.updated_at,
                    deleted: r.deleted,
                    payload: r.payload,
                };
                profile::seal_record(&unlocked.key, &unlocked.id, &collection, record)
            })
            .collect::<anyhow::Result<_>>()
            .map_err(internal)?;
        //provider secrets need their own scope
        if SECRET_COLLECTIONS.contains(&collection.as_str()) {
            let request = SecretsRequest::Push {
                collection,
                records,
            };
            return match self.secrets(&hub, request).await? {
                SecretsResponse::Pushed => Ok(()),
                other => Err(unexpected(other)),
            };
        }
        let request = SyncRequest::Push {
            collection,
            records,
        };
        match self.data(&hub, request).await? {
            SyncResponse::Pushed => Ok(()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    pub async fn pull(
        &self,
        hub: PairedHub,
        collection: String,
        since: u64,
    ) -> Result<PullPage, HubError> {
        let unlocked = self.unlocked()?;
        let name = collection.clone();
        let (records, cursor, more) = if SECRET_COLLECTIONS.contains(&collection.as_str()) {
            match self
                .secrets(&hub, SecretsRequest::Pull { collection, since })
                .await?
            {
                SecretsResponse::Records {
                    records,
                    cursor,
                    more,
                } => (records, cursor, more),
                other => return Err(unexpected(other)),
            }
        } else {
            match self
                .data(&hub, SyncRequest::Pull { collection, since })
                .await?
            {
                SyncResponse::Records {
                    records,
                    cursor,
                    more,
                } => (records, cursor, more),
                other => return Err(HubError::Internal(format!("unexpected answer {other:?}"))),
            }
        };
        let records = records
            .into_iter()
            .map(|record| {
                let r = profile::open_record(&unlocked.key, &unlocked.id, &name, record)?;
                Ok(SyncRecord {
                    id: r.id,
                    updated_at: r.updated_at,
                    deleted: r.deleted,
                    payload: r.payload,
                })
            })
            .collect::<anyhow::Result<_>>()
            .map_err(internal)?;
        Ok(PullPage {
            records,
            cursor,
            more,
        })
    }

    //waits until the hub has anything newer than since
    pub async fn watch(&self, hub: PairedHub, since: u64) -> Result<u64, HubError> {
        match self.data(&hub, SyncRequest::Watch { since }).await? {
            SyncResponse::Changed { cursor } => Ok(cursor),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //false while another device of the profile holds it
    pub async fn lease(&self, hub: PairedHub, name: String) -> Result<bool, HubError> {
        match self.secrets(&hub, SecretsRequest::Lease { name }).await? {
            SecretsResponse::Lease { granted } => Ok(granted),
            other => Err(unexpected(other)),
        }
    }

    pub async fn release(&self, hub: PairedHub, name: String) -> Result<(), HubError> {
        match self.secrets(&hub, SecretsRequest::Release { name }).await? {
            SecretsResponse::Released => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn providers(&self, hub: PairedHub) -> Result<Vec<HubProvider>, HubError> {
        let mut stream = self.stream(&hub, PROVIDERS_ALPN).await?;
        let response = stream.request(&ProvidersRequest::List).await?;
        stream.finish();
        match response {
            ProvidersResponse::Providers { providers } => Ok(providers
                .into_iter()
                .map(|p| HubProvider {
                    id: p.id,
                    name: p.name,
                    kind: p.kind,
                })
                .collect()),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //local entry point for providers
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub async fn start_proxy(self: Arc<Self>, hub: PairedHub) -> Result<ProxyInfo, HubError> {
        let mut proxy = self.proxy.lock().await;
        if let Some(info) = proxy.as_ref() {
            return Ok(info.clone());
        }
        let info = proxy::start(self.clone(), hub).await?;
        *proxy = Some(info.clone());
        Ok(info)
    }
}

impl HubClient {
    fn unlocked(&self) -> Result<Unlocked, HubError> {
        self.profile.lock().unwrap().clone().ok_or(HubError::Locked)
    }

    fn unlocked_with(
        &self,
        profile: ProfileInfo,
        key: profile::Key,
        locked: Option<sampler_hub_proto::LockedKey>,
    ) -> Result<UnlockedProfile, HubError> {
        *self.profile.lock().unwrap() = Some(Unlocked {
            id: profile.id.clone(),
            key,
        });
        Ok(UnlockedProfile {
            profile: hub_profile(profile),
            key: profile::encode_key(&key),
            lock: locked.map(|locked| ProfileLock {
                salt: locked.salt,
                memory_kib: locked.memory_kib,
                passes: locked.passes,
                wrapped: locked.wrapped,
            }),
        })
    }

    async fn derive(
        &self,
        hub: &PairedHub,
        profile: &str,
        password: String,
    ) -> Result<profile::Derived, HubError> {
        let request = ProfileRequest::Cost {
            profile: profile.to_string(),
        };
        let ProfileResponse::Cost {
            salt,
            memory_kib,
            passes,
        } = self.profile_request(hub, request).await?
        else {
            return Err(HubError::Internal("unexpected answer to a cost".into()));
        };
        blocking(move || profile::derive(&password, &salt, memory_kib, passes)).await
    }

    async fn profile_request(
        &self,
        hub: &PairedHub,
        request: ProfileRequest,
    ) -> Result<ProfileResponse, HubError> {
        let mut stream = self.stream(hub, PROFILES_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            ProfileResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    async fn login_request(
        &self,
        hub: &PairedHub,
        request: LoginRequest,
    ) -> Result<LoginResponse, HubError> {
        let mut stream = self.stream(hub, LOGIN_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            LoginResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    async fn admin_profiles(&self, hub: &PairedHub, request: AdminRequest) -> Result<(), HubError> {
        let mut stream = self.stream(hub, ADMIN_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            AdminResponse::Profiles { .. } => Ok(()),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn endpoint(&self) -> Result<&Endpoint, HubError> {
        self.endpoint
            .get_or_try_init(|| async {
                Endpoint::builder(presets::N0)
                    .secret_key(self.secret_key.clone())
                    .bind()
                    .await
                    .map_err(unreachable)
            })
            .await
    }

    async fn connect(&self, addr: EndpointAddr, alpn: &[u8]) -> Result<Connection, HubError> {
        let endpoint = self.endpoint().await?;
        let conn = within(CONNECT_TIMEOUT, endpoint.connect(addr, alpn)).await?;
        conn.map_err(|e| {
            //tls alert for an alpn the hub does not serve
            if e.to_string().contains("error 120") {
                HubError::Outdated
            } else {
                unreachable(e)
            }
        })
    }

    //reconnects once if the connection died
    async fn stream(&self, hub: &PairedHub, alpn: &'static [u8]) -> Result<Stream, HubError> {
        let key = (hub.id.clone(), alpn);
        for _ in 0..2 {
            let conn = {
                let mut connections = self.connections.lock().await;
                match connections.get(&key) {
                    Some(conn) if conn.close_reason().is_none() => conn.clone(),
                    _ => {
                        let conn = match self.connect(self.with_lan(paired_addr(hub)?), alpn).await
                        {
                            Ok(conn) => conn,
                            //offline networks still reach a hub nearby
                            Err(HubError::Unreachable(_)) if self.found_on_lan(&hub.id).await => {
                                self.connect(self.with_lan(paired_addr(hub)?), alpn).await?
                            }
                            Err(e) => return Err(e),
                        };
                        connections.insert(key.clone(), conn.clone());
                        conn
                    }
                }
            };
            match conn.open_bi().await {
                Ok((send, recv)) => return Ok(Stream { conn, send, recv }),
                Err(_) if refused(&conn) => return Err(HubError::Unauthorized),
                Err(_) => continue,
            }
        }
        Err(HubError::Unreachable("connection keeps closing".into()))
    }

    //one provider request through the hub
    pub(crate) async fn forward(
        &self,
        hub: &PairedHub,
        head: ForwardHead,
        body: &[u8],
    ) -> Result<Forwarded, HubError> {
        let mut stream = self.stream(hub, PROVIDERS_ALPN).await?;
        write_frame(&mut stream.send, &ProvidersRequest::Forward { head })
            .await
            .map_err(unreachable)?;
        stream.send.write_all(body).await.map_err(unreachable)?;
        stream.send.finish().map_err(unreachable)?;
        let response = read_frame(&mut stream.recv)
            .await
            .map_err(unreachable)?
            .ok_or_else(|| HubError::Unreachable("hub closed the stream".into()))?;
        match response {
            ProvidersResponse::Response { status, headers } => Ok(Forwarded::Response {
                status,
                headers,
                body: stream.recv,
            }),
            ProvidersResponse::Error { message } => Ok(Forwarded::Refused(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    //duplex, stream stays open
    pub(crate) async fn realtime(
        &self,
        hub: &PairedHub,
        head: ForwardHead,
    ) -> Result<(SendStream, RecvStream), HubError> {
        let mut stream = self.stream(hub, PROVIDERS_ALPN).await?;
        write_frame(&mut stream.send, &ProvidersRequest::Forward { head })
            .await
            .map_err(unreachable)?;
        let response = read_frame(&mut stream.recv)
            .await
            .map_err(unreachable)?
            .ok_or_else(|| HubError::Unreachable("hub closed the stream".into()))?;
        match response {
            ProvidersResponse::Response { status: 101, .. } => Ok((stream.send, stream.recv)),
            ProvidersResponse::Response { status, .. } => Err(HubError::Internal(format!(
                "realtime refused with {status}"
            ))),
            ProvidersResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn admin<T>(&self, hub: &PairedHub, request: AdminRequest) -> Result<T, HubError>
    where
        T: From<ProtoSettings>,
    {
        let mut stream = self.stream(hub, ADMIN_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            AdminResponse::Settings { settings } => Ok(settings.into()),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    fn with_lan(&self, addr: EndpointAddr) -> EndpointAddr {
        let known = self
            .lan
            .lock()
            .ok()
            .and_then(|lan| lan.get(&addr.id.to_string()).cloned());
        known
            .unwrap_or_default()
            .into_iter()
            .fold(addr, |addr, ip| addr.with_ip_addr(ip))
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    fn with_lan(&self, addr: EndpointAddr) -> EndpointAddr {
        addr
    }

    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    async fn browse_lan(&self, wait: Duration) -> Result<Vec<lan::Found>, HubError> {
        let found = tokio::task::spawn_blocking(move || lan::browse(wait))
            .await
            .map_err(|e| HubError::Internal(e.to_string()))?
            .map_err(HubError::Internal)?;
        if let Ok(mut known) = self.lan.lock() {
            for hub in &found {
                known.insert(hub.id.clone(), hub.addrs.clone());
            }
        }
        Ok(found)
    }

    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    async fn found_on_lan(&self, id: &str) -> bool {
        self.browse_lan(LAN_BROWSE)
            .await
            .is_ok_and(|found| found.iter().any(|hub| hub.id == id))
    }

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    async fn found_on_lan(&self, _id: &str) -> bool {
        false
    }

    async fn admin_devices(&self, hub: &PairedHub, request: AdminRequest) -> Result<(), HubError> {
        let mut stream = self.stream(hub, ADMIN_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            AdminResponse::Devices { .. } => Ok(()),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn models(
        &self,
        hub: &PairedHub,
        request: ModelsRequest,
    ) -> Result<ModelsResponse, HubError> {
        let mut stream = self.stream(hub, MODELS_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            ModelsResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    async fn models_status_of(
        &self,
        hub: &PairedHub,
        request: ModelsRequest,
    ) -> Result<LlamaStatus, HubError> {
        match self.models(hub, request).await? {
            ModelsResponse::Status { status } => Ok(LlamaStatus {
                state: status.state,
                backend: status.backend,
                build: status.build,
                latest: status.latest,
                channel: status.channel,
                message: status.message,
                installed: status
                    .installed
                    .into_iter()
                    .map(|m| InstalledModel {
                        file: m.file,
                        size: m.size,
                        repo: m.repo,
                    })
                    .collect(),
                downloads: status
                    .downloads
                    .into_iter()
                    .map(|d| ModelDownload {
                        repo: d.repo,
                        file: d.file,
                        received: d.received,
                        total: d.total,
                        error: d.error,
                    })
                    .collect(),
                cpu: status.usage.map(|u| u.cpu),
                memory: status.usage.map(|u| u.memory),
            }),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn data(&self, hub: &PairedHub, request: SyncRequest) -> Result<SyncResponse, HubError> {
        let mut stream = self.stream(hub, SYNC_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            SyncResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    async fn secrets(
        &self,
        hub: &PairedHub,
        request: SecretsRequest,
    ) -> Result<SecretsResponse, HubError> {
        let mut stream = self.stream(hub, SECRETS_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            SecretsResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }
}

//the body streams as the provider writes it
pub(crate) enum Forwarded {
    Response {
        status: u16,
        headers: Vec<(String, String)>,
        body: RecvStream,
    },
    Refused(String),
}

//one protocol session
struct Stream {
    conn: Connection,
    send: SendStream,
    recv: RecvStream,
}

impl Stream {
    async fn request<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        req: &Req,
    ) -> Result<Resp, HubError> {
        //big uploads get time while a silent hub fails fast
        let size = serde_json::to_vec(req).map_or(0, |bytes| bytes.len());
        let limit = REQUEST_TIMEOUT + Duration::from_secs((size / MIN_UPLOAD_BYTES_PER_SEC) as u64);
        within(limit, request(&mut self.recv, &mut self.send, req))
            .await?
            .map_err(|e| {
                if refused(&self.conn) {
                    HubError::Unauthorized
                } else {
                    unreachable(e)
                }
            })
    }

    fn finish(mut self) {
        let _ = self.send.finish();
    }
}

fn refused(conn: &Connection) -> bool {
    matches!(
        conn.close_reason(),
        Some(ConnectionError::ApplicationClosed(close))
            if u64::from(close.error_code) == u64::from(UNAUTHORIZED)
    )
}

//relays come from address lookup
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn nearby(hub: &NearbyHub) -> PairedHub {
    PairedHub {
        id: hub.id.clone(),
        name: hub.name.clone(),
        relays: Vec::new(),
    }
}

fn paired_addr(hub: &PairedHub) -> Result<EndpointAddr, HubError> {
    let id: EndpointId = hub
        .id
        .parse()
        .map_err(|_| HubError::Internal("invalid hub id".into()))?;
    let relays = hub
        .relays
        .iter()
        .map(|url| url.parse::<RelayUrl>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HubError::Internal("invalid relay url".into()))?;
    Ok(hub_addr(id, &relays))
}

fn internal(e: anyhow::Error) -> HubError {
    HubError::Internal(format!("{e:#}"))
}

fn unexpected_profile(response: ProfileResponse) -> HubError {
    HubError::Internal(format!("unexpected answer {response:?}"))
}

fn hub_profile(profile: ProfileInfo) -> HubProfile {
    HubProfile {
        id: profile.id,
        name: profile.name,
        role: match profile.role {
            ProfileRole::Admin => "admin".into(),
            ProfileRole::Member => "member".into(),
        },
        created_at: profile.created_at,
        butterfly: profile.butterfly,
    }
}

//argon2 must not block the async runtime
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> Result<T, HubError> {
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(|e| HubError::Internal(e.to_string()))?
            .map_err(internal)
    }
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    {
        work().map_err(internal)
    }
}

fn unexpected(response: SecretsResponse) -> HubError {
    HubError::Internal(format!("unexpected answer {response:?}"))
}

fn model_file(file: sampler_hub_proto::ModelFile) -> ModelFile {
    ModelFile {
        file: file.file,
        size: file.size,
    }
}

//argon2 is slow on purpose
async fn derive_code(code: String) -> Result<invite::InviteKeys, HubError> {
    let invalid = |e: anyhow::Error| HubError::InvalidLink(format!("{e:#}"));
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    {
        tokio::task::spawn_blocking(move || invite::derive(&code))
            .await
            .map_err(|e| HubError::Internal(e.to_string()))?
            .map_err(invalid)
    }
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    {
        invite::derive(&code).map_err(invalid)
    }
}

impl From<ProtoSettings> for HubSettings {
    fn from(settings: ProtoSettings) -> Self {
        Self {
            lan_discovery: settings.lan_discovery,
            share_providers: settings.share_providers,
            mcp_servers: settings.mcp_servers,
            models: settings.models,
            models_nightly: settings.models_nightly,
            ctx_size: settings.ctx_size,
            keep_alive: settings.keep_alive,
            parallel: settings.parallel,
        }
    }
}

//a cloud restore has no hub to ask
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export
)]
pub fn open_locked_key(lock: ProfileLock, password: String) -> Result<String, HubError> {
    let derived =
        profile::derive(&password, &lock.salt, lock.memory_kib, lock.passes).map_err(internal)?;
    let locked = sampler_hub_proto::LockedKey {
        salt: lock.salt,
        memory_kib: lock.memory_kib,
        passes: lock.passes,
        wrapped: lock.wrapped,
    };
    let key = profile::unlock(&derived, &locked).map_err(internal)?;
    Ok(profile::encode_key(&key))
}

//seals the cloud backup of a profile
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export
)]
pub fn profile_backup_key(key: String) -> Result<String, HubError> {
    let key = profile::decode_key(&key).map_err(internal)?;
    Ok(profile::encode_key(&profile::backup_key(&key)))
}

//apps draw pairing links without a qr library
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export
)]
pub fn qr_svg(text: String, dark: String, light: String) -> Result<String, HubError> {
    use qrcode::render::svg;
    let code =
        qrcode::QrCode::new(text.as_bytes()).map_err(|e| HubError::Internal(e.to_string()))?;
    Ok(code
        .render::<svg::Color>()
        .dark_color(svg::Color(&dark))
        .light_color(svg::Color(&light))
        .quiet_zone(false)
        .build())
}

//a silent hub must never hang the app
async fn within<T>(limit: Duration, future: impl Future<Output = T>) -> Result<T, HubError> {
    n0_future::time::timeout(limit, future)
        .await
        .map_err(|_| HubError::Unreachable("the hub did not answer in time".into()))
}

fn unreachable(e: impl std::fmt::Display) -> HubError {
    HubError::Unreachable(e.to_string())
}
