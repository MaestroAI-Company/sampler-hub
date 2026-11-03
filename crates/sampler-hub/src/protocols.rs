//transport agnostic hub protocols

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use sampler_hub_proto::{
    AdminRequest, AdminResponse, HubRequest, HubResponse, McpAction, PairRequest, PairResponse,
    ProfileInfo, ProfileRole, SCOPE_ADMIN, clean_device_name, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

use crate::db::Db;
use crate::profiles::Links;
use crate::store::{Device, Store, default_scopes, unix_now};

const MAX_DEVICE_NAME: usize = 64;

//proven by the transport
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    //read again for every stream
    pub profile: Option<String>,
}

impl Peer {
    fn short(&self) -> &str {
        &self.id[..self.id.len().min(10)]
    }

    //scoped protocols only admit bound devices
    pub fn profile(&self) -> anyhow::Result<&str> {
        self.profile
            .as_deref()
            .ok_or_else(|| anyhow!("this device has no profile"))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Access {
    //no device data so open
    Public,
    Paired,
    Scope(&'static str),
}

pub trait Protocol: Clone + Send + Sync + 'static {
    const ACCESS: Access;

    fn serve<R, W>(
        &self,
        peer: Peer,
        reader: R,
        writer: W,
    ) -> impl Future<Output = Result<()>> + Send
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send;
}

//checked again for every stream, a removed device stops at once
pub fn still_allowed(store: &Store, id: &str, access: Access) -> Result<Option<Peer>> {
    let device = store.device(id)?;
    let allowed = match (access, &device) {
        (Access::Public, _) => true,
        (_, None) => false,
        (Access::Paired, Some(_)) => true,
        (Access::Scope(scope), Some(device)) => {
            let profile = match &device.profile {
                Some(profile) => store.profile(profile)?,
                None => None,
            };
            //admin also needs an admin profile
            device.scopes.iter().any(|s| s == scope)
                && profile.is_some_and(|p| scope != SCOPE_ADMIN || p.role == ProfileRole::Admin)
        }
    };
    Ok(allowed.then(|| Peer {
        id: id.to_string(),
        profile: device.and_then(|device| device.profile),
    }))
}

//deny by default before any serve
pub fn admit(store: &Store, id: &str, access: Access) -> Result<Option<Peer>> {
    let peer = still_allowed(store, id, access)?;
    match &peer {
        Some(_) if store.device(id)?.is_some() => store.touch(id)?,
        Some(_) => {}
        None => warn!("refused {} on {access:?}", &id[..id.len().min(10)]),
    }
    Ok(peer)
}

//reachability check
#[derive(Debug, Clone)]
pub struct Ping;

impl Protocol for Ping {
    const ACCESS: Access = Access::Public;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        info!("ping from {}", peer.short());
        tokio::io::copy(&mut reader, &mut writer).await?;
        Ok(())
    }
}

//allowlists devices with a valid secret
#[derive(Debug, Clone)]
pub struct Pair {
    pub store: Arc<Store>,
    pub name: Arc<str>,
    pub links: Arc<Links>,
}

impl Protocol for Pair {
    const ACCESS: Access = Access::Public;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let Some(request) = read_frame::<_, PairRequest>(&mut reader).await? else {
            return Ok(());
        };
        let kind: String = request.kind.chars().take(MAX_DEVICE_NAME).collect();
        let device = Device {
            id: peer.id.clone(),
            name: request.name.chars().take(MAX_DEVICE_NAME).collect(),
            scopes: default_scopes(&kind),
            kind,
            paired_at: unix_now(),
            //qr codes come from the hub owner
            paired_by: None,
            last_seen: Some(unix_now()),
            //only a hash fits
            machine: request
                .machine
                .filter(|tag| tag.len() == 64 && tag.bytes().all(|b| b.is_ascii_hexdigit())),
            profile: None,
        };
        let label = format!("{} ({})", device.name, peer.short());
        let name = device.name.clone();
        let response = match self.store.finish_pairing(&request.secret, device)? {
            Ok(link) => {
                if let Some(link) = link {
                    self.links.attach(&link, &peer.id, &name);
                }
                info!("paired {label}");
                PairResponse::Paired {
                    hub_name: self.name.to_string(),
                }
            }
            Err(reason) => {
                warn!("refused pairing from {label}: {reason:?}");
                PairResponse::Rejected { reason }
            }
        };
        write_frame(&mut writer, &response).await
    }
}

//requests open to any paired device
#[derive(Debug, Clone)]
pub struct Session {
    pub store: Arc<Store>,
    pub name: Arc<str>,
    pub latest: tokio::sync::watch::Receiver<Option<String>>,
    pub usage: Arc<crate::usage::Usage>,
    //every device shows the model options
    pub settings: Arc<crate::settings::Settings>,
}

impl Protocol for Session {
    const ACCESS: Access = Access::Paired;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match request {
                HubRequest::Hello => {
                    //performance stays with admins
                    let usage = self.is_admin(&peer)?.then(|| {
                        let load = self.usage.hub();
                        sampler_hub_proto::HubUsage {
                            cpu: load.cpu,
                            memory: load.memory,
                        }
                    });
                    HubResponse::Hello {
                        name: self.name.to_string(),
                        version: env!("CARGO_PKG_VERSION").into(),
                        update: self.latest.borrow().clone(),
                        usage,
                        model_options: Some(self.settings.get().model_options()),
                    }
                }
                HubRequest::Devices => HubResponse::Devices {
                    devices: self.visible_devices(&peer)?,
                },
                HubRequest::Rename { name } => match clean_device_name(&name) {
                    Some(name) => {
                        self.store.rename_device(&peer.id, &name)?;
                        HubResponse::Renamed
                    }
                    None => HubResponse::Error {
                        message: "a device needs a name".into(),
                    },
                },
                HubRequest::Unpair => {
                    self.store.remove_device(&peer.id)?;
                    info!("{} unpaired itself", peer.short());
                    write_frame(&mut writer, &HubResponse::Unpaired).await?;
                    return Ok(());
                }
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Session {
    //admins see all devices
    fn is_admin(&self, peer: &Peer) -> Result<bool> {
        Ok(match &peer.profile {
            Some(profile) => self
                .store
                .profile(profile)?
                .is_some_and(|p| p.role == ProfileRole::Admin),
            None => false,
        })
    }

    fn visible_devices(&self, peer: &Peer) -> Result<Vec<Device>> {
        let devices = self.store.devices()?;
        let admin = self.is_admin(peer)?;
        Ok(devices
            .into_iter()
            .filter(|device| {
                admin
                    || device.id == peer.id
                    || (peer.profile.is_some() && device.profile == peer.profile)
            })
            .collect())
    }
}

#[derive(Clone)]
pub struct Admin {
    pub store: Arc<Store>,
    pub db: Arc<Db>,
    pub mcp: Arc<crate::mcp::Mcp>,
    pub data_dir: std::path::PathBuf,
    pub settings: Arc<crate::settings::Settings>,
    pub http: reqwest::Client,
    pub latest: tokio::sync::watch::Receiver<Option<String>>,
}

impl std::fmt::Debug for Admin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Admin")
    }
}

impl Protocol for Admin {
    const ACCESS: Access = Access::Scope(SCOPE_ADMIN);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match request {
                AdminRequest::Invite => match crate::invite::open(&self.data_dir).await {
                    Ok(invite) => {
                        info!("{} opened a pairing code", peer.short());
                        AdminResponse::Invite {
                            code: invite.code,
                            link: invite.link.to_string(),
                        }
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::Settings => AdminResponse::Settings {
                    settings: self.settings.get(),
                },
                AdminRequest::Update => match self.update().await {
                    Ok(version) => {
                        info!("{} asked for sampler hub {version}", peer.short());
                        write_frame(&mut writer, &AdminResponse::Updating { version }).await?;
                        restart_soon();
                        return Ok(());
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::RenameDevice { id, name } => match clean_device_name(&name) {
                    Some(name) if self.store.rename_device(&id, &name)? => {
                        info!("{} renamed a device to {name}", peer.short());
                        AdminResponse::Devices {
                            devices: self.store.devices()?,
                        }
                    }
                    Some(_) => AdminResponse::Error {
                        message: "no such device".into(),
                    },
                    None => AdminResponse::Error {
                        message: "a device needs a name".into(),
                    },
                },
                AdminRequest::RemoveDevice { id } => {
                    if self.store.remove_device(&id)? {
                        info!(
                            "{} removed device {}",
                            peer.short(),
                            &id[..id.len().min(10)]
                        );
                    }
                    AdminResponse::Devices {
                        devices: self.store.devices()?,
                    }
                }
                AdminRequest::RemoveProfile { id } => match self.remove_profile(&id).await {
                    Ok(profiles) => {
                        info!(
                            "{} removed profile {}",
                            peer.short(),
                            &id[..id.len().min(10)]
                        );
                        AdminResponse::Profiles { profiles }
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::SetRole { id, role } => match self.set_role(&id, role) {
                    Ok(profiles) => AdminResponse::Profiles { profiles },
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
                AdminRequest::McpServers => AdminResponse::McpServers {
                    servers: self.mcp.overview(),
                },
                AdminRequest::ManageMcp { id, action } => {
                    match self.manage_mcp(peer.profile()?, &id, action).await {
                        Ok(()) => {
                            info!("{} {action:?} mcp server {id}", peer.short());
                            AdminResponse::McpServers {
                                servers: self.mcp.overview(),
                            }
                        }
                        Err(e) => AdminResponse::Error {
                            message: format!("{e:#}"),
                        },
                    }
                }
                AdminRequest::SetSettings { settings } => match self.settings.set(settings) {
                    Ok(()) => {
                        info!("{} changed the hub settings", peer.short());
                        AdminResponse::Settings { settings }
                    }
                    Err(e) => AdminResponse::Error {
                        message: format!("{e:#}"),
                    },
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Admin {
    //acts as the owner, servers from before profiles belong to admins
    async fn manage_mcp(&self, admin: &str, id: &str, action: McpAction) -> Result<()> {
        let owner = self.mcp.owner_of(id)?.unwrap_or_else(|| admin.to_string());
        match action {
            McpAction::Start => self.mcp.enable(&owner, id, true),
            McpAction::Stop => self.mcp.enable(&owner, id, false),
            McpAction::Restart => self.mcp.restart(&owner, id),
            McpAction::Remove => self.mcp.remove(&owner, id).await,
        }
    }

    async fn remove_profile(&self, id: &str) -> Result<Vec<ProfileInfo>> {
        let profiles = self.store.profiles()?;
        let left: Vec<_> = profiles.iter().filter(|p| p.id != id).cloned().collect();
        if left.len() == profiles.len() {
            anyhow::bail!("no such profile");
        }
        ensure_admin(&left)?;
        //data first so nothing stays orphaned
        self.mcp.remove_owned(id).await;
        self.db.remove_profile(id)?;
        self.store.remove_profile(id)?;
        self.store.profiles()
    }

    fn set_role(&self, id: &str, role: ProfileRole) -> Result<Vec<ProfileInfo>> {
        let mut profiles = self.store.profiles()?;
        let profile = profiles
            .iter_mut()
            .find(|p| p.id == id)
            .ok_or_else(|| anyhow!("no such profile"))?;
        profile.role = role;
        ensure_admin(&profiles)?;
        self.store.set_role(id, role)?;
        self.store.profiles()
    }

    async fn update(&self) -> Result<String> {
        //the guard must not live across an await
        let known = self.latest.borrow().clone();
        let version = match known {
            Some(version) => version,
            None => crate::update::newer_release(&self.http)
                .await?
                .ok_or_else(|| anyhow::anyhow!("this hub is up to date"))?,
        };
        crate::update::apply(&self.http, &version).await?;
        Ok(version)
    }
}

//someone must keep managing the hub
fn ensure_admin(profiles: &[ProfileInfo]) -> Result<()> {
    if !profiles.is_empty() && !profiles.iter().any(|p| p.role == ProfileRole::Admin) {
        anyhow::bail!("the hub needs at least one admin profile");
    }
    Ok(())
}

//windows installers restart the hub themselves
fn restart_soon() {
    #[cfg(not(windows))]
    tokio::spawn(async {
        //lets the answer reach the device
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Err(e) = crate::update::restart() {
            warn!("restart after update failed: {e:#}");
        }
    });
}

#[cfg(test)]
mod tests {
    use sampler_hub_proto::{KIND_PHONE, KIND_WEB, SCOPE_MCP, SCOPE_SYNC};

    use super::*;

    fn temp_store() -> Store {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        Store::new(&std::env::temp_dir().join(format!("sampler-hub-admit-{suffix}")))
    }

    fn add_device(store: &Store, id: &str, kind: &str) {
        let secret = store.start_pairing().unwrap();
        let device = Device {
            id: id.into(),
            name: "device".into(),
            kind: kind.into(),
            scopes: default_scopes(kind),
            paired_at: 0,
            paired_by: None,
            last_seen: None,
            machine: None,
            profile: None,
        };
        store.finish_pairing(&secret, device).unwrap().unwrap();
    }

    fn allowed(store: &Store, id: &str, access: Access) -> bool {
        admit(store, id, access).unwrap().is_some()
    }

    #[test]
    fn denies_by_default() {
        let store = temp_store();
        add_device(&store, "known", "speaker");
        let owner = store.create_profile("owner").unwrap();
        store.bind_device("known", &owner.id).unwrap();
        assert!(allowed(&store, "unknown", Access::Public));
        assert!(!allowed(&store, "unknown", Access::Paired));
        assert!(!allowed(&store, "unknown", Access::Scope(SCOPE_SYNC)));
        assert!(allowed(&store, "known", Access::Paired));
        //unknown kinds get no scope
        assert!(!allowed(&store, "known", Access::Scope(SCOPE_ADMIN)));
    }

    #[test]
    fn unbound_devices_reach_no_scope() {
        let store = temp_store();
        add_device(&store, "phone", KIND_PHONE);
        assert!(allowed(&store, "phone", Access::Paired));
        assert!(!allowed(&store, "phone", Access::Scope(SCOPE_SYNC)));
        assert!(!allowed(&store, "phone", Access::Scope(SCOPE_ADMIN)));
        let owner = store.create_profile("owner").unwrap();
        store.bind_device("phone", &owner.id).unwrap();
        let peer = admit(&store, "phone", Access::Scope(SCOPE_SYNC)).unwrap();
        assert_eq!(peer.unwrap().profile, Some(owner.id));
        assert!(store.device("phone").unwrap().unwrap().last_seen.is_some());
    }

    #[test]
    fn admin_needs_an_admin_profile() {
        let store = temp_store();
        let owner = store.create_profile("owner").unwrap();
        let flatmate = store.create_profile("flatmate").unwrap();
        for (id, kind, profile) in [
            ("owner phone", KIND_PHONE, &owner),
            ("flatmate phone", KIND_PHONE, &flatmate),
            ("owner browser", KIND_WEB, &owner),
            ("flatmate browser", KIND_WEB, &flatmate),
        ] {
            add_device(&store, id, kind);
            store.bind_device(id, &profile.id).unwrap();
        }
        assert!(allowed(&store, "owner phone", Access::Scope(SCOPE_ADMIN)));
        //a member phone syncs but never manages
        assert!(allowed(&store, "flatmate phone", Access::Scope(SCOPE_SYNC)));
        assert!(!allowed(
            &store,
            "flatmate phone",
            Access::Scope(SCOPE_ADMIN)
        ));
        //an admin browser manages the hub, never its mcp servers
        assert!(allowed(&store, "owner browser", Access::Scope(SCOPE_ADMIN)));
        assert!(!allowed(&store, "owner browser", Access::Scope(SCOPE_MCP)));
        assert!(!allowed(
            &store,
            "flatmate browser",
            Access::Scope(SCOPE_ADMIN)
        ));
        store.set_role(&flatmate.id, ProfileRole::Admin).unwrap();
        assert!(allowed(
            &store,
            "flatmate phone",
            Access::Scope(SCOPE_ADMIN)
        ));
    }

    #[test]
    fn members_list_their_own_devices() {
        let store = Arc::new(temp_store());
        let owner = store.create_profile("owner").unwrap();
        let flatmate = store.create_profile("flatmate").unwrap();
        for (id, profile) in [("a", Some(&owner)), ("b", Some(&flatmate)), ("c", None)] {
            add_device(&store, id, KIND_PHONE);
            if let Some(profile) = profile {
                store.bind_device(id, &profile.id).unwrap();
            }
        }
        let session = Session {
            store: store.clone(),
            name: "hub".into(),
            latest: tokio::sync::watch::channel(None).1,
            usage: Default::default(),
            //no file, the defaults
            settings: Arc::new(
                crate::settings::Settings::load(
                    &std::env::temp_dir().join("sampler-hub-no-settings"),
                )
                .unwrap(),
            ),
        };
        let ids = |id: &str| -> Vec<String> {
            let peer = still_allowed(&store, id, Access::Paired).unwrap().unwrap();
            let mut ids: Vec<_> = session
                .visible_devices(&peer)
                .unwrap()
                .into_iter()
                .map(|d| d.id)
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(ids("a"), ["a", "b", "c"]);
        assert_eq!(ids("b"), ["b"]);
        assert_eq!(ids("c"), ["c"]);
    }
}
