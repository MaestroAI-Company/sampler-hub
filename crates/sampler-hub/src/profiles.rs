//sampler/profiles/1 for profiles and links
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use sampler_hub_proto::{
    LockedKey, LoginRequest, LoginResponse, ProfileInfo, ProfileRequest, ProfileResponse,
    ProfileRole, auth_hash, clean_device_name, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::db::{Db, ProfileKey};
use crate::protocols::{Access, Peer, Protocol};
use crate::settings::Settings;
use crate::store::{Device, PAIRING_TTL, Store, default_scopes, unix_now};

//free tries before the lockout grows
const FREE_TRIES: u32 = 5;
const FIRST_LOCKOUT: Duration = Duration::from_secs(30);
const MAX_LOCKOUT: Duration = Duration::from_secs(60 * 60);
//under the client request timeout
const WAIT: Duration = Duration::from_secs(15);
//the client refuses anything weaker
const MIN_MEMORY_KIB: u32 = 19 * 1024;
const MIN_PASSES: u32 = 2;

#[derive(Clone)]
pub struct Profiles {
    pub store: Arc<Store>,
    pub db: Arc<Db>,
    pub data_dir: PathBuf,
    pub links: Arc<Links>,
    pub tries: Arc<Tries>,
}

impl std::fmt::Debug for Profiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Profiles")
    }
}

impl Protocol for Profiles {
    const ACCESS: Access = Access::Paired;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(&peer, request).await {
                Ok(response) => response,
                Err(e) => ProfileResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Profiles {
    async fn handle(&self, peer: &Peer, request: ProfileRequest) -> Result<ProfileResponse> {
        Ok(match request {
            ProfileRequest::List => ProfileResponse::Profiles {
                profiles: self.visible_profiles(peer)?,
            },
            ProfileRequest::Cost { profile } => {
                let key = self.key(&profile)?;
                ProfileResponse::Cost {
                    salt: key.locked.salt,
                    memory_kib: key.locked.memory_kib,
                    passes: key.locked.passes,
                }
            }
            ProfileRequest::Create {
                name,
                locked,
                auth_hash,
            } => {
                self.unbound(peer)?;
                if self.links.joining(&peer.id) {
                    bail!("this device joins a profile through its link");
                }
                let name =
                    clean_device_name(&name).ok_or_else(|| anyhow!("a profile needs a name"))?;
                check_key(&locked, &auth_hash)?;
                let profile = self.store.create_profile(&name)?;
                let key = ProfileKey { locked, auth_hash };
                if let Err(e) = self.db.put_profile_key(&profile.id, &key) {
                    self.store.remove_profile(&profile.id)?;
                    return Err(e);
                }
                self.store.bind_device(&peer.id, &profile.id)?;
                info!("{} created profile {name}", short(&peer.id));
                ProfileResponse::Profile { profile }
            }
            ProfileRequest::Unlock { profile, auth_key } => {
                self.unbound(peer)?;
                let key = self.check_password(&profile, &auth_key)?;
                let info = self.profile(&profile)?;
                self.store.bind_device(&peer.id, &profile)?;
                info!("{} unlocked profile {}", short(&peer.id), info.name);
                ProfileResponse::Unlocked {
                    profile: info,
                    locked: key.locked,
                }
            }
            ProfileRequest::ChangePassword {
                auth_key,
                locked,
                auth_hash,
            } => {
                let profile = peer.profile()?;
                self.check_password(profile, &auth_key)?;
                check_key(&locked, &auth_hash)?;
                self.db
                    .put_profile_key(profile, &ProfileKey { locked, auth_hash })?;
                ProfileResponse::Done
            }
            //already unlocked, so nothing new to brute force
            ProfileRequest::LockedKey => ProfileResponse::LockedKey {
                locked: self.key(peer.profile()?)?.locked,
            },
            ProfileRequest::Rename { name } => {
                let name =
                    clean_device_name(&name).ok_or_else(|| anyhow!("a profile needs a name"))?;
                let profile = peer.profile()?;
                self.store.rename_profile(profile, &name)?;
                ProfileResponse::Profile {
                    profile: self.profile(profile)?,
                }
            }
            ProfileRequest::SetButterfly { butterfly } => {
                //an id, never free text
                if butterfly.is_empty()
                    || butterfly.len() > MAX_BUTTERFLY
                    || !butterfly.bytes().all(|b| b.is_ascii_alphanumeric())
                {
                    bail!("invalid butterfly");
                }
                let profile = peer.profile()?;
                self.store.set_butterfly(profile, &butterfly)?;
                ProfileResponse::Profile {
                    profile: self.profile(profile)?,
                }
            }
            ProfileRequest::LinkStart { commitment } => {
                let profile = peer.profile()?.to_string();
                let id = self.links.start(&peer.id, &profile, commitment)?;
                let invite = crate::invite::open_for(&self.data_dir, Some(&id)).await?;
                info!("{} opened a link code", short(&peer.id));
                ProfileResponse::Code {
                    code: invite.code,
                    link: invite.link.to_string(),
                }
            }
            ProfileRequest::LinkWatch => match self.links.watch(&peer.id).await? {
                Some((device, public)) => ProfileResponse::Joining { device, public },
                None => ProfileResponse::Waiting,
            },
            ProfileRequest::LinkReveal { public } => {
                self.links.reveal(&peer.id, public)?;
                ProfileResponse::Done
            }
            ProfileRequest::LinkApprove { sealed } => {
                let (joiner, profile) = self.links.approve(&peer.id, sealed)?;
                self.store.bind_device(&joiner, &profile)?;
                info!("{} linked {}", short(&peer.id), short(&joiner));
                ProfileResponse::Done
            }
            ProfileRequest::LinkDeny => {
                //the joiner only paired to link
                if let Some(joiner) = self.links.deny(&peer.id)? {
                    self.store.remove_device(&joiner)?;
                    warn!("{} refused a link from {}", short(&peer.id), short(&joiner));
                }
                ProfileResponse::Done
            }
            ProfileRequest::LinkJoin { public } => ProfileResponse::Joined {
                commitment: self.links.join(&peer.id, public)?,
            },
            ProfileRequest::LinkFetch => {
                let (public, sealed, profile, denied) = self.links.fetch(&peer.id).await?;
                let profile = match profile {
                    Some(id) => Some(self.profile(&id)?),
                    None => None,
                };
                ProfileResponse::Link {
                    public,
                    sealed,
                    profile,
                    denied,
                }
            }
        })
    }

    //members only see their own profile
    fn visible_profiles(&self, peer: &Peer) -> Result<Vec<ProfileInfo>> {
        let profiles = self.store.profiles()?;
        //unbound devices still pick one to unlock
        let Some(own) = &peer.profile else {
            return Ok(profiles);
        };
        let admin = profiles
            .iter()
            .any(|p| &p.id == own && p.role == ProfileRole::Admin);
        Ok(profiles
            .into_iter()
            .filter(|p| admin || &p.id == own)
            .collect())
    }

    //one profile per device for now
    fn unbound(&self, peer: &Peer) -> Result<()> {
        if peer.profile.is_some() {
            bail!("this device already has a profile");
        }
        Ok(())
    }

    fn profile(&self, id: &str) -> Result<ProfileInfo> {
        self.store
            .profile(id)?
            .ok_or_else(|| anyhow!("no such profile"))
    }

    fn key(&self, profile: &str) -> Result<ProfileKey> {
        self.db
            .profile_key(profile)?
            .ok_or_else(|| anyhow!("no such profile"))
    }

    fn check_password(&self, profile: &str, auth_key: &str) -> Result<ProfileKey> {
        self.tries.check(profile)?;
        let key = self.key(profile)?;
        if !same(&auth_hash(auth_key), &key.auth_hash) {
            self.tries.failed(profile);
            warn!("wrong password for profile {}", short(profile));
            bail!("wrong password");
        }
        self.tries.passed(profile);
        Ok(key)
    }
}

//sampler/login/1, open while the hub is discoverable
#[derive(Clone)]
pub struct Login {
    pub profiles: Profiles,
    pub settings: Arc<Settings>,
    pub name: Arc<str>,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Login")
    }
}

impl Protocol for Login {
    const ACCESS: Access = Access::Public;

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(&peer, request) {
                Ok(response) => response,
                Err(e) => LoginResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Login {
    fn handle(&self, peer: &Peer, request: LoginRequest) -> Result<LoginResponse> {
        //hidden hubs only pair with a code
        if !self.settings.get().lan_discovery {
            bail!("this hub only pairs with a code");
        }
        let profiles = &self.profiles;
        Ok(match request {
            LoginRequest::List => LoginResponse::Profiles {
                profiles: profiles.store.profiles()?,
            },
            LoginRequest::Cost { profile } => {
                let key = profiles.key(&profile)?;
                LoginResponse::Cost {
                    salt: key.locked.salt,
                    memory_kib: key.locked.memory_kib,
                    passes: key.locked.passes,
                }
            }
            LoginRequest::Login {
                profile,
                auth_key,
                name,
                kind,
                machine,
            } => {
                let key = profiles.check_password(&profile, &auth_key)?;
                let info = profiles.profile(&profile)?;
                let kind: String = kind.chars().take(MAX_KIND).collect();
                let name =
                    clean_device_name(&name).ok_or_else(|| anyhow!("a device needs a name"))?;
                profiles.store.add_device(Device {
                    id: peer.id.clone(),
                    name: name.clone(),
                    scopes: default_scopes(&kind),
                    kind,
                    paired_at: unix_now(),
                    paired_by: None,
                    last_seen: Some(unix_now()),
                    //only a hash fits
                    machine: machine
                        .filter(|tag| tag.len() == 64 && tag.bytes().all(|b| b.is_ascii_hexdigit())),
                    profile: Some(profile),
                })?;
                info!("{name} ({}) signed into {}", short(&peer.id), info.name);
                LoginResponse::Unlocked {
                    hub_name: self.name.to_string(),
                    profile: info,
                    locked: key.locked,
                }
            }
        })
    }
}

const MAX_KIND: usize = 64;
const MAX_BUTTERFLY: usize = 32;

fn check_key(locked: &LockedKey, auth_hash: &str) -> Result<()> {
    if locked.memory_kib < MIN_MEMORY_KIB || locked.passes < MIN_PASSES {
        bail!("password settings too weak");
    }
    if auth_hash.len() != 64 || !auth_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid password proof");
    }
    Ok(())
}

//wrong passwords per profile
#[derive(Default)]
pub struct Tries {
    by_profile: Mutex<HashMap<String, (u32, Option<Instant>)>>,
}

impl Tries {
    fn check(&self, profile: &str) -> Result<()> {
        let tries = self.by_profile.lock().unwrap();
        if let Some((_, Some(until))) = tries.get(profile) {
            let left = until.saturating_duration_since(Instant::now());
            if !left.is_zero() {
                bail!(
                    "too many wrong passwords, try again in {} s",
                    left.as_secs() + 1
                );
            }
        }
        Ok(())
    }

    fn failed(&self, profile: &str) {
        let mut tries = self.by_profile.lock().unwrap();
        let entry = tries.entry(profile.to_string()).or_default();
        entry.0 += 1;
        if entry.0 >= FREE_TRIES {
            //doubles past the free tries
            let doublings = (entry.0 - FREE_TRIES).min(10);
            let lockout = (FIRST_LOCKOUT * 2u32.pow(doublings)).min(MAX_LOCKOUT);
            entry.1 = Some(Instant::now() + lockout);
        }
    }

    fn passed(&self, profile: &str) {
        self.by_profile.lock().unwrap().remove(profile);
    }
}

//device links in progress
#[derive(Default)]
pub struct Links {
    by_id: Mutex<HashMap<String, Link>>,
    changed: Notify,
}

impl std::fmt::Debug for Links {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Links")
    }
}

struct Link {
    profile: String,
    approver: String,
    commitment: String,
    expires_at: Instant,
    joiner: Option<Joiner>,
    approver_public: Option<String>,
    sealed: Option<String>,
    denied: bool,
}

struct Joiner {
    device: String,
    name: String,
    public: Option<String>,
}

impl Links {
    fn start(&self, approver: &str, profile: &str, commitment: String) -> Result<String> {
        if commitment.len() != 64 {
            bail!("invalid link commitment");
        }
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|e| anyhow!("no system randomness: {e}"))?;
        let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let mut links = self.by_id.lock().unwrap();
        //one live link per approver
        links.retain(|_, link| link.approver != approver && link.expires_at > Instant::now());
        links.insert(
            id.clone(),
            Link {
                profile: profile.to_string(),
                approver: approver.to_string(),
                commitment,
                expires_at: Instant::now() + PAIRING_TTL,
                joiner: None,
                approver_public: None,
                sealed: None,
                denied: false,
            },
        );
        Ok(id)
    }

    //called by pairing once the code is used
    pub fn attach(&self, id: &str, device: &str, name: &str) {
        if let Some(link) = self.by_id.lock().unwrap().get_mut(id) {
            link.joiner = Some(Joiner {
                device: device.to_string(),
                name: name.to_string(),
                public: None,
            });
        }
        self.changed.notify_waiters();
    }

    fn joining(&self, device: &str) -> bool {
        self.by_id
            .lock()
            .unwrap()
            .values()
            .any(|link| link.joiner.as_ref().is_some_and(|j| j.device == device))
    }

    async fn watch(&self, approver: &str) -> Result<Option<(String, String)>> {
        self.wait(|links| {
            let link = by_approver(links, approver)?;
            Ok(link.joiner.as_ref().and_then(|joiner| {
                joiner
                    .public
                    .clone()
                    .map(|public| (joiner.name.clone(), public))
            }))
        })
        .await
    }

    fn reveal(&self, approver: &str, public: String) -> Result<()> {
        let mut links = self.by_id.lock().unwrap();
        let link = by_approver_mut(&mut links, approver)?;
        link.approver_public = Some(public);
        drop(links);
        self.changed.notify_waiters();
        Ok(())
    }

    fn approve(&self, approver: &str, sealed: String) -> Result<(String, String)> {
        let mut links = self.by_id.lock().unwrap();
        let link = by_approver_mut(&mut links, approver)?;
        let joiner = link
            .joiner
            .as_ref()
            .filter(|joiner| joiner.public.is_some() && link.approver_public.is_some())
            .map(|joiner| joiner.device.clone())
            .ok_or_else(|| anyhow!("no device is waiting for this link"))?;
        link.sealed = Some(sealed);
        let profile = link.profile.clone();
        drop(links);
        self.changed.notify_waiters();
        Ok((joiner, profile))
    }

    fn deny(&self, approver: &str) -> Result<Option<String>> {
        let mut links = self.by_id.lock().unwrap();
        let link = by_approver_mut(&mut links, approver)?;
        link.denied = true;
        let joiner = link.joiner.as_ref().map(|joiner| joiner.device.clone());
        drop(links);
        self.changed.notify_waiters();
        Ok(joiner)
    }

    fn join(&self, device: &str, public: String) -> Result<String> {
        let mut links = self.by_id.lock().unwrap();
        let link = by_joiner_mut(&mut links, device)?;
        let joiner = link.joiner.as_mut().expect("found by its joiner");
        if joiner.public.is_some() {
            bail!("this device already joined");
        }
        joiner.public = Some(public);
        let commitment = link.commitment.clone();
        drop(links);
        self.changed.notify_waiters();
        Ok(commitment)
    }

    #[allow(clippy::type_complexity)]
    async fn fetch(
        &self,
        device: &str,
    ) -> Result<(Option<String>, Option<String>, Option<String>, bool)> {
        let found = self
            .wait(|links| {
                let link = by_joiner(links, device)?;
                let ready = link.sealed.is_some() || link.denied;
                Ok(ready.then(|| {
                    (
                        link.approver_public.clone(),
                        link.sealed.clone(),
                        link.sealed.as_ref().map(|_| link.profile.clone()),
                        link.denied,
                    )
                }))
            })
            .await?;
        if let Some(found) = found {
            //delivered once
            self.by_id
                .lock()
                .unwrap()
                .retain(|_, link| !link.joiner.as_ref().is_some_and(|j| j.device == device));
            return Ok(found);
        }
        //the digits show before the approval
        let links = self.by_id.lock().unwrap();
        let link = by_joiner(&links, device)?;
        Ok((link.approver_public.clone(), None, None, false))
    }

    //until ready or the wait ends
    async fn wait<T>(
        &self,
        ready: impl Fn(&HashMap<String, Link>) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(found) = ready(&self.by_id.lock().unwrap())? {
                return Ok(Some(found));
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Ok(None);
            }
        }
    }
}

fn live(link: &Link) -> Result<()> {
    if link.expires_at <= Instant::now() {
        bail!("the link code expired");
    }
    Ok(())
}

fn by_approver<'a>(links: &'a HashMap<String, Link>, approver: &str) -> Result<&'a Link> {
    let link = links
        .values()
        .find(|link| link.approver == approver)
        .ok_or_else(|| anyhow!("no link in progress"))?;
    live(link)?;
    Ok(link)
}

fn by_approver_mut<'a>(
    links: &'a mut HashMap<String, Link>,
    approver: &str,
) -> Result<&'a mut Link> {
    let link = links
        .values_mut()
        .find(|link| link.approver == approver)
        .ok_or_else(|| anyhow!("no link in progress"))?;
    live(link)?;
    Ok(link)
}

fn by_joiner<'a>(links: &'a HashMap<String, Link>, device: &str) -> Result<&'a Link> {
    let link = links
        .values()
        .find(|link| link.joiner.as_ref().is_some_and(|j| j.device == device))
        .ok_or_else(|| anyhow!("this device joins no link"))?;
    live(link)?;
    Ok(link)
}

fn by_joiner_mut<'a>(links: &'a mut HashMap<String, Link>, device: &str) -> Result<&'a mut Link> {
    let link = links
        .values_mut()
        .find(|link| link.joiner.as_ref().is_some_and(|j| j.device == device))
        .ok_or_else(|| anyhow!("this device joins no link"))?;
    live(link)?;
    Ok(link)
}

//constant time against timing attacks
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

fn short(id: &str) -> &str {
    &id[..id.len().min(10)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockout_grows_after_free_tries() {
        let tries = Tries::default();
        for _ in 0..FREE_TRIES - 1 {
            tries.failed("p");
            assert!(tries.check("p").is_ok());
        }
        tries.failed("p");
        assert!(tries.check("p").is_err());
        //other profiles are not affected
        assert!(tries.check("q").is_ok());
        tries.passed("p");
        assert!(tries.check("p").is_ok());
    }

    #[test]
    fn weak_keys_are_refused() {
        let locked = LockedKey {
            salt: "c2FsdA==".into(),
            memory_kib: 64 * 1024,
            passes: 3,
            wrapped: "x".into(),
        };
        let hash = auth_hash("key");
        assert!(check_key(&locked, &hash).is_ok());
        assert!(check_key(&locked, "short").is_err());
        let weak = LockedKey {
            memory_kib: 1024,
            ..locked
        };
        assert!(check_key(&weak, &hash).is_err());
    }
}
