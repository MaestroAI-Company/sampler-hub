use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use sampler_hub_proto::{
    DeviceInfo, KIND_DESKTOP, KIND_PHONE, KIND_WEB, PairRejection, ProfileInfo, ProfileRole,
    SCOPE_ADMIN, SCOPE_MCP, SCOPE_PROVIDERS, SCOPE_SECRETS, SCOPE_SYNC,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const DEVICES_FILE: &str = "devices.json";
const PROFILES_FILE: &str = "profiles.json";
const PAIRING_FILE: &str = "pairing.json";
pub const PAIRING_TTL: Duration = Duration::from_secs(10 * 60);

//devices.json is the allowlist
pub type Device = DeviceInfo;

//phones and desktops get every scope
pub fn default_scopes(kind: &str) -> Vec<String> {
    match kind {
        KIND_PHONE | KIND_DESKTOP => [
            SCOPE_SYNC,
            SCOPE_SECRETS,
            SCOPE_PROVIDERS,
            SCOPE_MCP,
            SCOPE_ADMIN,
        ]
        .map(String::from)
        .to_vec(),
        //admin profiles manage the hub from a browser, never mcp servers
        KIND_WEB => [SCOPE_SYNC, SCOPE_SECRETS, SCOPE_PROVIDERS, SCOPE_ADMIN]
            .map(String::from)
            .to_vec(),
        _ => Vec::new(),
    }
}

#[derive(Serialize, Deserialize)]
struct PendingPairing {
    secret: String,
    expires_at: u64,
    //a device link waiting for this pairing
    #[serde(default)]
    link: Option<String>,
}

//atomic read modify write
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    lock: Mutex<()>,
}

impl Store {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            lock: Mutex::new(()),
        }
    }

    pub fn devices(&self) -> Result<Vec<Device>> {
        Ok(read_json(&self.dir.join(DEVICES_FILE))?.unwrap_or_default())
    }

    pub fn device(&self, id: &str) -> Result<Option<Device>> {
        Ok(self.devices()?.into_iter().find(|device| device.id == id))
    }

    pub fn touch(&self, id: &str) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let Some(device) = devices.iter_mut().find(|device| device.id == id) else {
            return Ok(());
        };
        device.last_seen = Some(unix_now());
        write_json(&self.dir.join(DEVICES_FILE), &devices)
    }

    //false when no such device
    pub fn rename_device(&self, id: &str, name: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let Some(device) = devices.iter_mut().find(|device| device.id == id) else {
            return Ok(false);
        };
        device.name = name.to_string();
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        Ok(true)
    }

    pub fn remove_device(&self, id: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let before = devices.len();
        devices.retain(|device| device.id != id);
        if devices.len() == before {
            return Ok(false);
        }
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        Ok(true)
    }

    pub fn profiles(&self) -> Result<Vec<ProfileInfo>> {
        Ok(read_json(&self.dir.join(PROFILES_FILE))?.unwrap_or_default())
    }

    pub fn profile(&self, id: &str) -> Result<Option<ProfileInfo>> {
        Ok(self
            .profiles()?
            .into_iter()
            .find(|profile| profile.id == id))
    }

    //first profile manages the hub
    pub fn create_profile(&self, name: &str) -> Result<ProfileInfo> {
        let _guard = self.lock.lock().unwrap();
        let mut profiles = self.profiles()?;
        let profile = ProfileInfo {
            id: random_hex()?,
            name: name.to_string(),
            role: if profiles.is_empty() {
                ProfileRole::Admin
            } else {
                ProfileRole::Member
            },
            created_at: unix_now(),
            butterfly: None,
        };
        profiles.push(profile.clone());
        write_json(&self.dir.join(PROFILES_FILE), &profiles)?;
        Ok(profile)
    }

    //false when no such profile
    pub fn set_role(&self, id: &str, role: ProfileRole) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut profiles = self.profiles()?;
        let Some(profile) = profiles.iter_mut().find(|profile| profile.id == id) else {
            return Ok(false);
        };
        profile.role = role;
        write_json(&self.dir.join(PROFILES_FILE), &profiles)?;
        Ok(true)
    }

    //its devices are unpaired with it
    pub fn remove_profile(&self, id: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut profiles = self.profiles()?;
        let before = profiles.len();
        profiles.retain(|profile| profile.id != id);
        if profiles.len() == before {
            return Ok(false);
        }
        let mut devices = self.devices()?;
        devices.retain(|device| device.profile.as_deref() != Some(id));
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        write_json(&self.dir.join(PROFILES_FILE), &profiles)?;
        Ok(true)
    }

    //scopes follow the kind, read at start
    pub fn refresh_scopes(&self) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let mut devices = self.devices()?;
        let mut changed = false;
        for device in &mut devices {
            let scopes = default_scopes(&device.kind);
            if device.scopes != scopes {
                device.scopes = scopes;
                changed = true;
            }
        }
        if changed {
            write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        }
        Ok(())
    }

    //false when device or profile is gone
    pub fn bind_device(&self, id: &str, profile: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        if self.profile(profile)?.is_none() {
            return Ok(false);
        }
        let mut devices = self.devices()?;
        let Some(device) = devices.iter_mut().find(|device| device.id == id) else {
            return Ok(false);
        };
        device.profile = Some(profile.to_string());
        //devices paired before a scope existed get it
        device.scopes = default_scopes(&device.kind);
        write_json(&self.dir.join(DEVICES_FILE), &devices)?;
        Ok(true)
    }

    pub fn set_butterfly(&self, id: &str, butterfly: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut profiles = self.profiles()?;
        let Some(profile) = profiles.iter_mut().find(|profile| profile.id == id) else {
            return Ok(false);
        };
        profile.butterfly = Some(butterfly.to_string());
        write_json(&self.dir.join(PROFILES_FILE), &profiles)?;
        Ok(true)
    }

    pub fn rename_profile(&self, id: &str, name: &str) -> Result<bool> {
        let _guard = self.lock.lock().unwrap();
        let mut profiles = self.profiles()?;
        let Some(profile) = profiles.iter_mut().find(|profile| profile.id == id) else {
            return Ok(false);
        };
        profile.name = name.to_string();
        write_json(&self.dir.join(PROFILES_FILE), &profiles)?;
        Ok(true)
    }

    pub fn start_pairing(&self) -> Result<String> {
        self.start_pairing_for(None)
    }

    //only the latest qr code works
    pub fn start_pairing_for(&self, link: Option<&str>) -> Result<String> {
        let _guard = self.lock.lock().unwrap();
        let secret = random_hex()?;
        let pending = PendingPairing {
            secret: secret.clone(),
            expires_at: unix_now() + PAIRING_TTL.as_secs(),
            link: link.map(String::from),
        };
        write_json(&self.dir.join(PAIRING_FILE), &pending)?;
        Ok(secret)
    }

    //the link the device joins, if any
    pub fn finish_pairing(
        &self,
        secret: &str,
        device: Device,
    ) -> Result<Result<Option<String>, PairRejection>> {
        let _guard = self.lock.lock().unwrap();
        let path = self.dir.join(PAIRING_FILE);
        let Some(pending) = read_json::<PendingPairing>(&path)? else {
            return Ok(Err(PairRejection::Invalid));
        };
        if pending.expires_at < unix_now() {
            std::fs::remove_file(&path)?;
            return Ok(Err(PairRejection::Expired));
        }
        if !same_secret(&pending.secret, secret) {
            return Ok(Err(PairRejection::Invalid));
        }
        //one time secret
        std::fs::remove_file(&path)?;
        self.put_device(device)?;
        Ok(Ok(pending.link))
    }

    //a password login pairs without a secret
    pub fn add_device(&self, device: Device) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        self.put_device(device)
    }

    fn put_device(&self, device: Device) -> Result<()> {
        let mut devices = self.devices()?;
        //a reinstalled app comes back with a new key
        let same_machine =
            |known: &Device| device.machine.is_some() && known.machine == device.machine;
        devices.retain(|known| known.id != device.id && !same_machine(known));
        devices.push(device);
        write_json(&self.dir.join(DEVICES_FILE), &devices)
    }
}

fn random_hex() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("no system randomness: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

//constant time against timing attacks
fn same_secret(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("invalid {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
}

//rename so crashes never truncate
fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp)
        .with_context(|| format!("cannot write {}", tmp.display()))?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path).with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> Store {
        let dir = std::env::temp_dir().join(format!("sampler-hub-store-{}", rand_suffix()));
        Store::new(&dir)
    }

    fn rand_suffix() -> String {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn device(id: &str) -> Device {
        Device {
            id: id.into(),
            name: "phone".into(),
            kind: KIND_PHONE.into(),
            scopes: default_scopes(KIND_PHONE),
            paired_at: 0,
            paired_by: None,
            last_seen: None,
            machine: None,
            profile: None,
        }
    }

    #[test]
    fn pairing_secret_works_once() {
        let store = temp_store();
        let secret = store.start_pairing().unwrap();
        assert_eq!(
            store.finish_pairing("wrong", device("a")).unwrap(),
            Err(PairRejection::Invalid)
        );
        assert_eq!(
            store.finish_pairing(&secret, device("a")).unwrap(),
            Ok(None)
        );
        assert_eq!(
            store.finish_pairing(&secret, device("b")).unwrap(),
            Err(PairRejection::Invalid)
        );
        assert!(store.device("a").unwrap().is_some());
        assert!(!store.device("b").unwrap().is_some());
        assert!(store.remove_device("a").unwrap());
        assert!(!store.device("a").unwrap().is_some());
    }

    #[test]
    fn first_profile_is_admin() {
        let store = temp_store();
        let owner = store.create_profile("owner").unwrap();
        let flatmate = store.create_profile("flatmate").unwrap();
        assert_eq!(owner.role, ProfileRole::Admin);
        assert_eq!(flatmate.role, ProfileRole::Member);
        assert!(store.set_role(&flatmate.id, ProfileRole::Admin).unwrap());
        assert_eq!(
            store.profile(&flatmate.id).unwrap().unwrap().role,
            ProfileRole::Admin
        );
    }

    #[test]
    fn removing_a_profile_unpairs_its_devices() {
        let store = temp_store();
        let owner = store.create_profile("owner").unwrap();
        let flatmate = store.create_profile("flatmate").unwrap();
        for (id, profile) in [("a", &owner), ("b", &flatmate), ("c", &flatmate)] {
            let secret = store.start_pairing().unwrap();
            store.finish_pairing(&secret, device(id)).unwrap().unwrap();
            assert!(store.bind_device(id, &profile.id).unwrap());
        }
        assert!(!store.bind_device("a", "missing").unwrap());
        assert!(store.remove_profile(&flatmate.id).unwrap());
        let left: Vec<_> = store.devices().unwrap().into_iter().map(|d| d.id).collect();
        assert_eq!(left, vec!["a"]);
        assert!(!store.remove_profile(&flatmate.id).unwrap());
    }

    #[test]
    fn link_pairings_name_their_link() {
        let store = temp_store();
        let secret = store.start_pairing_for(Some("link-1")).unwrap();
        assert_eq!(
            store.finish_pairing(&secret, device("a")).unwrap(),
            Ok(Some("link-1".into()))
        );
    }
}
