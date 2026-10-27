//toggles paired admins change from the app
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sampler_hub_proto::HubSettings;
use tokio::sync::watch;

const SETTINGS_FILE: &str = "settings.json";

#[derive(Debug)]
pub struct Settings {
    path: PathBuf,
    current: watch::Sender<HubSettings>,
}

impl Settings {
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(SETTINGS_FILE);
        let current = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid {}", path.display()))?,
            Err(e) if e.kind() == ErrorKind::NotFound => HubSettings::default(),
            Err(e) => return Err(e).with_context(|| format!("cannot read {}", path.display())),
        };
        Ok(Self {
            path,
            current: watch::Sender::new(current),
        })
    }

    pub fn get(&self) -> HubSettings {
        *self.current.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<HubSettings> {
        self.current.subscribe()
    }

    pub fn set(&self, settings: HubSettings) -> Result<()> {
        //written whole so a crash never leaves half a file
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&settings)?)?;
        std::fs::rename(&tmp, &self.path)?;
        self.current.send_replace(settings);
        Ok(())
    }
}
