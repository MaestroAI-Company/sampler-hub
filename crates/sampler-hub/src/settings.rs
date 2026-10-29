//toggles paired admins change from the app
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sampler_hub_proto::HubSettings;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

const SETTINGS_FILE: &str = "settings.json";
//1 keep alive defaults to 5 minutes
const SETTINGS_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Stored {
    #[serde(flatten)]
    settings: HubSettings,
    #[serde(default)]
    version: u32,
}

#[derive(Debug)]
pub struct Settings {
    path: PathBuf,
    current: watch::Sender<HubSettings>,
}

impl Settings {
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = data_dir.join(SETTINGS_FILE);
        let current = match std::fs::read(&path) {
            Ok(bytes) => upgrade(
                serde_json::from_slice(&bytes)
                    .with_context(|| format!("invalid {}", path.display()))?,
            ),
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
        let stored = Stored {
            settings,
            version: SETTINGS_VERSION,
        };
        //written whole so a crash never leaves half a file
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&stored)?)?;
        std::fs::rename(&tmp, &self.path)?;
        self.current.send_replace(settings);
        Ok(())
    }
}

fn upgrade(stored: Stored) -> HubSettings {
    let mut settings = stored.settings;
    //0 was the old implicit default
    if stored.version < 1 && settings.keep_alive == 0 {
        settings.keep_alive = HubSettings::default().keep_alive;
    }
    settings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sampler-hub-settings-{name}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn old_files_get_the_new_keep_alive() {
        let dir = temp_dir("old");
        std::fs::write(
            dir.join(SETTINGS_FILE),
            r#"{"lan_discovery":false,"keep_alive":0}"#,
        )
        .unwrap();
        let settings = Settings::load(&dir).unwrap().get();
        assert_eq!(settings.keep_alive, 300);
        assert!(!settings.lan_discovery);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn chosen_values_survive_a_reload() {
        let dir = temp_dir("chosen");
        let store = Settings::load(&dir).unwrap();
        assert_eq!(store.get().keep_alive, 300);
        for keep_alive in [0, 3600] {
            store
                .set(HubSettings {
                    keep_alive,
                    ..store.get()
                })
                .unwrap();
            assert_eq!(Settings::load(&dir).unwrap().get().keep_alive, keep_alive);
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
