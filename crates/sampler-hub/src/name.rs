//hub name, editable from app or cli
use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sampler_hub_proto::clean_device_name;
use tokio::sync::watch;

const NAME_FILE: &str = "name.txt";

//where the effective name comes from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    File,
    Config,
    Hostname,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::File => "set from the app or cli",
            Self::Config => "from config.toml",
            Self::Hostname => "machine hostname",
        })
    }
}

pub fn hostname() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

#[derive(Debug)]
pub struct HubName {
    path: PathBuf,
    config: Option<String>,
    current: watch::Sender<String>,
}

impl HubName {
    pub fn load(data_dir: &Path, config: Option<String>) -> Self {
        let mut name = Self {
            path: data_dir.join(NAME_FILE),
            config,
            current: watch::Sender::new(String::new()),
        };
        let (first, _) = name.resolve();
        name.current = watch::Sender::new(first);
        name
    }

    //file first, then config, then hostname
    pub fn resolve(&self) -> (String, Source) {
        let stored = std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|text| clean_device_name(&text));
        if let Some(name) = stored {
            return (name, Source::File);
        }
        match self.config.as_deref().and_then(clean_device_name) {
            Some(name) => (name, Source::Config),
            None => (hostname(), Source::Hostname),
        }
    }

    //rereads so cli edits show
    pub fn get(&self) -> String {
        let (name, _) = self.resolve();
        self.current.send_if_modified(|current| {
            let changed = *current != name;
            if changed {
                *current = name.clone();
            }
            changed
        });
        name
    }

    pub fn subscribe(&self) -> watch::Receiver<String> {
        self.current.subscribe()
    }

    //empty resets, returns the effective name
    pub fn set(&self, name: &str) -> Result<String> {
        match clean_device_name(name) {
            Some(name) => {
                //atomic write survives a crash
                let tmp = self.path.with_extension("txt.tmp");
                std::fs::write(&tmp, name)?;
                std::fs::rename(&tmp, &self.path)?;
            }
            None => match std::fs::remove_file(&self.path) {
                Err(e) if e.kind() != ErrorKind::NotFound => {
                    return Err(e)
                        .with_context(|| format!("cannot remove {}", self.path.display()));
                }
                _ => {}
            },
        }
        Ok(self.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sampler-hub-name-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn file_beats_config_beats_hostname() {
        let dir = temp_dir("order");
        let name = HubName::load(&dir, Some("From config".into()));
        assert_eq!(name.resolve(), ("From config".into(), Source::Config));
        assert_eq!(name.set("  Studio  ").unwrap(), "Studio");
        assert_eq!(name.resolve(), ("Studio".into(), Source::File));
        assert_eq!(name.set("").unwrap(), "From config");
        let bare = HubName::load(&dir, None);
        assert_eq!(bare.resolve(), (hostname(), Source::Hostname));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cli_edits_reach_subscribers() {
        let dir = temp_dir("watch");
        let name = HubName::load(&dir, None);
        let mut rx = name.subscribe();
        rx.borrow_and_update();
        std::fs::write(dir.join(NAME_FILE), "Edited").unwrap();
        assert_eq!(name.get(), "Edited");
        assert!(rx.has_changed().unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
