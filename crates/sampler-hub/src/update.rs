//releases from github, checked and installed by the hub
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{info, warn};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const RELEASES: &str = "https://github.com/MaestroAI-Company/sampler-hub/releases";
const LATEST_API: &str = "https://api.github.com/repos/MaestroAI-Company/sampler-hub/releases/latest";
//unauthenticated api calls are rate limited
const CHECK_EVERY: Duration = Duration::from_secs(6 * 60 * 60);

#[derive(Deserialize)]
struct Release {
    tag_name: String,
}

fn parse(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.trim_start_matches('v').split('.');
    let mut next = || parts.next()?.parse().ok();
    Some((next()?, next()?, next()?))
}

//none when this build is the newest
pub async fn newer_release(http: &reqwest::Client) -> Result<Option<String>> {
    let release: Release = http
        .get(LATEST_API)
        .header("user-agent", "sampler-hub")
        .header("accept", "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let latest = release.tag_name.trim_start_matches('v').to_string();
    Ok((parse(&latest) > parse(VERSION)).then_some(latest))
}

//keeps the newest release for hello answers
pub async fn watch_releases(http: reqwest::Client, latest: watch::Sender<Option<String>>) {
    loop {
        match newer_release(&http).await {
            Ok(found) => {
                if let Some(version) = &found {
                    info!("sampler hub {version} is available");
                }
                latest.send_replace(found);
            }
            Err(e) => warn!("update check failed: {e:#}"),
        }
        tokio::time::sleep(CHECK_EVERY).await;
    }
}

//the release file this platform updates from
pub fn asset() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => Some("sampler-hub-setup-windows-x86_64.exe"),
        ("windows", "aarch64") => Some("sampler-hub-setup-windows-aarch64.exe"),
        ("linux", "x86_64") => Some("sampler-hub-linux-x86_64"),
        ("linux", "aarch64") => Some("sampler-hub-linux-aarch64"),
        ("macos", "aarch64") => Some("sampler-hub-macos-aarch64"),
        _ => None,
    }
}

//checked against the release checksums
pub async fn download(http: &reqwest::Client, version: &str, asset: &str) -> Result<Vec<u8>> {
    let base = format!("{RELEASES}/download/v{version}");
    let sums = http
        .get(format!("{base}/SHA256SUMS"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let expected = sums
        .lines()
        .find_map(|line| {
            let mut parts = line.split_whitespace();
            let hash = parts.next()?;
            let name = parts.next()?.trim_start_matches('*');
            (name == asset).then(|| hash.to_ascii_lowercase())
        })
        .with_context(|| format!("{asset} is missing from the v{version} checksums"))?;
    let bytes = http
        .get(format!("{base}/{asset}"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let actual: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    ensure!(actual == expected, "checksum mismatch for {asset}");
    Ok(bytes.to_vec())
}

//saved next to the temp dir, the caller runs it
pub async fn download_installer(
    http: &reqwest::Client,
    version: &str,
) -> Result<std::path::PathBuf> {
    let asset = asset().context("no Sampler-Hub release for this platform")?;
    let bytes = download(http, version, asset).await?;
    let path = std::env::temp_dir().join(asset);
    std::fs::write(&path, bytes)?;
    Ok(path)
}

//windows reinstalls silently, others swap binaries
pub async fn apply(http: &reqwest::Client, version: &str) -> Result<()> {
    #[cfg(windows)]
    {
        let installer = download_installer(http, version).await?;
        //silent, it stops the hub then starts it again
        std::process::Command::new(installer)
            .args(["/S", "/R"])
            .spawn()?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let asset = asset().context("no Sampler-Hub release for this platform")?;
        let exe = std::env::current_exe()?;
        let hub = exe.with_file_name("sampler-hub");
        replace(&hub, &download(http, version, asset).await?)?;
        //macos also runs the tray twin
        let tray = exe.with_file_name("sampler-hubw");
        if cfg!(target_os = "macos") && tray.exists() {
            let bytes = download(http, version, "sampler-hubw-macos-aarch64").await?;
            replace(&tray, &bytes)?;
        }
        Ok(())
    }
}

//the running process keeps the old file
#[cfg(not(windows))]
fn replace(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let next = path.with_extension("new");
    std::fs::write(&next, bytes).with_context(|| {
        format!(
            "cannot write next to {}, update it with the tool that installed it",
            path.display()
        )
    })?;
    std::fs::set_permissions(&next, std::fs::Permissions::from_mode(0o755))?;
    std::fs::rename(&next, path)?;
    Ok(())
}

//starts the new binary in place of this process
#[cfg(not(windows))]
pub fn restart() -> Result<()> {
    use std::os::unix::process::CommandExt;
    let error = std::process::Command::new(std::env::current_exe()?)
        .args(std::env::args_os().skip(1))
        .exec();
    Err(error.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_compare_numerically() {
        assert!(parse("0.10.0") > parse("0.9.3"));
        assert!(parse("v1.0.0") > parse("0.99.99"));
        assert_eq!(parse("0.5.0"), parse("v0.5.0"));
        assert_eq!(parse("nope"), None);
    }
}
