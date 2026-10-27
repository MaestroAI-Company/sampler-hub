//seatbelt profile run through sandbox-exec
use std::ffi::OsString;
use std::path::Path;

use anyhow::{Context, Result};
use tokio::process::Command;

pub use super::unix::Process;
use super::{Child, Isolation, Policy};

const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

//deny by default, system files and the policy folders only
const BASE_PROFILE: &str = r#"(version 1)
(deny default)
(import "system.sb")
(allow process-fork)
(allow process-exec)
(allow signal (target same-sandbox))
(allow process-info* (target same-sandbox))
(allow sysctl-read)
(allow system-socket)
;unix sockets would run owner commands
(allow network-outbound
  (literal "/private/var/run/mDNSResponder")
  (remote ip))
(allow mach-lookup
  (global-name "com.apple.dnssd.service")
  (global-name "com.apple.trustd.agent")
  (global-name "com.apple.SecurityServer")
  (global-name "com.apple.system.opendirectoryd.libinfo")
  (global-name "com.apple.system.notification_center")
  (global-name "com.apple.logd"))
(allow file-read-metadata)
(allow file-read*
  (literal "/")
  (subpath "/usr")
  (subpath "/System")
  (subpath "/Library/Apple")
  (subpath "/Library/Preferences")
  (subpath "/private/etc")
  (subpath "/private/var/db/timezone")
  (subpath "/dev"))
(allow file-write*
  (literal "/dev/null")
  (literal "/dev/zero")
  (literal "/dev/tty"))
"#;

pub fn isolation() -> Isolation {
    if Path::new(SANDBOX_EXEC).exists() {
        Isolation::Full
    } else {
        Isolation::Partial
    }
}

pub fn forget(_name: &str) {}

//seatbelt matches real paths, not symlinks
fn quoted(path: &Path) -> Result<String> {
    let real = path
        .canonicalize()
        .with_context(|| format!("{} does not exist", path.display()))?;
    let text = real.to_string_lossy();
    Ok(format!(
        "\"{}\"",
        text.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

fn profile(policy: &Policy) -> Result<String> {
    let mut profile = BASE_PROFILE.to_string();
    for path in policy.shared.iter().chain(&policy.read) {
        profile.push_str(&format!("(allow file-read* (subpath {}))\n", quoted(path)?));
    }
    for path in &policy.write {
        profile.push_str(&format!(
            "(allow file-read* file-write* (subpath {}))\n",
            quoted(path)?
        ));
    }
    Ok(profile)
}

pub fn spawn(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    cwd: &Path,
    policy: &Policy,
) -> Result<Child> {
    let mut command = if isolation() == Isolation::Full {
        let mut command = Command::new(SANDBOX_EXEC);
        command.arg("-p").arg(profile(policy)?).arg(program);
        command
    } else {
        Command::new(program)
    };
    command
        .args(args)
        .env_clear()
        .envs(env.iter().cloned())
        .current_dir(cwd);
    super::unix::spawn(command)
}
