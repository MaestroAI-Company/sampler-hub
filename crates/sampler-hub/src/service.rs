//the hub running in the background, started with the session
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Result, ensure};

pub use imp::{install, start, status, stop, uninstall};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    NotInstalled,
    Running,
    Stopped,
}

//what runs in the background and how
#[derive(Debug, Clone)]
pub struct Launch {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

const TRAY: &str = if cfg!(windows) {
    "sampler-hubw.exe"
} else {
    "sampler-hubw"
};

impl Launch {
    //the tray twin next to a binary, headless on linux without it
    pub fn next_to(binary: &Path, data_dir: &Path) -> Result<Self> {
        let tray = binary.with_file_name(TRAY);
        let data_dir: OsString = data_dir.into();
        if tray.exists() {
            return Ok(Self {
                program: tray,
                args: vec!["--data-dir".into(), data_dir],
            });
        }
        ensure!(
            cfg!(target_os = "linux"),
            "{TRAY} is missing next to {}",
            binary.display()
        );
        Ok(Self {
            program: binary.to_path_buf(),
            args: vec!["--data-dir".into(), data_dir, "run".into()],
        })
    }
}

//systemd user unit or launchd agent
#[cfg(not(windows))]
mod imp {
    use anyhow::{Context, Result};
    use service_manager::{
        RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager,
        ServiceStartCtx, ServiceStatus, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx,
    };

    use super::{Launch, Status};

    const LABEL: &str = "company.maestroai.sampler-hub";

    fn label() -> ServiceLabel {
        LABEL.parse().expect("static service label")
    }

    fn manager() -> Result<Box<dyn ServiceManager>> {
        let mut manager = <dyn ServiceManager>::native().context("no supported service manager")?;
        manager.set_level(ServiceLevel::User)?;
        Ok(manager)
    }

    //replaces an older install and starts it
    pub fn install(launch: &Launch) -> Result<()> {
        if status()? != Status::NotInstalled {
            uninstall()?;
        }
        let manager = manager()?;
        manager.install(ServiceInstallCtx {
            label: label(),
            program: launch.program.clone(),
            args: launch.args.clone(),
            contents: None,
            username: None,
            working_directory: None,
            environment: None,
            autostart: true,
            restart_policy: RestartPolicy::OnFailure {
                delay_secs: Some(10),
                max_retries: None,
                reset_after_secs: None,
            },
        })?;
        manager.start(ServiceStartCtx { label: label() })?;
        Ok(())
    }

    pub fn uninstall() -> Result<()> {
        let manager = manager()?;
        //already stopped is fine
        let _ = manager.stop(ServiceStopCtx { label: label() });
        manager.uninstall(ServiceUninstallCtx { label: label() })?;
        Ok(())
    }

    pub fn start(_launch: &Launch) -> Result<()> {
        manager()?.start(ServiceStartCtx { label: label() })?;
        Ok(())
    }

    pub fn stop() -> Result<()> {
        manager()?.stop(ServiceStopCtx { label: label() })?;
        Ok(())
    }

    pub fn status() -> Result<Status> {
        Ok(
            match manager()?.status(ServiceStatusCtx { label: label() })? {
                ServiceStatus::NotInstalled => Status::NotInstalled,
                ServiceStatus::Running => Status::Running,
                ServiceStatus::Stopped(_) => Status::Stopped,
            },
        )
    }
}

//login start under the user so the keychain matches
#[cfg(windows)]
mod imp {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    use anyhow::{Result, bail};

    use super::{Launch, Status, TRAY};

    const RUN_KEY: &str = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run";
    const RUN_VALUE: &str = "SamplerHub";
    //outlives whoever started it
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    fn quiet(program: &str, args: &[&str]) -> Result<bool> {
        let status = Command::new(program)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .status()?;
        Ok(status.success())
    }

    fn running() -> Result<bool> {
        let filter = format!("IMAGENAME eq {TRAY}");
        let output = Command::new("tasklist")
            .args(["/FI", &filter, "/NH"])
            .creation_flags(CREATE_NO_WINDOW)
            .output()?;
        Ok(String::from_utf8_lossy(&output.stdout).contains(TRAY))
    }

    pub fn install(launch: &Launch) -> Result<()> {
        let mut command = format!("\"{}\"", launch.program.display());
        for arg in &launch.args {
            command.push_str(&format!(" \"{}\"", arg.to_string_lossy()));
        }
        let args = [
            "add", RUN_KEY, "/v", RUN_VALUE, "/t", "REG_SZ", "/d", &command, "/f",
        ];
        if !quiet("reg", &args)? {
            bail!("cannot write the login entry");
        }
        start(launch)
    }

    pub fn uninstall() -> Result<()> {
        stop()?;
        quiet("reg", &["delete", RUN_KEY, "/v", RUN_VALUE, "/f"])?;
        Ok(())
    }

    pub fn start(launch: &Launch) -> Result<()> {
        if !running()? {
            Command::new(&launch.program)
                .args(&launch.args)
                .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
                .spawn()?;
        }
        Ok(())
    }

    pub fn stop() -> Result<()> {
        if running()? {
            quiet("taskkill", &["/IM", TRAY, "/F"])?;
        }
        Ok(())
    }

    pub fn status() -> Result<Status> {
        let installed = quiet("reg", &["query", RUN_KEY, "/v", RUN_VALUE])?;
        Ok(match (installed, running()?) {
            (false, _) => Status::NotInstalled,
            (true, true) => Status::Running,
            (true, false) => Status::Stopped,
        })
    }
}
