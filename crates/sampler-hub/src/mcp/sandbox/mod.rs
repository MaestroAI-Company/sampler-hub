//os sandbox around every mcp server, nothing to install
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(target_os = "linux")]
use linux as imp;
#[cfg(target_os = "macos")]
use macos as imp;
#[cfg(windows)]
use windows as imp;

//what a sandboxed process may touch
#[derive(Debug, Clone)]
pub struct Policy {
    //names the windows app container
    pub name: String,
    //public files every sandbox may read
    pub shared: Vec<PathBuf>,
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    //files outside the policy are unreachable
    Full,
    //the os cannot restrict files here
    Partial,
}

pub type Reader = Box<dyn AsyncRead + Send + Unpin>;
pub type Writer = Box<dyn AsyncWrite + Send + Unpin>;

pub struct Child {
    pub stdin: Option<Writer>,
    pub stdout: Option<Reader>,
    pub stderr: Option<Reader>,
    process: imp::Process,
}

impl Child {
    pub fn id(&self) -> Option<u32> {
        self.process.id()
    }

    //exit code, none when killed by a signal
    pub async fn wait(&mut self) -> Result<Option<i32>> {
        self.process.wait().await
    }
}

pub fn isolation() -> Isolation {
    imp::isolation()
}

//the process tree dies with the child handle
pub fn spawn(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    cwd: &Path,
    policy: &Policy,
) -> Result<Child> {
    imp::spawn(program, args, env, cwd, policy)
}

//drops what the os kept for this policy
pub fn forget(name: &str) {
    imp::forget(name);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncReadExt;

    use super::*;

    fn temp_dir() -> PathBuf {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let name: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        let dir = std::env::temp_dir().join(format!("sampler-hub-sandbox-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        //macos temp sits behind a symlink
        if cfg!(unix) {
            dir.canonicalize().unwrap()
        } else {
            dir
        }
    }

    fn start(line: &str, home: &Path, policy: &Policy) -> Child {
        let (program, args): (PathBuf, Vec<OsString>) = if cfg!(windows) {
            let root = std::env::var_os("SystemRoot").unwrap();
            (
                PathBuf::from(root).join("System32").join("cmd.exe"),
                vec!["/d".into(), "/c".into(), line.into()],
            )
        } else {
            ("/bin/sh".into(), vec!["-c".into(), line.into()])
        };
        let mut env = Vec::new();
        if let Some(root) = std::env::var_os("SystemRoot") {
            env.push(("SystemRoot".into(), root));
            //windows rewrites it for app containers
            env.push(("LOCALAPPDATA".into(), home.into()));
        }
        env.push(("PATH".into(), "/usr/bin:/bin".into()));
        spawn(&program, &args, &env, home, policy).unwrap()
    }

    //runs a shell line inside the sandbox
    async fn shell(line: &str, home: &Path, policy: &Policy) -> (String, String) {
        let mut child = start(line, home, policy);
        drop(child.stdin.take());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let read = async {
            stdout.read_to_end(&mut out).await.unwrap();
            stderr.read_to_end(&mut err).await.unwrap();
            child.wait().await.unwrap()
        };
        tokio::time::timeout(Duration::from_secs(30), read)
            .await
            .unwrap();
        //cmd speaks the oem code page
        let (out, err) = (
            String::from_utf8_lossy(&out).into_owned(),
            String::from_utf8_lossy(&err).into_owned(),
        );
        (out, err)
    }

    #[tokio::test]
    async fn server_only_reaches_its_folders() {
        let root = temp_dir();
        let secret = root.join("secret.txt");
        std::fs::write(&secret, "hub key").unwrap();
        let package = root.join("package");
        let home = root.join("home");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        std::fs::write(package.join("readme.txt"), "package file").unwrap();
        let mut name = [0u8; 4];
        getrandom::fill(&mut name).unwrap();
        let policy = Policy {
            name: format!(
                "SamplerHub.test.{:02x}{:02x}{:02x}{:02x}",
                name[0], name[1], name[2], name[3]
            ),
            shared: Vec::new(),
            read: vec![package.clone()],
            write: vec![home.clone()],
        };

        //cmd parses its own command line, so no quotes
        let read = |path: &Path| {
            if cfg!(windows) {
                format!("type {}", path.display())
            } else {
                format!("cat '{}'", path.display())
            }
        };
        let (out, err) = shell(&read(&package.join("readme.txt")), &home, &policy).await;
        assert!(out.contains("package file"), "{out} {err}");
        let written = home.join("note.txt");
        let write = if cfg!(windows) {
            format!("echo saved> {}", written.display())
        } else {
            format!("echo saved > '{}'", written.display())
        };
        shell(&write, &home, &policy).await;
        assert!(std::fs::read_to_string(&written).unwrap().contains("saved"));

        if isolation() == Isolation::Full {
            let (out, err) = shell(&read(&secret), &home, &policy).await;
            assert!(!out.contains("hub key"), "the sandbox read {out}");
            assert!(!err.is_empty());
            let escape = root.join("escape.txt");
            let write = if cfg!(windows) {
                format!("echo x> {}", escape.display())
            } else {
                format!("echo x > '{}'", escape.display())
            };
            shell(&write, &home, &policy).await;
            assert!(!escape.exists(), "the sandbox wrote outside");
            let package_write = package.join("changed.txt");
            let write = if cfg!(windows) {
                format!("echo x> {}", package_write.display())
            } else {
                format!("echo x > '{}'", package_write.display())
            };
            shell(&write, &home, &policy).await;
            assert!(
                !package_write.exists(),
                "the sandbox wrote in a read only folder"
            );
        }
        forget(&policy.name);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn server_cannot_reach_host_sockets() {
        let root = temp_dir();
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        //fakes the systemd user bus
        let socket = root.join("bus.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let policy = Policy {
            name: "SamplerHub.test.socket".into(),
            shared: Vec::new(),
            read: Vec::new(),
            write: vec![home.clone()],
        };
        let line = format!(
            "python3 -c \"import socket; socket.socket(socket.AF_UNIX).connect('{}')\"",
            socket.display()
        );
        let (_, err) = shell(&line, &home, &policy).await;
        assert!(
            listener.accept().is_err(),
            "the sandbox reached a host socket"
        );
        assert!(err.contains("PermissionError"), "{err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn server_cannot_reach_host_sockets() {
        let root = temp_dir();
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        //fakes docker or the ssh agent
        let socket = root.join("agent.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let policy = Policy {
            name: "SamplerHub.test.socket".into(),
            shared: Vec::new(),
            read: Vec::new(),
            write: vec![home.clone()],
        };
        //xcode python shim fails sandboxed
        let line = format!("nc -U -w 1 '{}' < /dev/null", socket.display());
        shell(&line, &home, &policy).await;
        assert!(
            listener.accept().is_err(),
            "the sandbox reached a host socket"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn dropping_the_child_kills_its_tree() {
        let root = temp_dir();
        let home = root.join("home");
        std::fs::create_dir_all(&home).unwrap();
        let mut name = [0u8; 4];
        getrandom::fill(&mut name).unwrap();
        let policy = Policy {
            name: format!(
                "SamplerHub.test.{:02x}{:02x}{:02x}{:02x}",
                name[0], name[1], name[2], name[3]
            ),
            shared: Vec::new(),
            read: Vec::new(),
            write: vec![home.clone()],
        };
        //a grandchild that writes forever
        let beat = home.join("beat.txt");
        let line = if cfg!(windows) {
            let cmd = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
                .join("System32")
                .join("cmd.exe");
            format!(
                "{} /d /c for /l %i in (0,0,1) do echo x>>{}",
                cmd.display(),
                beat.display()
            )
        } else {
            format!("sh -c 'while true; do echo x >> {}; done'", beat.display())
        };
        let child = start(&line, &home, &policy);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(beat.exists(), "the grandchild never ran");
        drop(child);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let size = std::fs::metadata(&beat).unwrap().len();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            std::fs::metadata(&beat).unwrap().len(),
            size,
            "the grandchild still runs"
        );
        forget(&policy.name);
        let _ = std::fs::remove_dir_all(&root);
    }
}
