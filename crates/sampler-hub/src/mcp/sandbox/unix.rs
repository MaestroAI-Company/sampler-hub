//process groups so the whole tree dies together
use anyhow::{Context, Result};
use tokio::process::Command;

use super::Child;

pub struct Process {
    child: tokio::process::Child,
    group: Option<i32>,
}

impl Process {
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub async fn wait(&mut self) -> Result<Option<i32>> {
        Ok(self.child.wait().await?.code())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(group) = self.group {
            unsafe { libc::killpg(group, libc::SIGKILL) };
        }
    }
}

pub fn spawn(mut command: Command) -> Result<Child> {
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("cannot start the sandboxed process")?;
    let group = child.id().map(|id| id as i32);
    Ok(Child {
        stdin: child.stdin.take().map(|s| Box::new(s) as _),
        stdout: child.stdout.take().map(|s| Box::new(s) as _),
        stderr: child.stderr.take().map(|s| Box::new(s) as _),
        process: Process { child, group },
    })
}
