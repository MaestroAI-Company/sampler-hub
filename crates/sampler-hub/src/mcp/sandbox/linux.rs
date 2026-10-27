//landlock limits files from the kernel side
use std::ffi::OsString;
use std::path::Path;

use anyhow::Result;
use landlock::{
    ABI, Access, AccessFs, Ruleset, RulesetAttr, RulesetCreatedAttr, path_beneath_rules,
};
use tokio::process::Command;

pub use super::unix::Process;
use super::{Child, Isolation, Policy};

//system files programs need to start
const SYSTEM_READ: [&str; 11] = [
    "/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc", "/proc", "/sys", "/dev", "/run", "/opt",
];
const SYSTEM_WRITE: [&str; 3] = ["/dev/null", "/dev/zero", "/dev/tty"];

#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xc000_00b7;
//x32 calls share the x86_64 arch
const X32_SYSCALL_BIT: u32 = 0x4000_0000;

fn op(code: u32, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter {
        code: code as u16,
        jt,
        jf,
        k,
    }
}

//landlock skips connect on unix sockets
fn filter() -> Vec<libc::sock_filter> {
    let load = libc::BPF_LD | libc::BPF_W | libc::BPF_ABS;
    let eq = libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K;
    let set = libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K;
    let ret = libc::BPF_RET | libc::BPF_K;
    vec![
        op(load, 4, 0, 0),
        op(eq, AUDIT_ARCH, 0, 9),
        op(load, 0, 0, 0),
        op(set, X32_SYSCALL_BIT, 7, 0),
        //io_uring would bypass the socket check
        op(eq, libc::SYS_io_uring_setup as u32, 5, 0),
        op(eq, libc::SYS_socket as u32, 0, 2),
        op(load, 16, 0, 0),
        op(eq, libc::AF_UNIX as u32, 1, 0),
        op(ret, libc::SECCOMP_RET_ALLOW, 0, 0),
        op(ret, libc::SECCOMP_RET_ERRNO | libc::EACCES as u32, 0, 0),
        op(ret, libc::SECCOMP_RET_ERRNO | libc::ENOSYS as u32, 0, 0),
        op(ret, libc::SECCOMP_RET_KILL_PROCESS, 0, 0),
    ]
}

pub fn isolation() -> Isolation {
    //asking for the abi version probes support
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            1u32,
        )
    };
    if version >= 1 {
        Isolation::Full
    } else {
        Isolation::Partial
    }
}

pub fn forget(_name: &str) {}

pub fn spawn(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    cwd: &Path,
    policy: &Policy,
) -> Result<Child> {
    let abi = ABI::V5;
    //built here, applied in the child before exec
    let mut rules = Some(
        Ruleset::default()
            .handle_access(AccessFs::from_all(abi))?
            .create()?
            .add_rules(path_beneath_rules(SYSTEM_READ, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(&policy.shared, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(&policy.read, AccessFs::from_read(abi)))?
            .add_rules(path_beneath_rules(SYSTEM_WRITE, AccessFs::from_all(abi)))?
            .add_rules(path_beneath_rules(&policy.write, AccessFs::from_all(abi)))?,
    );
    let filter = filter();
    let mut command = Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(env.iter().cloned())
        .current_dir(cwd);
    unsafe {
        command.pre_exec(move || {
            //dies with the hub
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            if let Some(rules) = rules.take() {
                rules
                    .restrict_self()
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
            //needed to filter without root
            let zero: libc::c_ulong = 0;
            if libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                zero,
                zero,
                zero,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let program = libc::sock_fprog {
                len: filter.len() as libc::c_ushort,
                filter: filter.as_ptr() as *mut libc::sock_filter,
            };
            let mode = libc::SECCOMP_MODE_FILTER as libc::c_ulong;
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                mode,
                &program as *const libc::sock_fprog,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    super::unix::spawn(command)
}
