//app container per server, a job object for the process tree
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use std::ptr::{null, null_mut};
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_ALREADY_EXISTS, HANDLE, HANDLE_FLAG_INHERIT, LocalFree, SetHandleInformation,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertStringSidToSidW, EXPLICIT_ACCESS_W, GetNamedSecurityInfoW, NO_MULTIPLE_TRUSTEE,
    SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW, SetNamedSecurityInfoW, TRUSTEE_IS_SID,
    TRUSTEE_IS_UNKNOWN, TRUSTEE_W,
};
use windows_sys::Win32::Security::Isolation::{
    CreateAppContainerProfile, DeleteAppContainerProfile, DeriveAppContainerSidFromAppContainerName,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACL, DACL_SECURITY_INFORMATION, EqualSid, FreeSid, GetAce, PSID,
    SECURITY_ATTRIBUTES, SECURITY_CAPABILITIES, SID_AND_ATTRIBUTES,
    SUB_CONTAINERS_AND_OBJECTS_INHERIT,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALL_ACCESS, FILE_GENERIC_EXECUTE, FILE_GENERIC_READ,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_ACTIVE_PROCESS,
    JOB_OBJECT_LIMIT_JOB_MEMORY, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject,
};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::SystemServices::SE_GROUP_ENABLED;
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, GetProcessId,
    INFINITE, InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
    PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject,
};

use super::{Child, Isolation, Policy};

//internet and local network, never loopback
const CAPABILITIES: [&str; 2] = ["S-1-15-3-1", "S-1-15-3-3"];
//every app container carries this group
const ALL_APP_PACKAGES: &str = "S-1-15-2-1";
const MAX_PROCESSES: u32 = 64;
const MAX_MEMORY: usize = 4 * 1024 * 1024 * 1024;

pub struct Process {
    handle: Arc<OwnedHandle>,
    //closing it kills the whole tree
    _job: OwnedHandle,
}

impl Process {
    pub fn id(&self) -> Option<u32> {
        Some(unsafe { GetProcessId(self.handle.as_raw_handle() as HANDLE) })
    }

    pub async fn wait(&mut self) -> Result<Option<i32>> {
        let handle = self.handle.clone();
        tokio::task::spawn_blocking(move || {
            let raw = handle.as_raw_handle() as HANDLE;
            let mut code = 0u32;
            unsafe {
                WaitForSingleObject(raw, INFINITE);
                GetExitCodeProcess(raw, &mut code);
            }
            Some(code as i32)
        })
        .await
        .map_err(Into::into)
    }
}

pub fn isolation() -> Isolation {
    Isolation::Full
}

pub fn forget(name: &str) {
    unsafe {
        DeleteAppContainerProfile(wide(name).as_ptr());
    }
}

//a sid freed with the right function
struct Sid {
    raw: PSID,
    local: bool,
}

impl Drop for Sid {
    fn drop(&mut self) {
        unsafe {
            if self.local {
                LocalFree(self.raw);
            } else {
                FreeSid(self.raw);
            }
        }
    }
}

fn wide(text: impl AsRef<OsStr>) -> Vec<u16> {
    text.as_ref().encode_wide().chain([0]).collect()
}

fn last_error(what: &str) -> anyhow::Error {
    anyhow!("{what}: {}", std::io::Error::last_os_error())
}

fn string_sid(text: &str) -> Result<Sid> {
    let mut raw: PSID = null_mut();
    if unsafe { ConvertStringSidToSidW(wide(text).as_ptr(), &mut raw) } == 0 {
        return Err(last_error("invalid sid"));
    }
    Ok(Sid { raw, local: true })
}

fn container_sid(name: &str) -> Result<Sid> {
    let name = wide(name);
    let mut raw: PSID = null_mut();
    let created = unsafe {
        CreateAppContainerProfile(
            name.as_ptr(),
            name.as_ptr(),
            name.as_ptr(),
            null(),
            0,
            &mut raw,
        )
    };
    //hresult of an existing profile
    let exists = (0x8007_0000u32 | ERROR_ALREADY_EXISTS) as i32;
    if created == exists {
        let derived = unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut raw) };
        if derived < 0 {
            bail!("cannot find the app container ({derived:#x})");
        }
    } else if created < 0 {
        bail!("cannot create the app container ({created:#x})");
    }
    Ok(Sid { raw, local: false })
}

//skips folders that already grant this
fn grant(path: &Path, sid: &Sid, access: u32) -> Result<()> {
    let name = wide(path);
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor = null_mut();
    let status = unsafe {
        GetNamedSecurityInfoW(
            name.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        bail!(
            "cannot read the permissions of {} ({status})",
            path.display()
        );
    }
    let granted = unsafe { has_ace(dacl, sid.raw, access) };
    let result = if granted {
        Ok(())
    } else {
        let entry = EXPLICIT_ACCESS_W {
            grfAccessPermissions: access,
            grfAccessMode: SET_ACCESS,
            grfInheritance: SUB_CONTAINERS_AND_OBJECTS_INHERIT,
            Trustee: TRUSTEE_W {
                pMultipleTrustee: null_mut(),
                MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
                TrusteeForm: TRUSTEE_IS_SID,
                TrusteeType: TRUSTEE_IS_UNKNOWN,
                ptstrName: sid.raw as *mut u16,
            },
        };
        let mut updated: *mut ACL = null_mut();
        let status = unsafe { SetEntriesInAclW(1, &entry, dacl, &mut updated) };
        if status != 0 {
            Err(anyhow!(
                "cannot build the permissions of {} ({status})",
                path.display()
            ))
        } else {
            let status = unsafe {
                SetNamedSecurityInfoW(
                    name.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    updated,
                    null(),
                )
            };
            unsafe { LocalFree(updated as _) };
            if status == 0 {
                Ok(())
            } else {
                Err(anyhow!(
                    "cannot set the permissions of {} ({status})",
                    path.display()
                ))
            }
        }
    };
    unsafe { LocalFree(descriptor) };
    result
}

unsafe fn has_ace(dacl: *mut ACL, sid: PSID, access: u32) -> bool {
    if dacl.is_null() {
        return false;
    }
    let count = unsafe { (*dacl).AceCount };
    (0..count as u32).any(|i| {
        let mut ace = null_mut();
        if unsafe { GetAce(dacl, i, &mut ace) } == 0 {
            return false;
        }
        let ace = ace as *mut ACCESS_ALLOWED_ACE;
        //type 0 is an allow ace
        unsafe {
            (*ace).Header.AceType == 0
                && (*ace).Mask == access
                && EqualSid(&mut (*ace).SidStart as *mut u32 as PSID, sid) != 0
        }
    })
}

fn pipe() -> Result<(OwnedHandle, OwnedHandle)> {
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    let mut read: HANDLE = null_mut();
    let mut write: HANDLE = null_mut();
    if unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) } == 0 {
        return Err(last_error("cannot create a pipe"));
    }
    unsafe {
        Ok((
            OwnedHandle::from_raw_handle(read as _),
            OwnedHandle::from_raw_handle(write as _),
        ))
    }
}

fn keep_private(handle: &OwnedHandle) {
    unsafe { SetHandleInformation(handle.as_raw_handle() as HANDLE, HANDLE_FLAG_INHERIT, 0) };
}

//windows argv quoting rules
fn push_quoted(line: &mut Vec<u16>, arg: &OsStr) {
    let arg: Vec<u16> = arg.encode_wide().collect();
    let plain = !arg.is_empty()
        && !arg
            .iter()
            .any(|&c| c == b' ' as u16 || c == b'\t' as u16 || c == b'"' as u16);
    if !line.is_empty() {
        line.push(b' ' as u16);
    }
    if plain {
        line.extend(arg);
        return;
    }
    line.push(b'"' as u16);
    let mut backslashes = 0;
    for c in arg {
        if c == b'\\' as u16 {
            backslashes += 1;
            continue;
        }
        let extra = if c == b'"' as u16 { backslashes + 1 } else { 0 };
        line.extend(std::iter::repeat_n(b'\\' as u16, backslashes + extra));
        line.push(c);
        backslashes = 0;
    }
    line.extend(std::iter::repeat_n(b'\\' as u16, backslashes * 2));
    line.push(b'"' as u16);
}

fn environment_block(env: &[(OsString, OsString)]) -> Vec<u16> {
    let mut sorted: Vec<&(OsString, OsString)> = env.iter().collect();
    sorted.sort_by_key(|(k, _)| k.to_string_lossy().to_uppercase());
    let mut block = Vec::new();
    for (key, value) in sorted {
        block.extend(key.encode_wide());
        block.push(b'=' as u16);
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    block
}

fn job() -> Result<OwnedHandle> {
    let raw = unsafe { CreateJobObjectW(null(), null()) };
    if raw.is_null() {
        return Err(last_error("cannot create a job"));
    }
    let job = unsafe { OwnedHandle::from_raw_handle(raw as _) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        | JOB_OBJECT_LIMIT_ACTIVE_PROCESS
        | JOB_OBJECT_LIMIT_JOB_MEMORY;
    limits.BasicLimitInformation.ActiveProcessLimit = MAX_PROCESSES;
    limits.JobMemoryLimit = MAX_MEMORY;
    let set = unsafe {
        SetInformationJobObject(
            raw,
            JobObjectExtendedLimitInformation,
            &limits as *const _ as _,
            size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if set == 0 {
        return Err(last_error("cannot limit the job"));
    }
    Ok(job)
}

pub fn spawn(
    program: &Path,
    args: &[OsString],
    env: &[(OsString, OsString)],
    cwd: &Path,
    policy: &Policy,
) -> Result<Child> {
    let sid = container_sid(&policy.name)?;
    //one ace for all servers, set once
    let everyone = string_sid(ALL_APP_PACKAGES)?;
    for path in &policy.shared {
        grant(path, &everyone, FILE_GENERIC_READ | FILE_GENERIC_EXECUTE)?;
    }
    for path in &policy.read {
        grant(path, &sid, FILE_GENERIC_READ | FILE_GENERIC_EXECUTE)?;
    }
    for path in &policy.write {
        grant(path, &sid, FILE_ALL_ACCESS)?;
    }
    let capabilities: Vec<Sid> = CAPABILITIES
        .iter()
        .map(|s| string_sid(s))
        .collect::<Result<_>>()?;
    let mut attributes: Vec<SID_AND_ATTRIBUTES> = capabilities
        .iter()
        .map(|s| SID_AND_ATTRIBUTES {
            Sid: s.raw,
            Attributes: SE_GROUP_ENABLED as u32,
        })
        .collect();
    let security = SECURITY_CAPABILITIES {
        AppContainerSid: sid.raw,
        Capabilities: attributes.as_mut_ptr(),
        CapabilityCount: attributes.len() as u32,
        Reserved: 0,
    };

    let (stdin_read, stdin_write) = pipe()?;
    let (stdout_read, stdout_write) = pipe()?;
    let (stderr_read, stderr_write) = pipe()?;
    for parent_end in [&stdin_write, &stdout_read, &stderr_read] {
        keep_private(parent_end);
    }
    //only the three pipe ends reach the child
    let inherited: [HANDLE; 3] = [
        stdin_read.as_raw_handle() as HANDLE,
        stdout_write.as_raw_handle() as HANDLE,
        stderr_write.as_raw_handle() as HANDLE,
    ];

    let mut size = 0usize;
    unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut size) };
    let mut buffer = vec![0u8; size];
    let list = buffer.as_mut_ptr() as LPPROC_THREAD_ATTRIBUTE_LIST;
    if unsafe { InitializeProcThreadAttributeList(list, 2, 0, &mut size) } == 0 {
        return Err(last_error("cannot prepare the process"));
    }
    let result = (|| {
        let updated = unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                &security as *const _ as _,
                size_of::<SECURITY_CAPABILITIES>(),
                null_mut(),
                null(),
            ) != 0
                && UpdateProcThreadAttribute(
                    list,
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    inherited.as_ptr() as _,
                    size_of_val(&inherited),
                    null_mut(),
                    null(),
                ) != 0
        };
        if !updated {
            return Err(last_error("cannot prepare the process"));
        }
        let mut info = STARTUPINFOEXW::default();
        info.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        info.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        info.StartupInfo.hStdInput = inherited[0];
        info.StartupInfo.hStdOutput = inherited[1];
        info.StartupInfo.hStdError = inherited[2];
        info.lpAttributeList = list;

        let mut line = Vec::new();
        push_quoted(&mut line, program.as_os_str());
        for arg in args {
            push_quoted(&mut line, arg);
        }
        line.push(0);
        let program = wide(program);
        let cwd = wide(cwd);
        let block = environment_block(env);
        let mut process = PROCESS_INFORMATION::default();
        let created = unsafe {
            CreateProcessW(
                program.as_ptr(),
                line.as_mut_ptr(),
                null(),
                null(),
                1,
                EXTENDED_STARTUPINFO_PRESENT
                    | CREATE_SUSPENDED
                    | CREATE_UNICODE_ENVIRONMENT
                    | CREATE_NO_WINDOW,
                block.as_ptr() as _,
                cwd.as_ptr(),
                &info.StartupInfo,
                &mut process,
            )
        };
        if created == 0 {
            return Err(last_error("cannot start the sandboxed process"));
        }
        Ok(process)
    })();
    unsafe { DeleteProcThreadAttributeList(list) };
    let process = result?;
    let handle = unsafe { OwnedHandle::from_raw_handle(process.hProcess as _) };
    let job = match job() {
        Ok(job)
            if unsafe {
                AssignProcessToJobObject(job.as_raw_handle() as HANDLE, process.hProcess)
            } != 0 =>
        {
            job
        }
        other => {
            unsafe {
                TerminateProcess(process.hProcess, 1);
                CloseHandle(process.hThread);
            }
            return Err(other
                .err()
                .unwrap_or_else(|| last_error("cannot join the job")));
        }
    };
    unsafe {
        ResumeThread(process.hThread);
        CloseHandle(process.hThread);
    }
    drop((stdin_read, stdout_write, stderr_write));

    let file = |handle: OwnedHandle| tokio::fs::File::from_std(std::fs::File::from(handle));
    Ok(Child {
        stdin: Some(Box::new(file(stdin_write))),
        stdout: Some(Box::new(file(stdout_read))),
        stderr: Some(Box::new(file(stderr_read))),
        process: Process {
            handle: Arc::new(handle),
            _job: job,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quoted(args: &[&str]) -> String {
        let mut line = Vec::new();
        for arg in args {
            push_quoted(&mut line, OsStr::new(arg));
        }
        String::from_utf16(&line).unwrap()
    }

    #[test]
    fn arguments_survive_quoting() {
        assert_eq!(quoted(&["a", "b c"]), r#"a "b c""#);
        assert_eq!(quoted(&[""]), r#""""#);
        assert_eq!(quoted(&[r#"say "hi""#]), r#""say \"hi\"""#);
        assert_eq!(quoted(&[r"C:\dir with space\"]), r#""C:\dir with space\\""#);
        assert_eq!(quoted(&[r"C:\plain\path"]), r"C:\plain\path");
    }
}
