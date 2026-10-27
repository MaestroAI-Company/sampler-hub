//lets a terminal run sampler-hub by name
use std::path::Path;

use anyhow::{Context, Result};

pub use imp::{add, remove};

//the folder holding the running binary
pub fn add_self() -> Result<()> {
    add(&own_dir()?)
}

pub fn remove_self() -> Result<()> {
    remove(&own_dir()?)
}

fn own_dir() -> Result<std::path::PathBuf> {
    let exe = std::env::current_exe()?;
    exe.parent()
        .map(Path::to_path_buf)
        .context("binary has no folder")
}

//one marked line in the shell startup files
#[cfg(not(windows))]
mod imp {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use directories::BaseDirs;

    const MARK: &str = "# added by sampler-hub";

    fn startup_files() -> Result<Vec<PathBuf>> {
        let home = BaseDirs::new().context("no home directory")?;
        let mut files = Vec::new();
        for name in [".profile", ".bashrc", ".zshenv"] {
            let file = home.home_dir().join(name);
            //zsh is the default shell on macos
            let always = name == ".profile" || (name == ".zshenv" && cfg!(target_os = "macos"));
            if always || file.exists() {
                files.push(file);
            }
        }
        Ok(files)
    }

    //replaces the previous marked line
    fn rewrite(file: &Path, line: Option<&str>) -> Result<()> {
        let text = fs::read_to_string(file).unwrap_or_default();
        let mut lines: Vec<&str> = text.lines().filter(|l| !l.ends_with(MARK)).collect();
        lines.extend(line);
        let mut updated = lines.join("\n");
        if !lines.is_empty() {
            updated.push('\n');
        }
        if updated != text {
            fs::write(file, updated)?;
        }
        Ok(())
    }

    pub fn add(dir: &Path) -> Result<()> {
        let on_path = std::env::var_os("PATH")
            .is_some_and(|path| std::env::split_paths(&path).any(|d| d == dir));
        if on_path {
            return Ok(());
        }
        let line = format!("export PATH=\"{}:$PATH\" {MARK}", dir.display());
        for file in startup_files()? {
            rewrite(&file, Some(&line))?;
        }
        Ok(())
    }

    pub fn remove(_dir: &Path) -> Result<()> {
        for file in startup_files()? {
            if file.exists() {
                rewrite(&file, None)?;
            }
        }
        Ok(())
    }
}

//user path needs no admin rights
#[cfg(windows)]
mod imp {
    use std::os::windows::ffi::OsStringExt;
    use std::path::Path;
    use std::ptr::{null, null_mut};

    use anyhow::{Result, bail, ensure};
    use windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND;
    use windows_sys::Win32::System::Registry::{
        HKEY, HKEY_CURRENT_USER, KEY_READ, KEY_WRITE, REG_EXPAND_SZ, REG_SZ, RegCloseKey,
        RegOpenKeyExW, RegQueryValueExW, RegSetValueExW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        HWND_BROADCAST, SMTO_ABORTIFHUNG, SendMessageTimeoutW, WM_SETTINGCHANGE,
    };

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(Some(0)).collect()
    }

    //raw value keeps its %vars%
    fn read(key: HKEY, name: &[u16]) -> Result<(String, u32)> {
        let mut kind = 0;
        let mut size = 0;
        let code = unsafe {
            RegQueryValueExW(key, name.as_ptr(), null(), &mut kind, null_mut(), &mut size)
        };
        if code == ERROR_FILE_NOT_FOUND {
            return Ok((String::new(), REG_EXPAND_SZ));
        }
        ensure!(code == 0, "cannot read the user path ({code})");
        ensure!(
            kind == REG_SZ || kind == REG_EXPAND_SZ,
            "the user path has an unexpected type"
        );
        let mut buffer = vec![0u16; size as usize / 2 + 1];
        let mut size = (buffer.len() * 2) as u32;
        let code = unsafe {
            RegQueryValueExW(
                key,
                name.as_ptr(),
                null(),
                &mut kind,
                buffer.as_mut_ptr().cast(),
                &mut size,
            )
        };
        ensure!(code == 0, "cannot read the user path ({code})");
        buffer.truncate(size as usize / 2);
        while buffer.last() == Some(&0) {
            buffer.pop();
        }
        Ok((
            std::ffi::OsString::from_wide(&buffer)
                .to_string_lossy()
                .into_owned(),
            kind,
        ))
    }

    fn write(key: HKEY, name: &[u16], value: &str, kind: u32) -> Result<()> {
        let data = wide(value);
        let code = unsafe {
            RegSetValueExW(
                key,
                name.as_ptr(),
                0,
                kind,
                data.as_ptr().cast(),
                (data.len() * 2) as u32,
            )
        };
        ensure!(code == 0, "cannot write the user path ({code})");
        Ok(())
    }

    fn same(a: &str, b: &str) -> bool {
        a.trim_end_matches('\\')
            .eq_ignore_ascii_case(b.trim_end_matches('\\'))
    }

    //writes only when something changed
    fn edit(change: impl FnOnce(&mut Vec<String>)) -> Result<()> {
        let sub_key = wide("Environment");
        let name = wide("Path");
        let mut key = null_mut();
        let code = unsafe {
            RegOpenKeyExW(
                HKEY_CURRENT_USER,
                sub_key.as_ptr(),
                0,
                KEY_READ | KEY_WRITE,
                &mut key,
            )
        };
        if code != 0 {
            bail!("cannot open the user environment ({code})");
        }
        let result = (|| {
            let (value, kind) = read(key, &name)?;
            let old: Vec<String> = value
                .split(';')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            let mut entries = old.clone();
            change(&mut entries);
            if entries != old {
                write(key, &name, &entries.join(";"), kind)?;
                //open windows reload the path
                unsafe {
                    SendMessageTimeoutW(
                        HWND_BROADCAST,
                        WM_SETTINGCHANGE,
                        0,
                        sub_key.as_ptr() as isize,
                        SMTO_ABORTIFHUNG,
                        5000,
                        null_mut(),
                    );
                }
            }
            Ok(())
        })();
        unsafe { RegCloseKey(key) };
        result
    }

    pub fn add(dir: &Path) -> Result<()> {
        let dir = dir.to_string_lossy().into_owned();
        edit(|entries| {
            if !entries.iter().any(|e| same(e, &dir)) {
                entries.push(dir);
            }
        })
    }

    pub fn remove(dir: &Path) -> Result<()> {
        let dir = dir.to_string_lossy().into_owned();
        edit(|entries| entries.retain(|e| !same(e, &dir)))
    }
}
