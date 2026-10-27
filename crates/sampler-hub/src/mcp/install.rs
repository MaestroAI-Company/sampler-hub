//fetches packages and builds the command that runs them
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use sampler_hub_proto::{McpListing, McpSource};
use serde_json::Value;
use tokio::process::Command;

use super::mcpb::{self, Manifest};
use super::runtime::{Runtimes, scrubbed_env};
use crate::llama::hex_sha256;

pub async fn download_bundle(http: &reqwest::Client, url: &str, sha256: &str) -> Result<Vec<u8>> {
    let bytes = http
        .get(url)
        .header("user-agent", "sampler-hub")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(
        hex_sha256(&bytes) == sha256.to_ascii_lowercase(),
        "checksum mismatch for the bundle"
    );
    Ok(bytes.to_vec())
}

//returns the listing with fields a bundle declares
pub async fn install(
    runtimes: &Runtimes,
    http: &reqwest::Client,
    dir: &Path,
    listing: &McpListing,
) -> Result<McpListing> {
    let package = dir.join("package");
    let home = dir.join("home");
    std::fs::create_dir_all(&package)?;
    std::fs::create_dir_all(&home)?;
    let mut listing = listing.clone();
    match &listing.source {
        McpSource::Mcpb { url, sha256 } => {
            mcpb::unpack(&download_bundle(http, url, sha256).await?, &package)?;
            let manifest = Manifest::read(&package)?;
            manifest.check_supported()?;
            //the manifest decides, not the device
            listing.fields = manifest.fields();
        }
        McpSource::Npm {
            package: name,
            version,
        } => {
            runtimes.ensure_node().await?;
            let args: Vec<OsString> = vec![
                runtimes.npm_cli().into(),
                "install".into(),
                "--prefix".into(),
                package.clone().into(),
                "--ignore-scripts".into(),
                "--no-audit".into(),
                "--no-fund".into(),
                "--omit=dev".into(),
                "--loglevel=error".into(),
                format!("{name}@{version}").into(),
            ];
            let env = vec![
                ("npm_config_cache".into(), home.join(".npm").into()),
                ("npm_config_update_notifier".into(), "false".into()),
            ];
            let node = runtimes.node_exe();
            let path: Vec<PathBuf> = node.parent().map(Path::to_path_buf).into_iter().collect();
            run_install(&node, &args, env, &home, &path).await?;
            npm_entry(&package, name)?;
        }
        McpSource::Pypi {
            package: name,
            version,
        } => {
            let python = runtimes.python().await?;
            let venv = package.join("venv");
            let mut env = runtimes.uv_env();
            env.push(("UV_CACHE_DIR".into(), home.join(".cache").join("uv").into()));
            env.push(("UV_PYTHON_DOWNLOADS".into(), "never".into()));
            let uv = runtimes.uv_exe();
            let args: Vec<OsString> = vec![
                "venv".into(),
                venv.clone().into(),
                "--python".into(),
                python.into(),
            ];
            run_install(&uv, &args, env.clone(), &home, &[]).await?;
            env.push(("VIRTUAL_ENV".into(), venv.clone().into()));
            let args: Vec<OsString> = vec![
                "pip".into(),
                "install".into(),
                //wheels only, no build script runs
                "--no-build".into(),
                format!("{name}=={version}").into(),
            ];
            run_install(&uv, &args, env, &home, &[]).await?;
            python_entry(&venv, name)?;
        }
    }
    Ok(listing)
}

type Env = Vec<(OsString, OsString)>;

//what one start runs
pub struct Launch {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Env,
    pub path: Vec<PathBuf>,
}

pub async fn launch(
    runtimes: &Runtimes,
    listing: &McpListing,
    values: &BTreeMap<String, String>,
    package: &Path,
    home: &Path,
) -> Result<Launch> {
    let parent = |p: &Path| {
        p.parent()
            .map(Path::to_path_buf)
            .into_iter()
            .collect::<Vec<_>>()
    };
    match &listing.source {
        McpSource::Npm { package: name, .. } => {
            runtimes.ensure_node().await?;
            let program = runtimes.node_exe();
            let (extra, env) = command_line(listing, values, home)?;
            let mut args = runtimes.node_flags()?;
            args.push(npm_entry(package, name)?.to_string_lossy().into_owned());
            args.extend(extra);
            let path = parent(&program);
            Ok(Launch {
                program,
                args,
                env,
                path,
            })
        }
        McpSource::Pypi { package: name, .. } => {
            let program = python_entry(&package.join("venv"), name)?;
            let (args, env) = command_line(listing, values, home)?;
            let path = parent(&program);
            Ok(Launch {
                program,
                args,
                env,
                path,
            })
        }
        McpSource::Mcpb { .. } => {
            let manifest = Manifest::read(package)?;
            let launch = manifest.launch(package, home, values)?;
            let mut args = launch.args;
            let program = match launch.command.as_str() {
                "node" => {
                    runtimes.ensure_node().await?;
                    args.splice(0..0, runtimes.node_flags()?);
                    //manifests write paths with slashes
                    if cfg!(windows) {
                        for arg in &mut args {
                            if Path::new(arg.as_str()).is_absolute() {
                                *arg = arg.replace('/', "\\");
                            }
                        }
                    }
                    runtimes.node_exe()
                }
                "python" | "python3" => runtimes.python().await?,
                other => inside(package, Path::new(other))?,
            };
            let env = launch
                .env
                .into_iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v)))
                .collect();
            let path = parent(&program);
            Ok(Launch {
                program,
                args,
                env,
                path,
            })
        }
    }
}

//registry arguments and variables with their values
fn command_line(
    listing: &McpListing,
    values: &BTreeMap<String, String>,
    home: &Path,
) -> Result<(Vec<String>, Env)> {
    let fill = |value: &Option<String>, key: &Option<String>| -> Result<Option<String>> {
        if let Some(value) = value {
            return Ok(Some(value.replace("${HOME}", &home.to_string_lossy())));
        }
        let Some(key) = key else {
            return Ok(None);
        };
        if let Some(value) = values.get(key).filter(|v| !v.is_empty()) {
            return Ok(Some(value.clone()));
        }
        let field = listing.fields.iter().find(|f| &f.key == key);
        if let Some(default) = field.and_then(|f| f.default.clone()) {
            return Ok(Some(default));
        }
        ensure!(
            !field.is_some_and(|f| f.required),
            "{} is required",
            field.map_or(key.as_str(), |f| f.label.as_str())
        );
        Ok(None)
    };
    let mut args = Vec::new();
    for argument in &listing.arguments {
        match fill(&argument.value, &argument.field)? {
            Some(value) => {
                args.extend(argument.flag.clone());
                args.push(value);
            }
            None if argument.field.is_none() => args.extend(argument.flag.clone()),
            None => {}
        }
    }
    let mut env = Vec::new();
    for variable in &listing.env {
        if let Some(value) = fill(&variable.value, &variable.field)? {
            env.push((OsString::from(&variable.name), OsString::from(value)));
        }
    }
    Ok((args, env))
}

//bundled programs never leave the package
fn inside(package: &Path, program: &Path) -> Result<PathBuf> {
    let full = if program.is_absolute() {
        program.to_path_buf()
    } else {
        package.join(program)
    };
    let real = full
        .canonicalize()
        .with_context(|| format!("the bundle has no {}", program.display()))?;
    ensure!(
        real.starts_with(package.canonicalize()?),
        "the bundle runs a program outside itself"
    );
    Ok(real)
}

//the command an npm package declares
fn npm_entry(package: &Path, name: &str) -> Result<PathBuf> {
    let root = package.join("node_modules").join(name);
    let manifest: Value = serde_json::from_slice(&std::fs::read(root.join("package.json"))?)?;
    let short = name.rsplit('/').next().unwrap_or(name);
    let bin = match &manifest["bin"] {
        Value::String(bin) => bin.clone(),
        Value::Object(bins) => bins
            .get(short)
            .or_else(|| bins.values().next())
            .and_then(Value::as_str)
            .context("invalid bin entry")?
            .to_string(),
        _ => bail!("{name} declares no command"),
    };
    let bin = Path::new(&bin);
    ensure!(
        bin.components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir)),
        "{name} points its command outside the package"
    );
    //one separator style, scripts compare it to their own url
    Ok(bin.components().fold(root, |path, part| path.join(part)))
}

//the console script a python package declares
fn python_entry(venv: &Path, name: &str) -> Result<PathBuf> {
    let normalize = |s: &str| s.to_ascii_lowercase().replace(['-', '.'], "_");
    let wanted = normalize(name);
    let site = site_packages(venv).context("the venv has no site-packages")?;
    let mut scripts = Vec::new();
    for entry in std::fs::read_dir(&site)?.flatten() {
        let file = entry.file_name().to_string_lossy().into_owned();
        let Some(dist) = file.strip_suffix(".dist-info") else {
            continue;
        };
        let project = dist.rsplit_once('-').map_or(dist, |(p, _)| p);
        if normalize(project) != wanted {
            continue;
        }
        let points =
            std::fs::read_to_string(entry.path().join("entry_points.txt")).unwrap_or_default();
        let mut in_scripts = false;
        for line in points.lines().map(str::trim) {
            if line.starts_with('[') {
                in_scripts = line == "[console_scripts]";
            } else if in_scripts && let Some((script, _)) = line.split_once('=') {
                scripts.push(script.trim().to_string());
            }
        }
    }
    let script = scripts
        .iter()
        .find(|s| normalize(s) == wanted)
        .or(scripts.first())
        .with_context(|| format!("{name} declares no command"))?;
    Ok(if cfg!(windows) {
        venv.join("Scripts").join(format!("{script}.exe"))
    } else {
        venv.join("bin").join(script)
    })
}

fn site_packages(venv: &Path) -> Option<PathBuf> {
    if cfg!(windows) {
        return Some(venv.join("Lib").join("site-packages"));
    }
    std::fs::read_dir(venv.join("lib"))
        .ok()?
        .flatten()
        .map(|e| e.path().join("site-packages"))
        .find(|p| p.is_dir())
}

//installers run no package code, so outside the sandbox
async fn run_install(
    program: &Path,
    args: &[OsString],
    extra: Vec<(OsString, OsString)>,
    home: &Path,
    path: &[PathBuf],
) -> Result<()> {
    let mut env = scrubbed_env(home, path)?;
    env.extend(extra);
    let mut command = Command::new(program);
    command
        .args(args)
        .env_clear()
        .envs(env)
        .current_dir(home)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let output = command.output().await.context("cannot run the installer")?;
    if !output.status.success() {
        let errors = String::from_utf8_lossy(&output.stderr);
        let last: Vec<&str> = errors.lines().rev().take(8).collect();
        bail!(
            "install failed: {}",
            last.into_iter().rev().collect::<Vec<_>>().join("\n")
        );
    }
    Ok(())
}

//exact versions only, ranges change by themselves
pub fn check_source(source: &McpSource) -> Result<()> {
    let version_ok = |v: &str| {
        v.starts_with(|c: char| c.is_ascii_digit())
            && v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".+-!_".contains(&b))
    };
    match source {
        McpSource::Mcpb { url, sha256 } => {
            let url = reqwest::Url::parse(url).context("invalid bundle url")?;
            let local = matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"));
            ensure!(
                url.scheme() == "https" || (url.scheme() == "http" && local),
                "bundles download over https"
            );
            ensure!(
                sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid bundle checksum"
            );
        }
        McpSource::Npm { package, version } => {
            let part_ok = |p: &str| {
                !p.is_empty()
                    && !p.starts_with('.')
                    && p.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || b"-._~".contains(&b)
                    })
            };
            let name_ok = match package.strip_prefix('@') {
                Some(scoped) => scoped
                    .split_once('/')
                    .is_some_and(|(s, n)| part_ok(s) && part_ok(n)),
                None => part_ok(package),
            };
            ensure!(
                name_ok && package.len() <= 214,
                "invalid npm package {package}"
            );
            ensure!(version_ok(version), "{version} is not an exact version");
        }
        McpSource::Pypi { package, version } => {
            let name_ok = !package.is_empty()
                && package
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b))
                && package.starts_with(|c: char| c.is_ascii_alphanumeric());
            ensure!(name_ok, "invalid python package {package}");
            ensure!(version_ok(version), "{version} is not an exact version");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn npm(package: &str, version: &str) -> McpSource {
        McpSource::Npm {
            package: package.into(),
            version: version.into(),
        }
    }

    #[test]
    fn sources_need_exact_versions() {
        assert!(check_source(&npm("@modelcontextprotocol/server-everything", "1.0.0")).is_ok());
        assert!(check_source(&npm("left-pad", "^1.0.0")).is_err());
        assert!(check_source(&npm("left-pad", "latest")).is_err());
        assert!(check_source(&npm("../evil", "1.0.0")).is_err());
        assert!(check_source(&npm("@scope", "1.0.0")).is_err());
        let pypi = |package: &str, version: &str| McpSource::Pypi {
            package: package.into(),
            version: version.into(),
        };
        assert!(check_source(&pypi("mcp-server-time", "2026.10.10")).is_ok());
        assert!(check_source(&pypi("mcp-server-time", ">=1")).is_err());
        assert!(check_source(&pypi("x; rm -rf", "1")).is_err());
        let bundle = |url: &str| McpSource::Mcpb {
            url: url.into(),
            sha256: "a".repeat(64),
        };
        assert!(check_source(&bundle("https://example.com/x.mcpb")).is_ok());
        assert!(check_source(&bundle("http://127.0.0.1:8080/x.mcpb")).is_ok());
        assert!(check_source(&bundle("http://example.com/x.mcpb")).is_err());
    }
}
