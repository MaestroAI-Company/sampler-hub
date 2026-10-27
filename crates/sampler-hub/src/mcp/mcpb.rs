//mcp bundles, a zip with the server and its dependencies
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail, ensure};
use sampler_hub_proto::McpField;
use serde::Deserialize;
use serde_json::Value;

const MANIFEST: &str = "manifest.json";
//a bundle stays a reasonable size once unpacked
const MAX_UNPACKED_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub server: Server,
    #[serde(default)]
    pub user_config: BTreeMap<String, UserConfig>,
    pub compatibility: Option<Compatibility>,
}

#[derive(Debug, Deserialize)]
pub struct Server {
    #[serde(rename = "type")]
    pub kind: String,
    pub mcp_config: McpConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub platform_overrides: BTreeMap<String, Override>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Override {
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub env: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub sensitive: bool,
    pub default: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct Compatibility {
    pub platforms: Option<Vec<String>>,
}

//what the hub runs once values are filled in
#[derive(Debug, PartialEq)]
pub struct Launch {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

impl Manifest {
    pub fn read(dir: &Path) -> Result<Self> {
        let raw = std::fs::read(dir.join(MANIFEST)).context("the bundle has no manifest.json")?;
        serde_json::from_slice(&raw).context("invalid manifest.json")
    }

    //the manifest inside a bundle, nothing unpacked
    pub fn from_bundle(bytes: &[u8]) -> Result<Self> {
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
        let mut raw = Vec::new();
        archive
            .by_name(MANIFEST)
            .context("the bundle has no manifest.json")?
            .read_to_end(&mut raw)?;
        serde_json::from_slice(&raw).context("invalid manifest.json")
    }

    //user_config as form fields
    pub fn fields(&self) -> Vec<McpField> {
        self.user_config
            .iter()
            .map(|(key, field)| McpField {
                key: key.clone(),
                label: field.title.clone().unwrap_or_else(|| key.clone()),
                description: field.description.clone().unwrap_or_default(),
                kind: match field.kind.as_str() {
                    "directory" => "folder",
                    "file" => "file",
                    "number" => "number",
                    "boolean" => "boolean",
                    _ => "text",
                }
                .into(),
                required: field.required,
                secret: field.sensitive,
                default: match &field.default {
                    Some(Value::String(s)) => Some(s.clone()),
                    Some(Value::Null) | None => None,
                    Some(other) => Some(other.to_string()),
                },
            })
            .collect()
    }

    pub fn check_supported(&self) -> Result<()> {
        //uv bundles install their dependencies at start
        ensure!(
            matches!(self.server.kind.as_str(), "node" | "python" | "binary"),
            "{} bundles are not supported yet",
            self.server.kind
        );
        let Some(platforms) = self
            .compatibility
            .as_ref()
            .and_then(|c| c.platforms.as_ref())
        else {
            return Ok(());
        };
        ensure!(
            platforms.iter().any(|p| p == platform()),
            "this bundle does not run on {}",
            platform()
        );
        Ok(())
    }

    //fills ${...} placeholders, overrides first
    pub fn launch(
        &self,
        dir: &Path,
        home: &Path,
        config: &BTreeMap<String, String>,
    ) -> Result<Launch> {
        let base = &self.server.mcp_config;
        let over = base.platform_overrides.get(platform());
        let command = over
            .and_then(|o| o.command.clone())
            .unwrap_or_else(|| base.command.clone());
        let args = over
            .and_then(|o| o.args.clone())
            .unwrap_or_else(|| base.args.clone());
        let mut env = base.env.clone();
        if let Some(extra) = over.and_then(|o| o.env.clone()) {
            env.extend(extra);
        }

        for (key, field) in &self.user_config {
            ensure!(
                !field.required || config.contains_key(key) || field.default.is_some(),
                "{} is required",
                field.title.as_deref().unwrap_or(key)
            );
        }
        let fill = |text: &str| self.substitute(text, dir, home, config);
        Ok(Launch {
            command: fill(&command)?,
            args: args.iter().map(|a| fill(a)).collect::<Result<_>>()?,
            env: env
                .into_iter()
                .map(|(k, v)| Ok((k, fill(&v)?)))
                .collect::<Result<_>>()?,
        })
    }

    fn substitute(
        &self,
        text: &str,
        dir: &Path,
        home: &Path,
        config: &BTreeMap<String, String>,
    ) -> Result<String> {
        let mut out = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            let end = rest[start..]
                .find('}')
                .ok_or_else(|| anyhow!("unclosed placeholder in {text}"))?;
            let name = &rest[start + 2..start + end];
            let value = match name {
                "__dirname" => dir.to_string_lossy().into_owned(),
                "HOME" => home.to_string_lossy().into_owned(),
                "pathSeparator" | "/" => std::path::MAIN_SEPARATOR.to_string(),
                _ => match name.strip_prefix("user_config.") {
                    Some(key) => self.config_value(key, config)?,
                    None => bail!("unknown placeholder ${{{name}}}"),
                },
            };
            out.push_str(&value);
            rest = &rest[start + end + 1..];
        }
        out.push_str(rest);
        Ok(out)
    }

    fn config_value(&self, key: &str, config: &BTreeMap<String, String>) -> Result<String> {
        if let Some(value) = config.get(key) {
            return Ok(value.clone());
        }
        let field = self
            .user_config
            .get(key)
            .ok_or_else(|| anyhow!("the manifest declares no {key}"))?;
        Ok(match &field.default {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        })
    }
}

//platform names the manifest uses
fn platform() -> &'static str {
    match std::env::consts::OS {
        "windows" => "win32",
        "macos" => "darwin",
        other => other,
    }
}

//every entry must land inside the folder
pub fn unpack(bytes: &[u8], into: &Path) -> Result<()> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
    let mut total = 0u64;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let relative: PathBuf = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("unsafe path {:?} in the bundle", entry.name()))?;
        let path = into.join(relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&path)?;
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut data = Vec::new();
        (&mut entry)
            .take(MAX_UNPACKED_BYTES - total + 1)
            .read_to_end(&mut data)?;
        total += data.len() as u64;
        ensure!(total <= MAX_UNPACKED_BYTES, "the bundle is too large");
        std::fs::write(&path, data)?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode & 0o755))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn zip_of(files: &[(&str, &str)]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        for (name, body) in files {
            writer
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(body.as_bytes()).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn temp_dir() -> PathBuf {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).unwrap();
        let name: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        std::env::temp_dir().join(format!("sampler-hub-mcpb-{name}"))
    }

    #[test]
    fn escaping_paths_are_refused() {
        let dir = temp_dir();
        let bundle = zip_of(&[("../evil.txt", "x")]);
        assert!(unpack(&bundle, &dir.join("package")).is_err());
        assert!(!dir.join("evil.txt").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_fills_placeholders() {
        let dir = temp_dir();
        let manifest = r#"{
            "manifest_version": "0.3",
            "name": "notes",
            "version": "1.2.0",
            "server": {
                "type": "node",
                "entry_point": "server/index.js",
                "mcp_config": {
                    "command": "node",
                    "args": ["${__dirname}/server/index.js", "--root", "${user_config.root}"],
                    "env": { "NOTES_TOKEN": "${user_config.token}", "LEVEL": "${user_config.level}" }
                }
            },
            "user_config": {
                "root": { "type": "directory", "title": "Notes folder", "required": true },
                "token": { "type": "string", "sensitive": true },
                "level": { "type": "number", "default": 2 }
            },
            "compatibility": { "platforms": ["darwin", "win32", "linux"] }
        }"#;
        unpack(
            &zip_of(&[(MANIFEST, manifest), ("server/index.js", "")]),
            &dir,
        )
        .unwrap();
        let manifest = Manifest::read(&dir).unwrap();
        manifest.check_supported().unwrap();
        let fields = manifest.fields();
        assert_eq!(
            fields.iter().find(|f| f.key == "root").unwrap().kind,
            "folder"
        );
        assert!(fields.iter().find(|f| f.key == "token").unwrap().secret);
        assert_eq!(
            fields
                .iter()
                .find(|f| f.key == "level")
                .unwrap()
                .default
                .as_deref(),
            Some("2")
        );

        let home = dir.join("home");
        assert!(manifest.launch(&dir, &home, &BTreeMap::new()).is_err());
        let config = BTreeMap::from([("root".to_string(), "/data/notes".to_string())]);
        let launch = manifest.launch(&dir, &home, &config).unwrap();
        assert_eq!(launch.command, "node");
        assert_eq!(
            launch.args,
            [
                format!("{}/server/index.js", dir.display()),
                "--root".into(),
                "/data/notes".into()
            ]
        );
        assert_eq!(launch.env["NOTES_TOKEN"], "");
        assert_eq!(launch.env["LEVEL"], "2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn other_platforms_are_refused() {
        let manifest: Manifest = serde_json::from_str(
            r#"{"name":"x","version":"1","server":{"type":"binary","mcp_config":{"command":"x"}},
            "compatibility":{"platforms":["plan9"]}}"#,
        )
        .unwrap();
        assert!(manifest.check_supported().is_err());
    }
}
