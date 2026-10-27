//hand checked selection and registry search
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use anyhow::Result;
use sampler_hub_proto::{McpArgument, McpEnv, McpField, McpListing, McpSource};
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::warn;

pub const OFFICIAL_REGISTRY: &str = "https://registry.modelcontextprotocol.io";
const SEARCH_LIMIT: &str = "30";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const SERVERS_REPO: &str = "https://github.com/modelcontextprotocol/servers";

//osi approved, plus public domain dedications
const OPEN_LICENSES: [&str; 22] = [
    "MIT",
    "MIT-0",
    "Apache-2.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "0BSD",
    "Unlicense",
    "MPL-2.0",
    "EPL-2.0",
    "Zlib",
    "BSL-1.0",
    "GPL-2.0-only",
    "GPL-2.0-or-later",
    "GPL-3.0-only",
    "GPL-3.0-or-later",
    "LGPL-2.1-only",
    "LGPL-2.1-or-later",
    "LGPL-3.0-only",
    "LGPL-3.0-or-later",
    "AGPL-3.0-only",
    "AGPL-3.0-or-later",
];

//license lookups cost a request each
static LICENSES: LazyLock<Mutex<HashMap<String, Option<String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn field(key: &str, label: &str, description: &str, kind: &str, required: bool) -> McpField {
    McpField {
        key: key.into(),
        label: label.into(),
        description: description.into(),
        kind: kind.into(),
        required,
        secret: false,
        default: None,
    }
}

fn from_field(flag: Option<&str>, key: &str) -> McpArgument {
    McpArgument {
        flag: flag.map(Into::into),
        value: None,
        field: Some(key.into()),
    }
}

fn reference(
    id: &str,
    title: &str,
    description: &str,
    source: McpSource,
    offline: bool,
) -> McpListing {
    let version = match &source {
        McpSource::Npm { version, .. } | McpSource::Pypi { version, .. } => version.clone(),
        McpSource::Mcpb { .. } => String::new(),
    };
    McpListing {
        id: id.into(),
        title: title.into(),
        description: description.into(),
        version,
        publisher: "Model Context Protocol".into(),
        repository: Some(SERVERS_REPO.into()),
        //the repo moves from mit to apache, checked by hand
        license: Some("MIT, Apache-2.0".into()),
        source,
        fields: Vec::new(),
        arguments: Vec::new(),
        env: Vec::new(),
        offline,
        registry: None,
    }
}

fn npm(package: &str, version: &str) -> McpSource {
    McpSource::Npm {
        package: package.into(),
        version: version.into(),
    }
}

fn pypi(package: &str, version: &str) -> McpSource {
    McpSource::Pypi {
        package: package.into(),
        version: version.into(),
    }
}

//official reference servers that run locally
pub fn selection() -> Vec<McpListing> {
    let mut filesystem = reference(
        "filesystem",
        "Filesystem",
        "Read, write and search the files of one folder.",
        npm("@modelcontextprotocol/server-filesystem", "1.0.0"),
        true,
    );
    filesystem.fields = vec![field(
        "folder",
        "Folder",
        "The only folder the server may use.",
        "folder",
        true,
    )];
    filesystem.arguments = vec![from_field(None, "folder")];

    let mut memory = reference(
        "memory",
        "Memory",
        "A knowledge graph that remembers facts across conversations.",
        npm("@modelcontextprotocol/server-memory", "1.0.0"),
        true,
    );
    //its default file sits in the read only package
    memory.env = vec![McpEnv {
        name: "MEMORY_FILE_PATH".into(),
        value: Some("${HOME}/memory.jsonl".into()),
        field: None,
    }];

    let mut time = reference(
        "time",
        "Time",
        "Current time and conversions between time zones.",
        pypi("mcp-server-time", "2026.10.10"),
        true,
    );
    time.fields = vec![field(
        "timezone",
        "Time zone",
        "An IANA name such as Europe/Paris, the system zone otherwise.",
        "text",
        false,
    )];
    time.arguments = vec![from_field(Some("--local-timezone"), "timezone")];

    let thinking = reference(
        "sequential-thinking",
        "Sequential thinking",
        "Breaks a hard problem into revisable steps.",
        npm("@modelcontextprotocol/server-sequential-thinking", "1.0.0"),
        true,
    );

    let fetch = reference(
        "fetch",
        "Fetch",
        "Downloads web pages and turns them into Markdown.",
        pypi("mcp-server-fetch", "2026.10.10"),
        false,
    );

    vec![filesystem, memory, time, thinking, fetch]
}

//local, open source servers only
pub async fn search(
    http: &reqwest::Client,
    registries: &[String],
    query: &str,
) -> Result<Vec<McpListing>> {
    let mut found = Vec::new();
    for registry in registries {
        let url = reqwest::Url::parse_with_params(
            &format!("{}/v0/servers", registry.trim_end_matches('/')),
            &[
                ("search", query),
                ("version", "latest"),
                ("limit", SEARCH_LIMIT),
            ],
        )?;
        let page: Value = match get_json(http, url.as_str()).await {
            Ok(page) => page,
            Err(e) => {
                warn!("registry {registry} unavailable: {e:#}");
                continue;
            }
        };
        let entries = page["servers"].as_array().cloned().unwrap_or_default();
        found.extend(entries.iter().filter_map(|e| from_registry(e, registry)));
    }
    Ok(keep_open(http, found).await)
}

//the newest version of an installed server
pub async fn latest(
    http: &reqwest::Client,
    registry: &str,
    id: &str,
) -> Result<Option<McpListing>> {
    let listings = search(http, &[registry.to_string()], id).await?;
    Ok(listings.into_iter().find(|l| l.id == id))
}

async fn keep_open(http: &reqwest::Client, listings: Vec<McpListing>) -> Vec<McpListing> {
    let mut checks = JoinSet::new();
    for (index, listing) in listings.iter().enumerate() {
        let http = http.clone();
        let listing = listing.clone();
        checks.spawn(async move { (index, license(&http, &listing).await) });
    }
    let mut licenses = vec![None; listings.len()];
    while let Some(Ok((index, license))) = checks.join_next().await {
        licenses[index] = license;
    }
    listings
        .into_iter()
        .zip(licenses)
        .filter_map(|(mut listing, license)| {
            listing.license = Some(license?);
            Some(listing)
        })
        .collect()
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value> {
    Ok(http
        .get(url)
        .header("user-agent", "sampler-hub")
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

//mcpb first, it needs no install step
pub fn from_registry(entry: &Value, registry: &str) -> Option<McpListing> {
    let server = &entry["server"];
    let id = server["name"].as_str()?.to_string();
    let repository = server["repository"]["url"]
        .as_str()
        .filter(|u| !u.is_empty())?
        .to_string();
    let packages = server["packages"].as_array()?;
    let stdio = |p: &&Value| p["transport"]["type"].as_str().is_none_or(|t| t == "stdio");
    let package = ["mcpb", "npm", "pypi"].iter().find_map(|kind| {
        packages
            .iter()
            .filter(stdio)
            .find(|p| p["registryType"] == *kind)
    })?;
    let version = package["version"]
        .as_str()
        .or(server["version"].as_str())?
        .to_string();
    let identifier = package["identifier"].as_str()?.to_string();
    let source = match package["registryType"].as_str()? {
        "mcpb" => McpSource::Mcpb {
            url: identifier,
            sha256: package["fileSha256"].as_str()?.to_string(),
        },
        "npm" => McpSource::Npm {
            package: identifier,
            version: version.clone(),
        },
        _ => McpSource::Pypi {
            package: identifier,
            version: version.clone(),
        },
    };

    let mut fields = Vec::new();
    let mut env = Vec::new();
    for variable in package["environmentVariables"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let name = variable["name"].as_str()?.to_string();
        match variable["value"].as_str() {
            //templated values are not supported
            Some(value) if value.contains('{') => return None,
            Some(value) => env.push(McpEnv {
                name,
                value: Some(value.into()),
                field: None,
            }),
            None => {
                fields.push(registry_field(variable, &name, &name));
                env.push(McpEnv {
                    name: name.clone(),
                    value: None,
                    field: Some(name),
                });
            }
        }
    }
    let mut arguments = Vec::new();
    for (index, argument) in package["packageArguments"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let flag = (argument["type"] == "named")
            .then(|| argument["name"].as_str().map(String::from))
            .flatten();
        match argument["value"].as_str() {
            Some(value) if value.contains('{') => return None,
            Some(value) => arguments.push(McpArgument {
                flag,
                value: Some(value.into()),
                field: None,
            }),
            None => {
                let key = flag
                    .as_deref()
                    .map(|f| f.trim_start_matches('-').to_string())
                    .unwrap_or_else(|| format!("arg{index}"));
                let label = argument["valueHint"]
                    .as_str()
                    .or(flag.as_deref())
                    .unwrap_or(&key)
                    .to_string();
                fields.push(registry_field(argument, &key, &label));
                arguments.push(McpArgument {
                    flag,
                    value: None,
                    field: Some(key),
                });
            }
        }
    }

    let publisher = repository
        .strip_prefix("https://github.com/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or_else(|| id.split('/').next().unwrap_or(&id))
        .to_string();
    Some(McpListing {
        title: server["title"]
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| id.rsplit('/').next().unwrap_or(&id).to_string()),
        description: server["description"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        id,
        version,
        publisher,
        repository: Some(repository),
        license: None,
        source,
        fields,
        arguments,
        env,
        offline: false,
        registry: Some(registry.to_string()),
    })
}

fn registry_field(spec: &Value, key: &str, label: &str) -> McpField {
    McpField {
        key: key.into(),
        label: label.into(),
        description: spec["description"].as_str().unwrap_or_default().to_string(),
        kind: if spec["format"] == "filepath" {
            "folder"
        } else {
            "text"
        }
        .into(),
        required: spec["isRequired"].as_bool().unwrap_or(false),
        secret: spec["isSecret"].as_bool().unwrap_or(false),
        default: spec["default"].as_str().map(String::from),
    }
}

//package metadata first, the repository otherwise
async fn license(http: &reqwest::Client, listing: &McpListing) -> Option<String> {
    let key = format!("{:?}", listing.source);
    if let Some(known) = LICENSES.lock().unwrap().get(&key) {
        return known.clone();
    }
    let declared = match &listing.source {
        McpSource::Npm { package, version } => {
            let url = format!(
                "https://registry.npmjs.org/{}/{version}",
                package.replace('/', "%2f")
            );
            get_json(http, &url)
                .await
                .ok()
                .and_then(|p| match &p["license"] {
                    Value::String(s) => Some(s.clone()),
                    other => other["type"].as_str().map(String::from),
                })
        }
        McpSource::Pypi { package, version } => {
            let url = format!("https://pypi.org/pypi/{package}/{version}/json");
            get_json(http, &url)
                .await
                .ok()
                .and_then(|p| pypi_license(&p["info"]))
        }
        McpSource::Mcpb { .. } => None,
    };
    let license = match declared.filter(|l| is_open(l)) {
        Some(license) => Some(license),
        None => match listing.repository.as_deref() {
            Some(repo) => github_license(http, repo).await.filter(|l| is_open(l)),
            None => None,
        },
    };
    LICENSES.lock().unwrap().insert(key, license.clone());
    license
}

fn pypi_license(info: &Value) -> Option<String> {
    if let Some(expression) = info["license_expression"]
        .as_str()
        .filter(|e| !e.is_empty())
    {
        return Some(expression.into());
    }
    let classifiers = info["classifiers"].as_array()?;
    classifiers
        .iter()
        .filter_map(Value::as_str)
        .find_map(|c| c.strip_prefix("License :: OSI Approved :: "))
        .map(|name| spdx_of(name).unwrap_or(name).to_string())
}

//classifier names of the common licenses
fn spdx_of(classifier: &str) -> Option<&'static str> {
    Some(match classifier {
        "MIT License" => "MIT",
        "Apache Software License" => "Apache-2.0",
        "BSD License" => "BSD-3-Clause",
        "ISC License (ISCL)" => "ISC",
        "Mozilla Public License 2.0 (MPL 2.0)" => "MPL-2.0",
        "GNU General Public License v3 (GPLv3)" => "GPL-3.0-only",
        "GNU Affero General Public License v3" => "AGPL-3.0-only",
        "GNU Lesser General Public License v3 (LGPLv3)" => "LGPL-3.0-only",
        _ => return None,
    })
}

async fn github_license(http: &reqwest::Client, repository: &str) -> Option<String> {
    let path = repository
        .strip_prefix("https://github.com/")?
        .trim_end_matches(".git")
        .split('/')
        .take(2)
        .collect::<Vec<_>>()
        .join("/");
    let answer = get_json(
        http,
        &format!("https://api.github.com/repos/{path}/license"),
    )
    .await
    .ok()?;
    answer["license"]["spdx_id"].as_str().map(String::from)
}

//every part of an and, one part of an or
pub fn is_open(expression: &str) -> bool {
    let cleaned = expression.replace(['(', ')'], " ");
    let open = |part: &str| {
        OPEN_LICENSES
            .iter()
            .any(|l| l.eq_ignore_ascii_case(part.trim()))
    };
    if cleaned.contains(" AND ") {
        cleaned.split(" AND ").all(open)
    } else {
        cleaned.split(" OR ").any(open)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn licenses_must_be_open() {
        assert!(is_open("MIT"));
        assert!(is_open("(MIT OR Apache-2.0)"));
        assert!(is_open("MIT AND Apache-2.0"));
        assert!(!is_open("MIT AND LicenseRef-Proprietary"));
        assert!(!is_open("SEE LICENSE IN LICENSE"));
        assert!(!is_open("NOASSERTION"));
        assert!(!is_open("UNLICENSED"));
    }

    fn entry(packages: Value) -> Value {
        json!({ "server": {
            "name": "io.github.someone/notes",
            "description": "Notes",
            "version": "1.2.0",
            "repository": { "url": "https://github.com/someone/notes", "source": "github" },
            "packages": packages,
        }})
    }

    #[test]
    fn remote_and_unsourced_servers_are_skipped() {
        let remote = json!({ "server": { "name": "x/y", "repository": { "url": "https://github.com/x/y" },
            "remotes": [{ "type": "streamable-http", "url": "https://x.example/mcp" }] } });
        assert!(from_registry(&remote, OFFICIAL_REGISTRY).is_none());
        let mut unsourced =
            entry(json!([{ "registryType": "npm", "identifier": "notes", "version": "1.2.0" }]));
        unsourced["server"]["repository"] = json!({ "url": "" });
        assert!(from_registry(&unsourced, OFFICIAL_REGISTRY).is_none());
        let docker = entry(json!([{ "registryType": "oci", "identifier": "notes:1" }]));
        assert!(from_registry(&docker, OFFICIAL_REGISTRY).is_none());
    }

    #[test]
    fn registry_entries_become_forms() {
        let listing = from_registry(
            &entry(json!([
                { "registryType": "npm", "identifier": "notes", "version": "1.2.0",
                  "transport": { "type": "stdio" },
                  "environmentVariables": [
                    { "name": "NOTES_TOKEN", "isRequired": true, "isSecret": true, "description": "Token" },
                    { "name": "NOTES_MODE", "value": "local" }
                  ],
                  "packageArguments": [
                    { "type": "positional", "value": "serve" },
                    { "type": "named", "name": "--root", "format": "filepath", "isRequired": true }
                  ] },
                { "registryType": "mcpb", "identifier": "https://x.example/notes.mcpb", "fileSha256": "ab",
                  "transport": { "type": "stdio" } }
            ])),
            OFFICIAL_REGISTRY,
        )
        .unwrap();
        assert!(
            matches!(listing.source, McpSource::Mcpb { .. }),
            "mcpb wins"
        );
        assert_eq!(listing.publisher, "someone");

        let listing = from_registry(
            &entry(
                json!([{ "registryType": "npm", "identifier": "notes", "version": "1.2.0",
                "environmentVariables": [
                    { "name": "NOTES_TOKEN", "isRequired": true, "isSecret": true },
                    { "name": "NOTES_MODE", "value": "local" }
                ],
                "packageArguments": [
                    { "type": "positional", "value": "serve" },
                    { "type": "named", "name": "--root", "format": "filepath", "isRequired": true }
                ] }]),
            ),
            OFFICIAL_REGISTRY,
        )
        .unwrap();
        let token = listing
            .fields
            .iter()
            .find(|f| f.key == "NOTES_TOKEN")
            .unwrap();
        assert!(token.secret && token.required);
        let root = listing.fields.iter().find(|f| f.key == "root").unwrap();
        assert_eq!(root.kind, "folder");
        assert_eq!(listing.arguments[0].value.as_deref(), Some("serve"));
        assert_eq!(listing.arguments[1].flag.as_deref(), Some("--root"));
        assert_eq!(listing.env.len(), 2);
    }
}
