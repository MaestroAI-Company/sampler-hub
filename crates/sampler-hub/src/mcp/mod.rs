//mcp servers the hub installs, runs and shares
mod bridge;
mod catalog;
mod install;
mod mcpb;
mod protocol;
mod runtime;
pub mod sandbox;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use sampler_hub_proto::{
    ForwardHead, HubUsage, McpListing, McpServer, McpServerSummary, McpSource, ProviderInfo,
    ProvidersResponse, write_frame,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use bridge::Bridge;
pub use catalog::OFFICIAL_REGISTRY;
pub use protocol::McpAdmin;
use runtime::{Runtimes, scrubbed_env};
use sandbox::Policy;

use crate::db::Db;
use crate::store::Store;

//providers ids of hosted servers
pub const PROVIDER_PREFIX: &str = "mcp-";
const SPEC_FILE: &str = "server.json";
const REGISTRIES_FILE: &str = "registries.json";
const LOG_FILE: &str = "server.log";
const MAX_LOG_BYTES: u64 = 1024 * 1024;
const TAIL_LINES: usize = 200;
const MAX_BODY_BYTES: u64 = 4 * 1024 * 1024;
//a server crashing this often stays down
const MAX_CRASHES: usize = 5;
const CRASH_WINDOW: Duration = Duration::from_secs(600);
const UPDATE_EVERY: Duration = Duration::from_secs(24 * 3600);

//what server.json keeps, secrets live in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    pub listing: McpListing,
    #[serde(default)]
    pub values: BTreeMap<String, String>,
    pub enabled: bool,
    //the profile that installed it, none for admins
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
enum State {
    Installing,
    Starting,
    Running,
    Failed(String),
    Stopped,
}

impl State {
    //wire name and failure message
    fn label(&self) -> (&'static str, Option<String>) {
        match self {
            State::Installing => ("installing", None),
            State::Starting => ("starting", None),
            State::Running => ("running", None),
            State::Failed(error) => ("failed", Some(error.clone())),
            State::Stopped => ("stopped", None),
        }
    }
}

struct Server {
    id: String,
    dir: PathBuf,
    spec: Mutex<Spec>,
    state: Mutex<State>,
    bridge: Mutex<Option<Arc<Bridge>>>,
    tail: Mutex<VecDeque<String>>,
    tools: Mutex<Vec<String>>,
    update: Mutex<Option<String>>,
    task: Mutex<Option<JoinHandle<()>>>,
    //the running process, for its usage
    pid: Mutex<Option<u32>>,
}

impl Server {
    fn stop(&self) {
        //kill on drop ends the child
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
        *self.bridge.lock().unwrap() = None;
        *self.state.lock().unwrap() = State::Stopped;
    }

    fn title(&self) -> String {
        self.spec.lock().unwrap().listing.title.clone()
    }

    fn owner(&self) -> Option<String> {
        self.spec.lock().unwrap().owner.clone()
    }
}

pub struct Mcp {
    dir: PathBuf,
    data_dir: PathBuf,
    http: reqwest::Client,
    db: Arc<Db>,
    store: Arc<Store>,
    runtimes: Arc<Runtimes>,
    servers: Mutex<HashMap<String, Arc<Server>>>,
    updates: Mutex<Option<JoinHandle<()>>>,
    usage: crate::usage::Usage,
    //the hub wide mcp servers setting
    enabled: AtomicBool,
}

impl std::fmt::Debug for Mcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Mcp")
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        self.stop_all();
    }
}

impl Mcp {
    pub fn new(
        data_dir: &Path,
        http: reqwest::Client,
        db: Arc<Db>,
        store: Arc<Store>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dir: data_dir.join("mcp"),
            data_dir: data_dir.to_path_buf(),
            runtimes: Arc::new(Runtimes::new(data_dir, http.clone())),
            http,
            db,
            store,
            servers: Mutex::new(HashMap::new()),
            updates: Mutex::new(None),
            usage: Default::default(),
            enabled: AtomicBool::new(true),
        })
    }

    //none for servers from before profiles
    pub fn owner_of(&self, id: &str) -> Result<Option<String>> {
        Ok(self.server(id)?.owner())
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    //off stops every server, on starts the enabled ones again
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
        let servers: Vec<Arc<Server>> = self.servers.lock().unwrap().values().cloned().collect();
        for server in servers {
            if !on {
                server.stop();
            } else if server.spec.lock().unwrap().enabled {
                self.start(&server);
            }
        }
    }

    //servers installed earlier start with the hub
    pub fn start_installed(self: &Arc<Self>) {
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten().filter(|e| e.path().is_dir()) {
                let id = entry.file_name().to_string_lossy().into_owned();
                if id.starts_with('.') {
                    continue;
                }
                match read_spec(&entry.path()) {
                    //its profile was removed while the hub was off
                    Ok(spec) if spec.owner.as_ref().is_some_and(|o| !self.profile_exists(o)) => {
                        warn!("mcp server {id} removed with its profile");
                        sandbox::forget(&container_name(&entry.path()));
                        let _ = self.db.drop_mcp_secrets(&id);
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                    Ok(spec) => self.add(id, spec),
                    Err(e) => warn!("mcp server {id} skipped: {e:#}"),
                }
            }
        }
        let weak = Arc::downgrade(self);
        *self.updates.lock().unwrap() = Some(tokio::spawn(watch_updates(weak)));
    }

    pub fn stop_all(&self) {
        if let Some(task) = self.updates.lock().unwrap().take() {
            task.abort();
        }
        for server in self.servers.lock().unwrap().values() {
            server.stop();
        }
    }

    pub fn isolation(&self) -> &'static str {
        match sandbox::isolation() {
            sandbox::Isolation::Full => "full",
            sandbox::Isolation::Partial => "partial",
        }
    }

    pub fn catalog(&self) -> Vec<McpListing> {
        catalog::selection()
    }

    pub async fn search(&self, query: &str) -> Result<Vec<McpListing>> {
        catalog::search(&self.http, &self.registries(), query).await
    }

    //bundles carry their form in the manifest
    pub async fn prepare(&self, mut listing: McpListing) -> Result<McpListing> {
        install::check_source(&listing.source)?;
        if let McpSource::Mcpb { url, sha256 } = &listing.source {
            let bytes = install::download_bundle(&self.http, url, sha256).await?;
            let manifest = mcpb::Manifest::from_bundle(&bytes)?;
            manifest.check_supported()?;
            listing.fields = manifest.fields();
        }
        Ok(listing)
    }

    //answers at once, the install runs behind
    pub fn install(
        self: &Arc<Self>,
        owner: &str,
        listing: McpListing,
        values: BTreeMap<String, String>,
    ) -> Result<String> {
        install::check_source(&listing.source)?;
        let owner = Some(owner.to_string());
        let (plain, secrets) = self.split(&listing, values, owner.as_deref())?;
        let id = new_id()?;
        self.db.put_mcp_secrets(&id, &secrets)?;
        let spec = Spec {
            listing,
            values: plain,
            enabled: true,
            owner,
        };
        let server = self.insert(id.clone(), spec, State::Installing);
        self.behind(server);
        Ok(id)
    }

    //installs or reinstalls, then starts
    fn behind(self: &Arc<Self>, server: Arc<Server>) {
        let mcp = self.clone();
        tokio::spawn(async move {
            if let Err(e) = mcp.finish_install(&server).await {
                warn!("mcp server {} not installed: {e:#}", server.title());
                let _ = std::fs::remove_dir_all(server.dir.join("package"));
                *server.state.lock().unwrap() = State::Failed(format!("{e:#}"));
            }
        });
    }

    async fn finish_install(&self, server: &Arc<Server>) -> Result<()> {
        let spec = server.spec.lock().unwrap().clone();
        let listing =
            install::install(&self.runtimes, &self.http, &server.dir, &spec.listing).await?;
        //a bundle may declare other fields than the device saw
        let mut values = spec.values.clone();
        values.extend(self.db.mcp_secrets(&server.id)?);
        let (plain, secrets) = self.split(&listing, values, spec.owner.as_deref())?;
        let spec = Spec {
            listing,
            values: plain,
            enabled: spec.enabled,
            owner: spec.owner,
        };
        write_spec(&server.dir, &spec)?;
        self.db.put_mcp_secrets(&server.id, &secrets)?;
        info!(
            "mcp server {} installed as {}",
            spec.listing.title, server.id
        );
        let enabled = spec.enabled;
        *server.spec.lock().unwrap() = spec;
        *server.update.lock().unwrap() = None;
        if enabled {
            self.start(server);
        } else {
            *server.state.lock().unwrap() = State::Stopped;
        }
        Ok(())
    }

    pub fn configure(&self, owner: &str, id: &str, values: BTreeMap<String, String>) -> Result<()> {
        let server = self.ready(owner, id)?;
        let mut spec = server.spec.lock().unwrap().clone();
        let mut secrets = self.db.mcp_secrets(id)?;
        let mut merged: BTreeMap<String, String> = spec.values.clone();
        merged.extend(secrets.clone());
        merged.extend(values);
        let (plain, kept) = self.split(&spec.listing, merged, spec.owner.as_deref())?;
        spec.values = plain;
        secrets = kept;
        write_spec(&server.dir, &spec)?;
        self.db.put_mcp_secrets(id, &secrets)?;
        let enabled = spec.enabled;
        *server.spec.lock().unwrap() = spec;
        if enabled {
            self.start(&server);
        }
        Ok(())
    }

    pub fn enable(&self, owner: &str, id: &str, on: bool) -> Result<()> {
        let server = self.ready(owner, id)?;
        let mut spec = server.spec.lock().unwrap().clone();
        spec.enabled = on;
        write_spec(&server.dir, &spec)?;
        *server.spec.lock().unwrap() = spec;
        if on {
            self.start(&server);
        } else {
            server.stop();
        }
        Ok(())
    }

    pub fn restart(&self, owner: &str, id: &str) -> Result<()> {
        let server = self.ready(owner, id)?;
        ensure!(
            server.spec.lock().unwrap().enabled,
            "this server is turned off"
        );
        self.start(&server);
        Ok(())
    }

    //same values, newer package, behind
    pub async fn update(self: &Arc<Self>, owner: &str, id: &str) -> Result<()> {
        let server = self.ready(owner, id)?;
        let listing = server.spec.lock().unwrap().listing.clone();
        let registry = listing
            .registry
            .clone()
            .context("this server has no registry")?;
        let latest = catalog::latest(&self.http, &registry, &listing.id)
            .await?
            .context("the registry no longer lists this server")?;
        server.stop();
        *server.state.lock().unwrap() = State::Installing;
        let _ = std::fs::remove_dir_all(server.dir.join("package"));
        server.spec.lock().unwrap().listing = latest;
        self.behind(server);
        Ok(())
    }

    pub async fn remove(&self, owner: &str, id: &str) -> Result<()> {
        self.owned(owner, id)?;
        let server = self
            .servers
            .lock()
            .unwrap()
            .remove(id)
            .ok_or_else(|| anyhow!("no mcp server {id}"))?;
        server.stop();
        sandbox::forget(&container_name(&server.dir));
        self.db.drop_mcp_secrets(id)?;
        //windows keeps files locked until the child exits
        for _ in 0..20 {
            if std::fs::remove_dir_all(&server.dir).is_ok() || !server.dir.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        std::fs::remove_dir_all(&server.dir)
            .with_context(|| format!("cannot delete {}", server.dir.display()))
    }

    //a removed profile takes its servers along
    pub async fn remove_owned(&self, profile: &str) {
        let ids: Vec<String> = self
            .servers
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.owner().as_deref() == Some(profile))
            .map(|s| s.id.clone())
            .collect();
        for id in ids {
            if let Err(e) = self.remove(profile, &id).await {
                warn!("mcp server {id} not removed: {e:#}");
            }
        }
    }

    pub fn is_admin(&self, profile: &str) -> bool {
        self.store
            .profile(profile)
            .ok()
            .flatten()
            .is_some_and(|p| p.role == sampler_hub_proto::ProfileRole::Admin)
    }

    fn profile_exists(&self, profile: &str) -> bool {
        self.store.profile(profile).ok().flatten().is_some()
    }

    //servers with the load of their process tree
    fn measured(&self, keep: impl Fn(&Server) -> bool) -> Vec<(Arc<Server>, Option<HubUsage>)> {
        let servers: Vec<Arc<Server>> = self
            .servers
            .lock()
            .unwrap()
            .values()
            .filter(|s| keep(s))
            .cloned()
            .collect();
        let pids: Vec<Option<u32>> = servers.iter().map(|s| *s.pid.lock().unwrap()).collect();
        let roots: Vec<u32> = pids.iter().flatten().copied().collect();
        let mut loads = self.usage.measure(&roots).into_iter();
        servers
            .into_iter()
            .zip(pids)
            .map(|(server, pid)| {
                let usage = pid.and_then(|_| loads.next()).map(|load| HubUsage {
                    cpu: load.cpu,
                    memory: load.memory,
                });
                (server, usage)
            })
            .collect()
    }

    pub fn status(&self, owner: &str) -> Result<Vec<McpServer>> {
        let mut list = Vec::new();
        for (server, usage) in self.measured(|s| self.may_use(owner, s)) {
            let spec = server.spec.lock().unwrap().clone();
            let (state, message) = server.state.lock().unwrap().label();
            list.push(McpServer {
                id: server.id.clone(),
                listing: spec.listing,
                enabled: spec.enabled,
                state: state.into(),
                message,
                values: spec.values,
                secrets_set: self.db.mcp_secrets(&server.id)?.into_keys().collect(),
                tools: server.tools.lock().unwrap().clone(),
                update: server.update.lock().unwrap().clone(),
                usage,
            });
        }
        list.sort_by(|a, b| a.listing.title.cmp(&b.listing.title));
        Ok(list)
    }

    //every profile's servers without their values
    pub fn overview(&self) -> Vec<McpServerSummary> {
        let mut list: Vec<McpServerSummary> = self
            .measured(|_| true)
            .into_iter()
            .map(|(server, usage)| {
                let spec = server.spec.lock().unwrap().clone();
                McpServerSummary {
                    id: server.id.clone(),
                    owner: spec.owner,
                    title: spec.listing.title,
                    repository: spec.listing.repository,
                    enabled: spec.enabled,
                    state: server.state.lock().unwrap().label().0.into(),
                    tools: server.tools.lock().unwrap().len() as u32,
                    usage,
                }
            })
            .collect();
        list.sort_by(|a, b| a.title.cmp(&b.title));
        list
    }

    pub fn logs(&self, owner: &str, id: &str) -> Result<Vec<String>> {
        Ok(self
            .owned(owner, id)?
            .tail
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect())
    }

    //admins browse their home, members their own folder
    pub fn folders(
        &self,
        owner: &str,
        path: Option<&str>,
    ) -> Result<(String, Option<String>, Vec<String>)> {
        let owner = Some(owner);
        let home = self.folder_root(owner)?;
        let path = match path {
            Some(path) => self.check_folder(Path::new(path), owner)?,
            None => home.clone(),
        };
        let mut folders: Vec<String> = std::fs::read_dir(&path)?
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with('.'))
            .filter(|name| self.check_folder(&path.join(name), owner).is_ok())
            .collect();
        folders.sort_by_key(|f| f.to_lowercase());
        let parent = (path != home)
            .then(|| path.parent().map(|p| p.to_string_lossy().into_owned()))
            .flatten();
        Ok((path.to_string_lossy().into_owned(), parent, folders))
    }

    pub fn registries(&self) -> Vec<String> {
        std::fs::read(self.dir.join(REGISTRIES_FILE))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_else(|| vec![OFFICIAL_REGISTRY.to_string()])
    }

    pub fn set_registries(&self, urls: Vec<String>) -> Result<()> {
        for url in &urls {
            let parsed =
                reqwest::Url::parse(url).with_context(|| format!("invalid registry {url}"))?;
            ensure!(parsed.scheme() == "https", "registries use https");
        }
        std::fs::create_dir_all(&self.dir)?;
        std::fs::write(
            self.dir.join(REGISTRIES_FILE),
            serde_json::to_vec_pretty(&urls)?,
        )?;
        Ok(())
    }

    //running servers of this profile
    pub fn providers(&self, owner: &str) -> Vec<ProviderInfo> {
        let servers = self.servers.lock().unwrap();
        let mut list: Vec<ProviderInfo> = servers
            .values()
            .filter(|s| self.may_use(owner, s) && *s.state.lock().unwrap() == State::Running)
            .map(|s| ProviderInfo {
                id: format!("{PROVIDER_PREFIX}{}", s.id),
                name: s.title(),
                kind: "mcp".into(),
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    //plain json answers, no session or stream
    pub async fn forward<R, W>(
        &self,
        owner: &str,
        id: &str,
        head: &ForwardHead,
        reader: R,
        writer: &mut W,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let reply = |status: u16, body: Option<Value>| (status, body);
        let (status, body) = if head.method.eq_ignore_ascii_case("DELETE") {
            reply(200, None)
        } else if !head.method.eq_ignore_ascii_case("POST") {
            reply(405, None)
        } else {
            let mut raw = Vec::new();
            reader.take(MAX_BODY_BYTES).read_to_end(&mut raw).await?;
            //another profile's server looks absent
            let bridge = self
                .owned(owner, id)
                .ok()
                .and_then(|s| s.bridge.lock().unwrap().clone());
            match (serde_json::from_slice::<Value>(&raw), bridge) {
                (Err(_), _) => reply(400, Some(rpc_error(Value::Null, -32700, "invalid json"))),
                (Ok(message), None) => reply(
                    503,
                    Some(rpc_error(
                        message.get("id").cloned().unwrap_or(Value::Null),
                        -32603,
                        "this mcp server is not running",
                    )),
                ),
                (Ok(message), Some(bridge)) => match bridge.handle(message).await {
                    Some(answer) => reply(200, Some(answer)),
                    None => reply(202, None),
                },
            }
        };
        let headers = if body.is_some() {
            vec![("content-type".to_string(), "application/json".to_string())]
        } else {
            Vec::new()
        };
        write_frame(writer, &ProvidersResponse::Response { status, headers }).await?;
        if let Some(body) = body {
            writer.write_all(body.to_string().as_bytes()).await?;
        }
        writer.flush().await?;
        Ok(())
    }

    fn server(&self, id: &str) -> Result<Arc<Server>> {
        self.servers
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("no mcp server {id}"))
    }

    //servers from before profiles belong to admins
    fn may_use(&self, profile: &str, server: &Server) -> bool {
        match server.owner() {
            Some(owner) => owner == profile,
            None => self.is_admin(profile),
        }
    }

    fn owned(&self, profile: &str, id: &str) -> Result<Arc<Server>> {
        let server = self.server(id)?;
        ensure!(self.may_use(profile, &server), "no mcp server {id}");
        Ok(server)
    }

    fn add(&self, id: String, spec: Spec) {
        let enabled = spec.enabled;
        let server = self.insert(id, spec, State::Stopped);
        if enabled {
            self.start(&server);
        }
    }

    fn insert(&self, id: String, spec: Spec, state: State) -> Arc<Server> {
        let server = Arc::new(Server {
            dir: self.dir.join(&id),
            id: id.clone(),
            spec: Mutex::new(spec),
            state: Mutex::new(state),
            bridge: Mutex::new(None),
            tail: Mutex::new(VecDeque::new()),
            tools: Mutex::new(Vec::new()),
            update: Mutex::new(None),
            task: Mutex::new(None),
            pid: Mutex::new(None),
        });
        self.servers.lock().unwrap().insert(id, server.clone());
        server
    }

    //a server busy installing waits
    fn ready(&self, owner: &str, id: &str) -> Result<Arc<Server>> {
        let server = self.owned(owner, id)?;
        ensure!(
            *server.state.lock().unwrap() != State::Installing,
            "this server is still installing"
        );
        Ok(server)
    }

    fn start(&self, server: &Arc<Server>) {
        server.stop();
        if !self.enabled() {
            return;
        }
        let task = tokio::spawn(supervise(
            self.runtimes.clone(),
            self.db.clone(),
            server.clone(),
        ));
        *server.task.lock().unwrap() = Some(task);
    }

    //splits values by field, checks folders and keys
    fn split(
        &self,
        listing: &McpListing,
        values: BTreeMap<String, String>,
        owner: Option<&str>,
    ) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>)> {
        let mut plain = BTreeMap::new();
        let mut secrets = BTreeMap::new();
        for (key, value) in values {
            let field = listing
                .fields
                .iter()
                .find(|f| f.key == key)
                .with_context(|| format!("unknown setting {key}"))?;
            if value.is_empty() {
                continue;
            }
            if matches!(field.kind.as_str(), "folder" | "file") {
                self.check_folder(Path::new(&value), owner)?;
            }
            if field.secret {
                secrets.insert(key, value);
            } else {
                plain.insert(key, value);
            }
        }
        for field in listing
            .fields
            .iter()
            .filter(|f| f.required && f.default.is_none())
        {
            ensure!(
                plain.contains_key(&field.key) || secrets.contains_key(&field.key),
                "{} is required",
                field.label
            );
        }
        Ok((plain, secrets))
    }

    //members never reach the host disk
    fn folder_root(&self, owner: Option<&str>) -> Result<PathBuf> {
        match owner {
            Some(owner) if !self.is_admin(owner) => {
                let root = self.data_dir.join("profiles").join(owner).join("files");
                std::fs::create_dir_all(&root)?;
                Ok(root)
            }
            _ => home_dir(),
        }
    }

    fn check_folder(&self, path: &Path, owner: Option<&str>) -> Result<PathBuf> {
        let real = path
            .canonicalize()
            .with_context(|| format!("{} does not exist", path.display()))?;
        if let Some(owner) = owner.filter(|o| !self.is_admin(o)) {
            let root = self.folder_root(Some(owner))?.canonicalize()?;
            ensure!(
                real.starts_with(&root),
                "only your own folder can be shared"
            );
            return Ok(path.to_path_buf());
        }
        let home = home_dir()?.canonicalize()?;
        ensure!(
            real.starts_with(&home),
            "only folders of your home can be shared"
        );
        if let Ok(data) = self.data_dir.canonicalize() {
            ensure!(!real.starts_with(&data), "the hub data cannot be shared");
            ensure!(!data.starts_with(&real), "this folder holds the hub data");
        }
        Ok(path.to_path_buf())
    }
}

fn home_dir() -> Result<PathBuf> {
    directories::BaseDirs::new()
        .map(|d| d.home_dir().to_path_buf())
        .context("no home folder")
}

//one app container per server folder
fn container_name(dir: &Path) -> String {
    let id = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!("SamplerHub.mcp.{id}")
}

fn read_spec(dir: &Path) -> Result<Spec> {
    Ok(serde_json::from_slice(&std::fs::read(
        dir.join(SPEC_FILE),
    )?)?)
}

fn write_spec(dir: &Path, spec: &Spec) -> Result<()> {
    std::fs::write(dir.join(SPEC_FILE), serde_json::to_vec_pretty(spec)?)?;
    Ok(())
}

fn new_id() -> Result<String> {
    let mut bytes = [0u8; 6];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("no system randomness: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

//daily, only reported until an admin accepts
async fn watch_updates(mcp: Weak<Mcp>) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let Some(hub) = mcp.upgrade() else {
            return;
        };
        let servers: Vec<Arc<Server>> = hub.servers.lock().unwrap().values().cloned().collect();
        for server in servers {
            let listing = server.spec.lock().unwrap().listing.clone();
            let Some(registry) = listing.registry.as_deref() else {
                continue;
            };
            match catalog::latest(&hub.http, registry, &listing.id).await {
                Ok(Some(latest)) if latest.version != listing.version => {
                    *server.update.lock().unwrap() = Some(latest.version);
                }
                Ok(_) => {}
                Err(e) => warn!("no update check for {}: {e:#}", listing.title),
            }
        }
        drop(hub);
        tokio::time::sleep(UPDATE_EVERY).await;
    }
}

//restarts crashed servers until they crash too often
async fn supervise(runtimes: Arc<Runtimes>, db: Arc<Db>, server: Arc<Server>) {
    let mut crashes: VecDeque<Instant> = VecDeque::new();
    loop {
        *server.state.lock().unwrap() = State::Starting;
        let error = match run_once(&runtimes, &db, &server).await {
            Ok(Some(code)) => format!("exited with code {code}"),
            Ok(None) => "killed".to_string(),
            Err(e) => format!("{e:#}"),
        };
        *server.bridge.lock().unwrap() = None;
        warn!("mcp server {} stopped: {error}", server.title());
        let now = Instant::now();
        crashes.push_back(now);
        crashes.retain(|t| now.duration_since(*t) < CRASH_WINDOW);
        *server.state.lock().unwrap() = State::Failed(error);
        if crashes.len() >= MAX_CRASHES {
            return;
        }
        let backoff = Duration::from_secs(1 << crashes.len().min(5));
        tokio::time::sleep(backoff).await;
    }
}

async fn run_once(runtimes: &Runtimes, db: &Db, server: &Arc<Server>) -> Result<Option<i32>> {
    let package = server.dir.join("package");
    let home = server.dir.join("home");
    let spec = server.spec.lock().unwrap().clone();
    let mut values = spec.values.clone();
    values.extend(db.mcp_secrets(&server.id)?);
    let launch = install::launch(runtimes, &spec.listing, &values, &package, &home).await?;
    let mut env = scrubbed_env(&home, &launch.path)?;
    env.extend(launch.env);
    let args: Vec<OsString> = launch.args.into_iter().map(OsString::from).collect();
    //chosen folders, as paths and as roots
    let folders: Vec<PathBuf> = spec
        .listing
        .fields
        .iter()
        .filter(|f| matches!(f.kind.as_str(), "folder" | "file"))
        .filter_map(|f| values.get(&f.key))
        .map(PathBuf::from)
        .collect();
    let roots = folders
        .iter()
        .filter_map(|f| reqwest::Url::from_file_path(f).ok())
        .map(|uri| json!({ "uri": uri.as_str() }))
        .collect();
    //runs only its package, writes only its home and folders
    let mut write = vec![home.clone()];
    write.extend(folders);
    let policy = Policy {
        name: container_name(&server.dir),
        shared: vec![runtimes.dir().to_path_buf()],
        read: vec![package],
        write,
    };
    let mut child = sandbox::spawn(&launch.program, &args, &env, &home, &policy)
        .context("cannot start the server")?;
    *server.pid.lock().unwrap() = child.id();

    let stderr = child.stderr.take().context("no stderr")?;
    tokio::spawn(keep_log(stderr, server.clone()));
    let stdout = child.stdout.take().context("no stdout")?;
    let stdin = child.stdin.take().context("no stdin")?;
    let bridge = Bridge::connect(stdout, stdin, roots).await?;
    let listed = bridge
        .handle(json!({ "jsonrpc": "2.0", "id": "tools", "method": "tools/list" }))
        .await;
    *server.tools.lock().unwrap() = listed
        .and_then(|answer| answer["result"]["tools"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(String::from))
        .collect();
    *server.bridge.lock().unwrap() = Some(bridge);
    *server.state.lock().unwrap() = State::Running;
    info!("mcp server {} running", spec.listing.title);
    let status = child.wait().await;
    *server.pid.lock().unwrap() = None;
    status
}

//stderr goes to the log and the tail
async fn keep_log(stderr: sandbox::Reader, server: Arc<Server>) {
    let file = server.dir.join(LOG_FILE);
    if std::fs::metadata(&file).is_ok_and(|m| m.len() > MAX_LOG_BYTES) {
        let _ = std::fs::rename(&file, file.with_extension("log.old"));
    }
    let mut log = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file)
        .await
        .ok();
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(log) = log.as_mut() {
            let _ = log.write_all(format!("{line}\n").as_bytes()).await;
        }
        let mut tail = server.tail.lock().unwrap();
        if tail.len() == TAIL_LINES {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

#[cfg(test)]
mod tests {
    use sampler_hub_proto::McpField;

    use super::*;

    fn listing(fields: Vec<McpField>) -> McpListing {
        let mut listing = catalog::selection().remove(0);
        listing.fields = fields;
        listing
    }

    fn field(key: &str, kind: &str, required: bool, secret: bool) -> McpField {
        McpField {
            key: key.into(),
            label: key.into(),
            description: String::new(),
            kind: kind.into(),
            required,
            secret,
            default: None,
        }
    }

    #[tokio::test]
    async fn settings_are_checked_and_secrets_split() {
        let data = std::env::temp_dir().join(format!("sampler-hub-mcp-split-{}", new_id().unwrap()));
        std::fs::create_dir_all(&data).unwrap();
        let db = Arc::new(Db::open_with_key(&data.join("hub.db"), &[7u8; 32]).unwrap());
        let store = Arc::new(Store::new(&data));
        let owner = store.create_profile("owner").unwrap();
        let member = store.create_profile("member").unwrap();
        let mcp = Mcp::new(&data, crate::secrets::http_client().unwrap(), db, store);
        let admin = Some(owner.id.as_str());
        let listing = listing(vec![
            field("token", "text", true, true),
            field("mode", "text", false, false),
            field("folder", "folder", false, false),
        ]);
        let values = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>()
        };

        assert!(
            mcp.split(&listing, values(&[("mode", "x")]), admin)
                .is_err(),
            "required"
        );
        assert!(
            mcp.split(&listing, values(&[("token", "t"), ("other", "x")]), admin)
                .is_err(),
            "unknown"
        );
        let outside = data.join("data-folder");
        std::fs::create_dir_all(&outside).unwrap();
        let hub_data = values(&[("token", "t"), ("folder", &outside.to_string_lossy())]);
        assert!(mcp.split(&listing, hub_data, admin).is_err(), "hub data");
        let (plain, secrets) = mcp
            .split(&listing, values(&[("token", "t"), ("mode", "m")]), admin)
            .unwrap();
        assert_eq!(plain, values(&[("mode", "m")]));
        assert_eq!(secrets, values(&[("token", "t")]));

        //a member only shares its own folder
        let (root, _, _) = mcp.folders(&member.id, None).unwrap();
        let member = Some(member.id.as_str());
        let mine = Path::new(&root).join("notes");
        std::fs::create_dir_all(&mine).unwrap();
        let own = values(&[("token", "t"), ("folder", &mine.to_string_lossy())]);
        assert!(mcp.split(&listing, own, member).is_ok());
        let home = home_dir().unwrap();
        let host = values(&[("token", "t"), ("folder", &home.to_string_lossy())]);
        assert!(mcp.split(&listing, host, member).is_err(), "host disk");
        let _ = std::fs::remove_dir_all(&data);
    }
}
