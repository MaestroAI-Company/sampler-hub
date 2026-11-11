//llama.cpp on the hub, one llama-server in router mode
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use sampler_hub_proto::{
    CatalogModel, HubSettings, HubUsage, InstalledModel, LlamaStatus, ModelDownload, ModelFile,
    ModelOptions, ModelRepo, ModelsRequest, ModelsResponse, SCOPE_ADMIN, read_frame, write_frame,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::protocols::{Access, Peer, Protocol};

pub const PROVIDER_ID: &str = "llamacpp";
const RELEASES_API: &str = "https://api.github.com/repos/ggml-org/llama.cpp/releases";
const HF: &str = "https://huggingface.co";
//one model in memory, the router swaps on demand
const MODELS_MAX: &str = "1";
//companion files are not chat models
const COMPANIONS: [&str; 5] = ["mmproj-", "mtp-", "dflash-", "eagle3-", "draft-"];
//models needing mmproj, no reasoning
const MULTIMODAL_REPOS: [&str; 2] = ["LiquidAI/LFM3-Audio-2B-GGUF", "LiquidAI/LFM3-Omni-2B-GGUF"];
//first build with realtime
const REALTIME_BUILD: u64 = 11960;
const SEARCH_LIMIT: &str = "20";
const START_TIMEOUT: Duration = Duration::from_secs(60);
const START_POLL: Duration = Duration::from_millis(300);
//spare github calls between restarts
const RESOLVE_TTL: Duration = Duration::from_secs(30 * 60);

//official builds from the model makers, smallest first
const CATALOG: [(&str, &str, &str, u64); 14] = [
    (
        "ggml-org/Qwen3.5-0.8B-GGUF",
        "Qwen3.5-0.8B-Q4_0.gguf",
        "Qwen3.5 0.8B",
        563_036_064,
    ),
    (
        "LiquidAI/LFM2.5-1.2B-Instruct-GGUF",
        "LFM2.5-1.2B-Instruct-Q4_K_M.gguf",
        "LFM2.5 1.2B",
        730_895_168,
    ),
    //sizes include the mmproj companion
    (
        "LiquidAI/LFM3-Audio-2B-GGUF",
        "LFM3-Audio-2B-Q4_K_M.gguf",
        "LFM3 Audio 2B",
        1_272_044_064,
    ),
    (
        "LiquidAI/LFM3-Omni-2B-GGUF",
        "LFM3-Omni-2B-Q4_K_M.gguf",
        "LFM3 Omni 2B",
        1_391_216_800,
    ),
    (
        "mistralai/Ministral-3-3B-Instruct-2512-GGUF",
        "Ministral-3-3B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 3B",
        2_147_023_008,
    ),
    (
        "ggml-org/gemma-4-E2B-it-GGUF",
        "gemma-4-E2B-it-Q4_0.gguf",
        "Gemma 4 E2B",
        2_840_000_000,
    ),
    (
        "ggml-org/gemma-4-E4B-it-GGUF",
        "gemma-4-E4B-it-Q4_0.gguf",
        "Gemma 4 E4B",
        4_590_000_000,
    ),
    (
        "mistralai/Ministral-3-8B-Instruct-2512-GGUF",
        "Ministral-3-8B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 8B",
        5_198_911_904,
    ),
    (
        "ibm-granite/granite-4.2-8b-GGUF",
        "granite-4.2-8b-Q4_K_M.gguf",
        "Granite 4.2 8B",
        5_347_917_952,
    ),
    (
        "ggml-org/gemma-4-12B-it-GGUF",
        "gemma-4-12B-it-Q4_0.gguf",
        "Gemma 4 12B",
        7_220_000_000,
    ),
    (
        "mistralai/Ministral-3-14B-Instruct-2512-GGUF",
        "Ministral-3-14B-Instruct-2512-Q4_K_M.gguf",
        "Ministral 3 14B",
        8_239_593_024,
    ),
    (
        "ggml-org/gemma-4-26B-A4B-it-GGUF",
        "gemma-4-26B-A4B-it-Q4_0.gguf",
        "Gemma 4 26B A4B",
        14_620_000_000,
    ),
    (
        "ggml-org/Qwen3.8-27B-GGUF",
        "Qwen3.8-27B-Q4_K_M.gguf",
        "Qwen3.8 27B",
        18_970_000_000,
    ),
    (
        "ggml-org/Qwen3.6-35B-A3B-GGUF",
        "Qwen3.6-35B-A3B-Q4_K_M.gguf",
        "Qwen3.6 35B A3B",
        20_420_000_000,
    ),
];

pub fn catalog() -> Vec<CatalogModel> {
    CATALOG
        .iter()
        .map(|(repo, file, name, size)| CatalogModel {
            repo: repo.to_string(),
            file: file.to_string(),
            name: name.to_string(),
            size: *size,
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    Installing,
    Running,
    Failed,
}

impl State {
    fn as_str(self) -> &'static str {
        match self {
            State::Idle => "idle",
            State::Installing => "installing",
            State::Running => "running",
            State::Failed => "failed",
        }
    }
}

struct Download {
    repo: String,
    received: u64,
    total: u64,
    error: Option<String>,
    task: Option<JoinHandle<()>>,
}

struct Inner {
    state: State,
    backend: Option<String>,
    build: Option<String>,
    latest: Option<String>,
    message: Option<String>,
    port: Option<u16>,
    child: Option<Child>,
    downloads: HashMap<String, Download>,
    resolved: Option<Resolved>,
}

//build tag with its check time
struct Resolved {
    at: Instant,
    nightly: bool,
    tag: String,
}

//one downloaded llama-server build
struct Build {
    exe: PathBuf,
    tag: String,
}

pub struct Llama {
    dir: PathBuf,
    http: reqwest::Client,
    inner: Mutex<Inner>,
    //one restart at a time
    restarting: tokio::sync::Mutex<()>,
    //the hub wide models setting
    enabled: AtomicBool,
    //release pointer or newest nightly
    nightly: AtomicBool,
    //zero means none
    attached: AtomicU16,
    usage: crate::usage::Usage,
    options: Mutex<ModelOptions>,
}

impl std::fmt::Debug for Llama {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Llama")
    }
}

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct Asset {
    name: String,
    browser_download_url: String,
    //sha256:hex from github
    digest: Option<String>,
}

#[derive(Deserialize)]
struct HfModel {
    id: String,
    #[serde(default)]
    downloads: u64,
}

#[derive(Deserialize)]
struct HfEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    #[serde(default)]
    size: u64,
}

//a build of llama-server for one accelerator
struct Flavor {
    backend: &'static str,
    archive: &'static str,
    //cuda builds ship the runtime apart
    runtime: Option<&'static str>,
}

impl Llama {
    pub fn new(data_dir: &Path, http: reqwest::Client) -> Arc<Self> {
        Arc::new(Self {
            dir: data_dir.join("llama"),
            http,
            inner: Mutex::new(Inner {
                state: State::Idle,
                backend: None,
                build: None,
                latest: None,
                message: None,
                port: None,
                child: None,
                downloads: HashMap::new(),
                resolved: None,
            }),
            restarting: tokio::sync::Mutex::new(()),
            enabled: AtomicBool::new(true),
            nightly: AtomicBool::new(false),
            attached: AtomicU16::new(0),
            usage: Default::default(),
            options: Mutex::new(HubSettings::default().model_options()),
        })
    }

    //off stops the server until turned back on
    pub fn set_enabled(self: &Arc<Self>, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
        self.refresh();
    }

    //read at the next start
    pub fn set_options(&self, options: ModelOptions) {
        *self.options.lock().unwrap() = options;
    }

    //read at the next restart
    pub fn set_nightly(&self, on: bool) {
        self.nightly.store(on, Ordering::SeqCst);
    }

    fn models_dir(&self) -> PathBuf {
        self.dir.join("models")
    }

    //repo each downloaded file came from
    fn sources_path(&self) -> PathBuf {
        self.dir.join("sources.json")
    }

    fn sources(&self) -> HashMap<String, String> {
        std::fs::read(self.sources_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn set_source(&self, file: &str, repo: Option<&str>) -> Result<()> {
        //the inner lock serializes writers
        let _inner = self.inner.lock().unwrap();
        let mut sources = self.sources();
        match repo {
            Some(repo) => sources.insert(file.to_string(), repo.to_string()),
            None => sources.remove(file),
        };
        std::fs::write(self.sources_path(), serde_json::to_vec(&sources)?)?;
        Ok(())
    }

    //the openai endpoint once a model can answer
    pub fn base_url(&self) -> Option<String> {
        match self.attached.load(Ordering::SeqCst) {
            0 => {}
            port => return Some(format!("http://127.0.0.1:{port}/v1")),
        }
        let inner = self.inner.lock().unwrap();
        match (inner.state, inner.port) {
            (State::Running, Some(port)) => Some(format!("http://127.0.0.1:{port}/v1")),
            _ => None,
        }
    }

    //serve from a llama-server started elsewhere
    #[doc(hidden)]
    pub fn attach(&self, port: u16) {
        self.attached.store(port, Ordering::SeqCst);
    }

    pub fn installed(&self) -> Vec<InstalledModel> {
        let Ok(entries) = std::fs::read_dir(self.models_dir()) else {
            return Vec::new();
        };
        let mut sources = self.sources();
        let mut models: Vec<InstalledModel> = entries
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let (file, size) = if entry.path().is_dir() {
                    //router wants projector beside model
                    let file = format!("{name}.gguf");
                    let size = multimodal_size(&entry.path(), &file)?;
                    (file, size)
                } else {
                    (name, entry.metadata().ok()?.len())
                };
                let repo = sources.remove(&file);
                file.ends_with(".gguf")
                    .then_some(InstalledModel { file, size, repo })
            })
            .collect();
        models.sort_by(|a, b| a.file.cmp(&b.file));
        models
    }

    //folders needing a preset
    fn multimodal_names(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(self.models_dir()) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                multimodal_size(&entry.path(), &format!("{name}.gguf")).map(|_| name)
            })
            .collect()
    }

    //older builds lack the websocket
    pub fn supports_realtime(&self) -> bool {
        self.attached.load(Ordering::SeqCst) != 0
            || self
                .inner
                .lock()
                .unwrap()
                .build
                .as_deref()
                .and_then(build_number)
                .is_some_and(|number| number >= REALTIME_BUILD)
    }

    pub fn status(&self) -> LlamaStatus {
        let installed = self.installed();
        let nightly = self.nightly.load(Ordering::SeqCst);
        let inner = self.inner.lock().unwrap();
        let mut downloads: Vec<ModelDownload> = inner
            .downloads
            .iter()
            .map(|(file, d)| ModelDownload {
                repo: d.repo.clone(),
                file: file.clone(),
                received: d.received,
                total: d.total,
                error: d.error.clone(),
            })
            .collect();
        downloads.sort_by(|a, b| a.file.cmp(&b.file));
        let pid = inner.child.as_ref().and_then(|child| child.id());
        let status = LlamaStatus {
            state: inner.state.as_str().into(),
            backend: inner.backend.clone(),
            build: inner.build.clone(),
            latest: inner.latest.clone(),
            channel: Some(if nightly { "nightly" } else { "stable" }.into()),
            message: inner.message.clone(),
            installed,
            downloads,
            usage: None,
        };
        drop(inner);
        LlamaStatus {
            usage: pid
                .and_then(|pid| self.usage.measure(&[pid]).into_iter().next())
                .map(|load| HubUsage {
                    cpu: load.cpu,
                    memory: load.memory,
                }),
            ..status
        }
    }

    fn set_state(&self, state: State, message: Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        inner.state = state;
        inner.message = message;
    }

    //restarts the server so it sees the models on disk
    pub fn refresh(self: &Arc<Self>) {
        let llama = self.clone();
        tokio::spawn(async move {
            let _one = llama.restarting.lock().await;
            llama.stop_server();
            let enabled = llama.enabled.load(Ordering::SeqCst);
            if !enabled || llama.installed().is_empty() {
                llama.set_state(State::Idle, None);
                return;
            }
            if let Err(e) = llama.install_and_start().await {
                warn!("llama.cpp did not start: {e:#}");
                llama.set_state(State::Failed, Some(format!("{e:#}")));
            }
        });
    }

    pub fn stop_server(&self) {
        let mut inner = self.inner.lock().unwrap();
        //kill on drop ends the process
        inner.child = None;
        inner.port = None;
        if inner.state == State::Running {
            inner.state = State::Idle;
        }
    }

    async fn install_and_start(&self) -> Result<()> {
        self.set_state(State::Installing, None);
        //keep installed builds when lookup fails
        match self.resolve_tag().await {
            Ok(tag) => {
                self.inner.lock().unwrap().latest = Some(tag.clone());
                match self.start_build(&tag).await {
                    Ok(()) => {
                        self.prune(&tag);
                        Ok(())
                    }
                    Err(e) => self.start_older(e, Some(&tag)).await,
                }
            }
            Err(e) => self.start_older(e, None).await,
        }
    }

    //fall back to builds on disk
    async fn start_older(&self, failed: anyhow::Error, skipped: Option<&str>) -> Result<()> {
        for (older, backend, exe) in self.installed_builds(skipped) {
            match self.start(&exe).await {
                Ok(port) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.backend = Some(backend.clone());
                    inner.port = Some(port);
                    inner.build = Some(older.clone());
                    inner.state = State::Running;
                    inner.message = Some(match skipped {
                        Some(tag) => format!("{tag} did not start, running {older}"),
                        None => format!("build lookup failed, running {older}"),
                    });
                    info!("llama.cpp runs on {backend} at port {port} with {older}");
                    drop(inner);
                    self.prune(&older);
                    return Ok(());
                }
                Err(e) => warn!("llama.cpp {older} build failed to start: {e:#}"),
            }
        }
        Err(failed)
    }

    //start the tag on the best backend
    async fn start_build(&self, tag: &str) -> Result<()> {
        let mut last = anyhow!("no llama.cpp build for this computer");
        for flavor in &flavors() {
            let build = match self.install(flavor, tag).await {
                Ok(build) => build,
                Err(e) => {
                    warn!("llama.cpp {} build unavailable: {e:#}", flavor.backend);
                    last = e;
                    continue;
                }
            };
            //a gpu build without a working driver falls back
            match self.start(&build.exe).await {
                Ok(port) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.backend = Some(flavor.backend.into());
                    inner.port = Some(port);
                    inner.build = Some(build.tag);
                    inner.state = State::Running;
                    inner.message = None;
                    info!("llama.cpp runs on {} at port {port}", flavor.backend);
                    return Ok(());
                }
                Err(e) => {
                    warn!("llama.cpp {} build failed to start: {e:#}", flavor.backend);
                    last = e;
                }
            }
        }
        Err(last)
    }

    //resolve the tag for the channel
    async fn resolve_tag(&self) -> Result<String> {
        let nightly = self.nightly.load(Ordering::SeqCst);
        if let Some(tag) = self.cached_tag(nightly) {
            return Ok(tag);
        }
        let tag = if nightly {
            self.newest_nightly().await?
        } else {
            self.release_tag().await?
        };
        self.inner.lock().unwrap().resolved = Some(Resolved {
            at: Instant::now(),
            nightly,
            tag: tag.clone(),
        });
        Ok(tag)
    }

    fn cached_tag(&self, nightly: bool) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        let resolved = inner.resolved.as_ref()?;
        (resolved.nightly == nightly && resolved.at.elapsed() < RESOLVE_TTL)
            .then(|| resolved.tag.clone())
    }

    //the stable release names the build to use
    async fn release_tag(&self) -> Result<String> {
        let stable: Release = self.github(&format!("{RELEASES_API}/latest")).await?;
        let pointer = stable
            .assets
            .iter()
            .find(|a| a.name == "nightly-tag.txt")
            .context("the llama.cpp release names no build")?;
        let tag = String::from_utf8(self.download_verified(pointer).await?)?
            .trim()
            .to_string();
        ensure!(
            tag.bytes().all(|b| b.is_ascii_alphanumeric()),
            "unexpected llama.cpp build {tag}"
        );
        Ok(tag)
    }

    //newest nightly from the release list
    async fn newest_nightly(&self) -> Result<String> {
        let releases: Vec<Release> = self.github(&format!("{RELEASES_API}?per_page=10")).await?;
        pick_nightly(releases.iter().map(|r| r.tag_name.as_str()))
            .map(str::to_string)
            .context("github lists no llama.cpp nightly")
    }

    async fn install(&self, flavor: &Flavor, tag: &str) -> Result<Build> {
        let target = self
            .dir
            .join("server")
            .join(format!("{tag}-{}", flavor.backend));
        if let Some(exe) = find_server(&target) {
            if flavor.runtime.is_some() {
                colocate_runtime(&target, &exe)?;
            }
            return Ok(Build {
                exe,
                tag: tag.to_string(),
            });
        }

        let release: Release = self.github(&format!("{RELEASES_API}/tags/{tag}")).await?;
        let pick = |prefix: &str, suffix: &str| {
            release
                .assets
                .iter()
                .find(|a| a.name.starts_with(prefix) && a.name.ends_with(suffix))
        };
        let archive = pick("llama-", flavor.archive)
            .with_context(|| format!("{} has no {} build", release.tag_name, flavor.backend))?;
        let staging = target.with_extension("partial");
        let _ = std::fs::remove_dir_all(&staging);
        std::fs::create_dir_all(&staging)?;
        info!("installing llama.cpp {tag} for {}", flavor.backend);
        extract(
            &archive.name,
            &self.download_verified(archive).await?,
            &staging,
        )?;
        if let Some(runtime) = flavor.runtime {
            let runtime = pick("cudart-", runtime).context("the cuda runtime is missing")?;
            extract(
                &runtime.name,
                &self.download_verified(runtime).await?,
                &staging,
            )?;
        }
        let _ = std::fs::remove_dir_all(&target);
        std::fs::rename(&staging, &target)?;
        let exe = find_server(&target).context("the llama.cpp build has no llama-server")?;
        if flavor.runtime.is_some() {
            colocate_runtime(&target, &exe)?;
        }
        Ok(Build {
            exe,
            tag: tag.to_string(),
        })
    }

    //builds on disk sorted newest first
    fn installed_builds(&self, skip: Option<&str>) -> Vec<(String, String, PathBuf)> {
        let mut builds = Vec::new();
        let Ok(entries) = std::fs::read_dir(self.dir.join("server")) else {
            return builds;
        };
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Some((tag, backend)) = name.split_once('-') else {
                continue;
            };
            if Some(tag) == skip {
                continue;
            }
            if let Some(exe) = find_server(&entry.path()) {
                builds.push((tag.to_string(), backend.to_string(), exe));
            }
        }
        builds.sort_by_key(|b| std::cmp::Reverse(build_number(&b.0)));
        builds
    }

    //keep the running and previous build
    fn prune(&self, current: &str) {
        let Ok(entries) = std::fs::read_dir(self.dir.join("server")) else {
            return;
        };
        let names: Vec<String> = entries
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        for name in stale_builds(names, current) {
            let _ = std::fs::remove_dir_all(self.dir.join("server").join(name));
        }
    }

    async fn github<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        Ok(self
            .http
            .get(url)
            .header("user-agent", "sampler-hub")
            .header("accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    //github publishes a sha256 for every asset
    async fn download_verified(&self, asset: &Asset) -> Result<Vec<u8>> {
        let expected = asset
            .digest
            .as_deref()
            .and_then(|d| d.strip_prefix("sha256:"))
            .with_context(|| format!("{} has no checksum", asset.name))?
            .to_ascii_lowercase();
        let bytes = self
            .http
            .get(&asset.browser_download_url)
            .header("user-agent", "sampler-hub")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        ensure!(
            hex_sha256(&bytes) == expected,
            "checksum mismatch for {}",
            asset.name
        );
        Ok(bytes.to_vec())
    }

    async fn start(&self, exe: &Path) -> Result<u16> {
        let models = self.models_dir();
        std::fs::create_dir_all(&models)?;
        let port = free_port()?;
        let log = std::fs::File::create(self.dir.join("llama-server.log"))?;
        let presets = self.dir.join("presets.ini");
        std::fs::write(&presets, presets_ini(&self.multimodal_names()))?;
        let mut command = Command::new(exe);
        command
            .arg("--models-dir")
            .arg(&models)
            .arg("--models-preset")
            .arg(&presets)
            .args(["--host", "127.0.0.1", "--port", &port.to_string()])
            .args(["--no-webui", "--models-max", MODELS_MAX])
            .args(option_args(*self.options.lock().unwrap()))
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        let mut child = command.spawn()?;

        let url = format!("http://127.0.0.1:{port}/v1/models");
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait()? {
                bail!("llama-server exited with {status}, see llama-server.log");
            }
            let ready = self.http.get(&url).send().await;
            if ready.is_ok_and(|r| r.status().is_success()) {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "llama-server did not answer"
            );
            tokio::time::sleep(START_POLL).await;
        }
        self.inner.lock().unwrap().child = Some(child);
        Ok(port)
    }

    pub async fn search(&self, query: &str) -> Result<Vec<ModelRepo>> {
        let url = reqwest::Url::parse_with_params(
            &format!("{HF}/api/models"),
            &[
                ("search", query),
                ("filter", "gguf"),
                ("sort", "downloads"),
                ("limit", SEARCH_LIMIT),
            ],
        )?;
        let models: Vec<HfModel> = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(models
            .into_iter()
            .map(|m| ModelRepo {
                repo: m.id,
                downloads: m.downloads,
            })
            .collect())
    }

    //chat models only, split files are left out
    pub async fn files(&self, repo: &str) -> Result<Vec<ModelFile>> {
        check_repo(repo)?;
        let entries: Vec<HfEntry> = self
            .http
            .get(format!("{HF}/api/models/{repo}/tree/main"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.kind == "file" && check_file(&e.path).is_ok())
            .filter(|e| !COMPANIONS.iter().any(|p| e.path.starts_with(p)))
            //some repos suffix the vision projector
            .filter(|e| !e.path.contains("-mmproj"))
            .filter(|e| !e.path.contains("-of-0"))
            .map(|e| ModelFile {
                file: e.path,
                size: e.size,
            })
            .collect())
    }

    pub fn download(self: &Arc<Self>, repo: String, file: String) -> Result<()> {
        check_repo(&repo)?;
        check_file(&file)?;
        let mut inner = self.inner.lock().unwrap();
        if inner
            .downloads
            .get(&file)
            .is_some_and(|d| d.error.is_none())
        {
            return Ok(());
        }
        let llama = self.clone();
        let task = tokio::spawn({
            let (repo, file) = (repo.clone(), file.clone());
            async move {
                match llama.fetch(&repo, &file).await {
                    Ok(()) => {
                        llama.inner.lock().unwrap().downloads.remove(&file);
                        info!("downloaded {file}");
                        llama.refresh();
                    }
                    Err(e) => {
                        warn!("download of {file} failed: {e:#}");
                        if let Some(d) = llama.inner.lock().unwrap().downloads.get_mut(&file) {
                            d.error = Some(format!("{e:#}"));
                        }
                    }
                }
            }
        });
        inner.downloads.insert(
            file,
            Download {
                repo,
                received: 0,
                total: 0,
                error: None,
                task: Some(task),
            },
        );
        Ok(())
    }

    //resumes a partial file left by a restart
    async fn fetch(&self, repo: &str, file: &str) -> Result<()> {
        let Some(projector) = companion(repo, file) else {
            return self.fetch_into(repo, file, &self.models_dir()).await;
        };
        //router pairs projector by folder
        let dir = self.models_dir().join(file.trim_end_matches(".gguf"));
        //model last so install is complete
        self.fetch_into(repo, &projector, &dir).await?;
        self.fetch_into(repo, file, &dir).await
    }

    async fn fetch_into(&self, repo: &str, file: &str, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let part = dir.join(format!("{file}.part"));
        //progress keyed by model name
        let tracked = file.strip_prefix("mmproj-").unwrap_or(file);
        let mut have = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
        let mut request = self.http.get(format!("{HF}/{repo}/resolve/main/{file}"));
        if have > 0 {
            request = request.header("range", format!("bytes={have}-"));
        }
        let mut response = request.send().await?.error_for_status()?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            have = 0;
        }
        let total = have + response.content_length().unwrap_or(0);
        let mut out = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(have > 0)
            .truncate(have == 0)
            .open(&part)
            .await?;
        let mut received = have;
        self.progress(tracked, received, total);
        while let Some(chunk) = response.chunk().await? {
            out.write_all(&chunk).await?;
            received += chunk.len() as u64;
            self.progress(tracked, received, total);
        }
        out.flush().await?;
        drop(out);
        ensure!(total == 0 || received == total, "the download ended early");
        if !file.starts_with("mmproj-") {
            self.set_source(file, Some(repo))?;
        }
        std::fs::rename(&part, dir.join(file))?;
        Ok(())
    }

    fn progress(&self, file: &str, received: u64, total: u64) {
        if let Some(d) = self.inner.lock().unwrap().downloads.get_mut(file) {
            d.received = received;
            d.total = total;
        }
    }

    pub fn cancel(&self, file: &str) -> Result<()> {
        check_file(file)?;
        if let Some(d) = self.inner.lock().unwrap().downloads.remove(file)
            && let Some(task) = d.task
        {
            task.abort();
        }
        let _ = std::fs::remove_file(self.models_dir().join(format!("{file}.part")));
        //drop half downloaded folder
        let dir = self.models_dir().join(file.trim_end_matches(".gguf"));
        if dir.is_dir() && multimodal_size(&dir, file).is_none() {
            let _ = std::fs::remove_dir_all(dir);
        }
        Ok(())
    }

    pub fn delete(self: &Arc<Self>, file: &str) -> Result<()> {
        check_file(file)?;
        //windows keeps a loaded model locked
        self.stop_server();
        let folder = self.models_dir().join(file.trim_end_matches(".gguf"));
        if folder.is_dir() {
            std::fs::remove_dir_all(folder)
        } else {
            std::fs::remove_file(self.models_dir().join(file))
        }
        .with_context(|| format!("cannot delete {file}"))?;
        self.set_source(file, None)?;
        self.refresh();
        Ok(())
    }
}

//projector beside its model
fn companion(repo: &str, file: &str) -> Option<String> {
    MULTIMODAL_REPOS
        .contains(&repo)
        .then(|| format!("mmproj-{file}"))
}

//none unless both files exist
fn multimodal_size(dir: &Path, file: &str) -> Option<u64> {
    let model = std::fs::metadata(dir.join(file)).ok()?.len();
    let projector = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .find(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("mmproj-") && name.ends_with(".gguf")
        })?
        .metadata()
        .ok()?
        .len();
    Some(model + projector)
}

//one preset per multimodal model
fn presets_ini(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("[{name}]\nreasoning = off\n"))
        .collect::<Vec<_>>()
        .join("\n")
}

//best accelerator first, the cpu build always works
fn flavors() -> Vec<Flavor> {
    let mut list = Vec::new();
    let flavor = |backend, archive, runtime| Flavor {
        backend,
        archive,
        runtime,
    };
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => {
            if Path::new(r"C:\Windows\System32\nvcuda.dll").exists() {
                list.push(flavor(
                    "cuda",
                    "-bin-win-cuda-12.4-x64.zip",
                    Some("-bin-win-cuda-12.4-x64.zip"),
                ));
            }
            if Path::new(r"C:\Windows\System32\vulkan-1.dll").exists() {
                list.push(flavor("vulkan", "-bin-win-vulkan-x64.zip", None));
            }
            list.push(flavor("cpu", "-bin-win-cpu-x64.zip", None));
        }
        ("linux", "x86_64") => {
            if Path::new("/proc/driver/nvidia/version").exists() {
                list.push(flavor(
                    "cuda",
                    "-bin-ubuntu-cuda-12.8-x64.tar.gz",
                    Some("-bin-ubuntu-cuda-12.8-x64.tar.gz"),
                ));
            }
            if has_vulkan_linux() {
                list.push(flavor("vulkan", "-bin-ubuntu-vulkan-x64.tar.gz", None));
            }
            list.push(flavor("cpu", "-bin-ubuntu-x64.tar.gz", None));
        }
        ("linux", "aarch64") => {
            if has_vulkan_linux() {
                list.push(flavor("vulkan", "-bin-ubuntu-vulkan-arm64.tar.gz", None));
            }
            list.push(flavor("cpu", "-bin-ubuntu-arm64.tar.gz", None));
        }
        ("macos", "aarch64") => list.push(flavor("metal", "-bin-macos-arm64.tar.gz", None)),
        ("macos", "x86_64") => list.push(flavor("cpu", "-bin-macos-x64.tar.gz", None)),
        _ => {}
    }
    list
}

fn has_vulkan_linux() -> bool {
    [
        "/usr/lib64/libvulkan.so.1",
        "/usr/lib/x86_64-linux-gnu/libvulkan.so.1",
        "/usr/lib/aarch64-linux-gnu/libvulkan.so.1",
        "/usr/lib/libvulkan.so.1",
    ]
    .iter()
    .any(|path| Path::new(path).exists())
}

pub(crate) fn extract(name: &str, bytes: &[u8], into: &Path) -> Result<()> {
    if name.ends_with(".zip") {
        zip::ZipArchive::new(std::io::Cursor::new(bytes))?.extract(into)?;
    } else {
        tar::Archive::new(flate2::read::GzDecoder::new(bytes)).unpack(into)?;
    }
    Ok(())
}

//cuda runtime unpacks in its own folder
fn colocate_runtime(dir: &Path, exe: &Path) -> Result<()> {
    let Some(bin) = exe.parent() else { return Ok(()) };
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next)?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path != bin {
                    stack.push(path);
                }
            } else if path.file_name().and_then(|f| f.to_str()).is_some_and(|f| f.starts_with("libcu")) {
                let moved = bin.join(entry.file_name());
                if !moved.exists() {
                    std::fs::rename(&path, moved)?;
                }
            }
        }
    }
    Ok(())
}

//archives may nest the binaries in a folder
fn find_server(dir: &Path) -> Option<PathBuf> {
    let name = server_name();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|f| f == name) {
                return Some(path);
            }
        }
    }
    None
}

//nightly builds are named b11669
fn build_number(name: &str) -> Option<u64> {
    name.strip_prefix('b')?.parse().ok()
}

//the server binary inside a build
fn server_name() -> &'static str {
    if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    }
}

//highest nightly among release names
fn pick_nightly<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    tags.into_iter()
        .filter_map(|name| Some((build_number(name)?, name)))
        .max_by_key(|(number, _)| *number)
        .map(|(_, name)| name)
}

//stale build folders to delete
fn stale_builds(names: Vec<String>, current: &str) -> Vec<String> {
    let mut newest: Option<(u64, String)> = None;
    for name in &names {
        let Some((tag, _)) = name.split_once('-') else {
            continue;
        };
        if tag == current {
            continue;
        }
        let Some(number) = build_number(tag) else {
            continue;
        };
        if newest.as_ref().is_none_or(|(kept, _)| number > *kept) {
            newest = Some((number, tag.to_string()));
        }
    }
    let keep = newest.map(|(_, tag)| tag);
    names
        .into_iter()
        .filter(|name| {
            let tag = name.split_once('-').map_or(name.as_str(), |(tag, _)| tag);
            tag != current && keep.as_deref() != Some(tag)
        })
        .collect()
}

fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

//owner/name from hugging face, nothing else
fn check_repo(repo: &str) -> Result<()> {
    let ok_part = |p: &str| {
        !p.is_empty()
            && !p.starts_with('.')
            && p.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    };
    match repo.split_once('/') {
        Some((owner, name)) if ok_part(owner) && ok_part(name) => Ok(()),
        _ => Err(anyhow!("invalid model repository {repo}")),
    }
}

//a plain gguf file name, never a path
fn check_file(file: &str) -> Result<()> {
    let plain = !file.starts_with('.')
        && file
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b));
    if plain && file.ends_with(".gguf") {
        Ok(())
    } else {
        Err(anyhow!("invalid model file {file}"))
    }
}

//admins manage the models from their devices
#[derive(Clone)]
pub struct Models {
    pub llama: Arc<Llama>,
}

impl std::fmt::Debug for Models {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Models")
    }
}

impl Protocol for Models {
    const ACCESS: Access = Access::Scope(SCOPE_ADMIN);

    async fn serve<R, W>(&self, _peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(request).await {
                Ok(response) => response,
                Err(e) => ModelsResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Models {
    async fn handle(&self, request: ModelsRequest) -> Result<ModelsResponse> {
        let status = |llama: &Llama| ModelsResponse::Status {
            status: llama.status(),
        };
        Ok(match request {
            ModelsRequest::Catalog => ModelsResponse::Catalog { models: catalog() },
            ModelsRequest::Search { query } => ModelsResponse::Repos {
                repos: self.llama.search(&query).await?,
            },
            ModelsRequest::Files { repo } => ModelsResponse::Files {
                files: self.llama.files(&repo).await?,
            },
            ModelsRequest::Download { repo, file } => {
                self.llama.download(repo, file)?;
                status(&self.llama)
            }
            ModelsRequest::Cancel { file } => {
                self.llama.cancel(&file)?;
                status(&self.llama)
            }
            ModelsRequest::Delete { file } => {
                self.llama.delete(&file)?;
                status(&self.llama)
            }
            ModelsRequest::Status => status(&self.llama),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_stay_inside_the_models_dir() {
        assert!(check_file("Qwen3.5-0.8B-Q4_0.gguf").is_ok());
        assert!(check_file("../evil.gguf").is_err());
        assert!(check_file("dir/model.gguf").is_err());
        assert!(check_file("model.bin").is_err());
        assert!(check_repo("ggml-org/gemma-4-E2B-it-GGUF").is_ok());
        assert!(check_repo("ggml-org/../x").is_err());
        assert!(check_repo("noslash").is_err());
    }

    #[test]
    fn catalog_files_are_valid() {
        for model in catalog() {
            assert!(check_repo(&model.repo).is_ok(), "{}", model.repo);
            assert!(check_file(&model.file).is_ok(), "{}", model.file);
        }
    }

    #[test]
    fn nightly_picks_the_highest_build() {
        assert_eq!(pick_nightly(["v0.7.0", "b11818", "b11819"]), Some("b11819"));
        assert_eq!(pick_nightly(["v0.7.0"]), None);
        assert_eq!(build_number("b11669"), Some(11669));
        assert_eq!(build_number("v0.7.0"), None);
    }

    #[test]
    fn prune_keeps_the_running_build_and_the_one_before() {
        let names = |list: &[&str]| list.iter().map(|name| name.to_string()).collect();
        assert!(stale_builds(names(&["b11669-vulkan", "b11819-vulkan"]), "b11819").is_empty());
        assert_eq!(
            stale_builds(
                names(&["b11600-vulkan", "b11669-vulkan", "b11819-vulkan"]),
                "b11819"
            ),
            ["b11600-vulkan"]
        );
    }

    #[tokio::test]
    async fn older_builds_come_after_the_newest() {
        let dir = std::env::temp_dir().join(format!("sampler-hub-builds-{}", std::process::id()));
        let server = dir.join("llama").join("server");
        for name in ["b11669-vulkan", "b11819-vulkan", "b11600-cpu"] {
            let build = server.join(name);
            std::fs::create_dir_all(&build).unwrap();
            std::fs::write(build.join(server_name()), b"").unwrap();
        }
        let llama = Llama::new(&dir, crate::secrets::http_client().unwrap());
        let names = |skip| {
            llama
                .installed_builds(skip)
                .into_iter()
                .map(|(tag, backend, _)| format!("{tag}-{backend}"))
                .collect::<Vec<_>>()
        };
        assert_eq!(names(Some("b11819")), ["b11669-vulkan", "b11600-cpu"]);
        assert_eq!(names(None)[0], "b11819-vulkan");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lfm3_models_come_with_their_projector() {
        for model in catalog().iter().filter(|m| m.repo.contains("LFM3")) {
            assert_eq!(
                companion(&model.repo, &model.file),
                Some(format!("mmproj-{}", model.file))
            );
        }
        assert_eq!(companion("ggml-org/gemma-4-E2B-it-GGUF", "a.gguf"), None);
        assert_eq!(
            presets_ini(&["A".into(), "B".into()]),
            "[A]
reasoning = off

[B]
reasoning = off
"
        );
    }

    #[tokio::test]
    async fn installed_reads_the_multimodal_folder() {
        let dir = std::env::temp_dir().join(format!("sampler-hub-mm-{}", std::process::id()));
        let models = dir.join("llama").join("models");
        let folder = models.join("LFM3-Omni-2B-Q4_K_M");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("LFM3-Omni-2B-Q4_K_M.gguf"), [0u8; 10]).unwrap();
        let llama = Llama::new(&dir, crate::secrets::http_client().unwrap());
        //no projector means not installed
        assert!(llama.installed().is_empty());
        std::fs::write(folder.join("mmproj-LFM3-Omni-2B-Q4_K_M.gguf"), [0u8; 4]).unwrap();
        let installed = llama.installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].file, "LFM3-Omni-2B-Q4_K_M.gguf");
        assert_eq!(installed[0].size, 14);
        assert_eq!(llama.multimodal_names(), ["LFM3-Omni-2B-Q4_K_M"]);
        llama.delete("LFM3-Omni-2B-Q4_K_M.gguf").unwrap();
        assert!(!folder.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn status_names_the_channel() {
        let dir = std::env::temp_dir().join(format!("sampler-hub-status-{}", std::process::id()));
        let llama = Llama::new(&dir, crate::secrets::http_client().unwrap());
        assert_eq!(llama.status().channel.as_deref(), Some("stable"));
        llama.set_nightly(true);
        let status = llama.status();
        assert_eq!(status.channel.as_deref(), Some("nightly"));
        assert!(status.build.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

//router flags, the model processes inherit them
fn option_args(options: ModelOptions) -> Vec<String> {
    let mut args = Vec::new();
    if options.ctx_size > 0 {
        args.extend(["--ctx-size".into(), options.ctx_size.to_string()]);
    }
    if options.keep_alive > 0 {
        args.extend([
            "--sleep-idle-seconds".into(),
            options.keep_alive.to_string(),
        ]);
    }
    if options.parallel > 0 {
        args.extend(["--parallel".into(), options.parallel.to_string()]);
    }
    args
}

#[cfg(test)]
mod option_tests {
    use super::*;

    #[test]
    fn zero_options_keep_llama_defaults() {
        assert!(
            option_args(ModelOptions {
                ctx_size: 0,
                keep_alive: 0,
                parallel: 0,
            })
            .is_empty()
        );
        let args = option_args(ModelOptions {
            ctx_size: 8192,
            keep_alive: 300,
            parallel: 2,
        });
        assert_eq!(
            args,
            [
                "--ctx-size",
                "8192",
                "--sleep-idle-seconds",
                "300",
                "--parallel",
                "2"
            ]
        );
    }
}
