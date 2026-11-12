use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, SecretKey};
use iroh_tickets::endpoint::EndpointTicket;
use sampler_hub_proto::{
    ADMIN_ALPN, HUB_ALPN, LOGIN_ALPN, MCP_ALPN, MODELS_ALPN, PAIR_ALPN, PING_ALPN, PROFILES_ALPN,
    PROVIDERS_ALPN, PairingLink, SECRETS_ALPN, SYNC_ALPN, UNAUTHORIZED, hub_addr,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::{self, Config};
use crate::db::Db;
use crate::lan;
use crate::llama::{Llama, Models};
use crate::mcp::{Mcp, McpAdmin};
use crate::profiles::{Links, Login, Profiles};
use crate::protocols::{Admin, Pair, Ping, Protocol, Session, admit, still_allowed};
use crate::providers::{Providers, Target, default_targets};
use crate::secrets::{Secrets, http_client};
use crate::settings::Settings;
use crate::store::Store;
use crate::sync::DataSync;
use crate::update;

async fn bind(config: &Config, key: Option<SecretKey>, port: Option<u16>) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0).relay_mode(config.relay_mode());
    if let Some(key) = key {
        builder = builder.secret_key(key);
    }
    if let Some(port) = port {
        builder = builder
            .bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))?
            .bind_addr(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)))?;
    }
    Ok(builder.bind().await?)
}

pub async fn run(data_dir: &Path, shutdown: impl Future<Output = ()>) -> Result<()> {
    run_with_targets(data_dir, shutdown, default_targets()).await
}

//tests point the relay at fake providers
pub async fn run_with_targets(
    data_dir: &Path,
    shutdown: impl Future<Output = ()>,
    targets: Vec<Target>,
) -> Result<()> {
    run_with_llama_port(data_dir, shutdown, targets, None).await
}

//tests attach a fake llama-server
#[doc(hidden)]
pub async fn run_with_llama_port(
    data_dir: &Path,
    shutdown: impl Future<Output = ()>,
    targets: Vec<Target>,
    llama_port: Option<u16>,
) -> Result<()> {
    let running = start(data_dir, targets, llama_port).await?;
    shutdown.await;
    running.stop().await
}

struct Running {
    router: Router,
    llama: Arc<Llama>,
    mcp: Arc<Mcp>,
    lan: tokio::task::JoinHandle<()>,
    updates: tokio::task::JoinHandle<()>,
    features: tokio::task::JoinHandle<()>,
    _lock: std::fs::File,
}

impl Running {
    async fn stop(self) -> Result<()> {
        info!("hub stopping");
        self.llama.stop_server();
        self.mcp.stop_all();
        self.lan.abort();
        self.updates.abort();
        self.features.abort();
        self.router.shutdown().await?;
        Ok(())
    }
}

async fn start(data_dir: &Path, targets: Vec<Target>, llama_port: Option<u16>) -> Result<Running> {
    //held until the hub stops
    let lock = lock(data_dir)?;
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let endpoint = bind(&config, Some(key), config.port).await?;
    info!(
        "hub {} started, data in {}",
        endpoint.id(),
        data_dir.display()
    );

    //relays reconnect if offline at boot
    tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            endpoint.online().await;
            info!("hub reachable");
        }
    });

    let settings = Arc::new(Settings::load(data_dir)?);
    let lan = tokio::spawn(follow_lan_setting(
        settings.subscribe(),
        endpoint.id().to_string(),
        config.hub_name(),
        endpoint.bound_sockets(),
    ));
    let store = Arc::new(Store::new(data_dir));
    //devices paired before a scope changed get it
    store.refresh_scopes()?;
    let db = Arc::new(Db::open(data_dir)?);
    let http = http_client()?;
    let (sender, latest) = tokio::sync::watch::channel(None);
    let updates = tokio::spawn(update::watch_releases(http.clone(), sender));
    let llama = Llama::new(data_dir, http.clone());
    if let Some(port) = llama_port {
        llama.attach(port);
    }
    let mcp = Mcp::new(data_dir, http.clone(), db.clone(), store.clone());
    //an admin may have turned either off
    mcp.set_enabled(settings.get().mcp_servers);
    mcp.start_installed();
    let features = tokio::spawn(follow_features(
        settings.subscribe(),
        llama.clone(),
        mcp.clone(),
    ));
    let name: Arc<str> = config.hub_name().into();
    let links = Arc::new(Links::default());
    //one lockout for both password paths
    let profiles = Profiles {
        store: store.clone(),
        db: db.clone(),
        data_dir: data_dir.to_path_buf(),
        links: links.clone(),
        tries: Default::default(),
    };
    let router = Router::builder(endpoint)
        .accept(PING_ALPN, OverIroh::new(Ping, &store))
        .accept(
            PAIR_ALPN,
            OverIroh::new(
                Pair {
                    store: store.clone(),
                    name: name.clone(),
                    links,
                },
                &store,
            ),
        )
        .accept(
            HUB_ALPN,
            OverIroh::new(
                Session {
                    store: store.clone(),
                    name: name.clone(),
                    latest: latest.clone(),
                    usage: Default::default(),
                    settings: settings.clone(),
                },
                &store,
            ),
        )
        .accept(
            ADMIN_ALPN,
            OverIroh::new(
                Admin {
                    store: store.clone(),
                    db: db.clone(),
                    mcp: mcp.clone(),
                    data_dir: data_dir.to_path_buf(),
                    invites: Default::default(),
                    settings: settings.clone(),
                    http: http.clone(),
                    latest,
                },
                &store,
            ),
        )
        .accept(PROFILES_ALPN, OverIroh::new(profiles.clone(), &store))
        .accept(
            LOGIN_ALPN,
            OverIroh::new(
                Login {
                    profiles,
                    settings: settings.clone(),
                    name,
                },
                &store,
            ),
        )
        .accept(
            MODELS_ALPN,
            OverIroh::new(
                Models {
                    llama: llama.clone(),
                },
                &store,
            ),
        )
        .accept(
            MCP_ALPN,
            OverIroh::new(McpAdmin { mcp: mcp.clone() }, &store),
        )
        .accept(
            SYNC_ALPN,
            OverIroh::new(DataSync { db: db.clone() }, &store),
        )
        .accept(
            SECRETS_ALPN,
            OverIroh::new(
                Secrets {
                    db: db.clone(),
                    leases: Default::default(),
                },
                &store,
            ),
        )
        .accept(
            PROVIDERS_ALPN,
            OverIroh::new(
                Providers {
                    http,
                    targets: Arc::new(targets),
                    settings,
                    llama: llama.clone(),
                    mcp: mcp.clone(),
                },
                &store,
            ),
        )
        .spawn();
    Ok(Running {
        router,
        llama,
        mcp,
        lan,
        updates,
        features,
        _lock: lock,
    })
}

//models and mcp servers follow their hub setting
async fn follow_features(
    mut settings: tokio::sync::watch::Receiver<sampler_hub_proto::HubSettings>,
    llama: Arc<Llama>,
    mcp: Arc<Mcp>,
) {
    let mut last: Option<sampler_hub_proto::HubSettings> = None;
    loop {
        let current = *settings.borrow_and_update();
        llama.set_options(current.model_options());
        llama.set_nightly(current.models_nightly);
        update::set_automatic(current.update_checks);
        //models already on disk start serving, new options need a restart
        if last.is_none_or(|last| {
            last.models != current.models
                || last.model_options() != current.model_options()
                || last.models_nightly != current.models_nightly
        }) {
            llama.set_enabled(current.models);
        }
        if last.is_some_and(|last| last.mcp_servers != current.mcp_servers) {
            mcp.set_enabled(current.mcp_servers);
        }
        last = Some(current);
        if settings.changed().await.is_err() {
            return;
        }
    }
}

//announces while the setting allows it
async fn follow_lan_setting(
    mut settings: tokio::sync::watch::Receiver<sampler_hub_proto::HubSettings>,
    id: String,
    name: String,
    sockets: Vec<SocketAddr>,
) {
    let Some(port) = sockets.iter().find(|s| s.is_ipv4()).map(|s| s.port()) else {
        return;
    };
    loop {
        let announcement = if settings.borrow_and_update().lan_discovery {
            lan::announce(&id, &name, port)
                .inspect_err(|e| warn!("not announced on the local network: {e:#}"))
                .ok()
        } else {
            None
        };
        if settings.changed().await.is_err() {
            return;
        }
        drop(announcement);
    }
}

const LOCK_FILE: &str = "hub.lock";
const LOG_FILE: &str = "hub.log";
//older lines move to hub.log.old
const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024;

//two hubs on one data dir would fork the database
fn lock(data_dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("cannot create {}", data_dir.display()))?;
    let file = std::fs::File::create(data_dir.join(LOCK_FILE))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => {
            bail!("another hub already runs on {}", data_dir.display())
        }
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

//true while a hub holds the data dir
pub fn is_running(data_dir: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new()
        .write(true)
        .open(data_dir.join(LOCK_FILE))
    else {
        return false;
    };
    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
}

//clients find addresses through lookup
pub fn ticket(data_dir: &Path) -> Result<EndpointTicket> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    Ok(EndpointTicket::new(hub_addr(key.public(), &config.relays)))
}

pub fn pairing_link(data_dir: &Path) -> Result<PairingLink> {
    pairing_link_for(data_dir, None)
}

//a link pairing binds once a device approves it
pub fn pairing_link_for(data_dir: &Path, link: Option<&str>) -> Result<PairingLink> {
    let config = Config::load(data_dir)?;
    let key = config::load_or_create_key(data_dir)?;
    let secret = Store::new(data_dir).start_pairing_for(link)?;
    Ok(PairingLink {
        hub: key.public(),
        relays: config.relays,
        secret,
    })
}

pub async fn ping(data_dir: &Path, ticket: EndpointTicket, count: u32) -> Result<()> {
    let config = Config::load(data_dir)?;
    let endpoint = bind(&config, None, None).await?;
    let started = Instant::now();
    let conn = endpoint
        .connect(ticket.endpoint_addr().clone(), PING_ALPN)
        .await?;
    println!("connected in {:?}", started.elapsed());

    let (mut send, mut recv) = conn.open_bi().await?;
    for i in 1..=count {
        let started = Instant::now();
        send.write_all(b"ping").await?;
        let mut echo = [0u8; 4];
        recv.read_exact(&mut echo).await?;
        ensure!(&echo == b"ping", "hub answered with unexpected bytes");
        println!("{i}: {:?} via {}", started.elapsed(), path_kind(&conn));
        if i < count {
            //leaves time for hole punching
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    send.finish()?;
    conn.close(0u32.into(), b"bye");
    endpoint.close().await;
    Ok(())
}

fn path_kind(conn: &Connection) -> &'static str {
    match conn.paths().iter().find(|path| path.is_selected()) {
        Some(path) if path.is_ip() => "direct",
        Some(path) if path.is_relay() => "relay",
        _ => "unknown",
    }
}

fn user_error(e: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(e.into())
}

//each bi stream is one protocol session
#[derive(Debug, Clone)]
struct OverIroh<P> {
    protocol: P,
    store: Arc<Store>,
}

impl<P> OverIroh<P> {
    fn new(protocol: P, store: &Arc<Store>) -> Self {
        Self {
            protocol,
            store: store.clone(),
        }
    }
}

impl<P: Protocol + std::fmt::Debug> ProtocolHandler for OverIroh<P> {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let id = conn.remote_id().to_string();
        if admit(&self.store, &id, P::ACCESS)
            .map_err(user_error)?
            .is_none()
        {
            conn.close(UNAUTHORIZED.into(), b"unauthorized");
            return Ok(());
        }
        //ends when the client closes the connection
        while let Ok((mut send, recv)) = conn.accept_bi().await {
            let Some(peer) = still_allowed(&self.store, &id, P::ACCESS).map_err(user_error)? else {
                conn.close(UNAUTHORIZED.into(), b"removed");
                return Ok(());
            };
            let protocol = self.protocol.clone();
            tokio::spawn(async move {
                match protocol.serve(peer, recv, &mut send).await {
                    Ok(()) => {
                        let _ = send.finish();
                    }
                    Err(e) => warn!("stream failed: {e:#}"),
                }
            });
        }
        Ok(())
    }
}

//windowless runs have no console
pub fn init_logging(data_dir: &Path, to_file: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,iroh=warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if to_file {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(LOG_FILE);
        if std::fs::metadata(&path).is_ok_and(|m| m.len() > MAX_LOG_BYTES) {
            std::fs::rename(&path, path.with_extension("log.old"))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        builder
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(file))
            .init();
    } else {
        builder.with_writer(std::io::stderr).init();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_hub_per_data_dir() {
        let dir = std::env::temp_dir().join(format!("sampler-hub-lock-{}", std::process::id()));
        assert!(!is_running(&dir));
        let held = lock(&dir).unwrap();
        assert!(is_running(&dir));
        assert!(lock(&dir).is_err());
        drop(held);
        assert!(!is_running(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
