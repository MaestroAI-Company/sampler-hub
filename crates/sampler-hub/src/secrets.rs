use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use sampler_hub_proto::{
    SCOPE_SECRETS, SECRET_COLLECTIONS, SecretsRequest, SecretsResponse, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::db::Db;
use crate::protocols::{Access, Peer, Protocol};

const PULL_LIMIT: usize = 200;
//long enough for a token refresh, short if a device dies
const LEASE_TTL: Duration = Duration::from_secs(30);

pub fn http_client() -> Result<reqwest::Client> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .connect_timeout(Duration::from_secs(10))
        .build()?)
}

//profile and lock name, holding device and expiry
pub type Leases = Arc<Mutex<HashMap<(String, String), (String, Instant)>>>;

//provider configs, sealed by each profile
#[derive(Clone)]
pub struct Secrets {
    pub db: Arc<Db>,
    //the hub only locks, it never sees the token
    pub leases: Leases,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secrets")
    }
}

impl Protocol for Secrets {
    const ACCESS: Access = Access::Scope(SCOPE_SECRETS);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(&peer, request).await {
                Ok(response) => response,
                Err(e) => SecretsResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl Secrets {
    async fn handle(&self, peer: &Peer, request: SecretsRequest) -> Result<SecretsResponse> {
        Ok(match request {
            SecretsRequest::Push {
                collection,
                records,
            } => {
                check_collection(&collection)?;
                self.db
                    .push(peer.profile()?, &collection, &peer.id, &records)?;
                SecretsResponse::Pushed
            }
            SecretsRequest::Pull { collection, since } => {
                check_collection(&collection)?;
                let (records, cursor, more) =
                    self.db
                        .pull(peer.profile()?, &collection, since, PULL_LIMIT)?;
                SecretsResponse::Records {
                    records,
                    cursor,
                    more,
                }
            }
            SecretsRequest::Lease { name } => {
                let key = (peer.profile()?.to_string(), name);
                let mut leases = self.leases.lock().unwrap();
                let now = Instant::now();
                leases.retain(|_, (_, until)| *until > now);
                let granted = match leases.get(&key) {
                    Some((holder, _)) => *holder == peer.id,
                    None => true,
                };
                if granted {
                    leases.insert(key, (peer.id.clone(), now + LEASE_TTL));
                }
                SecretsResponse::Lease { granted }
            }
            SecretsRequest::Release { name } => {
                let key = (peer.profile()?.to_string(), name);
                let mut leases = self.leases.lock().unwrap();
                if leases
                    .get(&key)
                    .is_some_and(|(holder, _)| *holder == peer.id)
                {
                    leases.remove(&key);
                }
                SecretsResponse::Released
            }
        })
    }
}

fn check_collection(collection: &str) -> Result<()> {
    if !SECRET_COLLECTIONS.contains(&collection) {
        bail!("unknown collection {collection}");
    }
    Ok(())
}
