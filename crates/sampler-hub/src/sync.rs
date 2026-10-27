use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use sampler_hub_proto::{
    SCOPE_SYNC, SYNC_COLLECTIONS, SyncRequest, SyncResponse, read_frame, write_frame,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::db::Db;
use crate::protocols::{Access, Peer, Protocol};

const PULL_LIMIT: usize = 500;
//under the client request timeout
const WATCH_TIMEOUT: Duration = Duration::from_secs(15);

//conversations messages blobs and settings
#[derive(Clone)]
pub struct DataSync {
    pub db: Arc<Db>,
}

impl std::fmt::Debug for DataSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DataSync")
    }
}

impl Protocol for DataSync {
    const ACCESS: Access = Access::Scope(SCOPE_SYNC);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        while let Some(request) = read_frame(&mut reader).await? {
            let response = match self.handle(&peer, request).await {
                Ok(response) => response,
                Err(e) => SyncResponse::Error {
                    message: format!("{e:#}"),
                },
            };
            write_frame(&mut writer, &response).await?;
        }
        Ok(())
    }
}

impl DataSync {
    async fn handle(&self, peer: &Peer, request: SyncRequest) -> Result<SyncResponse> {
        Ok(match request {
            SyncRequest::Push {
                collection,
                records,
            } => {
                check_collection(&collection)?;
                self.db
                    .push(peer.profile()?, &collection, &peer.id, &records)?;
                SyncResponse::Pushed
            }
            SyncRequest::Pull { collection, since } => {
                check_collection(&collection)?;
                let (records, cursor, more) =
                    self.db
                        .pull(peer.profile()?, &collection, since, PULL_LIMIT)?;
                SyncResponse::Records {
                    records,
                    cursor,
                    more,
                }
            }
            SyncRequest::Watch { since } => {
                let profile = peer.profile()?;
                let mut changes = self.db.subscribe();
                //other profiles wake it too
                let wait = async {
                    while self.db.latest(profile)? <= since {
                        changes.changed().await?;
                    }
                    anyhow::Ok(())
                };
                //a timeout answers anyway so the client loops
                if let Ok(result) = tokio::time::timeout(WATCH_TIMEOUT, wait).await {
                    result?;
                }
                SyncResponse::Changed {
                    cursor: self.db.latest(profile)?,
                }
            }
        })
    }
}

fn check_collection(collection: &str) -> Result<()> {
    if !SYNC_COLLECTIONS.contains(&collection) {
        bail!("unknown collection {collection}");
    }
    Ok(())
}
