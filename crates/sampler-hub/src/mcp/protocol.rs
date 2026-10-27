//sampler/mcp/1, members manage their servers
use std::sync::Arc;

use anyhow::Result;
use anyhow::ensure;
use sampler_hub_proto::{McpRequest, McpResponse, SCOPE_MCP, read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Mcp;
use crate::protocols::{Access, Peer, Protocol};

#[derive(Debug, Clone)]
pub struct McpAdmin {
    pub mcp: Arc<Mcp>,
}

impl Protocol for McpAdmin {
    const ACCESS: Access = Access::Scope(SCOPE_MCP);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let Some(request) = read_frame(&mut reader).await? else {
            return Ok(());
        };
        let response = self
            .handle(peer.profile()?, request)
            .await
            .unwrap_or_else(|e| McpResponse::Error {
                message: format!("{e:#}"),
            });
        write_frame(&mut writer, &response).await
    }
}

impl McpAdmin {
    async fn handle(&self, owner: &str, request: McpRequest) -> Result<McpResponse> {
        let mcp = &self.mcp;
        //turned off by an admin, nothing new runs
        let starts = matches!(
            request,
            McpRequest::Install { .. }
                | McpRequest::Enable { on: true, .. }
                | McpRequest::Restart { .. }
                | McpRequest::Update { .. }
        );
        ensure!(
            !starts || mcp.enabled(),
            "mcp servers are turned off on this sampler"
        );
        let status = || -> Result<McpResponse> {
            Ok(McpResponse::Status {
                isolation: mcp.isolation().into(),
                servers: mcp.status(owner)?,
            })
        };
        Ok(match request {
            McpRequest::Catalog => McpResponse::Listings {
                listings: mcp.catalog(),
            },
            McpRequest::Search { query } => McpResponse::Listings {
                listings: mcp.search(&query).await?,
            },
            McpRequest::Prepare { listing } => McpResponse::Listing {
                listing: Box::new(mcp.prepare(listing).await?),
            },
            McpRequest::Install { listing, values } => McpResponse::Installed {
                id: mcp.install(owner, listing, values)?,
            },
            McpRequest::Configure { id, values } => {
                mcp.configure(owner, &id, values)?;
                status()?
            }
            McpRequest::Enable { id, on } => {
                mcp.enable(owner, &id, on)?;
                status()?
            }
            McpRequest::Restart { id } => {
                mcp.restart(owner, &id)?;
                status()?
            }
            McpRequest::Update { id } => {
                mcp.update(owner, &id).await?;
                status()?
            }
            McpRequest::Remove { id } => {
                mcp.remove(owner, &id).await?;
                status()?
            }
            McpRequest::Status => status()?,
            McpRequest::Logs { id } => McpResponse::Logs {
                lines: mcp.logs(owner, &id)?,
            },
            McpRequest::Folders { path } => {
                let (path, parent, folders) = mcp.folders(owner, path.as_deref())?;
                McpResponse::Folders {
                    path,
                    parent,
                    folders,
                }
            }
            McpRequest::Registries => McpResponse::Registries {
                urls: mcp.registries(),
            },
            McpRequest::SetRegistries { urls } => {
                ensure!(mcp.is_admin(owner), "only an admin changes the registries");
                mcp.set_registries(urls)?;
                McpResponse::Registries {
                    urls: mcp.registries(),
                }
            }
        })
    }
}
