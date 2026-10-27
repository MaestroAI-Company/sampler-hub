//mcp servers hosted on the hub, admins only
use std::collections::{BTreeMap, HashMap};

use sampler_hub_proto as proto;
use sampler_hub_proto::{ADMIN_ALPN, AdminRequest, AdminResponse, MCP_ALPN, McpRequest, McpResponse};
use serde::{Deserialize, Serialize};

use crate::{HubClient, HubError, PairedHub};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Enum)
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpSource {
    Mcpb { url: String, sha256: String },
    Npm { package: String, version: String },
    Pypi { package: String, version: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpField {
    pub key: String,
    pub label: String,
    pub description: String,
    pub kind: String,
    pub required: bool,
    pub secret: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpArgument {
    pub flag: Option<String>,
    pub value: Option<String>,
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpEnv {
    pub name: String,
    pub value: Option<String>,
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpListing {
    pub id: String,
    pub title: String,
    pub description: String,
    pub version: String,
    pub publisher: String,
    pub repository: Option<String>,
    pub license: Option<String>,
    pub source: McpSource,
    pub fields: Vec<McpField>,
    pub arguments: Vec<McpArgument>,
    pub env: Vec<McpEnv>,
    pub offline: bool,
    pub registry: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpServer {
    pub id: String,
    pub listing: McpListing,
    pub enabled: bool,
    pub state: String,
    pub message: Option<String>,
    pub values: HashMap<String, String>,
    pub secrets_set: Vec<String>,
    pub tools: Vec<String>,
    pub update: Option<String>,
    pub cpu: Option<f32>,
    pub memory: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpStatus {
    //full or partial
    pub isolation: String,
    pub servers: Vec<McpServer>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpFolders {
    pub path: String,
    pub parent: Option<String>,
    pub folders: Vec<String>,
}

//any profile's server, for admins
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    derive(uniffi::Record)
)]
#[serde(rename_all = "camelCase")]
pub struct McpServerSummary {
    pub id: String,
    pub owner: Option<String>,
    pub title: String,
    pub repository: Option<String>,
    pub enabled: bool,
    pub state: String,
    pub tools: u32,
    pub cpu: Option<f32>,
    pub memory: Option<u64>,
}

impl From<proto::McpListing> for McpListing {
    fn from(listing: proto::McpListing) -> Self {
        Self {
            id: listing.id,
            title: listing.title,
            description: listing.description,
            version: listing.version,
            publisher: listing.publisher,
            repository: listing.repository,
            license: listing.license,
            source: match listing.source {
                proto::McpSource::Mcpb { url, sha256 } => McpSource::Mcpb { url, sha256 },
                proto::McpSource::Npm { package, version } => McpSource::Npm { package, version },
                proto::McpSource::Pypi { package, version } => McpSource::Pypi { package, version },
            },
            fields: listing
                .fields
                .into_iter()
                .map(|f| McpField {
                    key: f.key,
                    label: f.label,
                    description: f.description,
                    kind: f.kind,
                    required: f.required,
                    secret: f.secret,
                    default: f.default,
                })
                .collect(),
            arguments: listing
                .arguments
                .into_iter()
                .map(|a| McpArgument {
                    flag: a.flag,
                    value: a.value,
                    field: a.field,
                })
                .collect(),
            env: listing
                .env
                .into_iter()
                .map(|e| McpEnv {
                    name: e.name,
                    value: e.value,
                    field: e.field,
                })
                .collect(),
            offline: listing.offline,
            registry: listing.registry,
        }
    }
}

impl From<McpListing> for proto::McpListing {
    fn from(this: McpListing) -> Self {
        Self {
            id: this.id,
            title: this.title,
            description: this.description,
            version: this.version,
            publisher: this.publisher,
            repository: this.repository,
            license: this.license,
            source: match this.source {
                McpSource::Mcpb { url, sha256 } => proto::McpSource::Mcpb { url, sha256 },
                McpSource::Npm { package, version } => proto::McpSource::Npm { package, version },
                McpSource::Pypi { package, version } => proto::McpSource::Pypi { package, version },
            },
            fields: this
                .fields
                .into_iter()
                .map(|f| proto::McpField {
                    key: f.key,
                    label: f.label,
                    description: f.description,
                    kind: f.kind,
                    required: f.required,
                    secret: f.secret,
                    default: f.default,
                })
                .collect(),
            arguments: this
                .arguments
                .into_iter()
                .map(|a| proto::McpArgument {
                    flag: a.flag,
                    value: a.value,
                    field: a.field,
                })
                .collect(),
            env: this
                .env
                .into_iter()
                .map(|e| proto::McpEnv {
                    name: e.name,
                    value: e.value,
                    field: e.field,
                })
                .collect(),
            offline: this.offline,
            registry: this.registry,
        }
    }
}

impl From<proto::McpServer> for McpServer {
    fn from(server: proto::McpServer) -> Self {
        Self {
            id: server.id,
            listing: server.listing.into(),
            enabled: server.enabled,
            state: server.state,
            message: server.message,
            values: server.values.into_iter().collect(),
            secrets_set: server.secrets_set,
            tools: server.tools,
            update: server.update,
            cpu: server.usage.map(|u| u.cpu),
            memory: server.usage.map(|u| u.memory),
        }
    }
}

#[cfg_attr(
    not(all(target_family = "wasm", target_os = "unknown")),
    uniffi::export(async_runtime = "tokio")
)]
impl HubClient {
    //the hand checked selection
    pub async fn mcp_catalog(&self, hub: PairedHub) -> Result<Vec<McpListing>, HubError> {
        self.mcp_listings(&hub, McpRequest::Catalog).await
    }

    //local open source servers of the hub registries
    pub async fn mcp_search(
        &self,
        hub: PairedHub,
        query: String,
    ) -> Result<Vec<McpListing>, HubError> {
        self.mcp_listings(&hub, McpRequest::Search { query }).await
    }

    //fills the form of a bundle
    pub async fn mcp_prepare(
        &self,
        hub: PairedHub,
        listing: McpListing,
    ) -> Result<McpListing, HubError> {
        let request = McpRequest::Prepare {
            listing: listing.into(),
        };
        match self.mcp(&hub, request).await? {
            McpResponse::Listing { listing } => Ok((*listing).into()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn mcp_install(
        &self,
        hub: PairedHub,
        listing: McpListing,
        values: HashMap<String, String>,
    ) -> Result<String, HubError> {
        let request = McpRequest::Install {
            listing: listing.into(),
            values: values.into_iter().collect(),
        };
        match self.mcp(&hub, request).await? {
            McpResponse::Installed { id } => Ok(id),
            other => Err(unexpected(other)),
        }
    }

    //missing keys stay, empty values clear
    pub async fn mcp_configure(
        &self,
        hub: PairedHub,
        id: String,
        values: HashMap<String, String>,
    ) -> Result<McpStatus, HubError> {
        let values: BTreeMap<String, String> = values.into_iter().collect();
        self.mcp_status_of(&hub, McpRequest::Configure { id, values })
            .await
    }

    pub async fn mcp_enable(
        &self,
        hub: PairedHub,
        id: String,
        on: bool,
    ) -> Result<McpStatus, HubError> {
        self.mcp_status_of(&hub, McpRequest::Enable { id, on })
            .await
    }

    pub async fn mcp_restart(&self, hub: PairedHub, id: String) -> Result<McpStatus, HubError> {
        self.mcp_status_of(&hub, McpRequest::Restart { id }).await
    }

    pub async fn mcp_update(&self, hub: PairedHub, id: String) -> Result<McpStatus, HubError> {
        self.mcp_status_of(&hub, McpRequest::Update { id }).await
    }

    pub async fn mcp_remove(&self, hub: PairedHub, id: String) -> Result<McpStatus, HubError> {
        self.mcp_status_of(&hub, McpRequest::Remove { id }).await
    }

    pub async fn mcp_status(&self, hub: PairedHub) -> Result<McpStatus, HubError> {
        self.mcp_status_of(&hub, McpRequest::Status).await
    }

    pub async fn mcp_logs(&self, hub: PairedHub, id: String) -> Result<Vec<String>, HubError> {
        match self.mcp(&hub, McpRequest::Logs { id }).await? {
            McpResponse::Logs { lines } => Ok(lines),
            other => Err(unexpected(other)),
        }
    }

    //folders on the hub machine for a folder field
    pub async fn mcp_folders(
        &self,
        hub: PairedHub,
        path: Option<String>,
    ) -> Result<McpFolders, HubError> {
        match self.mcp(&hub, McpRequest::Folders { path }).await? {
            McpResponse::Folders {
                path,
                parent,
                folders,
            } => Ok(McpFolders {
                path,
                parent,
                folders,
            }),
            other => Err(unexpected(other)),
        }
    }

    pub async fn mcp_registries(&self, hub: PairedHub) -> Result<Vec<String>, HubError> {
        self.mcp_urls(&hub, McpRequest::Registries).await
    }

    //every profile's servers, on the admin channel so browsers reach it
    pub async fn mcp_overview(&self, hub: PairedHub) -> Result<Vec<McpServerSummary>, HubError> {
        self.mcp_admin(&hub, AdminRequest::McpServers).await
    }

    //start, stop, restart or remove any profile's server
    pub async fn mcp_manage(
        &self,
        hub: PairedHub,
        id: String,
        action: String,
    ) -> Result<Vec<McpServerSummary>, HubError> {
        let action = match action.as_str() {
            "start" => proto::McpAction::Start,
            "stop" => proto::McpAction::Stop,
            "restart" => proto::McpAction::Restart,
            "remove" => proto::McpAction::Remove,
            other => return Err(HubError::Internal(format!("unknown action {other}"))),
        };
        self.mcp_admin(&hub, AdminRequest::ManageMcp { id, action })
            .await
    }

    //raw sampler/mcp/1 json, for bridges that speak json
    pub async fn mcp_json(&self, hub: PairedHub, request: String) -> Result<String, HubError> {
        let request: McpRequest = serde_json::from_str(&request)
            .map_err(|e| HubError::Internal(format!("invalid mcp request: {e}")))?;
        let response = self.mcp(&hub, request).await?;
        serde_json::to_string(&response).map_err(|e| HubError::Internal(e.to_string()))
    }

    pub async fn mcp_set_registries(
        &self,
        hub: PairedHub,
        urls: Vec<String>,
    ) -> Result<Vec<String>, HubError> {
        self.mcp_urls(&hub, McpRequest::SetRegistries { urls })
            .await
    }
}

impl HubClient {
    async fn mcp_admin(
        &self,
        hub: &PairedHub,
        request: AdminRequest,
    ) -> Result<Vec<McpServerSummary>, HubError> {
        let mut stream = self.stream(hub, ADMIN_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            AdminResponse::McpServers { servers } => Ok(servers
                .into_iter()
                .map(|s| McpServerSummary {
                    id: s.id,
                    owner: s.owner,
                    title: s.title,
                    repository: s.repository,
                    enabled: s.enabled,
                    state: s.state,
                    tools: s.tools,
                    cpu: s.usage.map(|u| u.cpu),
                    memory: s.usage.map(|u| u.memory),
                })
                .collect()),
            AdminResponse::Error { message } => Err(HubError::Internal(message)),
            other => Err(HubError::Internal(format!("unexpected answer {other:?}"))),
        }
    }

    async fn mcp(&self, hub: &PairedHub, request: McpRequest) -> Result<McpResponse, HubError> {
        let mut stream = self.stream(hub, MCP_ALPN).await?;
        let response = stream.request(&request).await?;
        stream.finish();
        match response {
            McpResponse::Error { message } => Err(HubError::Internal(message)),
            other => Ok(other),
        }
    }

    async fn mcp_listings(
        &self,
        hub: &PairedHub,
        request: McpRequest,
    ) -> Result<Vec<McpListing>, HubError> {
        match self.mcp(hub, request).await? {
            McpResponse::Listings { listings } => {
                Ok(listings.into_iter().map(Into::into).collect())
            }
            other => Err(unexpected(other)),
        }
    }

    async fn mcp_status_of(
        &self,
        hub: &PairedHub,
        request: McpRequest,
    ) -> Result<McpStatus, HubError> {
        match self.mcp(hub, request).await? {
            McpResponse::Status { isolation, servers } => Ok(McpStatus {
                isolation,
                servers: servers.into_iter().map(Into::into).collect(),
            }),
            other => Err(unexpected(other)),
        }
    }

    async fn mcp_urls(
        &self,
        hub: &PairedHub,
        request: McpRequest,
    ) -> Result<Vec<String>, HubError> {
        match self.mcp(hub, request).await? {
            McpResponse::Registries { urls } => Ok(urls),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(answer: McpResponse) -> HubError {
    HubError::Internal(format!("unexpected answer {answer:?}"))
}
