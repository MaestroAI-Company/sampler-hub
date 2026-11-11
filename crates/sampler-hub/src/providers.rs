use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use sampler_hub_proto::{
    ForwardHead, ProviderInfo, ProvidersRequest, ProvidersResponse, SCOPE_PROVIDERS, read_frame,
    write_frame,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

use crate::protocols::{Access, Peer, Protocol};

const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_REQUEST_BYTES: u64 = 32 * 1024 * 1024;
//only headers providers need
const FORWARDED_HEADERS: [&str; 3] = ["content-type", "accept", "authorization"];

#[derive(Debug, Clone)]
pub struct Target {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub base: String,
    pub probe: String,
}

//the only targets the relay ever reaches
pub fn default_targets() -> Vec<Target> {
    let target = |id: &str, name: &str, kind: &str, base: &str, probe: &str| Target {
        id: id.into(),
        name: name.into(),
        kind: kind.into(),
        base: base.into(),
        probe: probe.into(),
    };
    vec![
        target(
            "ollama",
            "Ollama",
            "ollama",
            "http://127.0.0.1:11434",
            "/api/tags",
        ),
        target(
            "lmstudio",
            "LM Studio",
            "openai",
            "http://127.0.0.1:1234/v1",
            "/models",
        ),
    ]
}

#[derive(Clone)]
pub struct Providers {
    pub http: reqwest::Client,
    pub targets: Arc<Vec<Target>>,
    pub settings: Arc<crate::settings::Settings>,
    //models the hub runs itself
    pub llama: Arc<crate::llama::Llama>,
    pub mcp: Arc<crate::mcp::Mcp>,
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Providers")
    }
}

impl Protocol for Providers {
    const ACCESS: Access = Access::Scope(SCOPE_PROVIDERS);

    async fn serve<R, W>(&self, peer: Peer, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let Some(request) = read_frame(&mut reader).await? else {
            return Ok(());
        };
        match request {
            ProvidersRequest::List => {
                let providers = self.detect(peer.profile()?).await;
                write_frame(&mut writer, &ProvidersResponse::Providers { providers }).await
            }
            ProvidersRequest::Forward { head } => {
                if let Err(e) = self.forward(&peer, head, reader, &mut writer).await {
                    write_frame(
                        &mut writer,
                        &ProvidersResponse::Error {
                            message: format!("{e:#}"),
                        },
                    )
                    .await?;
                }
                Ok(())
            }
        }
    }
}

impl Providers {
    async fn detect(&self, profile: &str) -> Vec<ProviderInfo> {
        //each profile reaches its own servers
        let mut found = self.mcp.providers(profile);
        let settings = self.settings.get();
        if !settings.share_providers {
            return found;
        }
        //listed while starting or failed too
        if settings.models && !self.llama.installed().is_empty() {
            found.push(ProviderInfo {
                id: crate::llama::PROVIDER_ID.into(),
                name: if settings.models_nightly {
                    "llama.cpp nightly"
                } else {
                    "llama.cpp"
                }
                .into(),
                kind: "openai".into(),
            });
        }
        for target in self.targets.iter() {
            let probe = self
                .http
                .get(format!("{}{}", target.base, target.probe))
                .timeout(PROBE_TIMEOUT);
            if probe.send().await.is_ok_and(|r| r.status().is_success()) {
                found.push(ProviderInfo {
                    id: target.id.clone(),
                    name: target.name.clone(),
                    kind: target.kind.clone(),
                });
            }
        }
        found
    }

    async fn forward<R, W>(
        &self,
        peer: &Peer,
        head: ForwardHead,
        reader: R,
        writer: &mut W,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        if let Some(id) = head.provider.strip_prefix(crate::mcp::PROVIDER_PREFIX) {
            return self
                .mcp
                .forward(peer.profile()?, id, &head, reader, writer)
                .await;
        }
        if !self.settings.get().share_providers {
            bail!("this hub does not share its providers");
        }
        let base = if head.provider == crate::llama::PROVIDER_ID {
            self.llama
                .base_url()
                .ok_or_else(|| anyhow!("llama.cpp is not running"))?
        } else {
            self.targets
                .iter()
                .find(|t| t.id == head.provider)
                .ok_or_else(|| anyhow!("unknown provider {}", head.provider))?
                .base
                .clone()
        };
        //a leading slash keeps the host fixed
        if !head.path.starts_with('/') {
            bail!("invalid path");
        }
        if is_realtime(&head.path) {
            ensure!(
                head.provider == crate::llama::PROVIDER_ID,
                "only llama.cpp serves realtime"
            );
            ensure!(
                head.method.eq_ignore_ascii_case("GET"),
                "realtime opens with GET"
            );
            ensure!(
                self.llama.supports_realtime(),
                "this llama.cpp build has no realtime endpoint"
            );
            let root = base.strip_suffix("/v1").unwrap_or(&base);
            let url = format!("{}{}", root.replacen("http", "ws", 1), head.path);
            return relay_realtime(&url, reader, writer).await;
        }
        let mut body = Vec::new();
        reader
            .take(MAX_REQUEST_BYTES)
            .read_to_end(&mut body)
            .await?;

        let method = reqwest::Method::from_bytes(head.method.as_bytes())?;
        let mut request = self
            .http
            .request(method, format!("{base}{}", head.path))
            .body(body);
        for (name, value) in &head.headers {
            if FORWARDED_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                request = request.header(name, value);
            }
        }
        let mut response = request.send().await?;

        let headers = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|v| vec![("content-type".to_string(), v.to_string())])
            .unwrap_or_default();
        let status = response.status().as_u16();
        write_frame(writer, &ProvidersResponse::Response { status, headers }).await?;
        //streamed so tokens appear live
        while let Some(chunk) = response.chunk().await? {
            writer.write_all(&chunk).await?;
            writer.flush().await?;
        }
        Ok(())
    }
}

fn is_realtime(path: &str) -> bool {
    path.split('?').next() == Some("/v1/realtime")
}

//relay text until a side closes
async fn relay_realtime<R, W>(url: &str, mut reader: R, writer: &mut W) -> Result<()>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let (socket, _) = tokio_tungstenite::connect_async(url).await?;
    write_frame(
        writer,
        &ProvidersResponse::Response {
            status: 101,
            headers: Vec::new(),
        },
    )
    .await?;
    let (mut to_llama, mut from_llama) = socket.split();
    let upstream = async {
        while let Some(text) = read_frame::<_, String>(&mut reader).await? {
            to_llama.send(Message::text(text)).await?;
        }
        anyhow::Ok(())
    };
    let downstream = async {
        while let Some(message) = from_llama.next().await {
            match message? {
                Message::Text(text) => write_frame(writer, &text.as_str()).await?,
                Message::Close(_) => break,
                _ => {}
            }
        }
        anyhow::Ok(())
    };
    //after 101 errors just end
    let ended = tokio::select! {
        done = upstream => done,
        done = downstream => done,
    };
    if let Err(e) = ended {
        tracing::debug!("realtime session ended: {e:#}");
    }
    Ok(())
}
