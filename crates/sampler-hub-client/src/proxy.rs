//lets existing providers reach the hub
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use sampler_hub_proto::ForwardHead;
use sampler_hub_proto::{read_frame, write_frame};
use tokio::net::TcpListener;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_util::io::ReaderStream;

use crate::{Forwarded, HubClient, HubError, PairedHub, ProxyInfo};

type ProxyBody = BoxBody<Bytes, std::io::Error>;

pub(crate) async fn start(client: Arc<HubClient>, hub: PairedHub) -> Result<ProxyInfo, HubError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| HubError::Internal(format!("cannot open the local proxy: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| HubError::Internal(e.to_string()))?
        .port();
    //other apps can reach localhost too
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| HubError::Internal(e.to_string()))?;
    let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    let shared = Arc::new((client, hub, token.clone()));
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                continue;
            };
            let shared = shared.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let shared = shared.clone();
                    async move {
                        let (client, hub, token) = &*shared;
                        Ok::<_, Infallible>(match forward(client, hub, token, req).await {
                            Ok(response) => response,
                            Err(e) => plain(StatusCode::BAD_GATEWAY, e.to_string()),
                        })
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    Ok(ProxyInfo { port, token })
}

//paths look like /<token>/<provider>/<rest>
async fn forward(
    client: &HubClient,
    hub: &PairedHub,
    token: &str,
    req: Request<Incoming>,
) -> Result<Response<ProxyBody>, HubError> {
    let target = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let mut parts = target.trim_start_matches('/').splitn(3, '/');
    let (Some(given), Some(provider)) = (parts.next(), parts.next()) else {
        return Ok(plain(StatusCode::NOT_FOUND, "unknown path".into()));
    };
    if !same_token(given, token) {
        return Ok(plain(StatusCode::FORBIDDEN, "forbidden".into()));
    }
    let head = ForwardHead {
        provider: provider.to_string(),
        method: req.method().to_string(),
        path: format!("/{}", parts.next().unwrap_or("")),
        headers: req
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect(),
    };
    if is_websocket(&req) {
        return realtime(client, hub, head, req).await;
    }
    //hub only accepts real upgrades
    if head.path.split('?').next() == Some("/v1/realtime") {
        return Ok(plain(StatusCode::BAD_REQUEST, "websocket only".into()));
    }
    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|e| HubError::Internal(e.to_string()))?
        .to_bytes();

    match client.forward(hub, head, &body).await? {
        Forwarded::Response {
            status,
            headers,
            body,
        } => {
            let mut builder = Response::builder().status(status);
            for (name, value) in headers {
                builder = builder.header(name, value);
            }
            //streamed so tokens appear live
            let body = StreamBody::new(ReaderStream::new(body).map(|chunk| chunk.map(Frame::data)));
            builder
                .body(BodyExt::boxed(body))
                .map_err(|e| HubError::Internal(e.to_string()))
        }
        Forwarded::Refused(message) => Ok(plain(StatusCode::BAD_GATEWAY, message)),
    }
}

fn is_websocket(req: &Request<Incoming>) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

//hub must accept before upgrading
async fn realtime(
    client: &HubClient,
    hub: &PairedHub,
    head: ForwardHead,
    mut req: Request<Incoming>,
) -> Result<Response<ProxyBody>, HubError> {
    let Some(key) = req
        .headers()
        .get("sec-websocket-key")
        .and_then(|v| v.to_str().ok())
        .map(|key| derive_accept_key(key.as_bytes()))
    else {
        return Ok(plain(StatusCode::BAD_REQUEST, "no websocket key".into()));
    };
    let (mut send, mut recv) = match client.realtime(hub, head).await {
        Ok(stream) => stream,
        Err(e) => return Ok(plain(StatusCode::BAD_GATEWAY, e.to_string())),
    };
    let upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        let Ok(upgraded) = upgrade.await else {
            return;
        };
        let socket =
            WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await;
        let (mut to_app, mut from_app) = socket.split();
        let upstream = async {
            while let Some(message) = from_app.next().await {
                match message? {
                    Message::Text(text) => write_frame(&mut send, &text.as_str()).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            anyhow::Ok(())
        };
        let downstream = async {
            while let Some(text) = read_frame::<_, String>(&mut recv).await? {
                to_app.send(Message::text(text)).await?;
            }
            anyhow::Ok(())
        };
        //either side ending closes the other
        tokio::select! {
            _ = upstream => {}
            _ = downstream => {}
        }
        let _ = to_app.close().await;
        let _ = send.finish();
    });
    let mut response = plain(StatusCode::SWITCHING_PROTOCOLS, String::new());
    let headers = response.headers_mut();
    headers.insert("upgrade", "websocket".parse().unwrap());
    headers.insert("connection", "Upgrade".parse().unwrap());
    headers.insert("sec-websocket-accept", key.parse().unwrap());
    Ok(response)
}

fn plain(status: StatusCode, message: String) -> Response<ProxyBody> {
    let body = Full::new(Bytes::from(message)).map_err(|never| match never {});
    let mut response = Response::new(BodyExt::boxed(body));
    *response.status_mut() = status;
    response
}

fn same_token(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}
