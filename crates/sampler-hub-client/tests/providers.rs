//needs network for address lookup
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use sampler_hub::providers::Target;
use sampler_hub_client::{HubClient, HubError, PairedHub, SyncRecord};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn random_hex<const N: usize>() -> String {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).unwrap();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn new_client() -> Arc<HubClient> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).unwrap();
    HubClient::new(key.to_vec()).unwrap()
}

//fake ollama
async fn fake_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| async move {
                    let path = req.uri().path().to_string();
                    let body = req.into_body().collect().await.unwrap().to_bytes();
                    let body = String::from_utf8_lossy(&body).to_string();
                    let (status, reply) = match path.as_str() {
                        "/api/tags" => (200, r#"{"models":[]}"#.to_string()),
                        "/api/chat" => (200, format!("echo:{body}")),
                        _ => (404, String::new()),
                    };
                    let mut response = Response::new(Full::new(Bytes::from(reply)));
                    *response.status_mut() = hyper::StatusCode::from_u16(status).unwrap();
                    Ok::<_, Infallible>(response)
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    base
}

async fn pair(client: &HubClient, data_dir: &std::path::Path, name: &str) -> PairedHub {
    let link = sampler_hub::hub::pairing_link(data_dir).unwrap().to_string();
    //fresh hubs publish their address slowly
    for _ in 0..10 {
        match client
            .pair(link.clone(), name.into(), "phone".into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) => {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await
            }
            result => {
                let paired = result.unwrap();
                join(data_dir, client);
                return paired;
            }
        }
    }
    panic!("hub never became reachable");
}

//binds a device to the first profile
fn join(data_dir: &std::path::Path, client: &HubClient) {
    let store = sampler_hub::store::Store::new(data_dir);
    let profile = match store.profiles().unwrap().into_iter().next() {
        Some(profile) => profile,
        None => store.create_profile("owner").unwrap(),
    };
    assert!(store.bind_device(&client.device_id(), &profile.id).unwrap());
    client
        .set_profile(profile.id.clone(), test_key(&profile.id))
        .unwrap();
}

//devices of one profile share it
fn test_key(profile: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(&profile.as_bytes()[..32])
}

async fn http_post(port: u16, path: &str, body: &str) -> String {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

fn record(id: &str, updated_at: u64, payload: &str) -> SyncRecord {
    SyncRecord {
        id: id.into(),
        updated_at,
        deleted: false,
        payload: Some(payload.into()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn providers_and_secrets() {
    let fake = fake_server().await;
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-e2e-{}", random_hex::<8>()));
    let targets = vec![Target {
        id: "fake".into(),
        name: "Fake".into(),
        kind: "ollama".into(),
        base: fake.clone(),
        probe: "/api/tags".into(),
    }];

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            sampler_hub::hub::run_with_targets(
                &data_dir,
                async {
                    let _ = stop_rx.await;
                },
                targets,
            )
            .await
        }
    });

    let phone = new_client();
    let laptop = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    pair(&laptop, &data_dir, "laptop").await;

    //relay
    let providers = phone.providers(hub_ref.clone()).await.unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, "fake");
    let proxy = phone.clone().start_proxy(hub_ref.clone()).await.unwrap();
    let ok = http_post(
        proxy.port,
        &format!("/{}/fake/api/chat", proxy.token),
        "hello",
    )
    .await;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    assert!(ok.contains("echo:hello"), "{ok}");
    let denied = http_post(proxy.port, "/wrong/fake/api/chat", "hello").await;
    assert!(denied.starts_with("HTTP/1.1 403"), "{denied}");

    //provider configs sync last writer wins
    phone
        .push(
            hub_ref.clone(),
            "providers".into(),
            vec![record("srv", 10, "new")],
        )
        .await
        .unwrap();
    laptop
        .push(
            hub_ref.clone(),
            "providers".into(),
            vec![record("srv", 5, "old")],
        )
        .await
        .unwrap();
    let page = laptop
        .pull(hub_ref.clone(), "providers".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].payload.as_deref(), Some("new"));
    //one device of the profile refreshes at a time
    let name = "oauth:mcp".to_string();
    assert!(phone.lease(hub_ref.clone(), name.clone()).await.unwrap());
    assert!(!laptop.lease(hub_ref.clone(), name.clone()).await.unwrap());
    assert!(phone.lease(hub_ref.clone(), name.clone()).await.unwrap());
    laptop.release(hub_ref.clone(), name.clone()).await.unwrap();
    assert!(!laptop.lease(hub_ref.clone(), name.clone()).await.unwrap());
    phone.release(hub_ref.clone(), name.clone()).await.unwrap();
    assert!(laptop.lease(hub_ref.clone(), name).await.unwrap());

    let unknown = phone.pull(hub_ref.clone(), "nope".into(), 0).await;
    assert!(matches!(unknown, Err(HubError::Internal(_))), "{unknown:?}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
