//needs network for address lookup
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use sampler_hub::providers::Target;
use sampler_hub_client::{HubClient, HubError, PairedHub};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::{accept_hdr_async, connect_async};

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

async fn pair(client: &HubClient, data_dir: &std::path::Path, name: &str) -> PairedHub {
    let link = sampler_hub::hub::pairing_link(data_dir)
        .unwrap()
        .to_string();
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

//fake llama, echoes frames and uri
#[allow(clippy::result_large_err)]
async fn fake_llama() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut uri = String::new();
                let callback = |req: &Request, response: Response| {
                    uri = req.uri().to_string();
                    Ok(response)
                };
                let Ok(mut socket) = accept_hdr_async(socket, callback).await else {
                    return;
                };
                socket
                    .send(Message::text(format!("hello {uri}")))
                    .await
                    .unwrap();
                while let Some(Ok(message)) = socket.next().await {
                    match message {
                        Message::Text(text) => {
                            socket
                                .send(Message::text(format!("echo:{text}")))
                                .await
                                .unwrap();
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            });
        }
    });
    port
}

async fn http_get(port: u16, path: &str) -> String {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1
Host: 127.0.0.1
Connection: close

"
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

async fn next_text<S>(socket: &mut S) -> String
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match socket.next().await.unwrap().unwrap() {
        Message::Text(text) => text.to_string(),
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn realtime_reaches_llama_through_the_proxy() {
    let llama_port = fake_llama().await;
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-rt-{}", random_hex::<8>()));
    let targets = vec![Target {
        id: "fake".into(),
        name: "Fake".into(),
        kind: "ollama".into(),
        base: format!("http://127.0.0.1:{llama_port}"),
        probe: "/api/tags".into(),
    }];
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            sampler_hub::hub::run_with_llama_port(
                &data_dir,
                async {
                    let _ = stop_rx.await;
                },
                targets,
                Some(llama_port),
            )
            .await
        }
    });
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    let proxy = phone.clone().start_proxy(hub_ref).await.unwrap();

    //frames and query reach llama
    let url = format!(
        "ws://127.0.0.1:{}/{}/llamacpp/v1/realtime?model=lfm",
        proxy.port, proxy.token
    );
    let (mut socket, response) = connect_async(url).await.unwrap();
    assert_eq!(response.status(), 101);
    assert_eq!(next_text(&mut socket).await, "hello /v1/realtime?model=lfm");
    socket.send(Message::text("one")).await.unwrap();
    socket.send(Message::text("two")).await.unwrap();
    assert_eq!(next_text(&mut socket).await, "echo:one");
    assert_eq!(next_text(&mut socket).await, "echo:two");
    socket.close(None).await.unwrap();

    //other providers never upgrade
    let other = format!(
        "ws://127.0.0.1:{}/{}/fake/v1/realtime",
        proxy.port, proxy.token
    );
    assert!(connect_async(other).await.is_err());
    let wrong = format!("ws://127.0.0.1:{}/wrong/llamacpp/v1/realtime", proxy.port);
    assert!(connect_async(wrong).await.is_err());
    //plain get never upgrades
    let plain = http_get(
        proxy.port,
        &format!("/{}/llamacpp/v1/realtime", proxy.token),
    )
    .await;
    assert!(!plain.starts_with("HTTP/1.1 101"), "{plain}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

//needs real llama-server, set LLAMA_PORT
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a llama-server router running LFM3-Omni"]
async fn realtime_with_a_real_model() {
    let llama_port: u16 = std::env::var("LLAMA_PORT").unwrap().parse().unwrap();
    let model = std::env::var("LLAMA_MODEL").unwrap();
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-rt-{}", random_hex::<8>()));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            sampler_hub::hub::run_with_llama_port(
                &data_dir,
                async {
                    let _ = stop_rx.await;
                },
                Vec::new(),
                Some(llama_port),
            )
            .await
        }
    });
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    let proxy = phone.clone().start_proxy(hub_ref).await.unwrap();
    let url = format!(
        "ws://127.0.0.1:{}/{}/llamacpp/v1/realtime?model={model}",
        proxy.port, proxy.token
    );
    //shares proxy with other clients
    if let Ok(path) = std::env::var("PROXY_URL_FILE") {
        std::fs::write(path, &url).unwrap();
    }

    let (mut socket, _) = connect_async(&url).await.unwrap();
    let created = next_text(&mut socket).await;
    assert!(created.contains("session.created"), "{created}");
    let update = r#"{"type":"session.update","session":{"type":"realtime","instructions":"Respond in interleaved text and audio.","output_modalities":["text","audio"]}}"#;
    socket.send(Message::text(update)).await.unwrap();
    let updated = next_text(&mut socket).await;
    assert!(updated.contains("session.updated"), "{updated}");
    let item = r#"{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"Say hello in one short sentence."}]}}"#;
    socket.send(Message::text(item)).await.unwrap();
    socket
        .send(Message::text(r#"{"type":"response.create"}"#))
        .await
        .unwrap();
    let mut audio = false;
    loop {
        let event = next_text(&mut socket).await;
        audio |= event.contains("response.output_audio.delta");
        if event.contains(r#""type":"response.done""#) {
            break;
        }
    }
    assert!(audio);

    if let Ok(secs) = std::env::var("HOLD_SECS") {
        tokio::time::sleep(std::time::Duration::from_secs(secs.parse().unwrap())).await;
    }
    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
