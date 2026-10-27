//downloads llama.cpp and a small model, run with --ignored
use std::sync::Arc;
use std::time::Duration;

use sampler_hub_client::{HubClient, HubError, PairedHub};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

async fn pair(client: &HubClient, data_dir: &std::path::Path) -> PairedHub {
    let link = sampler_hub::hub::pairing_link(data_dir).unwrap().to_string();
    for _ in 0..10 {
        match client
            .pair(link.clone(), "phone".into(), "phone".into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
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
        "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads llama.cpp and a model"]
async fn hub_runs_llama_models() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-llama-{}", random_hex::<8>()));
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.clone();
        async move {
            sampler_hub::hub::run(&data_dir, async {
                let _ = stop_rx.await;
            })
            .await
        }
    });
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir).await;

    let catalog = phone.models_catalog(hub_ref.clone()).await.unwrap();
    let small = catalog.iter().min_by_key(|m| m.size).unwrap().clone();
    let files = phone
        .models_files(hub_ref.clone(), small.repo.clone())
        .await
        .unwrap();
    assert!(files.iter().any(|f| f.file == small.file), "{files:?}");
    assert!(files.iter().all(|f| !f.file.starts_with("mmproj-")));

    phone
        .models_download(hub_ref.clone(), small.repo.clone(), small.file.clone())
        .await
        .unwrap();
    //download, install and first start take a while
    let mut status = phone.models_status(hub_ref.clone()).await.unwrap();
    for _ in 0..600 {
        if status.state == "running" || status.state == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        status = phone.models_status(hub_ref.clone()).await.unwrap();
    }
    assert_eq!(status.state, "running", "{status:?}");
    assert!(
        status
            .installed
            .iter()
            .any(|m| m.file == small.file && m.repo.as_deref() == Some(small.repo.as_str()))
    );

    let providers = phone.providers(hub_ref.clone()).await.unwrap();
    assert!(
        providers.iter().any(|p| p.id == "llamacpp"),
        "{providers:?}"
    );
    let proxy = phone.clone().start_proxy(hub_ref.clone()).await.unwrap();
    let model = small.file.trim_end_matches(".gguf");
    let answer = http_post(
        proxy.port,
        &format!("/{}/llamacpp/chat/completions", proxy.token),
        &format!(
            r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}],"max_tokens":8}}"#
        ),
    )
    .await;
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.contains("chat.completion"), "{answer}");

    let status = phone
        .models_delete(hub_ref.clone(), small.file.clone())
        .await
        .unwrap();
    assert!(status.installed.is_empty());

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
