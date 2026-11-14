//needs network for address lookup
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use sampler_hub_client::{HubClient, HubError, PairedHub};

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).unwrap();
    bytes
}

fn temp_data_dir() -> std::path::PathBuf {
    let suffix: String = random_bytes::<8>()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::env::temp_dir().join(format!("sampler-hub-relay-{suffix}"))
}

//pair a fresh client with the hub
async fn pair_with_hub(
    data_dir: &std::path::Path,
) -> (
    std::sync::Arc<HubClient>,
    PairedHub,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let link = sampler_hub::hub::pairing_link(data_dir)
        .unwrap()
        .to_string();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let hub = tokio::spawn({
        let data_dir = data_dir.to_path_buf();
        async move {
            sampler_hub::hub::run(&data_dir, async {
                let _ = stop_rx.await;
            })
            .await
        }
    });
    let client = HubClient::new(random_bytes::<32>().to_vec()).unwrap();
    //fresh hubs publish their address slowly
    let mut attempts = 0;
    let paired = loop {
        match client
            .pair(link.clone(), "test phone".into(), "phone".into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) if attempts < 10 => attempts += 1,
            result => break result.unwrap(),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    };
    (client, paired, hub, stop_tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn custom_relay_pair_and_status() {
    //plain http relay avoids tls certificates
    let mut config = ServerConfig::default();
    config.relay = Some(RelayConfig::new((std::net::Ipv4Addr::LOCALHOST, 0)));
    let relay = Server::spawn(config).await.unwrap();
    let relay_url = format!("http://{}/", relay.http_addr().unwrap());

    let data_dir = temp_data_dir();
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("relays = [\"{relay_url}\"]\n"),
    )
    .unwrap();

    let (client, paired, hub, stop_tx) = pair_with_hub(&data_dir).await;
    assert!(paired.relays.contains(&relay_url), "{:?}", paired.relays);
    let status = client.status(paired).await.unwrap();
    //the pairing handshake had to cross the custom relay
    assert!(relay.metrics().server.bytes_recv.get() > 0);
    assert_eq!(status.relays, Some(vec![relay_url]));

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    relay.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_relays_report_empty() {
    let data_dir = temp_data_dir();
    let (client, paired, hub, stop_tx) = pair_with_hub(&data_dir).await;
    let status = client.status(paired).await.unwrap();
    assert_eq!(status.relays, Some(vec![]));

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
