//needs network for address lookup
use sampler_hub_client::{HubClient, HubError};

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).unwrap();
    bytes
}

#[tokio::test(flavor = "multi_thread")]
async fn pair_status_unpair() {
    let suffix: String = random_bytes::<8>()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-e2e-{suffix}"));
    let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();

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
    let status = client.status(paired.clone()).await.unwrap();
    assert_eq!(status.name, paired.name);
    let devices = client.devices(paired.clone()).await.unwrap();
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].kind, "phone");
    assert!(devices[0].scopes.iter().any(|scope| scope == "admin"));

    //the secret is one time
    let other = HubClient::new(random_bytes::<32>().to_vec()).unwrap();
    let again = other
        .pair(link, "intruder".into(), "phone".into(), None)
        .await;
    assert!(matches!(again, Err(HubError::PairingRejected)), "{again:?}");
    assert!(matches!(
        other.status(paired.clone()).await,
        Err(HubError::Unauthorized)
    ));
    assert!(matches!(
        other.devices(paired.clone()).await,
        Err(HubError::Unauthorized)
    ));

    client.unpair(paired.clone()).await.unwrap();
    assert!(matches!(
        client.status(paired).await,
        Err(HubError::Unauthorized)
    ));

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
