//needs network for address lookup
use std::sync::Arc;
use std::time::Duration;

use sampler_hub_client::{HubClient, HubError, PairedHub, SyncRecord};

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
    let link = sampler_hub::hub::pairing_link(data_dir).unwrap().to_string();
    //fresh hubs publish their address slowly
    for _ in 0..10 {
        match client
            .pair(link.clone(), name.into(), "phone".into(), None)
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

fn record(id: &str, updated_at: u64, payload: String) -> SyncRecord {
    SyncRecord {
        id: id.into(),
        updated_at,
        deleted: false,
        payload: Some(payload),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn data_sync_and_watch() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-sync-{}", random_hex::<8>()));
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
    let laptop = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    pair(&laptop, &data_dir, "laptop").await;

    //the laptop wakes up as soon as the phone writes
    let watcher = tokio::spawn({
        let laptop = laptop.clone();
        let hub_ref = hub_ref.clone();
        async move { laptop.watch(hub_ref, 0).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    phone
        .push(
            hub_ref.clone(),
            "conversations".into(),
            vec![record("conv", 10, r#"{"name":"hello"}"#.into())],
        )
        .await
        .unwrap();
    let cursor = watcher.await.unwrap().unwrap();
    assert!(cursor > 0);
    let page = laptop
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    //larger than the old 1 mib frame limit
    let image = "a".repeat(3 * 1024 * 1024);
    phone
        .push(
            hub_ref.clone(),
            "blobs".into(),
            vec![record("img", 1, image.clone())],
        )
        .await
        .unwrap();
    let page = laptop
        .pull(hub_ref.clone(), "blobs".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records[0].payload.as_deref(), Some(image.as_str()));

    //cloud accounts no longer sync
    assert!(phone
        .push(
            hub_ref.clone(),
            "accounts".into(),
            vec![record("cloud", 5, r#"{"provider":"nextcloud"}"#.into())],
        )
        .await
        .is_err());

    //the hub lists the same id
    let devices = laptop.devices(hub_ref.clone()).await.unwrap();
    assert!(devices.iter().any(|d| d.id == phone.device_id()));

    //a browser reads the list without admin rights
    let browser = new_client();
    let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();
    browser
        .pair(link, "browser".into(), "web".into(), None)
        .await
        .unwrap();
    join(&data_dir, &browser);
    assert_eq!(browser.devices(hub_ref.clone()).await.unwrap().len(), 3);

    //a device without profile cannot sync
    let store = sampler_hub::store::Store::new(&data_dir);
    let flatmate = store.create_profile("flatmate").unwrap();
    let stranger = new_client();
    let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();
    stranger
        .pair(link, "stranger".into(), "phone".into(), None)
        .await
        .unwrap();
    assert!(matches!(
        stranger
            .pull(hub_ref.clone(), "conversations".into(), 0)
            .await,
        Err(HubError::Locked)
    ));
    stranger
        .set_profile(flatmate.id.clone(), test_key(&flatmate.id))
        .unwrap();
    assert!(matches!(
        stranger
            .pull(hub_ref.clone(), "conversations".into(), 0)
            .await,
        Err(HubError::Unauthorized)
    ));

    //another profile sees its own data only
    assert!(
        store
            .bind_device(&stranger.device_id(), &flatmate.id)
            .unwrap()
    );
    assert_eq!(
        stranger.watch(hub_ref.clone(), 0).await.unwrap(),
        0,
        "the phone writes never wake another profile"
    );
    stranger
        .push(
            hub_ref.clone(),
            "conversations".into(),
            vec![record("conv", 20, r#"{"name":"theirs"}"#.into())],
        )
        .await
        .unwrap();
    let theirs = stranger
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(theirs.records.len(), 1);
    assert_eq!(
        theirs.records[0].payload.as_deref(),
        Some(r#"{"name":"theirs"}"#)
    );
    let mine = laptop
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(
        mine.records[0].payload.as_deref(),
        Some(r#"{"name":"hello"}"#)
    );
    //a member lists its own devices only
    let listed = stranger.devices(hub_ref.clone()).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].profile.as_deref(), Some(flatmate.id.as_str()));

    let unknown = laptop.pull(hub_ref.clone(), "nope".into(), 0).await;
    assert!(matches!(unknown, Err(HubError::Internal(_))), "{unknown:?}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_by_code() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-code-{}", random_hex::<8>()));
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

    //a paired phone asks the hub for a code
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    let invite = phone.invite(hub_ref.clone()).await.unwrap();

    //a wrong code never reaches the invite
    let stranger = new_client();
    assert!(
        stranger
            .pair_with_code("0000-0000".into(), "stranger".into(), "web".into(), None)
            .await
            .is_err()
    );

    //typed in lower case with spaces
    let browser = new_client();
    let typed = invite.code.to_lowercase().replace('-', " ");
    let paired = browser
        .pair_with_code(typed, "browser".into(), "web".into(), None)
        .await
        .unwrap();
    assert_eq!(paired.id, hub_ref.id);
    let devices = browser.devices(paired).await.unwrap();
    assert!(devices.iter().any(|d| d.id == browser.device_id()));

    //a reinstall pairs again with a new key from the same machine
    let old_install = new_client();
    let new_install = new_client();
    for install in [&old_install, &new_install] {
        let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();
        install
            .pair(
                link,
                "tablet".into(),
                "phone".into(),
                Some("same-os-id".into()),
            )
            .await
            .unwrap();
    }
    let ids: Vec<String> = phone
        .devices(hub_ref.clone())
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.id)
        .collect();
    assert!(ids.contains(&new_install.device_id()) && !ids.contains(&old_install.device_id()));

    //the code works once
    let late = new_client();
    assert!(
        late.pair_with_code(invite.code, "late".into(), "web".into(), None)
            .await
            .is_err()
    );

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn hub_settings_and_lan() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-lan-{}", random_hex::<8>()));
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
    let hub_ref = pair(&phone, &data_dir, "phone").await;

    let mut settings = phone.hub_settings(hub_ref.clone()).await.unwrap();
    assert!(settings.lan_discovery && settings.share_providers);
    settings.share_providers = false;
    settings.lan_discovery = false;
    let saved = phone
        .set_hub_settings(hub_ref.clone(), settings.clone())
        .await
        .unwrap();
    assert!(!saved.share_providers);
    assert!(phone.providers(hub_ref.clone()).await.unwrap().is_empty());

    //a browser has no admin rights
    let browser = new_client();
    let link = sampler_hub::hub::pairing_link(&data_dir).unwrap().to_string();
    browser
        .pair(link, "browser".into(), "web".into(), None)
        .await
        .unwrap();
    assert!(matches!(
        browser.hub_settings(hub_ref.clone()).await,
        Err(HubError::Unauthorized)
    ));

    //it still renames itself, admins rename and remove anyone
    browser
        .rename_device(hub_ref.clone(), browser.device_id(), "  my tab  ".into())
        .await
        .unwrap();
    phone
        .rename_device(hub_ref.clone(), phone.device_id(), "pocket".into())
        .await
        .unwrap();
    let names: Vec<String> = phone
        .devices(hub_ref.clone())
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert!(names.contains(&"my tab".to_string()) && names.contains(&"pocket".to_string()));
    phone
        .remove_device(hub_ref.clone(), browser.device_id())
        .await
        .unwrap();
    //its open connection is refused on the next request
    assert!(matches!(
        browser.devices(hub_ref.clone()).await,
        Err(HubError::Unauthorized)
    ));

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}

//ci runners often block multicast, run with --ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs multicast on the local network"]
async fn nearby_hubs_on_lan() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-mdns-{}", random_hex::<8>()));
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

    //announced by default
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    let nearby = phone.nearby_hubs(3000).await.unwrap();
    assert!(nearby.iter().any(|h| h.id == hub_ref.id), "{nearby:?}");

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
