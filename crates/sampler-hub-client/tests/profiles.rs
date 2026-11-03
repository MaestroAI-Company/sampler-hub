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
            result => return result.unwrap(),
        }
    }
    panic!("hub never became reachable");
}

//a fresh code publishes its address slowly
async fn pair_code(client: &HubClient, code: &str, name: &str) {
    for _ in 0..10 {
        match client
            .pair_with_code(code.into(), name.into(), "desktop".into(), None)
            .await
        {
            Err(HubError::Unreachable(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
            result => return result.map(|_| ()).unwrap(),
        }
    }
    panic!("code never became reachable");
}

fn record(id: &str, payload: &str) -> SyncRecord {
    SyncRecord {
        id: id.into(),
        updated_at: 10,
        deleted: false,
        payload: Some(payload.into()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn profiles_lock_and_link() {
    let data_dir = std::env::temp_dir().join(format!("sampler-hub-profiles-{}", random_hex::<8>()));
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

    //the first profile manages the hub
    let phone = new_client();
    let hub_ref = pair(&phone, &data_dir, "phone").await;
    assert!(phone.profiles(hub_ref.clone()).await.unwrap().is_empty());
    let owner = phone
        .create_profile(hub_ref.clone(), "Charly".into(), "correct horse".into())
        .await
        .unwrap();
    assert_eq!(owner.profile.role, "admin");
    //a cloud backup reopens the key with the password alone
    let lock = phone.locked_key(hub_ref.clone()).await.unwrap();
    let reopened = sampler_hub_client::open_locked_key(lock.clone(), "correct horse".into()).unwrap();
    assert_eq!(reopened, owner.key);
    assert!(sampler_hub_client::open_locked_key(lock, "wrong".into()).is_err());
    phone
        .push(
            hub_ref.clone(),
            "conversations".into(),
            vec![record("conv", r#"{"title":"secret plans"}"#)],
        )
        .await
        .unwrap();

    //the hub keeps no clear payload
    let raw = std::fs::read(data_dir.join("hub.db")).unwrap();
    assert!(!raw.windows(12).any(|w| w == b"secret plans"));

    //a reinstall comes back with the password
    let reinstall = new_client();
    pair(&reinstall, &data_dir, "reinstall").await;
    let wrong = reinstall
        .unlock_profile(hub_ref.clone(), owner.profile.id.clone(), "wrong".into())
        .await;
    assert!(wrong.is_err(), "{wrong:?}");
    let back = reinstall
        .unlock_profile(
            hub_ref.clone(),
            owner.profile.id.clone(),
            "correct horse".into(),
        )
        .await
        .unwrap();
    assert_eq!(back.key, owner.key);
    let page = reinstall
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records[0].id, "conv");
    assert_eq!(
        page.records[0].payload.as_deref(),
        Some(r#"{"title":"secret plans"}"#)
    );

    //a flatmate is a member with its own data
    let flatmate = new_client();
    pair(&flatmate, &data_dir, "flatmate").await;
    let member = flatmate
        .create_profile(hub_ref.clone(), "Ethan".into(), "battery staple".into())
        .await
        .unwrap();
    assert_eq!(member.profile.role, "member");
    let theirs = flatmate
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert!(theirs.records.is_empty());
    assert!(flatmate.invite(hub_ref.clone()).await.is_err());

    //the card look reaches the other profiles
    let styled = flatmate
        .set_profile_butterfly(hub_ref.clone(), "butterfly3".into())
        .await
        .unwrap();
    assert_eq!(styled.butterfly.as_deref(), Some("butterfly3"));
    let listed = phone.profiles(hub_ref.clone()).await.unwrap();
    let theirs_card = listed.iter().find(|p| p.id == member.profile.id).unwrap();
    assert_eq!(theirs_card.butterfly.as_deref(), Some("butterfly3"));
    assert!(
        flatmate
            .set_profile_butterfly(hub_ref.clone(), "<script>".into())
            .await
            .is_err()
    );

    //members see neither other profiles nor performance
    let seen = flatmate.profiles(hub_ref.clone()).await.unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].id, member.profile.id);
    assert!(flatmate.status(hub_ref.clone()).await.unwrap().memory.is_none());
    assert_eq!(phone.profiles(hub_ref.clone()).await.unwrap().len(), 2);
    assert!(phone.status(hub_ref.clone()).await.unwrap().memory.is_some());

    //nearby hub signs in without code
    let nearby = sampler_hub_client::NearbyHub {
        id: hub_ref.id.clone(),
        name: hub_ref.name.clone(),
        addrs: Vec::new(),
    };
    let visitor = new_client();
    //a hidden hub only pairs with a code
    let mut settings = phone.hub_settings(hub_ref.clone()).await.unwrap();
    settings.lan_discovery = false;
    phone
        .set_hub_settings(hub_ref.clone(), settings.clone())
        .await
        .unwrap();
    assert!(visitor.nearby_profiles(nearby.clone()).await.is_err());
    settings.lan_discovery = true;
    phone
        .set_hub_settings(hub_ref.clone(), settings)
        .await
        .unwrap();
    assert_eq!(visitor.nearby_profiles(nearby.clone()).await.unwrap().len(), 2);
    let wrong = visitor
        .sign_in(
            nearby.clone(),
            owner.profile.id.clone(),
            "wrong".into(),
            "visitor".into(),
            "phone".into(),
            None,
        )
        .await;
    assert!(wrong.is_err(), "{wrong:?}");
    assert!(matches!(
        visitor.profiles(hub_ref.clone()).await,
        Err(HubError::Unauthorized)
    ));
    let signed = visitor
        .sign_in(
            nearby,
            owner.profile.id.clone(),
            "correct horse".into(),
            "visitor".into(),
            "phone".into(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(signed.profile.key, owner.key);
    let page = visitor
        .pull(signed.hub.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records[0].id, "conv");

    //too many wrong passwords lock the profile
    let guesser = new_client();
    pair(&guesser, &data_dir, "guesser").await;
    for _ in 0..5 {
        let _ = guesser
            .unlock_profile(hub_ref.clone(), member.profile.id.clone(), "guess".into())
            .await;
    }
    let locked = guesser
        .unlock_profile(
            hub_ref.clone(),
            member.profile.id.clone(),
            "battery staple".into(),
        )
        .await;
    assert!(
        matches!(&locked, Err(HubError::Internal(m)) if m.contains("too many")),
        "{locked:?}"
    );

    //a link code hands the key over
    let invite = phone.link_start(hub_ref.clone()).await.unwrap();
    let laptop = new_client();
    pair_code(&laptop, &invite.code, "laptop").await;
    laptop.link_join(hub_ref.clone()).await.unwrap();
    let joining = loop {
        if let Some(joining) = phone.link_watch(hub_ref.clone()).await.unwrap() {
            break joining;
        }
    };
    assert_eq!(joining.device, "laptop");
    let waiting = laptop.link_fetch(hub_ref.clone()).await.unwrap();
    assert_eq!(waiting.digits.as_deref(), Some(joining.digits.as_str()));
    assert!(waiting.profile.is_none());
    phone.link_approve(hub_ref.clone()).await.unwrap();
    let linked = laptop.link_fetch(hub_ref.clone()).await.unwrap();
    let linked = linked.profile.expect("the key arrived");
    assert_eq!(linked.key, owner.key);
    let page = laptop
        .pull(hub_ref.clone(), "conversations".into(), 0)
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);

    //a refused link unpairs the device
    let invite = phone.link_start(hub_ref.clone()).await.unwrap();
    let intruder = new_client();
    pair_code(&intruder, &invite.code, "intruder").await;
    intruder.link_join(hub_ref.clone()).await.unwrap();
    while phone.link_watch(hub_ref.clone()).await.unwrap().is_none() {}
    phone.link_deny(hub_ref.clone()).await.unwrap();
    let denied = intruder.link_fetch(hub_ref.clone()).await;
    assert!(
        matches!(&denied, Ok(progress) if progress.denied) || denied.is_err(),
        "{denied:?}"
    );
    assert!(matches!(
        intruder.profiles(hub_ref.clone()).await,
        Err(HubError::Unauthorized)
    ));

    //a new password keeps the same key
    phone
        .change_password(hub_ref.clone(), "correct horse".into(), "new horse".into())
        .await
        .unwrap();
    let again = new_client();
    pair(&again, &data_dir, "again").await;
    let unlocked = again
        .unlock_profile(
            hub_ref.clone(),
            owner.profile.id.clone(),
            "new horse".into(),
        )
        .await
        .unwrap();
    assert_eq!(unlocked.key, owner.key);

    //the last admin stays, a removed member loses access
    assert!(
        phone
            .set_profile_role(hub_ref.clone(), owner.profile.id.clone(), "member".into())
            .await
            .is_err()
    );
    phone
        .remove_profile(hub_ref.clone(), member.profile.id.clone())
        .await
        .unwrap();
    assert!(matches!(
        flatmate
            .pull(hub_ref.clone(), "conversations".into(), 0)
            .await,
        Err(HubError::Unauthorized)
    ));

    stop_tx.send(()).unwrap();
    hub.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&data_dir);
}
