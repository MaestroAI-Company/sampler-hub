//needs network for address lookup
use iroh::Endpoint;
use iroh::endpoint::{Connection, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use sampler_hub_client::{HubClient, HubError, PairedHub};
use sampler_hub_proto::HUB_ALPN;

#[derive(Debug, Clone)]
struct Idle;

impl ProtocolHandler for Idle {
    async fn accept(&self, _conn: Connection) -> Result<(), AcceptError> {
        Ok(())
    }
}

//an older hub serves fewer protocols
#[tokio::test(flavor = "multi_thread")]
async fn missing_protocol_reads_as_outdated() {
    let endpoint = Endpoint::bind(presets::N0).await.unwrap();
    endpoint.online().await;
    let router = Router::builder(endpoint).accept(HUB_ALPN, Idle).spawn();
    let hub = PairedHub {
        id: router.endpoint().id().to_string(),
        name: "old".into(),
        relays: Vec::new(),
    };

    let mut key = [0u8; 32];
    getrandom::fill(&mut key).unwrap();
    let client = HubClient::new(key.to_vec()).unwrap();
    //fresh endpoints publish their address slowly
    let mut result = client.providers(hub.clone()).await;
    for _ in 0..10 {
        if !matches!(result, Err(HubError::Unreachable(_))) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        result = client.providers(hub.clone()).await;
    }
    assert!(matches!(result, Err(HubError::Outdated)), "{result:?}");
    router.shutdown().await.unwrap();
}
