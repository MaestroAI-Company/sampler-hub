use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use iroh::Endpoint;
use iroh::endpoint::{Connection, presets};
use sampler_hub_proto::invite::{self, INVITE_ALPN, InviteKeys, InviteRequest, InviteResponse};
use sampler_hub_proto::{PairingLink, read_frame, write_frame};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::config::Config;
use crate::hub;
use crate::store::PAIRING_TTL;

//wrong codes before the invite closes
const MAX_ATTEMPTS: u32 = 5;
//a silent device must not hold the code
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Invite {
    pub code: String,
    pub link: PairingLink,
    //true once a device used the code
    pub done: oneshot::Receiver<bool>,
}

//a fresh pairing secret, reachable by its code too
pub async fn open(data_dir: &Path) -> Result<Invite> {
    open_for(data_dir, None).await
}

pub async fn open_for(data_dir: &Path, device_link: Option<&str>) -> Result<Invite> {
    let config = Config::load(data_dir)?;
    let link = hub::pairing_link_for(data_dir, device_link)?;
    let link_text = link.to_string();
    let code = invite::new_code()?;
    //argon2 blocks for a moment
    let keys = tokio::task::spawn_blocking({
        let code = code.clone();
        move || invite::derive(&code)
    })
    .await??;
    let endpoint = Endpoint::builder(presets::N0)
        .relay_mode(config.relay_mode())
        .secret_key(keys.endpoint.clone())
        .alpns(vec![INVITE_ALPN.to_vec()])
        .bind()
        .await?;
    let (finished, done) = oneshot::channel();
    tokio::spawn(async move {
        let used = serve(endpoint, keys, link_text).await;
        let _ = finished.send(used);
    });
    Ok(Invite { code, link, done })
}

//until used, expired or guessed at too often
async fn serve(endpoint: Endpoint, keys: InviteKeys, link: String) -> bool {
    let expiry = tokio::time::sleep(PAIRING_TTL);
    tokio::pin!(expiry);
    let mut attempts = 0;
    let mut used = false;
    loop {
        let incoming = tokio::select! {
            _ = &mut expiry => break,
            incoming = endpoint.accept() => incoming,
        };
        let Some(incoming) = incoming else { break };
        let exchange = async {
            let conn = incoming.await?;
            let granted = answer(&conn, &keys, &link).await?;
            anyhow::Ok((conn, granted))
        };
        match tokio::time::timeout(EXCHANGE_TIMEOUT, exchange)
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out")))
        {
            Ok((conn, true)) => {
                info!("pairing code used by {}", conn.remote_id().fmt_short());
                //let the answer reach the device
                tokio::time::timeout(Duration::from_secs(5), conn.closed())
                    .await
                    .ok();
                used = true;
                break;
            }
            Ok((_, false)) => {
                attempts += 1;
                if attempts >= MAX_ATTEMPTS {
                    warn!("pairing code closed after {attempts} wrong proofs");
                    break;
                }
            }
            Err(e) => warn!("pairing code exchange failed: {e:#}"),
        }
    }
    endpoint.close().await;
    used
}

async fn answer(conn: &Connection, keys: &InviteKeys, link: &str) -> Result<bool> {
    let (mut send, mut recv) = conn.accept_bi().await?;
    let Some(InviteRequest { proof }) = read_frame(&mut recv).await? else {
        return Ok(false);
    };
    let granted = keys.verify(&conn.remote_id(), &proof);
    let response = if granted {
        InviteResponse::Link { link: link.into() }
    } else {
        InviteResponse::Rejected
    };
    write_frame(&mut send, &response).await?;
    send.finish()?;
    Ok(granted)
}
