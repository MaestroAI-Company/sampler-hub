//hubs announce themselves to devices on the same network
use std::collections::HashMap;

use anyhow::{Result, anyhow};
use mdns_sd::{ServiceDaemon, ServiceInfo};
use sampler_hub_proto::LAN_SERVICE;
use tracing::{info, warn};

pub struct Announcement {
    daemon: ServiceDaemon,
    fullname: String,
}

//devices still need a code to pair
pub fn announce(id: &str, name: &str, port: u16) -> Result<Announcement> {
    let daemon = ServiceDaemon::new().map_err(|e| anyhow!("mdns: {e}"))?;
    let host = format!("sampler-hub-{}.local.", &id[..id.len().min(12)]);
    let properties = HashMap::from([("id".to_string(), id.to_string())]);
    let service = ServiceInfo::new(LAN_SERVICE, name, &host, "", port, properties)
        .map_err(|e| anyhow!("mdns: {e}"))?
        .enable_addr_auto();
    let fullname = service.get_fullname().to_string();
    daemon.register(service).map_err(|e| anyhow!("mdns: {e}"))?;
    info!("announced on the local network as {name}");
    Ok(Announcement { daemon, fullname })
}

impl Drop for Announcement {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.fullname) {
            warn!("mdns unregister failed: {e}");
        }
        let _ = self.daemon.shutdown();
    }
}
