//hubs announced on the same network
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mdns_sd::{ServiceDaemon, ServiceEvent};
use sampler_hub_proto::LAN_SERVICE;

pub(crate) struct Found {
    pub id: String,
    pub name: String,
    pub addrs: Vec<SocketAddr>,
}

//blocks for the whole wait
pub(crate) fn browse(wait: Duration) -> Result<Vec<Found>, String> {
    let daemon = ServiceDaemon::new().map_err(|e| e.to_string())?;
    let events = daemon.browse(LAN_SERVICE).map_err(|e| e.to_string())?;
    let deadline = Instant::now() + wait;
    let mut found: Vec<Found> = Vec::new();
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        let Ok(event) = events.recv_timeout(left) else {
            break;
        };
        let ServiceEvent::ServiceResolved(service) = event else {
            continue;
        };
        let Some(id) = service.get_property_val_str("id") else {
            continue;
        };
        let suffix = format!(".{LAN_SERVICE}");
        let name = service
            .fullname
            .strip_suffix(&suffix)
            .unwrap_or(&service.fullname)
            .to_string();
        let addrs = service
            .addresses
            .iter()
            .map(|ip| SocketAddr::new(ip.to_ip_addr(), service.port))
            .collect();
        found.retain(|hub| hub.id != id);
        found.push(Found {
            id: id.to_string(),
            name,
            addrs,
        });
    }
    let _ = daemon.shutdown();
    Ok(found)
}
