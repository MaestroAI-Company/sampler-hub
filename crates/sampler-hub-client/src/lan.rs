//hubs announced on the same network
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
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
    let (multicast, direct) = std::thread::scope(|scope| {
        let direct = scope.spawn(|| browse_direct(wait));
        (browse_multicast(wait), direct.join().unwrap_or_default())
    });
    let mut found = multicast?;
    for hub in direct {
        if !found.iter().any(|known| known.id == hub.id) {
            found.push(hub);
        }
    }
    Ok(found)
}

fn browse_multicast(wait: Duration) -> Result<Vec<Found>, String> {
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

//some wifi drops multicast replies
fn browse_direct(wait: Duration) -> Vec<Found> {
    let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) else {
        return Vec::new();
    };
    let query = ptr_query(LAN_SERVICE);
    let group = SocketAddr::from((Ipv4Addr::new(224, 0, 0, 251), 5353));
    let deadline = Instant::now() + wait;
    let mut next_query = Instant::now();
    let mut found: Vec<Found> = Vec::new();
    let mut buf = [0u8; 9000];
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        if Instant::now() >= next_query {
            let _ = socket.send_to(&query, group);
            next_query = Instant::now() + Duration::from_secs(1);
        }
        let until_query = next_query.saturating_duration_since(Instant::now());
        let timeout = left.min(until_query).max(Duration::from_millis(10));
        let _ = socket.set_read_timeout(Some(timeout));
        let Ok((len, _)) = socket.recv_from(&mut buf) else {
            continue;
        };
        for hub in parse_reply(&buf[..len]).unwrap_or_default() {
            found.retain(|known| known.id != hub.id);
            found.push(hub);
        }
    }
    found
}

fn ptr_query(service: &str) -> Vec<u8> {
    let mut packet = vec![0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in service.split('.').filter(|label| !label.is_empty()) {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.extend_from_slice(&[0, 0, 12, 0, 1]);
    packet
}

enum Record {
    Ptr(String),
    Srv(u16, String),
    Txt(Vec<String>),
    Ip(IpAddr),
}

fn parse_reply(packet: &[u8]) -> Option<Vec<Found>> {
    let count = |at: usize| Some(u16::from_be_bytes([*packet.get(at)?, *packet.get(at + 1)?]));
    let questions = count(4)?;
    let records = count(6)? as usize + count(8)? as usize + count(10)? as usize;
    let mut at = 12;
    for _ in 0..questions {
        at = read_name(packet, at)?.1 + 4;
    }
    let mut all: Vec<(String, Record)> = Vec::new();
    for _ in 0..records {
        let (owner, end) = read_name(packet, at)?;
        let kind = count(end)?;
        let len = count(end + 8)? as usize;
        let data = end + 10;
        let rdata = packet.get(data..data + len)?;
        let record = match kind {
            12 => Some(Record::Ptr(read_name(packet, data)?.0)),
            33 if len >= 6 => Some(Record::Srv(
                count(data + 4)?,
                read_name(packet, data + 6)?.0,
            )),
            16 => Some(Record::Txt(read_txt(rdata))),
            1 if len == 4 => Some(Record::Ip(
                Ipv4Addr::new(rdata[0], rdata[1], rdata[2], rdata[3]).into(),
            )),
            28 if len == 16 => Some(Record::Ip(
                Ipv6Addr::from(<[u8; 16]>::try_from(rdata).ok()?).into(),
            )),
            _ => None,
        };
        if let Some(record) = record {
            all.push((owner.to_ascii_lowercase(), record));
        }
        at = data + len;
    }
    let mut ips: HashMap<&str, Vec<IpAddr>> = HashMap::new();
    for (owner, record) in &all {
        if let Record::Ip(ip) = record {
            ips.entry(owner.as_str()).or_default().push(*ip);
        }
    }
    let suffix = format!(".{LAN_SERVICE}");
    let mut found = Vec::new();
    for (_, record) in &all {
        let Record::Ptr(instance) = record else {
            continue;
        };
        let key = instance.to_ascii_lowercase();
        let mut srv = None;
        let mut id = None;
        for (_, record) in all.iter().filter(|(owner, _)| *owner == key) {
            match record {
                Record::Srv(port, target) => srv = Some((*port, target.to_ascii_lowercase())),
                Record::Txt(entries) => {
                    id = entries
                        .iter()
                        .find_map(|entry| entry.strip_prefix("id="))
                        .map(str::to_string)
                }
                _ => {}
            }
        }
        let (Some((port, target)), Some(id)) = (srv, id) else {
            continue;
        };
        let addrs = ips
            .get(target.as_str())
            .into_iter()
            .flatten()
            .map(|ip| SocketAddr::new(*ip, port))
            .collect();
        let name = instance
            .strip_suffix(&suffix)
            .unwrap_or(instance)
            .to_string();
        found.push(Found { id, name, addrs });
    }
    Some(found)
}

//names may point back into the packet
fn read_name(packet: &[u8], mut at: usize) -> Option<(String, usize)> {
    let mut labels: Vec<String> = Vec::new();
    let mut end = None;
    for _ in 0..128 {
        let len = *packet.get(at)? as usize;
        if len == 0 {
            labels.push(String::new());
            return Some((labels.join("."), end.unwrap_or(at + 1)));
        }
        if len & 0xc0 == 0xc0 {
            end.get_or_insert(at + 2);
            at = ((len & 0x3f) << 8) | *packet.get(at + 1)? as usize;
            continue;
        }
        let label = packet.get(at + 1..at + 1 + len)?;
        labels.push(String::from_utf8_lossy(label).into_owned());
        at += 1 + len;
    }
    None
}

fn read_txt(mut data: &[u8]) -> Vec<String> {
    let mut entries = Vec::new();
    while let Some((&len, rest)) = data.split_first() {
        let Some(entry) = rest.get(..len as usize) else {
            break;
        };
        entries.push(String::from_utf8_lossy(entry).into_owned());
        data = &rest[len as usize..];
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_unicast_reply() {
        let reply = b"\x124\x84\x00\x00\x01\x00\x01\x00\x00\x00\x03\x0c_sampler-hub\x04_udp\x05local\x00\x00\x0c\x00\x01\xc0\x0c\x00\x0c\x00\x01\x00\x00\x00\x0a\x00\x0a\x07maestro\xc0\x0c\xc05\x00!\x00\x01\x00\x00\x00\x0a\x00!\x00\x00\x00\x00\x90\xfb\x18sampler-hub-4db7e6ecf835\xc0\x1e\xc05\x00\x10\x00\x01\x00\x00\x00\x0a\x00DCid=4db7e6ecf835de685c2b308ac9ef926bc17c0b74726b2e1c59fc74c5510b4a1c\xc0Q\x00\x01\x00\x01\x00\x00\x00\x0a\x00\x04\xc0\xa8\x01:";
        let found = parse_reply(reply).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "maestro");
        assert!(found[0].id.starts_with("4db7e6ecf835"));
        assert_eq!(found[0].addrs, vec!["192.168.1.58:37115".parse().unwrap()]);
    }
}
