//! Local-LAN mDNS announce + browse. Service type: `_wt._udp.local.`.
//!
//! v0.2 scope: maintain a shared in-memory map of NodeId → LanPeer (last-seen address, name).
//! Other code (e.g. `wt ls --local`) reads from this map. mDNS does NOT auto-add peers to the
//! durable `peers` table — the user still pairs explicitly. The discovery layer just tells
//! the user "this NodeId is on your LAN right now."

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::sync::Mutex;
use tracing::{debug, info, warn};
use wt_proto::NodeId;

pub const SERVICE_TYPE: &str = "_wt._udp.local.";

#[derive(Debug, Clone)]
pub struct LanPeer {
    pub nodeid: NodeId,
    pub addresses: Vec<IpAddr>,
    pub port: u16,
    pub instance: String,
    pub last_seen_ms: u64,
}

pub type LanPeers = Arc<Mutex<HashMap<NodeId, LanPeer>>>;

/// Spawn the mDNS responder + browser. Returns the handle (kept so caller can shut down).
pub struct Mdns {
    daemon: ServiceDaemon,
    lan_peers: LanPeers,
    own_nodeid: NodeId,
}

impl Mdns {
    pub fn start(nodeid: NodeId, port: u16) -> Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let lan_peers: LanPeers = Arc::new(Mutex::new(HashMap::new()));

        let nodeid_hex = hex::encode(nodeid.0);
        let instance = format!("wt-{}", &nodeid_hex[..16]);
        let host = format!("{instance}.local.");
        let properties = [
            ("nodeid".to_string(), nodeid_hex.clone()),
            ("v".to_string(), "0.2".to_string()),
        ];

        // Best-effort: detect local IPs via mdns-sd's auto-addr feature.
        let info = ServiceInfo::new(SERVICE_TYPE, &instance, &host, "", port, &properties[..])?
            .enable_addr_auto();
        daemon.register(info)?;
        info!(nodeid = %nodeid, port, "mDNS service registered");

        // Start the browser task.
        let receiver = daemon.browse(SERVICE_TYPE)?;
        let peers = lan_peers.clone();
        let me = nodeid;
        tokio::task::spawn_blocking(move || browse_loop(receiver, peers, me));

        Ok(Self {
            daemon,
            lan_peers,
            own_nodeid: nodeid,
        })
    }

    pub fn lan_peers(&self) -> LanPeers {
        self.lan_peers.clone()
    }

    pub fn own_nodeid(&self) -> NodeId {
        self.own_nodeid
    }

    pub fn shutdown(&self) {
        // mdns-sd returns a `flume::Receiver` for shutdown ack; we don't need to wait on it.
        if let Err(e) = self.daemon.shutdown() {
            warn!(?e, "mdns shutdown error");
        }
    }
}

fn browse_loop(receiver: mdns_sd::Receiver<ServiceEvent>, peers: LanPeers, own: NodeId) {
    while let Ok(event) = receiver.recv_timeout(Duration::from_secs(30)) {
        match event {
            ServiceEvent::ServiceResolved(svc) => {
                let Some(nodeid_str) = svc
                    .txt_properties
                    .iter()
                    .find(|p| p.key() == "nodeid")
                    .map(|p| p.val_str().to_string())
                else {
                    continue;
                };
                let Ok(nodeid) = nodeid_str.parse::<NodeId>() else {
                    debug!(?nodeid_str, "ignoring mdns peer with malformed nodeid TXT");
                    continue;
                };
                if nodeid == own {
                    continue; // skip self
                }
                let lp = LanPeer {
                    nodeid,
                    addresses: svc.addresses.iter().map(|s| s.to_ip_addr()).collect(),
                    port: svc.port,
                    instance: svc.fullname.clone(),
                    last_seen_ms: now_ms(),
                };
                debug!(peer = %nodeid, addrs = ?lp.addresses, "mdns peer resolved");
                if let Ok(mut map) = peers.lock() {
                    map.insert(nodeid, lp);
                }
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                if let Ok(mut map) = peers.lock() {
                    map.retain(|_, v| v.instance != fullname);
                }
            }
            _ => {}
        }
    }
    debug!("mdns browse loop exiting");
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
