//! iroh endpoint, accept loop, and outbound connect helpers.

use std::time::Duration;

use anyhow::{Context, Result};
use iroh::{endpoint::presets, Endpoint, EndpointAddr};
use tracing::{debug, info, warn};
use wt_proto::ticket::AddrTicket;
use wt_proto::NodeId;

use crate::identity::Identity;

pub const ALPN: &[u8] = b"wt/1";

#[derive(Clone)]
pub struct Transport {
    endpoint: Endpoint,
}

impl Transport {
    /// Build the endpoint bound on a random UDP port, using the install identity.
    pub async fn bind(identity: &Identity) -> Result<Self> {
        let ep = Endpoint::builder(presets::N0)
            .secret_key(identity.secret_key().clone())
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .context("bind iroh endpoint")?;
        info!(nodeid = %identity.nodeid(), "iroh endpoint bound");
        Ok(Self { endpoint: ep })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Wait until we've established at least one relay handshake, with a timeout. Best-effort:
    /// if no relays are reachable, return Ok(()) so the daemon still serves local peers.
    pub async fn wait_online(&self, timeout: Duration) -> Result<()> {
        match tokio::time::timeout(timeout, self.endpoint.online()).await {
            Ok(_) => Ok(()),
            Err(_) => {
                warn!(
                    "endpoint did not reach a relay within {:?}, continuing offline-of-relay",
                    timeout
                );
                Ok(())
            }
        }
    }

    /// Snapshot the local endpoint's full addressing info as a pasteable ticket.
    pub fn local_ticket(&self) -> AddrTicket {
        let addr = self.endpoint.addr();
        let nodeid = NodeId(addr.id.as_bytes().to_owned());
        let relay_url = addr.relay_urls().next().map(|u| u.to_string());
        let direct_addrs: Vec<_> = addr.ip_addrs().copied().collect();
        AddrTicket {
            nodeid,
            relay_url,
            direct_addrs,
        }
    }

    /// Connect using a previously-shared `AddrTicket` (which provides explicit relay + direct
    /// addresses — no DNS lookup needed).
    pub async fn connect_ticket(&self, ticket: &AddrTicket) -> Result<iroh::endpoint::Connection> {
        let pk =
            iroh::PublicKey::from_bytes(&ticket.nodeid.0).context("build pubkey from NodeId")?;
        let mut addr = EndpointAddr::new(pk);
        if let Some(relay) = &ticket.relay_url {
            if let Ok(url) = relay.parse::<iroh::RelayUrl>() {
                addr = addr.with_relay_url(url);
            }
        }
        for sa in &ticket.direct_addrs {
            addr = addr.with_ip_addr(*sa);
        }
        debug!(target = %ticket.nodeid, "dialing peer via ticket");
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .context("iroh connect (ticket)")?;
        Ok(conn)
    }

    /// Connect by NodeId alone (relies on iroh discovery/DNS).
    pub async fn connect(&self, target: NodeId) -> Result<iroh::endpoint::Connection> {
        let pk = iroh::PublicKey::from_bytes(&target.0).context("build pubkey from NodeId")?;
        let addr = EndpointAddr::new(pk);
        debug!(target = %target, "dialing peer by nodeid (discovery)");
        let conn = self
            .endpoint
            .connect(addr, ALPN)
            .await
            .context("iroh connect")?;
        Ok(conn)
    }

    pub async fn close(&self) {
        self.endpoint.close().await;
    }
}
