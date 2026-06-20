//! A pasteable string carrying everything a peer needs to dial this endpoint:
//! NodeId + relay URL (if known) + direct socket addresses (if known).
//!
//! Format: `wt1:` + base32(CBOR(AddrTicket)). Forward-compat lives in CBOR's optional fields.

use std::net::SocketAddr;

use data_encoding::BASE32_NOPAD;
use serde::{Deserialize, Serialize};

use crate::NodeId;

pub const TICKET_PREFIX: &str = "wt1:";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddrTicket {
    pub nodeid: NodeId,
    pub relay_url: Option<String>,
    pub direct_addrs: Vec<SocketAddr>,
}

impl AddrTicket {
    pub fn encode(&self) -> Result<String, TicketError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf).map_err(|_| TicketError::Encode)?;
        Ok(format!("{TICKET_PREFIX}{}", BASE32_NOPAD.encode(&buf)))
    }

    pub fn decode(s: &str) -> Result<Self, TicketError> {
        let s = s.trim();
        let body = s
            .strip_prefix(TICKET_PREFIX)
            .ok_or(TicketError::WrongPrefix)?;
        let bytes = BASE32_NOPAD
            .decode(body.as_bytes())
            .map_err(|_| TicketError::Base32)?;
        let t: AddrTicket = ciborium::from_reader(&bytes[..]).map_err(|_| TicketError::Decode)?;
        Ok(t)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TicketError {
    #[error("ticket missing `wt1:` prefix")]
    WrongPrefix,
    #[error("ticket body is not valid base32")]
    Base32,
    #[error("ticket CBOR decode failed")]
    Decode,
    #[error("ticket CBOR encode failed")]
    Encode,
}
