//! Wire and IPC types for `wt`. No I/O. Serializable via serde + CBOR (ciborium).

pub mod ipc;
pub mod ticket;
pub mod token;
pub mod wire;

pub use ipc::{
    AgentInfo, AgentMsgKind, ConnInfo, FsMode, GroupInfo, IpcError, IpcErrorCode, IpcEvent,
    IpcRequest, PeerFilter, PeerInfo, PeerSelector, SessionInfo, TokenInfo, WhoAmIInfo,
};
pub use ticket::{AddrTicket, TicketError};
pub use token::{Cap, CapabilityToken, SignedToken, TokenId};
pub use wire::{Ack, MessageFrame, StreamOpen};

/// 32-byte Ed25519 public key. Wraps for type clarity at API boundaries.
/// Equal to iroh's EndpointId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({})", hex::encode(&self.0[..6]))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl std::str::FromStr for NodeId {
    type Err = NodeIdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s).map_err(|_| NodeIdParseError::NotHex)?;
        if bytes.len() != 32 {
            return Err(NodeIdParseError::WrongLength(bytes.len()));
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(Self(out))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeIdParseError {
    #[error("node id is not valid hex")]
    NotHex,
    #[error("node id must be 32 bytes (64 hex chars), got {0}")]
    WrongLength(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{IpcError, IpcErrorCode, IpcEvent};
    use crate::ticket::{AddrTicket, TICKET_PREFIX};
    use crate::token::{Cap, CapabilityToken, SignedToken};
    use crate::wire::{MessageFrame, StreamOpen};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn node(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    #[test]
    fn ipc_error_event_roundtrips_preserving_code_and_message() {
        let ev = IpcEvent::Err(IpcError::new(IpcErrorCode::Expired, "token expired"));
        let mut buf = Vec::new();
        ciborium::into_writer(&ev, &mut buf).unwrap();
        let decoded: IpcEvent = ciborium::from_reader(&buf[..]).unwrap();
        match decoded {
            IpcEvent::Err(e) => {
                assert_eq!(e.code, IpcErrorCode::Expired);
                assert_eq!(e.message, "token expired");
            }
            other => panic!("expected Err, got {other:?}"),
        }
        // `IpcError` is a real std::error::Error (so it flows through anyhow with its code intact).
        let dyn_err: &dyn std::error::Error = &IpcError::internal("boom");
        assert_eq!(dyn_err.to_string(), "boom");
    }

    #[test]
    fn nodeid_display_parse_roundtrips() {
        let n = node(0xab);
        let s = n.to_string();

        assert_eq!(s.len(), 64);
        assert_eq!(s.parse::<NodeId>().unwrap(), n);
        assert!("zz".parse::<NodeId>().is_err());
        assert!("abcd".parse::<NodeId>().is_err());
    }

    #[test]
    fn ticket_encode_decode_roundtrips_and_rejects_bad_inputs() {
        let ticket = AddrTicket {
            nodeid: node(7),
            relay_url: Some("https://relay.example".to_string()),
            direct_addrs: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4242)],
        };

        let encoded = ticket.encode().unwrap();
        assert!(encoded.starts_with(TICKET_PREFIX));

        let decoded = AddrTicket::decode(&format!("  {encoded}\n")).unwrap();
        assert_eq!(decoded.nodeid, ticket.nodeid);
        assert_eq!(decoded.relay_url, ticket.relay_url);
        assert_eq!(decoded.direct_addrs, ticket.direct_addrs);

        assert!(AddrTicket::decode("abc").is_err());
        assert!(AddrTicket::decode("wt1:not-valid-base32!").is_err());
        assert!(AddrTicket::decode("wt1:AA").is_err());
    }

    #[test]
    fn cap_parse_display_is_strict() {
        assert_eq!("msg".parse::<Cap>().unwrap(), Cap::Msg);
        assert_eq!(Cap::Msg.to_string(), "msg");
        assert!("MSG".parse::<Cap>().is_err());
        assert!("exec".parse::<Cap>().is_err());
    }

    #[test]
    fn cbor_wire_types_roundtrip() {
        let token = SignedToken {
            claims_cbor: vec![1, 2, 3],
            sig: vec![4; 64],
        };
        let open = StreamOpen::Msg {
            token: token.clone(),
            channel: "default".to_string(),
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&open, &mut buf).unwrap();
        let decoded: StreamOpen = ciborium::from_reader(&buf[..]).unwrap();
        match decoded {
            StreamOpen::Msg { token, channel } => {
                assert_eq!(token.sig, vec![4; 64]);
                assert_eq!(channel, "default");
            }
        }

        let msg = MessageFrame {
            seq: 99,
            ts_ms: 1234,
            payload: b"{\"user\":\"hello\"}".to_vec(),
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&msg, &mut buf).unwrap();
        let decoded: MessageFrame = ciborium::from_reader(&buf[..]).unwrap();
        assert_eq!(decoded.seq, 99);
        assert_eq!(decoded.ts_ms, 1234);
        assert_eq!(decoded.payload, msg.payload);
    }

    #[test]
    fn token_claims_roundtrip_preserves_binary_ids() {
        let claims = CapabilityToken {
            iss: node(1),
            sub: node(2),
            exp: 42,
            caps: vec![Cap::Msg],
            id: [9; 16],
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&claims, &mut buf).unwrap();
        let decoded: CapabilityToken = ciborium::from_reader(&buf[..]).unwrap();
        assert_eq!(decoded.iss, claims.iss);
        assert_eq!(decoded.sub, claims.sub);
        assert_eq!(decoded.exp, claims.exp);
        assert_eq!(decoded.caps, claims.caps);
        assert_eq!(decoded.id, claims.id);
    }
}
