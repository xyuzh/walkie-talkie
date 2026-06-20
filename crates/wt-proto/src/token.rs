//! Capability tokens. CBOR-encoded claims, Ed25519-signed by the issuer.

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// One capability granted by a token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cap {
    /// Open `Msg` streams to the issuer.
    Msg,
    // v0.3+: ExecRun(Vec<String>), ExecShell,
    // v0.4+: PtyAllocate,
}

impl std::str::FromStr for Cap {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "msg" => Ok(Cap::Msg),
            other => Err(format!("unknown capability: {other}")),
        }
    }
}

impl std::fmt::Display for Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Cap::Msg => f.write_str("msg"),
        }
    }
}

/// 16-byte unique identifier for a token. Used as the revocation key.
pub type TokenId = [u8; 16];

/// The claims body that gets CBOR-encoded and signed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityToken {
    pub iss: NodeId,
    pub sub: NodeId,
    /// Unix epoch seconds.
    pub exp: u64,
    pub caps: Vec<Cap>,
    pub id: TokenId,
}

/// A signed token = CBOR claims + Ed25519 signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedToken {
    /// CBOR-encoded `CapabilityToken`.
    pub claims_cbor: Vec<u8>,
    /// 64-byte Ed25519 signature over `claims_cbor`.
    pub sig: Vec<u8>,
}
