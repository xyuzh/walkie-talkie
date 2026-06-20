//! Wire frames carried on iroh bidi streams under the `wt/1` ALPN.

use serde::{Deserialize, Serialize};

use crate::token::SignedToken;

/// First frame on any new bidi stream. Identifies the logical service for this stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOpen {
    /// Agent message stream. `channel` is a free-form label.
    Msg { token: SignedToken, channel: String },
    // v0.3+: Exec { token, argv, env }
    // v0.4+: Pty  { token, term, rows, cols }
    // later:  File { token, op }
}

/// One agent message. `payload` is opaque to `wt` (typically UTF-8 JSON like `{"user": "..."}`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageFrame {
    /// Per-stream monotonic sequence number, set by the sender.
    pub seq: u64,
    /// Unix epoch milliseconds at send time on the originator.
    pub ts_ms: u64,
    /// Application bytes. `wt` does not parse this.
    pub payload: Vec<u8>,
}

/// Acknowledgement of a `MessageFrame`. Sent on the reverse direction of the same bidi stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub seq: u64,
}
