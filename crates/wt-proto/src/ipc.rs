//! Frames exchanged between the CLI and the daemon over a Unix domain socket.

use serde::{Deserialize, Serialize};

use crate::token::{Cap, TokenId};
use crate::NodeId;

/// How a CLI command refers to a peer: by short name or by NodeId.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerSelector {
    Name(String),
    NodeId(NodeId),
}

/// Stable machine-readable failure category for an IPC request. The CLI maps these to exit
/// codes; the human `message` carries the detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IpcErrorCode {
    /// Caller is not authorized (bad/forged token, wrong issuer/subject, unknown peer).
    Unauthorized,
    /// Token has expired.
    Expired,
    /// Token has been revoked.
    Revoked,
    /// Named/identified peer is not in the registry.
    PeerNotKnown,
    /// Requested entity (peer, token, …) was not found.
    NotFound,
    /// Malformed or invalid request.
    BadRequest,
    /// Feature exists in the protocol but isn't implemented yet.
    Unimplemented,
    /// Unexpected internal failure.
    Internal,
}

/// A structured IPC error: a stable `code` plus a human-readable `message`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: IpcErrorCode,
    pub message: String,
}

impl IpcError {
    pub fn new(code: IpcErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(IpcErrorCode::Internal, message)
    }
}

impl std::fmt::Display for IpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for IpcError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerFilter {
    All,
    Connected,
    Remote,
    Local, // mDNS — v0.2+; empty in v0.1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub nodeid: NodeId,
    pub name: String,
    pub source: PeerSource,
    pub state: PeerState,
    pub last_seen_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerSource {
    Manual,
    Mdns, // v0.2+
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerState {
    Connected { open_streams: u32 },
    Idle,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnInfo {
    pub peer_name: String,
    pub nodeid: NodeId,
    pub since_ms: u64,
    pub streams: Vec<String>,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rtt_us: Option<u64>,
    pub via_relay: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub id: TokenId,
    pub iss: NodeId,
    pub sub: NodeId,
    pub exp: u64,
    pub caps: Vec<Cap>,
    pub revoked: bool,
}

// ===== v3 local orchestration: groups / sessions / agents =====

/// Per-session workspace provisioning mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsMode {
    /// Git worktree of the base repo on a fresh branch.
    Worktree,
    /// A fresh empty folder.
    New,
}

/// Kind of a message on the local agent bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentMsgKind {
    /// A supervised child finished a turn; payload is its output (child → prime).
    TurnOutput,
    /// The prime's reply, fed to the child as its next turn (prime → child).
    TurnInput,
    /// Free-form message between agents in a group.
    User,
    /// Lifecycle/control notice (e.g. child exited).
    Control,
    /// A child's intermediate assistant text, forwarded for audit (child → prime). Only emitted
    /// when the session was spawned with `--trace`.
    Trace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    pub name: String,
    pub created_at_ms: u64,
    pub session_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub group: String,
    pub name: String,
    pub child_agent: String,
    pub fs_mode: FsMode,
    pub workspace_path: String,
    pub branch: Option<String>,
    pub status: String,
    /// The child agent's runtime status (running / awaiting_input / exited), if known.
    pub child_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub group: String,
    pub name: String,
    pub role: String,
    pub status: String,
    pub dir: Option<String>,
    pub pid: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoAmIInfo {
    pub group: String,
    pub agent: String,
    pub role: String,
    pub session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcRequest {
    Status,
    NodeId,
    /// Return this daemon's full `AddrTicket` (NodeId + relay + direct addrs).
    Ticket,
    PeerAdd {
        nodeid: NodeId,
        name: String,
        addr_blob: Option<Vec<u8>>,
    },
    PeerRm {
        selector: PeerSelector,
    },
    PeerList {
        filter: PeerFilter,
    },
    ConnList,
    ConnClose {
        selector: PeerSelector,
    },
    TokenGrant {
        peer: PeerSelector,
        caps: Vec<Cap>,
        ttl_secs: Option<u64>,
    },
    TokenImport {
        raw: Vec<u8>,
    },
    TokenList,
    TokenRevoke {
        id: TokenId,
    },
    Send {
        peer: PeerSelector,
        channel: String,
        payload: Vec<u8>,
    },
    /// Drain backlog from the inbox matching the filter, then either exit (follow=false) or
    /// continue tailing the live broadcast (follow=true). `since_ms` filters the backlog to
    /// `enqueued_at_ms > since_ms`; `None` returns full history.
    RecvSubscribe {
        peer: Option<PeerSelector>,
        channel: Option<String>,
        since_ms: Option<u64>,
        follow: bool,
    },

    // ===== v3 local orchestration =====
    /// Create a named group + its prime agent; returns the prime's token.
    GroupNew {
        name: String,
    },
    GroupList,
    SessionList {
        group: String,
    },
    AgentList {
        group: Option<String>,
        session: Option<String>,
    },
    /// Provision a session workspace, launch + supervise a child harness in it. (v0.3 / M2)
    Spawn {
        token: String,
        group: String,
        session: String,
        base_dir: String,
        fs_mode: FsMode,
        branch: Option<String>,
        label: Option<String>,
        prompt: String,
        harness_argv: Option<Vec<String>>,
        /// Notify the prime once if a turn is idle this long (no harness output). None = disabled.
        idle_timeout_secs: Option<u64>,
        /// Claude permission mode for the child (e.g. "plan", "acceptEdits", "bypassPermissions").
        /// Ignored when the harness is overridden via `harness_argv` / `$WT_HARNESS_CMD`.
        permission_mode: Option<String>,
        /// Launch the child with `--dangerously-skip-permissions`.
        skip_permissions: bool,
        /// Forward the child's intermediate assistant text to the prime as `trace` messages.
        trace: bool,
        /// Spawn this session as the group's **prime coordinator**: the agent is registered with
        /// `role = "prime"` (so it can spawn + command worker sessions itself) and its harness is
        /// given orchestration instructions. The human drives it from the dashboard; worker sessions
        /// are read-only. Normal workers leave this `false`.
        coordinator: bool,
    },
    /// Send a message into a session (the daemon resolves the counterparty from the session).
    AgentSend {
        token: String,
        session: String,
        kind: AgentMsgKind,
        payload: Vec<u8>,
    },
    /// Drain the caller's bus backlog (optionally one session) then optionally tail live.
    AgentRecv {
        token: String,
        session: Option<String>,
        since_ms: Option<u64>,
        follow: bool,
        /// Default recv: return only undelivered messages and mark them consumed. `--all` / `--since`
        /// set this false for a non-destructive view.
        consume: bool,
    },
    AgentKill {
        token: String,
        agent: String,
    },
    SessionClose {
        token: String,
        session: String,
        discard: bool,
    },
    WhoAmI {
        token: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcEvent {
    Ok,
    Err(IpcError),
    StatusInfo {
        nodeid: NodeId,
        version: String,
        endpoint_bound: bool,
    },
    NodeIdValue(NodeId),
    TicketValue(String),
    PeerListItem(PeerInfo),
    PeerListEnd,
    ConnListItem(ConnInfo),
    ConnListEnd,
    TokenIssued {
        raw: Vec<u8>,
        info: TokenInfo,
    },
    TokenListItem(TokenInfo),
    TokenListEnd,
    RecvMsg {
        from: NodeId,
        from_name: Option<String>,
        channel: String,
        payload: Vec<u8>,
        ts_ms: u64,
    },
    /// Emitted exactly once between the historical backlog and the live tail.
    RecvBacklogEnd,

    // ===== v3 local orchestration =====
    GroupCreated {
        group: String,
        token: String,
    },
    Spawned {
        group: String,
        session: String,
        token: String,
        workspace: String,
    },
    GroupListItem(GroupInfo),
    GroupListEnd,
    SessionListItem(SessionInfo),
    SessionListEnd,
    AgentListItem(AgentInfo),
    AgentListEnd,
    AgentMsg {
        session: String,
        from_agent: String,
        kind: AgentMsgKind,
        payload: Vec<u8>,
        ts_ms: u64,
    },
    /// Emitted exactly once between the agent backlog and the live tail.
    AgentBacklogEnd,
    WhoAmIInfo(WhoAmIInfo),
}
