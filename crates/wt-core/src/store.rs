//! SQLite store: peers, tokens, messages, meta. Single DB at `~/.wt/data/state.db`.
//!
//! The `Connection` is owned by a dedicated OS thread (the "DB actor"). `Store` is a cheap,
//! cloneable handle that ships closures to that thread over an unbounded channel and awaits the
//! result on a oneshot. This keeps blocking `rusqlite` calls off the tokio runtime threads and
//! serializes all access to the single writer without a cross-async mutex.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::{mpsc, oneshot};
use wt_proto::token::{Cap, TokenId};
use wt_proto::NodeId;

use crate::paths;

/// Per-(group, session) retention cap on the local message bus. `agent_msg_enqueue` trims rows
/// older than the last `MAX_MSGS_PER_SESSION`, so a never-reading recipient (e.g. a dead prime) or
/// a very chatty child cannot grow the DB without bound. With consume-on-read the *unconsumed*
/// working set normally stays far smaller; this is the backstop. (Lowered under `cfg(test)` so the
/// trim is cheap to exercise.)
#[cfg(not(test))]
const MAX_MSGS_PER_SESSION: i64 = 5000;
#[cfg(test)]
const MAX_MSGS_PER_SESSION: i64 = 8;

/// Ordered schema migrations. Index `i` (0-based) is schema version `i + 1`. To evolve the
/// schema, append a new entry — never edit or reorder existing ones. The runner applies every
/// step whose version is greater than the stored `schema_version`.
const MIGRATIONS: &[&str] = &[
    // v1 — identity registry + capability tokens.
    "CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value BLOB NOT NULL
     );
     CREATE TABLE IF NOT EXISTS peers (
        nodeid       BLOB PRIMARY KEY,
        name         TEXT NOT NULL UNIQUE,
        added_at_ms  INTEGER NOT NULL,
        last_seen_ms INTEGER,
        addr_blob    BLOB
     );
     CREATE TABLE IF NOT EXISTS tokens (
        id        BLOB PRIMARY KEY,
        iss       BLOB NOT NULL,
        sub       BLOB NOT NULL,
        exp       INTEGER NOT NULL,
        caps      TEXT NOT NULL,
        raw       BLOB NOT NULL,
        revoked   INTEGER NOT NULL DEFAULT 0
     );
     CREATE INDEX IF NOT EXISTS tokens_iss_sub ON tokens(iss, sub);",
    // v2 — persisted message log (outbox + inbox combined). PK `(sender, channel_id, seq)` is
    // both the receiver-side dedup key and the sender-side per-channel sequence.
    "CREATE TABLE IF NOT EXISTS messages (
        id_sender       BLOB NOT NULL,
        id_channel      BLOB NOT NULL,
        id_seq          INTEGER NOT NULL,
        direction       INTEGER NOT NULL,
        channel         TEXT NOT NULL,
        peer_nodeid     BLOB NOT NULL,
        payload         BLOB NOT NULL,
        enqueued_at_ms  INTEGER NOT NULL,
        delivered_at_ms INTEGER,
        PRIMARY KEY (id_sender, id_channel, id_seq)
     ) WITHOUT ROWID;
     CREATE INDEX IF NOT EXISTS messages_peer_chan_dir
         ON messages(peer_nodeid, channel, direction, enqueued_at_ms);
     CREATE INDEX IF NOT EXISTS messages_outbox_pending
         ON messages(enqueued_at_ms) WHERE direction = 0 AND delivered_at_ms IS NULL;",
    // v3 — local orchestration layer: named groups / agents / sessions + an in-daemon message
    // bus. Entirely separate from the peer `messages` table; routing here never touches iroh.
    // Identifiers are human names (group/session/agent); the per-agent token is a separate secret
    // stored only as a hash.
    "CREATE TABLE IF NOT EXISTS groups (
        name          TEXT PRIMARY KEY,
        created_at_ms INTEGER NOT NULL
     );
     CREATE TABLE IF NOT EXISTS agents (
        group_name    TEXT NOT NULL,
        name          TEXT NOT NULL,
        token_hash    BLOB NOT NULL UNIQUE,
        role          TEXT NOT NULL,
        dir           TEXT,
        pid           INTEGER,
        status        TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        last_seen_ms  INTEGER,
        PRIMARY KEY (group_name, name)
     );
     CREATE TABLE IF NOT EXISTS sessions (
        group_name     TEXT NOT NULL,
        name           TEXT NOT NULL,
        prime_agent    TEXT NOT NULL,
        child_agent    TEXT NOT NULL,
        fs_mode        TEXT NOT NULL,
        base_dir       TEXT,
        workspace_path TEXT NOT NULL,
        branch         TEXT,
        status         TEXT NOT NULL,
        created_at_ms  INTEGER NOT NULL,
        PRIMARY KEY (group_name, name)
     );
     CREATE TABLE IF NOT EXISTS agent_messages (
        group_name     TEXT NOT NULL,
        session_name   TEXT NOT NULL,
        seq            INTEGER NOT NULL,
        from_agent     TEXT NOT NULL,
        to_agent       TEXT NOT NULL,
        kind           TEXT NOT NULL,
        payload        BLOB NOT NULL,
        enqueued_at_ms INTEGER NOT NULL,
        consumed_at_ms INTEGER,
        PRIMARY KEY (group_name, session_name, seq)
     ) WITHOUT ROWID;
     CREATE INDEX IF NOT EXISTS agent_messages_inbox
         ON agent_messages(group_name, to_agent, enqueued_at_ms);",
];

/// Current target schema version = number of migration steps.
pub fn target_schema_version() -> i64 {
    MIGRATIONS.len() as i64
}

/// Compute a stable 16-byte channel id by hashing the human-readable channel name with blake3.
/// Stable across versions and platforms; ~128-bit collision resistance.
pub fn channel_id(name: &str) -> [u8; 16] {
    let h = blake3::hash(name.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.as_bytes()[..16]);
    out
}

/// Direction of a stored message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out = 0,
    In = 1,
}

impl Direction {
    pub fn from_i64(v: i64) -> Option<Self> {
        match v {
            0 => Some(Direction::Out),
            1 => Some(Direction::In),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageRow {
    pub sender: NodeId,
    pub channel_id: [u8; 16],
    pub seq: u64,
    pub direction: Direction,
    pub channel: String,
    pub peer_nodeid: NodeId,
    pub payload: Vec<u8>,
    pub enqueued_at_ms: u64,
    pub delivered_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PeerRow {
    pub nodeid: NodeId,
    pub name: String,
    pub added_at_ms: u64,
    pub last_seen_ms: Option<u64>,
    /// Optional CBOR-encoded `AddrTicket` — direct dial hints from when the peer was added.
    pub addr_blob: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TokenRow {
    pub id: TokenId,
    pub iss: NodeId,
    pub sub: NodeId,
    pub exp: u64,
    pub caps: Vec<Cap>,
    pub raw: Vec<u8>,
    pub revoked: bool,
}

// ===== v3 local-orchestration rows (keyed by human names) =====

#[derive(Debug, Clone)]
pub struct GroupRow {
    pub name: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AgentRow {
    pub group_name: String,
    pub name: String,
    /// blake3(token) — the secret itself is never stored.
    pub token_hash: Vec<u8>,
    pub role: String, // "prime" | "child"
    pub dir: Option<String>,
    pub pid: Option<i64>,
    pub status: String, // "running" | "idle" | "awaiting_input" | "exited"
    pub created_at_ms: u64,
    pub last_seen_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SessionRow {
    pub group_name: String,
    pub name: String,
    pub prime_agent: String,
    pub child_agent: String,
    pub fs_mode: String, // "worktree" | "new"
    pub base_dir: Option<String>,
    pub workspace_path: String,
    pub branch: Option<String>,
    pub status: String, // "active" | "closed"
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AgentMsgRow {
    pub group_name: String,
    pub session_name: String,
    pub seq: u64,
    pub from_agent: String,
    pub to_agent: String,
    pub kind: String, // "turn_output" | "turn_input" | "user" | "control"
    pub payload: Vec<u8>,
    pub enqueued_at_ms: u64,
    pub consumed_at_ms: Option<u64>,
}

/// A unit of work for the DB actor thread.
type DbJob = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

/// Cheap, cloneable handle to the SQLite store. All methods are async and run their SQL on the
/// dedicated DB thread.
#[derive(Clone)]
pub struct Store {
    tx: mpsc::UnboundedSender<DbJob>,
}

impl Store {
    /// Open the default store at `~/.wt/data/state.db` (honors `WT_HOME`).
    pub fn open() -> Result<Self> {
        paths::ensure_dirs()?;
        let p = paths::state_db_path();
        Self::open_at(&p)
    }

    /// Open a store at an explicit path. Runs PRAGMAs + migrations synchronously on the caller
    /// thread (so open/migration errors propagate), then moves the `Connection` onto a dedicated
    /// worker thread.
    pub fn open_at(path: &Path) -> Result<Self> {
        let mut conn =
            Connection::open(path).with_context(|| format!("open sqlite at {}", path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )?;
        run_migrations(&mut conn).context("run schema migrations")?;

        let (tx, mut rx) = mpsc::unbounded_channel::<DbJob>();
        std::thread::Builder::new()
            .name("wt-store".to_string())
            .spawn(move || {
                // `blocking_recv` is valid here: this is a plain OS thread, not a runtime worker.
                while let Some(job) = rx.blocking_recv() {
                    job(&mut conn);
                }
                // All handles dropped → channel closed → exit, dropping `conn` (WAL checkpoint).
            })
            .context("spawn wt-store thread")?;

        Ok(Self { tx })
    }

    /// Run `f` against the connection on the DB thread and await its result.
    async fn call<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let (otx, orx) = oneshot::channel();
        let job: DbJob = Box::new(move |conn| {
            let _ = otx.send(f(conn));
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow!("store DB thread is gone"))?;
        orx.await
            .map_err(|_| anyhow!("store DB thread dropped the reply"))?
    }

    // ===== Meta =====

    pub async fn meta_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key = key.to_string();
        self.call(move |conn| {
            let v = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = ?1",
                    params![key],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?;
            Ok(v)
        })
        .await
    }

    // ===== Peers =====

    pub async fn peer_add(
        &self,
        nodeid: NodeId,
        name: &str,
        addr_blob: Option<&[u8]>,
    ) -> Result<()> {
        let name = name.to_string();
        let addr_blob = addr_blob.map(|b| b.to_vec());
        self.call(move |conn| {
            let now_ms = unix_ms();
            conn.execute(
                "INSERT INTO peers(nodeid, name, added_at_ms, addr_blob) VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(nodeid) DO UPDATE SET
                    name = excluded.name,
                    addr_blob = COALESCE(excluded.addr_blob, peers.addr_blob)",
                params![&nodeid.0[..], name, now_ms as i64, addr_blob],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn peer_remove(&self, sel: &PeerSelectorBytes) -> Result<usize> {
        let sel = sel.clone();
        self.call(move |conn| {
            let n = match sel {
                PeerSelectorBytes::NodeId(n) => {
                    conn.execute("DELETE FROM peers WHERE nodeid = ?1", params![&n.0[..]])?
                }
                PeerSelectorBytes::Name(s) => {
                    conn.execute("DELETE FROM peers WHERE name = ?1", params![s])?
                }
            };
            Ok(n)
        })
        .await
    }

    pub async fn peer_get(&self, sel: &PeerSelectorBytes) -> Result<Option<PeerRow>> {
        let sel = sel.clone();
        self.call(move |conn| {
            let row = match sel {
                PeerSelectorBytes::NodeId(n) => conn
                    .query_row(
                        "SELECT nodeid, name, added_at_ms, last_seen_ms, addr_blob FROM peers WHERE nodeid = ?1",
                        params![&n.0[..]],
                        row_to_peer,
                    )
                    .optional()?,
                PeerSelectorBytes::Name(s) => conn
                    .query_row(
                        "SELECT nodeid, name, added_at_ms, last_seen_ms, addr_blob FROM peers WHERE name = ?1",
                        params![s],
                        row_to_peer,
                    )
                    .optional()?,
            };
            Ok(row)
        })
        .await
    }

    pub async fn peer_list(&self) -> Result<Vec<PeerRow>> {
        self.call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT nodeid, name, added_at_ms, last_seen_ms, addr_blob FROM peers ORDER BY name",
            )?;
            let rows = stmt
                .query_map([], row_to_peer)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn peer_touch_seen(&self, nodeid: &NodeId) -> Result<()> {
        let nodeid = *nodeid;
        self.call(move |conn| {
            let now_ms = unix_ms();
            conn.execute(
                "UPDATE peers SET last_seen_ms = ?1 WHERE nodeid = ?2",
                params![now_ms as i64, &nodeid.0[..]],
            )?;
            Ok(())
        })
        .await
    }

    // ===== Tokens =====

    pub async fn token_insert(&self, row: &TokenRow) -> Result<()> {
        let row = row.clone();
        self.call(move |conn| {
            let caps_json = serde_json::to_string(&row.caps)?;
            conn.execute(
                "INSERT INTO tokens(id, iss, sub, exp, caps, raw, revoked) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(id) DO UPDATE SET raw = excluded.raw, revoked = excluded.revoked",
                params![
                    &row.id[..],
                    &row.iss.0[..],
                    &row.sub.0[..],
                    row.exp as i64,
                    caps_json,
                    row.raw,
                    row.revoked as i64,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn token_list(&self) -> Result<Vec<TokenRow>> {
        self.call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, iss, sub, exp, caps, raw, revoked FROM tokens ORDER BY exp DESC",
            )?;
            let rows = stmt
                .query_map([], row_to_token)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn token_find(&self, id: &TokenId) -> Result<Option<TokenRow>> {
        let id = *id;
        self.call(move |conn| {
            let row = conn
                .query_row(
                    "SELECT id, iss, sub, exp, caps, raw, revoked FROM tokens WHERE id = ?1",
                    params![&id[..]],
                    row_to_token,
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    /// Find the newest non-revoked, non-expired token issued by `iss` to `sub` that carries
    /// `required`. Indexed by `tokens_iss_sub`; the cap check runs in Rust over the (tiny)
    /// candidate set since caps are stored as a JSON TEXT column.
    pub async fn find_token_with_cap(
        &self,
        iss: NodeId,
        sub: NodeId,
        required: Cap,
        now_secs: u64,
    ) -> Result<Option<TokenRow>> {
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, iss, sub, exp, caps, raw, revoked FROM tokens
                 WHERE iss = ?1 AND sub = ?2 AND revoked = 0 AND exp > ?3
                 ORDER BY exp DESC",
            )?;
            let mut rows = stmt.query_map(
                params![&iss.0[..], &sub.0[..], now_secs as i64],
                row_to_token,
            )?;
            for row in rows.by_ref() {
                let row = row?;
                if row.caps.iter().any(|c| c == &required) {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn token_revoke(&self, id: &TokenId) -> Result<usize> {
        let id = *id;
        self.call(move |conn| {
            Ok(conn.execute(
                "UPDATE tokens SET revoked = 1 WHERE id = ?1",
                params![&id[..]],
            )?)
        })
        .await
    }

    // ===== Outbox =====

    /// Append a new outbound message. Assigns `seq` as `MAX(seq for (sender, channel_id)) + 1`.
    /// Returns the assigned MessageRow (with seq + enqueued_at_ms populated).
    pub async fn outbox_enqueue(
        &self,
        sender: NodeId,
        peer_nodeid: NodeId,
        channel: &str,
        payload: Vec<u8>,
    ) -> Result<MessageRow> {
        let channel = channel.to_string();
        self.call(move |conn| {
            let cid = channel_id(&channel);
            let now = unix_ms();
            let tx = conn.transaction()?;
            let next_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(id_seq), 0) + 1 FROM messages WHERE id_sender = ?1 AND id_channel = ?2",
                params![&sender.0[..], &cid[..]],
                |row| row.get(0),
            )?;
            tx.execute(
                "INSERT INTO messages(
                    id_sender, id_channel, id_seq, direction, channel,
                    peer_nodeid, payload, enqueued_at_ms, delivered_at_ms
                 ) VALUES(?1, ?2, ?3, 0, ?4, ?5, ?6, ?7, NULL)",
                params![
                    &sender.0[..],
                    &cid[..],
                    next_seq,
                    channel,
                    &peer_nodeid.0[..],
                    &payload,
                    now as i64,
                ],
            )?;
            tx.commit()?;
            Ok(MessageRow {
                sender,
                channel_id: cid,
                seq: next_seq as u64,
                direction: Direction::Out,
                channel,
                peer_nodeid,
                payload,
                enqueued_at_ms: now,
                delivered_at_ms: None,
            })
        })
        .await
    }

    /// Distinct peers that currently have undelivered outbound messages. Used at startup to
    /// resume delivery for each peer with a backlog.
    pub async fn outbox_pending_peers(&self) -> Result<Vec<NodeId>> {
        self.call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT peer_nodeid FROM messages
                 WHERE direction = 0 AND delivered_at_ms IS NULL",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    fixed_32(bytes, 0, "messages.peer_nodeid")
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows.into_iter().map(NodeId).collect())
        })
        .await
    }

    /// Return up to `limit` pending outbound rows whose `peer_nodeid` equals the given peer,
    /// oldest first.
    pub async fn outbox_pending_for_peer(
        &self,
        peer: &NodeId,
        limit: usize,
    ) -> Result<Vec<MessageRow>> {
        let peer = *peer;
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id_sender, id_channel, id_seq, direction, channel, peer_nodeid,
                        payload, enqueued_at_ms, delivered_at_ms
                 FROM messages
                 WHERE direction = 0 AND delivered_at_ms IS NULL AND peer_nodeid = ?1
                 ORDER BY enqueued_at_ms ASC
                 LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![&peer.0[..], limit as i64], row_to_message)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Mark a specific outbound message as delivered (now()).
    pub async fn outbox_mark_delivered(
        &self,
        sender: &NodeId,
        channel_id_bytes: &[u8; 16],
        seq: u64,
    ) -> Result<()> {
        let sender = *sender;
        let cid = *channel_id_bytes;
        self.call(move |conn| {
            let now = unix_ms();
            conn.execute(
                "UPDATE messages SET delivered_at_ms = ?1
                 WHERE id_sender = ?2 AND id_channel = ?3 AND id_seq = ?4 AND direction = 0",
                params![now as i64, &sender.0[..], &cid[..], seq as i64],
            )?;
            Ok(())
        })
        .await
    }

    // ===== Inbox =====

    /// Record an inbound message. Returns `true` iff this is the first time we've seen
    /// `(sender, channel_id, seq)` — duplicates return `false` and are dropped silently.
    pub async fn inbox_record(
        &self,
        sender: NodeId,
        channel: &str,
        seq: u64,
        payload: &[u8],
        ts_ms: u64,
    ) -> Result<bool> {
        let channel = channel.to_string();
        let payload = payload.to_vec();
        self.call(move |conn| {
            let cid = channel_id(&channel);
            let now_delivered = unix_ms();
            let n = conn.execute(
                "INSERT OR IGNORE INTO messages(
                    id_sender, id_channel, id_seq, direction, channel,
                    peer_nodeid, payload, enqueued_at_ms, delivered_at_ms
                 ) VALUES(?1, ?2, ?3, 1, ?4, ?1, ?5, ?6, ?7)",
                params![
                    &sender.0[..],
                    &cid[..],
                    seq as i64,
                    channel,
                    payload,
                    ts_ms as i64,
                    now_delivered as i64,
                ],
            )?;
            Ok(n > 0)
        })
        .await
    }

    /// Read inbound messages matching optional `peer`/`channel` filters since
    /// `after_enqueued_ms` (exclusive), up to `limit`, in chronological order. A single query
    /// with nullable-bind predicates covers all four filter combinations.
    pub async fn inbox_backlog(
        &self,
        peer: Option<&NodeId>,
        channel: Option<&str>,
        after_enqueued_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<MessageRow>> {
        let peer = peer.map(|p| p.0.to_vec());
        let channel = channel.map(|c| c.to_string());
        let after = after_enqueued_ms.map(|v| v as i64).unwrap_or(0);
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id_sender, id_channel, id_seq, direction, channel, peer_nodeid,
                        payload, enqueued_at_ms, delivered_at_ms
                 FROM messages
                 WHERE direction = 1
                   AND (?1 IS NULL OR peer_nodeid = ?1)
                   AND (?2 IS NULL OR channel    = ?2)
                   AND enqueued_at_ms > ?3
                 ORDER BY enqueued_at_ms ASC
                 LIMIT ?4",
            )?;
            let rows = stmt
                .query_map(params![peer, channel, after, limit as i64], row_to_message)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    // ===== v3: groups =====

    /// Create a named group. Returns `true` if newly created, `false` if the name already exists.
    pub async fn group_create(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.call(move |conn| {
            let n = conn.execute(
                "INSERT OR IGNORE INTO groups(name, created_at_ms) VALUES(?1, ?2)",
                params![name, unix_ms() as i64],
            )?;
            Ok(n > 0)
        })
        .await
    }

    pub async fn group_list(&self) -> Result<Vec<GroupRow>> {
        self.call(|conn| {
            let mut stmt = conn
                .prepare("SELECT name, created_at_ms FROM groups ORDER BY created_at_ms, rowid")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(GroupRow {
                        name: row.get(0)?,
                        created_at_ms: row.get::<_, i64>(1)? as u64,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn group_exists(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.call(move |conn| {
            let found: Option<i64> = conn
                .query_row("SELECT 1 FROM groups WHERE name = ?1", params![name], |r| {
                    r.get(0)
                })
                .optional()?;
            Ok(found.is_some())
        })
        .await
    }

    // ===== v3: agents =====

    /// Register an agent. Returns `false` if `(group, name)` already exists.
    pub async fn agent_register(&self, row: &AgentRow) -> Result<bool> {
        let row = row.clone();
        self.call(move |conn| {
            let n = conn.execute(
                "INSERT OR IGNORE INTO agents(
                    group_name, name, token_hash, role, dir, pid, status, created_at_ms, last_seen_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    row.group_name,
                    row.name,
                    row.token_hash,
                    row.role,
                    row.dir,
                    row.pid,
                    row.status,
                    row.created_at_ms as i64,
                    row.last_seen_ms.map(|v| v as i64),
                ],
            )?;
            Ok(n > 0)
        })
        .await
    }

    /// Resolve an agent by the blake3 hash of its bearer token (the local-auth lookup).
    pub async fn agent_by_token(&self, token_hash: &[u8]) -> Result<Option<AgentRow>> {
        let token_hash = token_hash.to_vec();
        self.call(move |conn| {
            let row = conn
                .query_row(
                    "SELECT group_name, name, token_hash, role, dir, pid, status, created_at_ms, last_seen_ms
                     FROM agents WHERE token_hash = ?1",
                    params![token_hash],
                    row_to_agent,
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn agent_get(&self, group: &str, name: &str) -> Result<Option<AgentRow>> {
        let group = group.to_string();
        let name = name.to_string();
        self.call(move |conn| {
            let row = conn
                .query_row(
                    "SELECT group_name, name, token_hash, role, dir, pid, status, created_at_ms, last_seen_ms
                     FROM agents WHERE group_name = ?1 AND name = ?2",
                    params![group, name],
                    row_to_agent,
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    /// List agents, optionally scoped to one group, ordered by creation time.
    pub async fn agent_list(&self, group: Option<&str>) -> Result<Vec<AgentRow>> {
        let group = group.map(|g| g.to_string());
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, name, token_hash, role, dir, pid, status, created_at_ms, last_seen_ms
                 FROM agents
                 WHERE (?1 IS NULL OR group_name = ?1)
                 ORDER BY created_at_ms, rowid",
            )?;
            let rows = stmt
                .query_map(params![group], row_to_agent)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Update an agent's status (and pid if `Some`), bumping `last_seen_ms`.
    pub async fn agent_set_status(
        &self,
        group: &str,
        name: &str,
        status: &str,
        pid: Option<i64>,
    ) -> Result<()> {
        let group = group.to_string();
        let name = name.to_string();
        let status = status.to_string();
        self.call(move |conn| {
            conn.execute(
                "UPDATE agents SET status = ?1, pid = COALESCE(?2, pid), last_seen_ms = ?3
                 WHERE group_name = ?4 AND name = ?5",
                params![status, pid, unix_ms() as i64, group, name],
            )?;
            Ok(())
        })
        .await
    }

    /// On startup, reconcile state left by a previous (possibly crashed) daemon: a fresh daemon has
    /// no live children, so any agent still `starting`/`running`/`awaiting_input` and any session
    /// still `active` is stale → mark them `exited`/`closed`. Returns the agent count reconciled.
    /// No processes are killed — children self-terminate when the daemon dies and their stdio pipes
    /// break.
    pub async fn reconcile_stale_agents(&self) -> Result<usize> {
        self.call(|conn| {
            let n = conn.execute(
                "UPDATE agents SET status = 'exited'
                 WHERE status IN ('starting', 'running', 'awaiting_input')",
                [],
            )?;
            conn.execute(
                "UPDATE sessions SET status = 'closed' WHERE status = 'active'",
                [],
            )?;
            Ok(n)
        })
        .await
    }

    // ===== v3: sessions =====

    /// Create a session. Returns `false` if `(group, name)` already exists.
    pub async fn session_create(&self, row: &SessionRow) -> Result<bool> {
        let row = row.clone();
        self.call(move |conn| {
            let n = conn.execute(
                "INSERT OR IGNORE INTO sessions(
                    group_name, name, prime_agent, child_agent, fs_mode, base_dir,
                    workspace_path, branch, status, created_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    row.group_name,
                    row.name,
                    row.prime_agent,
                    row.child_agent,
                    row.fs_mode,
                    row.base_dir,
                    row.workspace_path,
                    row.branch,
                    row.status,
                    row.created_at_ms as i64,
                ],
            )?;
            Ok(n > 0)
        })
        .await
    }

    pub async fn session_get(&self, group: &str, name: &str) -> Result<Option<SessionRow>> {
        let group = group.to_string();
        let name = name.to_string();
        self.call(move |conn| {
            let row = conn
                .query_row(
                    "SELECT group_name, name, prime_agent, child_agent, fs_mode, base_dir,
                            workspace_path, branch, status, created_at_ms
                     FROM sessions WHERE group_name = ?1 AND name = ?2",
                    params![group, name],
                    row_to_session,
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn session_list(&self, group: &str) -> Result<Vec<SessionRow>> {
        let group = group.to_string();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, name, prime_agent, child_agent, fs_mode, base_dir,
                        workspace_path, branch, status, created_at_ms
                 FROM sessions WHERE group_name = ?1 ORDER BY created_at_ms, rowid",
            )?;
            let rows = stmt
                .query_map(params![group], row_to_session)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn session_close(&self, group: &str, name: &str) -> Result<usize> {
        let group = group.to_string();
        let name = name.to_string();
        self.call(move |conn| {
            Ok(conn.execute(
                "UPDATE sessions SET status = 'closed' WHERE group_name = ?1 AND name = ?2",
                params![group, name],
            )?)
        })
        .await
    }

    // ===== v3: local message bus =====

    /// Append a message to the local bus. Assigns `seq = MAX(seq for (group, session)) + 1`.
    pub async fn agent_msg_enqueue(
        &self,
        group: &str,
        session: &str,
        from_agent: &str,
        to_agent: &str,
        kind: &str,
        payload: Vec<u8>,
    ) -> Result<AgentMsgRow> {
        let group = group.to_string();
        let session = session.to_string();
        let from_agent = from_agent.to_string();
        let to_agent = to_agent.to_string();
        let kind = kind.to_string();
        self.call(move |conn| {
            let now = unix_ms();
            let tx = conn.transaction()?;
            let next_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM agent_messages
                 WHERE group_name = ?1 AND session_name = ?2",
                params![group, session],
                |row| row.get(0),
            )?;
            tx.execute(
                "INSERT INTO agent_messages(
                    group_name, session_name, seq, from_agent, to_agent, kind,
                    payload, enqueued_at_ms, consumed_at_ms
                 ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
                params![group, session, next_seq, from_agent, to_agent, kind, payload, now as i64],
            )?;
            // Retention cap: keep only the last MAX_MSGS_PER_SESSION rows for this (group, session).
            let trimmed = tx.execute(
                "DELETE FROM agent_messages
                 WHERE group_name = ?1 AND session_name = ?2 AND seq <= ?3",
                params![group, session, next_seq - MAX_MSGS_PER_SESSION],
            )?;
            if trimmed > 0 {
                tracing::debug!(%group, %session, trimmed, "trimmed agent_messages to retention cap");
            }
            tx.commit()?;
            Ok(AgentMsgRow {
                group_name: group,
                session_name: session,
                seq: next_seq as u64,
                from_agent,
                to_agent,
                kind,
                payload,
                enqueued_at_ms: now,
                consumed_at_ms: None,
            })
        })
        .await
    }

    /// Backlog destined for `to_agent` within a group (optionally one session), enqueued after
    /// `after_ms` (exclusive), oldest first, up to `limit`.
    pub async fn agent_msg_backlog(
        &self,
        group: &str,
        to_agent: &str,
        session: Option<&str>,
        after_ms: Option<u64>,
        limit: usize,
        unconsumed_only: bool,
    ) -> Result<Vec<AgentMsgRow>> {
        let group = group.to_string();
        let to_agent = to_agent.to_string();
        let session = session.map(|s| s.to_string());
        let after = after_ms.map(|v| v as i64).unwrap_or(0);
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, session_name, seq, from_agent, to_agent, kind,
                        payload, enqueued_at_ms, consumed_at_ms
                 FROM agent_messages
                 WHERE group_name = ?1 AND to_agent = ?2
                   AND (?3 IS NULL OR session_name = ?3)
                   AND enqueued_at_ms > ?4
                   AND (?6 = 0 OR consumed_at_ms IS NULL)
                 ORDER BY enqueued_at_ms ASC, seq ASC
                 LIMIT ?5",
            )?;
            let rows = stmt
                .query_map(
                    params![
                        group,
                        to_agent,
                        session,
                        after,
                        limit as i64,
                        unconsumed_only as i64
                    ],
                    row_to_agent_msg,
                )?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn agent_msg_mark_consumed(
        &self,
        group: &str,
        session: &str,
        seq: u64,
    ) -> Result<()> {
        let group = group.to_string();
        let session = session.to_string();
        self.call(move |conn| {
            conn.execute(
                "UPDATE agent_messages SET consumed_at_ms = ?1
                 WHERE group_name = ?2 AND session_name = ?3 AND seq = ?4",
                params![unix_ms() as i64, group, session, seq as i64],
            )?;
            Ok(())
        })
        .await
    }

    /// Unconsumed messages addressed to `to_agent` in `group` with the given `kind`, oldest first
    /// by `seq`. The supervisor uses this to feed pending `turn_input`s durably (DB-as-truth) — the
    /// broadcast/timer are only wakeups, so a dropped or lagged broadcast never strips a reply. For
    /// a child agent, `to_agent` == its session name, so all its turn-inputs share one `seq` space.
    pub async fn agent_msg_pending(
        &self,
        group: &str,
        to_agent: &str,
        kind: &str,
    ) -> Result<Vec<AgentMsgRow>> {
        let group = group.to_string();
        let to_agent = to_agent.to_string();
        let kind = kind.to_string();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, session_name, seq, from_agent, to_agent, kind,
                        payload, enqueued_at_ms, consumed_at_ms
                 FROM agent_messages
                 WHERE group_name = ?1 AND to_agent = ?2 AND kind = ?3 AND consumed_at_ms IS NULL
                 ORDER BY seq ASC",
            )?;
            let rows = stmt
                .query_map(params![group, to_agent, kind], row_to_agent_msg)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Full transcript for one session — **both directions** (child→prime and prime→child) —
    /// with `seq > after_seq`, oldest first. Unlike `agent_msg_backlog` (which filters by
    /// `to_agent`, i.e. one direction), the web dashboard needs the whole thread. The
    /// `(group_name, session_name, seq)` primary key makes this an index-friendly scan.
    pub async fn agent_msg_session_log(
        &self,
        group: &str,
        session: &str,
        after_seq: u64,
    ) -> Result<Vec<AgentMsgRow>> {
        let group = group.to_string();
        let session = session.to_string();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, session_name, seq, from_agent, to_agent, kind,
                        payload, enqueued_at_ms, consumed_at_ms
                 FROM agent_messages
                 WHERE group_name = ?1 AND session_name = ?2 AND seq > ?3
                 ORDER BY seq ASC",
            )?;
            let rows = stmt
                .query_map(params![group, session, after_seq as i64], row_to_agent_msg)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Unified message feed for an **entire group** — every session's messages interleaved in
    /// time order, enqueued after `after_ms` (exclusive). Backs the prime command console, which
    /// dumps all conversations into one timeline.
    pub async fn agent_msg_group_feed(&self, group: &str, after_ms: u64) -> Result<Vec<AgentMsgRow>> {
        let group = group.to_string();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT group_name, session_name, seq, from_agent, to_agent, kind,
                        payload, enqueued_at_ms, consumed_at_ms
                 FROM agent_messages
                 WHERE group_name = ?1 AND enqueued_at_ms > ?2
                 ORDER BY enqueued_at_ms ASC, session_name ASC, seq ASC",
            )?;
            let rows = stmt
                .query_map(params![group, after_ms as i64], row_to_agent_msg)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    /// Heartbeat: bump an agent's `last_seen_ms` without touching its status. The supervisor calls
    /// this on its poll tick so a live session keeps a fresh timestamp; a session whose supervisor
    /// has died (orphaned, or never existed) goes stale, which the dashboard surfaces.
    pub async fn agent_touch(&self, group: &str, name: &str) -> Result<()> {
        let group = group.to_string();
        let name = name.to_string();
        self.call(move |conn| {
            conn.execute(
                "UPDATE agents SET last_seen_ms = ?1 WHERE group_name = ?2 AND name = ?3",
                params![unix_ms() as i64, group, name],
            )?;
            Ok(())
        })
        .await
    }

    /// Test-only escape hatch: run an arbitrary closure against the connection.
    #[cfg(test)]
    pub(crate) async fn test_with_conn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        self.call(f).await
    }
}

/// Apply pending schema migrations. Reads the stored `schema_version` (0 if absent) and runs
/// every `MIGRATIONS` step beyond it inside one transaction, then advances the version.
fn run_migrations(conn: &mut Connection) -> Result<()> {
    // `meta` must exist before we can read the version pointer.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);",
    )?;
    let current: i64 = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| {
                let v: Vec<u8> = row.get(0)?;
                Ok(String::from_utf8_lossy(&v).parse::<i64>().unwrap_or(0))
            },
        )
        .optional()?
        .unwrap_or(0);

    let target = target_schema_version();
    if current >= target {
        return Ok(());
    }

    let tx = conn.transaction()?;
    for (i, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        tx.execute_batch(sql)
            .with_context(|| format!("apply migration step {}", i + 1))?;
    }
    tx.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![target.to_string().into_bytes()],
    )?;
    tx.commit()?;
    Ok(())
}

fn row_to_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<MessageRow> {
    let sender = fixed_32(row.get(0)?, 0, "messages.id_sender")?;
    let cid = fixed_16(row.get(1)?, 1, "messages.id_channel")?;
    let dir = Direction::from_i64(row.get::<_, i64>(3)?).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Integer,
            "invalid direction".into(),
        )
    })?;
    let peer = fixed_32(row.get(5)?, 5, "messages.peer_nodeid")?;
    Ok(MessageRow {
        sender: NodeId(sender),
        channel_id: cid,
        seq: row.get::<_, i64>(2)? as u64,
        direction: dir,
        channel: row.get(4)?,
        peer_nodeid: NodeId(peer),
        payload: row.get(6)?,
        enqueued_at_ms: row.get::<_, i64>(7)? as u64,
        delivered_at_ms: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
    })
}

fn row_to_peer(row: &rusqlite::Row<'_>) -> rusqlite::Result<PeerRow> {
    let id_bytes: Vec<u8> = row.get(0)?;
    let nb = fixed_32(id_bytes, 0, "nodeid")?;
    Ok(PeerRow {
        nodeid: NodeId(nb),
        name: row.get(1)?,
        added_at_ms: row.get::<_, i64>(2)? as u64,
        last_seen_ms: row.get::<_, Option<i64>>(3)?.map(|v| v as u64),
        addr_blob: row.get::<_, Option<Vec<u8>>>(4)?,
    })
}

fn row_to_token(row: &rusqlite::Row<'_>) -> rusqlite::Result<TokenRow> {
    let id_bytes: Vec<u8> = row.get(0)?;
    let id = fixed_16(id_bytes, 0, "id")?;
    let iss_bytes: Vec<u8> = row.get(1)?;
    let sub_bytes: Vec<u8> = row.get(2)?;
    let iss = fixed_32(iss_bytes, 1, "iss")?;
    let sub = fixed_32(sub_bytes, 2, "sub")?;
    let caps_json: String = row.get(4)?;
    let caps: Vec<Cap> = serde_json::from_str(&caps_json).unwrap_or_default();
    Ok(TokenRow {
        id,
        iss: NodeId(iss),
        sub: NodeId(sub),
        exp: row.get::<_, i64>(3)? as u64,
        caps,
        raw: row.get(5)?,
        revoked: row.get::<_, i64>(6)? != 0,
    })
}

fn row_to_agent(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRow> {
    Ok(AgentRow {
        group_name: row.get(0)?,
        name: row.get(1)?,
        token_hash: row.get(2)?,
        role: row.get(3)?,
        dir: row.get(4)?,
        pid: row.get(5)?,
        status: row.get(6)?,
        created_at_ms: row.get::<_, i64>(7)? as u64,
        last_seen_ms: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
    })
}

fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionRow> {
    Ok(SessionRow {
        group_name: row.get(0)?,
        name: row.get(1)?,
        prime_agent: row.get(2)?,
        child_agent: row.get(3)?,
        fs_mode: row.get(4)?,
        base_dir: row.get(5)?,
        workspace_path: row.get(6)?,
        branch: row.get(7)?,
        status: row.get(8)?,
        created_at_ms: row.get::<_, i64>(9)? as u64,
    })
}

fn row_to_agent_msg(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentMsgRow> {
    Ok(AgentMsgRow {
        group_name: row.get(0)?,
        session_name: row.get(1)?,
        seq: row.get::<_, i64>(2)? as u64,
        from_agent: row.get(3)?,
        to_agent: row.get(4)?,
        kind: row.get(5)?,
        payload: row.get(6)?,
        enqueued_at_ms: row.get::<_, i64>(7)? as u64,
        consumed_at_ms: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
    })
}

fn fixed_16(bytes: Vec<u8>, col: usize, name: &'static str) -> rusqlite::Result<[u8; 16]> {
    let len = bytes.len();
    bytes.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            col,
            rusqlite::types::Type::Blob,
            format!("{name} must be 16 bytes, got {len}").into(),
        )
    })
}

fn fixed_32(bytes: Vec<u8>, col: usize, name: &'static str) -> rusqlite::Result<[u8; 32]> {
    let len = bytes.len();
    bytes.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            col,
            rusqlite::types::Type::Blob,
            format!("{name} must be 32 bytes, got {len}").into(),
        )
    })
}

/// Selector with already-resolved bytes — keeps store independent of IPC proto enums.
#[derive(Debug, Clone)]
pub enum PeerSelectorBytes {
    Name(String),
    NodeId(NodeId),
}

pub fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn unix_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_db_path() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "wt-store-test-{}-{}-{}.db",
            std::process::id(),
            unix_ms(),
            n
        ))
    }

    fn open_temp() -> Store {
        Store::open_at(&temp_db_path()).unwrap()
    }

    fn node(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    fn token(byte: u8) -> TokenRow {
        TokenRow {
            id: [byte; 16],
            iss: node(1),
            sub: node(2),
            exp: unix_secs() + 3600,
            caps: vec![Cap::Msg],
            raw: vec![byte, byte + 1],
            revoked: false,
        }
    }

    #[tokio::test]
    async fn open_creates_state_db_and_schema_version() {
        let store = open_temp();
        let version = store.meta_get("schema_version").await.unwrap().unwrap();
        assert_eq!(
            String::from_utf8(version).unwrap(),
            target_schema_version().to_string()
        );
    }

    #[tokio::test]
    async fn migration_upgrades_v1_db_to_current() {
        // Seed a fresh DB with only the v1 schema + schema_version=1, simulating an old install.
        let path = temp_db_path();
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(MIGRATIONS[0]).unwrap();
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?1)",
                params![b"1".to_vec()],
            )
            .unwrap();
            // No `messages` table yet.
            let has_messages: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='messages'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(has_messages, 0);
        }
        // Opening through the store should run the v2 migration.
        let store = Store::open_at(&path).unwrap();
        let version = store.meta_get("schema_version").await.unwrap().unwrap();
        assert_eq!(
            String::from_utf8(version).unwrap(),
            target_schema_version().to_string()
        );
        let has_messages = store
            .test_with_conn(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='messages'",
                    [],
                    |r| r.get::<_, i64>(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(has_messages, 1);
    }

    #[tokio::test]
    async fn peer_add_update_preserves_ticket_when_update_has_none() {
        let store = open_temp();
        store
            .peer_add(node(9), "bob", Some(&[1, 2, 3]))
            .await
            .unwrap();
        store.peer_add(node(9), "robert", None).await.unwrap();

        assert!(store
            .peer_get(&PeerSelectorBytes::Name("bob".to_string()))
            .await
            .unwrap()
            .is_none());
        let row = store
            .peer_get(&PeerSelectorBytes::NodeId(node(9)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.name, "robert");
        assert_eq!(row.addr_blob, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn peer_name_must_be_unique() {
        let store = open_temp();
        store.peer_add(node(1), "same", None).await.unwrap();
        assert!(store.peer_add(node(2), "same", None).await.is_err());
    }

    #[tokio::test]
    async fn peer_remove_by_name_and_nodeid_reports_counts() {
        let store = open_temp();
        store.peer_add(node(1), "a", None).await.unwrap();
        store.peer_add(node(2), "b", None).await.unwrap();

        assert_eq!(
            store
                .peer_remove(&PeerSelectorBytes::Name("a".to_string()))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .peer_remove(&PeerSelectorBytes::NodeId(node(2)))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .peer_remove(&PeerSelectorBytes::Name("missing".to_string()))
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn token_insert_revoke_and_find_roundtrip() {
        let store = open_temp();
        let row = token(8);
        store.token_insert(&row).await.unwrap();

        let found = store.token_find(&row.id).await.unwrap().unwrap();
        assert_eq!(found.id, row.id);
        assert_eq!(found.iss, row.iss);
        assert_eq!(found.sub, row.sub);
        assert_eq!(found.caps, row.caps);
        assert!(!found.revoked);

        assert_eq!(store.token_revoke(&row.id).await.unwrap(), 1);
        assert!(store.token_find(&row.id).await.unwrap().unwrap().revoked);
        assert_eq!(store.token_revoke(&[7; 16]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn token_insert_conflict_updates_revoked_state() {
        let store = open_temp();
        let mut row = token(8);
        store.token_insert(&row).await.unwrap();
        row.revoked = true;
        row.raw = vec![99];
        store.token_insert(&row).await.unwrap();

        let found = store.token_find(&row.id).await.unwrap().unwrap();
        assert!(found.revoked);
        assert_eq!(found.raw, vec![99]);
    }

    #[tokio::test]
    async fn find_token_with_cap_picks_valid_newest_and_skips_bad() {
        let store = open_temp();
        let iss = node(1);
        let sub = node(2);
        let now = unix_secs();

        // expired
        let mut expired = token(10);
        expired.iss = iss;
        expired.sub = sub;
        expired.exp = now.saturating_sub(10);
        store.token_insert(&expired).await.unwrap();
        // revoked
        let mut revoked = token(11);
        revoked.iss = iss;
        revoked.sub = sub;
        revoked.exp = now + 3600;
        revoked.revoked = true;
        store.token_insert(&revoked).await.unwrap();
        // valid, far expiry
        let mut good = token(12);
        good.iss = iss;
        good.sub = sub;
        good.exp = now + 7200;
        store.token_insert(&good).await.unwrap();
        // wrong cap (none) — simulate by empty caps
        let mut nocap = token(13);
        nocap.iss = iss;
        nocap.sub = sub;
        nocap.exp = now + 9999;
        nocap.caps = vec![];
        store.token_insert(&nocap).await.unwrap();

        let found = store
            .find_token_with_cap(iss, sub, Cap::Msg, now)
            .await
            .unwrap()
            .expect("should find the valid msg-cap token");
        // `good` (id 12) is the newest non-revoked, non-expired token that carries Msg.
        assert_eq!(found.id, [12u8; 16]);

        // No token for an unknown subject.
        assert!(store
            .find_token_with_cap(iss, node(99), Cap::Msg, now)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn corrupt_nodeid_lengths_are_rejected_on_read() {
        let store = open_temp();
        store
            .test_with_conn(|conn| {
                conn.execute(
                    "INSERT INTO peers(nodeid, name, added_at_ms) VALUES(?1, 'bad', 0)",
                    params![vec![1u8; 31]],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(store.peer_list().await.is_err());
    }

    #[tokio::test]
    async fn channel_id_is_stable_and_distinguishes_names() {
        assert_eq!(channel_id("default"), channel_id("default"));
        assert_ne!(channel_id("default"), channel_id("Default"));
        assert_ne!(channel_id("alpha"), channel_id("beta"));
        let _ = channel_id("");
    }

    #[tokio::test]
    async fn outbox_enqueue_assigns_monotonic_seq_per_channel() {
        let store = open_temp();
        let me = node(1);
        let bob = node(2);

        let r1 = store
            .outbox_enqueue(me, bob, "default", b"hi 1".to_vec())
            .await
            .unwrap();
        let r2 = store
            .outbox_enqueue(me, bob, "default", b"hi 2".to_vec())
            .await
            .unwrap();
        let r_other = store
            .outbox_enqueue(me, bob, "other", b"alt".to_vec())
            .await
            .unwrap();
        let r3 = store
            .outbox_enqueue(me, bob, "default", b"hi 3".to_vec())
            .await
            .unwrap();

        assert_eq!(r1.seq, 1);
        assert_eq!(r2.seq, 2);
        assert_eq!(r3.seq, 3);
        assert_eq!(r_other.seq, 1);
        assert!(matches!(r1.direction, Direction::Out));
        assert!(r1.delivered_at_ms.is_none());
    }

    #[tokio::test]
    async fn outbox_pending_for_peer_and_distinct_peers() {
        let store = open_temp();
        let me = node(1);
        let bob = node(2);
        let carol = node(3);

        store
            .outbox_enqueue(me, bob, "x", b"1".to_vec())
            .await
            .unwrap();
        store
            .outbox_enqueue(me, carol, "x", b"2".to_vec())
            .await
            .unwrap();
        store
            .outbox_enqueue(me, bob, "x", b"3".to_vec())
            .await
            .unwrap();

        let bob_pending = store.outbox_pending_for_peer(&bob, 10).await.unwrap();
        assert_eq!(bob_pending.len(), 2);
        assert!(bob_pending.iter().all(|r| r.peer_nodeid == bob));

        let carol_pending = store.outbox_pending_for_peer(&carol, 10).await.unwrap();
        assert_eq!(carol_pending.len(), 1);

        let mut peers = store.outbox_pending_peers().await.unwrap();
        peers.sort_by_key(|n| n.0);
        assert_eq!(peers, vec![bob, carol]);

        // After delivering bob's messages, only carol remains pending.
        for r in &bob_pending {
            store
                .outbox_mark_delivered(&r.sender, &r.channel_id, r.seq)
                .await
                .unwrap();
        }
        let peers = store.outbox_pending_peers().await.unwrap();
        assert_eq!(peers, vec![carol]);
    }

    #[tokio::test]
    async fn inbox_record_dedups_duplicate_seq() {
        let store = open_temp();
        let alice = node(3);

        assert!(store
            .inbox_record(alice, "default", 1, b"hi", 1000)
            .await
            .unwrap());
        assert!(!store
            .inbox_record(alice, "default", 1, b"hi", 1000)
            .await
            .unwrap());
        assert!(store
            .inbox_record(alice, "default", 2, b"hi 2", 2000)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn inbox_backlog_filters_by_peer_channel_and_since() {
        let store = open_temp();
        let alice = node(3);
        let carol = node(4);

        store
            .inbox_record(alice, "chat", 1, b"a1", 100)
            .await
            .unwrap();
        store
            .inbox_record(alice, "chat", 2, b"a2", 200)
            .await
            .unwrap();
        store
            .inbox_record(alice, "ops", 1, b"a3", 300)
            .await
            .unwrap();
        store
            .inbox_record(carol, "chat", 1, b"c1", 400)
            .await
            .unwrap();

        let all = store.inbox_backlog(None, None, None, 100).await.unwrap();
        assert_eq!(all.len(), 4);

        let only_alice = store
            .inbox_backlog(Some(&alice), None, None, 100)
            .await
            .unwrap();
        assert_eq!(only_alice.len(), 3);
        assert!(only_alice.iter().all(|r| r.peer_nodeid == alice));

        let alice_chat = store
            .inbox_backlog(Some(&alice), Some("chat"), None, 100)
            .await
            .unwrap();
        assert_eq!(alice_chat.len(), 2);
        assert!(alice_chat.iter().all(|r| r.channel == "chat"));

        let chat_only = store
            .inbox_backlog(None, Some("chat"), None, 100)
            .await
            .unwrap();
        assert_eq!(chat_only.len(), 3);

        let recent = store
            .inbox_backlog(None, None, Some(150), 100)
            .await
            .unwrap();
        assert_eq!(recent.len(), 3);
        assert!(recent.iter().all(|r| r.enqueued_at_ms > 150));

        let limited = store.inbox_backlog(None, None, None, 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        assert!(limited[0].enqueued_at_ms <= limited[1].enqueued_at_ms);
    }

    // ===== v3 orchestration layer =====

    fn agent_row(group: &str, name: &str, hash: &[u8], role: &str) -> AgentRow {
        AgentRow {
            group_name: group.to_string(),
            name: name.to_string(),
            token_hash: hash.to_vec(),
            role: role.to_string(),
            dir: None,
            pid: None,
            status: "running".to_string(),
            created_at_ms: unix_ms(),
            last_seen_ms: None,
        }
    }

    fn session_row(group: &str, name: &str) -> SessionRow {
        SessionRow {
            group_name: group.to_string(),
            name: name.to_string(),
            prime_agent: "prime".to_string(),
            child_agent: name.to_string(),
            fs_mode: "worktree".to_string(),
            base_dir: Some("/proj".to_string()),
            workspace_path: format!("/tmp/wt/{group}/{name}"),
            branch: Some(format!("wt/{group}/{name}")),
            status: "active".to_string(),
            created_at_ms: unix_ms(),
        }
    }

    #[tokio::test]
    async fn group_create_is_idempotent_by_name() {
        let store = open_temp();
        assert!(store.group_create("myapp").await.unwrap());
        assert!(!store.group_create("myapp").await.unwrap());
        assert!(store.group_exists("myapp").await.unwrap());
        assert!(!store.group_exists("other").await.unwrap());
        let groups = store.group_list().await.unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "myapp");
    }

    #[tokio::test]
    async fn agent_register_unique_per_group_and_lookup_by_token() {
        let store = open_temp();
        store.group_create("g").await.unwrap();
        assert!(store
            .agent_register(&agent_row("g", "prime", b"hash-prime", "prime"))
            .await
            .unwrap());
        // Same (group, name) again is not re-created (name uniqueness within a group).
        assert!(!store
            .agent_register(&agent_row("g", "prime", b"hash-prime-2", "prime"))
            .await
            .unwrap());

        store
            .agent_register(&agent_row("g", "frontend", b"hash-fe", "child"))
            .await
            .unwrap();

        let found = store.agent_by_token(b"hash-fe").await.unwrap().unwrap();
        assert_eq!(found.name, "frontend");
        assert_eq!(found.role, "child");
        assert!(store.agent_by_token(b"nope").await.unwrap().is_none());

        assert_eq!(store.agent_list(Some("g")).await.unwrap().len(), 2);

        store
            .agent_set_status("g", "frontend", "awaiting_input", Some(4242))
            .await
            .unwrap();
        let fe = store.agent_get("g", "frontend").await.unwrap().unwrap();
        assert_eq!(fe.status, "awaiting_input");
        assert_eq!(fe.pid, Some(4242));
    }

    #[tokio::test]
    async fn session_create_list_and_close() {
        let store = open_temp();
        store.group_create("g").await.unwrap();
        assert!(store
            .session_create(&session_row("g", "frontend"))
            .await
            .unwrap());
        assert!(!store
            .session_create(&session_row("g", "frontend"))
            .await
            .unwrap());
        store
            .session_create(&session_row("g", "backend"))
            .await
            .unwrap();

        let list = store.session_list("g").await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].name, "frontend");

        assert_eq!(store.session_close("g", "frontend").await.unwrap(), 1);
        let fe = store.session_get("g", "frontend").await.unwrap().unwrap();
        assert_eq!(fe.status, "closed");
    }

    #[tokio::test]
    async fn agent_msg_seq_is_monotonic_per_session() {
        let store = open_temp();
        let a = store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "frontend",
                "prime",
                "turn_output",
                b"a".to_vec(),
            )
            .await
            .unwrap();
        let b = store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "frontend",
                "prime",
                "turn_output",
                b"b".to_vec(),
            )
            .await
            .unwrap();
        let c = store
            .agent_msg_enqueue(
                "g",
                "backend",
                "backend",
                "prime",
                "turn_output",
                b"c".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!((a.seq, b.seq, c.seq), (1, 2, 1));
    }

    #[tokio::test]
    async fn agent_msg_backlog_filters_by_recipient_session_and_since() {
        let store = open_temp();
        store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "prime",
                "frontend",
                "turn_input",
                b"do x".to_vec(),
            )
            .await
            .unwrap();
        store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "frontend",
                "prime",
                "turn_output",
                b"done x".to_vec(),
            )
            .await
            .unwrap();
        store
            .agent_msg_enqueue(
                "g",
                "backend",
                "backend",
                "prime",
                "turn_output",
                b"done y".to_vec(),
            )
            .await
            .unwrap();

        // Everything addressed to the prime in group g (across sessions).
        let to_prime = store
            .agent_msg_backlog("g", "prime", None, None, 100, false)
            .await
            .unwrap();
        assert_eq!(to_prime.len(), 2);
        assert!(to_prime.iter().all(|m| m.to_agent == "prime"));

        // Scoped to the frontend session only.
        let fe = store
            .agent_msg_backlog("g", "prime", Some("frontend"), None, 100, false)
            .await
            .unwrap();
        assert_eq!(fe.len(), 1);
        assert_eq!(fe[0].payload, b"done x");

        // Addressed to the frontend child (the prime's reply).
        let to_fe = store
            .agent_msg_backlog("g", "frontend", None, None, 100, false)
            .await
            .unwrap();
        assert_eq!(to_fe.len(), 1);
        assert_eq!(to_fe[0].kind, "turn_input");

        // `since` in the future excludes everything.
        let recent = store
            .agent_msg_backlog("g", "prime", None, Some(unix_ms() + 10_000), 100, false)
            .await
            .unwrap();
        assert!(recent.is_empty());
    }

    #[tokio::test]
    async fn agent_msg_pending_filters_kind_and_consumed() {
        let store = open_temp();
        store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "prime",
                "frontend",
                "turn_input",
                b"t1".to_vec(),
            )
            .await
            .unwrap();
        let r2 = store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "prime",
                "frontend",
                "turn_input",
                b"t2".to_vec(),
            )
            .await
            .unwrap();
        // A non-turn_input to the same child is excluded.
        store
            .agent_msg_enqueue(
                "g",
                "frontend",
                "prime",
                "frontend",
                "user",
                b"note".to_vec(),
            )
            .await
            .unwrap();

        let pending = store
            .agent_msg_pending("g", "frontend", "turn_input")
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].payload, b"t1");
        assert_eq!(pending[1].payload, b"t2");

        // Consuming the first leaves only the second, in order.
        store
            .agent_msg_mark_consumed("g", "frontend", pending[0].seq)
            .await
            .unwrap();
        let pending2 = store
            .agent_msg_pending("g", "frontend", "turn_input")
            .await
            .unwrap();
        assert_eq!(pending2.len(), 1);
        assert_eq!(pending2[0].seq, r2.seq);
    }

    #[tokio::test]
    async fn agent_msg_backlog_unconsumed_only_filters_consumed() {
        let store = open_temp();
        let a = store
            .agent_msg_enqueue("g", "s", "child", "prime", "turn_output", b"one".to_vec())
            .await
            .unwrap();
        store
            .agent_msg_enqueue("g", "s", "child", "prime", "turn_output", b"two".to_vec())
            .await
            .unwrap();
        store
            .agent_msg_mark_consumed("g", "s", a.seq)
            .await
            .unwrap();

        let unconsumed = store
            .agent_msg_backlog("g", "prime", None, None, 100, true)
            .await
            .unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].payload, b"two");

        let all = store
            .agent_msg_backlog("g", "prime", None, None, 100, false)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn reconcile_stale_agents_exits_live_and_closes_sessions() {
        let store = open_temp();
        store.group_create("g").await.unwrap();
        store
            .agent_register(&agent_row("g", "running1", b"h1", "child"))
            .await
            .unwrap();
        let mut done = agent_row("g", "done1", b"h2", "child");
        done.status = "exited".to_string();
        store.agent_register(&done).await.unwrap();
        store
            .session_create(&session_row("g", "running1"))
            .await
            .unwrap();

        let n = store.reconcile_stale_agents().await.unwrap();
        assert_eq!(n, 1); // only running1 was live

        assert_eq!(
            store
                .agent_get("g", "running1")
                .await
                .unwrap()
                .unwrap()
                .status,
            "exited"
        );
        assert_eq!(
            store.agent_get("g", "done1").await.unwrap().unwrap().status,
            "exited"
        );
        assert_eq!(
            store
                .session_get("g", "running1")
                .await
                .unwrap()
                .unwrap()
                .status,
            "closed"
        );
    }

    #[tokio::test]
    async fn agent_msg_enqueue_trims_to_retention_cap() {
        let store = open_temp();
        // The cfg(test) cap is 8; enqueue 12 to one (group, session).
        for i in 0..12 {
            store
                .agent_msg_enqueue(
                    "g",
                    "s",
                    "child",
                    "x",
                    "turn_output",
                    format!("m{i}").into_bytes(),
                )
                .await
                .unwrap();
        }
        let rows = store
            .agent_msg_backlog("g", "x", Some("s"), None, 1000, false)
            .await
            .unwrap();
        assert_eq!(rows.len(), MAX_MSGS_PER_SESSION as usize); // 8
        assert_eq!(rows.first().unwrap().seq, 5); // 12 - 8 + 1
        assert_eq!(rows.last().unwrap().seq, 12);
    }
}
