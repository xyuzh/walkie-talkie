//! Long-lived state owned by the daemon: identity, store, transport, live connection table,
//! and per-peer delivery.
//!
//! Delivery is **per-peer**: each peer with outbound traffic gets its own task that owns that
//! peer's `SendStream`s and drains only that peer's outbox slice. A slow or unreachable peer
//! therefore stalls only its own task — never delivery to other peers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use iroh::endpoint::SendStream;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, info, warn};
use wt_core::auth;
use wt_core::framing;
use wt_core::identity::Identity;
use wt_core::paths;
use wt_core::services::msg as msg_svc;
use wt_core::store::{unix_ms, PeerSelectorBytes, Store};
use wt_core::transport::Transport;
use wt_proto::ipc::{AgentMsgKind, ConnInfo, IpcError, IpcErrorCode, IpcEvent};
use wt_proto::token::Cap;
use wt_proto::wire::{MessageFrame, StreamOpen};
use wt_proto::NodeId;

/// How many outbox rows a per-peer task pulls per batch.
const DELIVERY_BATCH: usize = 64;
/// Per-peer retry interval after a failed delivery (scoped to that peer only).
const DELIVERY_RETRY: Duration = Duration::from_secs(5);

/// One live link to a peer. Display-only state for `wt conn`; actual stream ownership lives in
/// the per-peer delivery task (outbound) or the inbound recv task (inbound).
pub struct PeerLink {
    pub nodeid: NodeId,
    pub name: String,
    pub since_ms: u64,
    pub via_relay: bool,
    pub stream_labels: Vec<String>,
}

/// An event on the local agent bus (v3 orchestration). Broadcast in-process on `agent_bcast`;
/// `AgentRecv` subscribers keep only the events whose `group` + `to_agent` match them. Carries the
/// routing fields (`group`, `to_agent`) that the client-facing `IpcEvent::AgentMsg` omits.
#[derive(Clone)]
pub struct BusEvent {
    pub group: String,
    pub session: String,
    pub from_agent: String,
    pub to_agent: String,
    pub kind: AgentMsgKind,
    pub payload: Vec<u8>,
    pub ts_ms: u64,
    /// Per-(group, session) sequence of the durable row, so a live-tail consumer can mark it read.
    pub seq: u64,
}

pub(crate) fn kind_to_str(k: AgentMsgKind) -> &'static str {
    match k {
        AgentMsgKind::TurnOutput => "turn_output",
        AgentMsgKind::TurnInput => "turn_input",
        AgentMsgKind::User => "user",
        AgentMsgKind::Control => "control",
        AgentMsgKind::Trace => "trace",
    }
}

pub(crate) fn kind_from_str(s: &str) -> AgentMsgKind {
    match s {
        "turn_output" => AgentMsgKind::TurnOutput,
        "turn_input" => AgentMsgKind::TurnInput,
        "control" => AgentMsgKind::Control,
        "trace" => AgentMsgKind::Trace,
        _ => AgentMsgKind::User,
    }
}

pub struct DaemonState {
    pub identity: Identity,
    pub store: Store,
    pub transport: Transport,
    pub conns: Arc<Mutex<HashMap<NodeId, PeerLink>>>,
    pub recv_bcast: broadcast::Sender<IpcEvent>,
    /// Local agent-bus broadcast (v3 orchestration). `AgentRecv` subscribers filter by group +
    /// recipient; the prime's reply path and child supervisors both ride this channel.
    pub agent_bcast: broadcast::Sender<BusEvent>,
    /// Wake handle per peer with a running delivery task. Absent ⇒ no task yet.
    peer_wakers: Mutex<HashMap<NodeId, mpsc::Sender<()>>>,
    /// Supervisor abort handles for spawned child harnesses, keyed by (group, agent). Aborting a
    /// handle drops the task's `Harness` (kill_on_drop) and reaps the child process.
    children: Mutex<HashMap<(String, String), tokio::task::AbortHandle>>,
    /// Broadcast that tells per-peer delivery tasks to exit (daemon shutdown / test teardown).
    shutdown_tx: broadcast::Sender<()>,
    /// mDNS announcer + browser, or `None` if mDNS init failed.
    pub mdns: Option<crate::mdns::Mdns>,
}

impl DaemonState {
    pub async fn start() -> Result<Self> {
        paths::ensure_dirs()?;
        ensure_no_other_daemon().await?;

        let identity = Identity::load_or_create().context("load/create identity")?;
        let store = Store::open()?;
        // Reconcile state left by a previous (possibly crashed) daemon — a fresh daemon has no live
        // children, so any still-"running" agent / "active" session is stale. (No process killing;
        // orphaned harnesses self-terminate when our stdio pipes break.)
        match store.reconcile_stale_agents().await {
            Ok(n) if n > 0 => info!(
                reconciled = n,
                "reconciled stale agents from a previous daemon"
            ),
            Ok(_) => {}
            Err(e) => warn!(?e, "failed to reconcile stale agents on startup"),
        }
        let transport = Transport::bind(&identity).await?;
        let _ = transport.wait_online(Duration::from_secs(5)).await;
        let (recv_bcast, _) = broadcast::channel(1024);
        let (agent_bcast, _) = broadcast::channel(1024);
        let (shutdown_tx, _) = broadcast::channel(1);

        // mDNS announce + browse on the LAN. Best-effort: if mdns init fails (e.g. CI sandbox
        // without multicast), continue without it.
        let nodeid = identity.nodeid();
        let mdns_port = transport
            .endpoint()
            .bound_sockets()
            .first()
            .map(|sa| sa.port())
            .unwrap_or(0);
        let mdns = if mdns_port == 0 {
            None
        } else {
            match crate::mdns::Mdns::start(nodeid, mdns_port) {
                Ok(m) => Some(m),
                Err(e) => {
                    warn!(?e, "mDNS init failed; continuing without local discovery");
                    None
                }
            }
        };

        write_pidfile().context("write pidfile")?;

        info!(nodeid = %identity.nodeid(), "daemon ready");
        Ok(Self {
            identity,
            store,
            transport,
            conns: Arc::new(Mutex::new(HashMap::new())),
            recv_bcast,
            agent_bcast,
            peer_wakers: Mutex::new(HashMap::new()),
            children: Mutex::new(HashMap::new()),
            shutdown_tx,
            mdns,
        })
    }

    pub async fn shutdown(&self) {
        self.shutdown_signal();
        // Abort supervisor tasks; each drops its `Harness` (kill_on_drop) and reaps the child.
        {
            let mut children = self.children.lock().await;
            for (_, h) in children.drain() {
                h.abort();
            }
        }
        if let Some(m) = self.mdns.as_ref() {
            m.shutdown();
        }
        self.transport.close().await;
        let _ = paths::unlink_if_exists(&paths::daemon_pid_path());
        let _ = paths::unlink_if_exists(&paths::daemon_sock_path());
    }

    /// Fire the shutdown broadcast (sync; usable from `Drop`). Per-peer delivery tasks observe it
    /// and exit.
    pub fn shutdown_signal(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Resume delivery for every peer that has undelivered outbound messages (called at startup
    /// so a daemon restart drains its backlog).
    pub async fn resume_delivery(self: &Arc<Self>) {
        match self.store.outbox_pending_peers().await {
            Ok(peers) => {
                for peer in peers {
                    self.notify_peer(peer).await;
                }
            }
            Err(e) => warn!(?e, "failed to scan outbox for delivery resume"),
        }
    }

    /// Wake (or lazily spawn) the delivery task for `peer`.
    pub async fn notify_peer(self: &Arc<Self>, peer: NodeId) {
        let mut wakers = self.peer_wakers.lock().await;
        if let Some(tx) = wakers.get(&peer) {
            match tx.try_send(()) {
                Ok(()) => return,
                // A wake is already queued; the task will drain on its next loop.
                Err(TrySendError::Full(_)) => return,
                // Task has exited — fall through to respawn.
                Err(TrySendError::Closed(_)) => {
                    wakers.remove(&peer);
                }
            }
        }
        let (tx, rx) = mpsc::channel::<()>(1);
        wakers.insert(peer, tx);
        let state = self.clone();
        tokio::spawn(per_peer_delivery_task(state, peer, rx));
    }

    /// Accept incoming iroh connections and dispatch their first stream.
    pub async fn run_accept_loop(self: Arc<Self>) {
        loop {
            let incoming = match self.transport.endpoint().accept().await {
                Some(i) => i,
                None => {
                    info!("endpoint accept returned None, exiting accept loop");
                    return;
                }
            };
            let state = self.clone();
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => {
                        if let Err(e) = state.handle_connection(conn).await {
                            warn!(?e, "handle_connection ended with error");
                        }
                    }
                    Err(e) => warn!(?e, "incoming connection failed to complete"),
                }
            });
        }
    }

    async fn handle_connection(&self, conn: iroh::endpoint::Connection) -> Result<()> {
        let remote_pk = conn.remote_id();
        let remote = NodeId(remote_pk.as_bytes().to_owned());
        let via_relay = false;
        debug!(peer = %remote, "incoming connection");

        auth::require_peer_known(&self.store, &remote)
            .await
            .map_err(|e| anyhow!("peer not known: {e}"))?;

        loop {
            let (send, mut recv) = match conn.accept_bi().await {
                Ok(s) => s,
                Err(e) => {
                    debug!(?e, "accept_bi ended");
                    break;
                }
            };
            let stream_open = msg_svc::read_stream_open(&mut recv).await?;
            match stream_open {
                StreamOpen::Msg { token, channel } => {
                    if let Err(e) = auth::verify_token(
                        &token,
                        self.identity.nodeid(),
                        remote,
                        Cap::Msg,
                        &self.store,
                    )
                    .await
                    {
                        warn!(peer = %remote, ?e, "Msg stream auth rejected");
                        let _ = send_inline_err(send, &format!("auth: {e}")).await;
                        continue;
                    }
                    info!(peer = %remote, channel = %channel, "Msg stream accepted");
                    let peer_name = self
                        .store
                        .peer_get(&PeerSelectorBytes::NodeId(remote))
                        .await
                        .ok()
                        .flatten()
                        .map(|p| p.name);
                    let _ = self.store.peer_touch_seen(&remote).await;
                    self.spawn_inbound_msg(remote, peer_name, channel, send, recv, via_relay)
                        .await;
                }
            }
        }
        let mut conns = self.conns.lock().await;
        conns.remove(&remote);
        Ok(())
    }

    async fn spawn_inbound_msg(
        &self,
        remote: NodeId,
        peer_name: Option<String>,
        channel: String,
        _send_unused: iroh::endpoint::SendStream,
        recv: iroh::endpoint::RecvStream,
        via_relay: bool,
    ) {
        let label = format!("msg:{channel}");
        {
            let mut conns = self.conns.lock().await;
            conns
                .entry(remote)
                .and_modify(|link| {
                    if !link.stream_labels.iter().any(|s| s == &label) {
                        link.stream_labels.push(label.clone());
                    }
                })
                .or_insert_with(|| PeerLink {
                    nodeid: remote,
                    name: peer_name
                        .clone()
                        .unwrap_or_else(|| hex::encode(&remote.0[..6])),
                    since_ms: unix_ms(),
                    via_relay,
                    stream_labels: vec![label.clone()],
                });
        }

        let (out_tx, mut out_rx) = mpsc::channel::<MessageFrame>(256);
        let bcast = self.recv_bcast.clone();
        let store = self.store.clone();
        let conns = self.conns.clone();
        let from_name = peer_name;
        let recv_task = tokio::spawn(msg_svc::run_recv_loop(recv, out_tx));
        let channel_for_task = channel.clone();
        tokio::spawn(async move {
            while let Some(mf) = out_rx.recv().await {
                // v0.2: dedup by (sender, channel, seq).
                let recorded = match store
                    .inbox_record(remote, &channel_for_task, mf.seq, &mf.payload, mf.ts_ms)
                    .await
                {
                    Ok(true) => true,
                    Ok(false) => {
                        debug!(
                            peer = %remote,
                            channel = %channel_for_task,
                            seq = mf.seq,
                            "duplicate inbound message dropped"
                        );
                        false
                    }
                    Err(e) => {
                        warn!(?e, "inbox_record failed; emitting without persisting");
                        true
                    }
                };
                if recorded {
                    let _ = bcast.send(IpcEvent::RecvMsg {
                        from: remote,
                        from_name: from_name.clone(),
                        channel: channel_for_task.clone(),
                        payload: mf.payload,
                        ts_ms: mf.ts_ms,
                    });
                }
            }
            let _ = recv_task.await;
            let mut conns = conns.lock().await;
            if let Some(link) = conns.get_mut(&remote) {
                link.stream_labels.retain(|s| s != &label);
            }
        });
    }

    /// Open a fresh outbound `Msg` stream to `target` on `channel`, write the `StreamOpen` frame,
    /// and return the SendStream. The caller (per-peer delivery task) owns the stream.
    pub async fn open_outbound_stream(&self, target: NodeId, channel: &str) -> Result<SendStream> {
        let peer = self
            .store
            .peer_get(&PeerSelectorBytes::NodeId(target))
            .await?
            .ok_or_else(|| anyhow!("peer not in local registry"))?;
        let now = wt_core::store::unix_secs();
        let local = self.identity.nodeid();
        // Token issued BY target TO us, carrying the Msg cap — indexed lookup, not a full scan.
        let tok = self
            .store
            .find_token_with_cap(target, local, Cap::Msg, now)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "no valid msg-cap token from {} present locally — ask them to grant + import",
                    target
                )
            })?;
        let signed: wt_proto::token::SignedToken =
            ciborium::from_reader(&tok.raw[..]).context("decode stored signed token")?;

        let conn = if let Some(blob) = peer.addr_blob.as_ref() {
            let ticket: wt_proto::ticket::AddrTicket =
                ciborium::from_reader(&blob[..]).context("decode stored peer ticket")?;
            self.transport.connect_ticket(&ticket).await?
        } else {
            self.transport.connect(target).await?
        };
        let (mut send, _recv_unused) = conn.open_bi().await?;

        let so = StreamOpen::Msg {
            token: signed,
            channel: channel.to_string(),
        };
        msg_svc::write_stream_open(&mut send, &so).await?;

        // Record presence for `wt conn`.
        let label = format!("msg:{channel}");
        let mut conns = self.conns.lock().await;
        conns
            .entry(target)
            .and_modify(|link| {
                if !link.stream_labels.iter().any(|s| s == &label) {
                    link.stream_labels.push(label.clone());
                }
            })
            .or_insert_with(|| PeerLink {
                nodeid: target,
                name: peer.name.clone(),
                since_ms: unix_ms(),
                via_relay: false,
                stream_labels: vec![label],
            });
        Ok(send)
    }

    /// Drop the `wt conn` label for a (peer, channel) outbound stream we just lost.
    async fn forget_outbound_stream(&self, target: NodeId, channel: &str) {
        let label = format!("msg:{channel}");
        let mut conns = self.conns.lock().await;
        if let Some(link) = conns.get_mut(&target) {
            link.stream_labels.retain(|s| s != &label);
        }
    }

    pub async fn list_conns(&self) -> Vec<ConnInfo> {
        let conns = self.conns.lock().await;
        let mut out = Vec::with_capacity(conns.len());
        for link in conns.values() {
            out.push(ConnInfo {
                peer_name: link.name.clone(),
                nodeid: link.nodeid,
                since_ms: link.since_ms,
                streams: link.stream_labels.clone(),
                rx_bytes: 0,
                tx_bytes: 0,
                rtt_us: None,
                via_relay: link.via_relay,
            });
        }
        out
    }

    /// Enqueue a message on the local bus and publish it for live subscribers. Shared by the
    /// `AgentSend` IPC handler and the harness supervisor. Returns the assigned per-session `seq`
    /// (useful when the caller needs to immediately mark the row consumed, e.g. recording the
    /// initial spawn prompt for display without re-feeding it to the harness).
    pub(crate) async fn bus_enqueue(
        &self,
        group: &str,
        session: &str,
        from: &str,
        to: &str,
        kind: AgentMsgKind,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let row = self
            .store
            .agent_msg_enqueue(group, session, from, to, kind_to_str(kind), payload.clone())
            .await?;
        let _ = self.agent_bcast.send(BusEvent {
            group: group.to_string(),
            session: session.to_string(),
            from_agent: from.to_string(),
            to_agent: to.to_string(),
            kind,
            payload,
            ts_ms: row.enqueued_at_ms,
            seq: row.seq,
        });
        Ok(row.seq)
    }

    /// Register a supervisor's abort handle for a spawned child agent.
    pub(crate) async fn register_child(
        &self,
        group: &str,
        agent: &str,
        handle: tokio::task::AbortHandle,
    ) {
        self.children
            .lock()
            .await
            .insert((group.to_string(), agent.to_string()), handle);
    }

    /// Abort + deregister a spawned child's supervisor. Returns whether one was present.
    pub(crate) async fn kill_child(&self, group: &str, agent: &str) -> bool {
        if let Some(h) = self
            .children
            .lock()
            .await
            .remove(&(group.to_string(), agent.to_string()))
        {
            h.abort();
            true
        } else {
            false
        }
    }

    /// Remove a child's entry from the supervisor map (called when its task exits naturally).
    pub(crate) async fn forget_child(&self, group: &str, agent: &str) {
        self.children
            .lock()
            .await
            .remove(&(group.to_string(), agent.to_string()));
    }
}

/// One peer's delivery loop. Owns this peer's outbound `SendStream`s (keyed by channel) and
/// drains only this peer's outbox slice. Errors back off and retry without touching other peers.
async fn per_peer_delivery_task(
    state: Arc<DaemonState>,
    peer: NodeId,
    mut wake: mpsc::Receiver<()>,
) {
    let mut shutdown = state.shutdown_tx.subscribe();
    let mut streams: HashMap<String, SendStream> = HashMap::new();
    loop {
        // Drain-first: a freshly spawned task delivers immediately.
        if let Err(e) = drain_peer(&state, peer, &mut streams).await {
            warn!(%peer, ?e, "per-peer delivery error; dropping streams, will retry");
            streams.clear();
        }
        tokio::select! {
            v = wake.recv() => {
                if v.is_none() {
                    break; // all wake senders dropped
                }
            }
            _ = tokio::time::sleep(DELIVERY_RETRY) => {} // retry any rows still pending
            _ = shutdown.recv() => {
                debug!(%peer, "delivery task shutting down");
                break;
            }
        }
    }
    // Remove our waker so a future notify respawns a fresh task.
    let mut wakers = state.peer_wakers.lock().await;
    wakers.remove(&peer);
}

/// Deliver all currently-pending outbound rows for `peer`. Returns `Ok(())` when the peer's
/// outbox is drained; returns `Err` (so the caller backs off) on the first write/open failure.
async fn drain_peer(
    state: &Arc<DaemonState>,
    peer: NodeId,
    streams: &mut HashMap<String, SendStream>,
) -> Result<()> {
    loop {
        let rows = state
            .store
            .outbox_pending_for_peer(&peer, DELIVERY_BATCH)
            .await?;
        if rows.is_empty() {
            return Ok(());
        }
        for row in rows {
            if !streams.contains_key(&row.channel) {
                let stream = state
                    .open_outbound_stream(peer, &row.channel)
                    .await
                    .context("open outbound stream")?;
                streams.insert(row.channel.clone(), stream);
            }
            let send = streams.get_mut(&row.channel).unwrap();

            let frame = MessageFrame {
                seq: row.seq,
                ts_ms: row.enqueued_at_ms,
                payload: row.payload.clone(),
            };
            let mut buf = Vec::new();
            ciborium::into_writer(&frame, &mut buf).context("encode MessageFrame")?;
            if let Err(e) = framing::write_cbor_frame(send, &buf).await {
                // Stream broke — drop it + deregister, then propagate to trigger backoff/retry.
                streams.remove(&row.channel);
                state.forget_outbound_stream(peer, &row.channel).await;
                return Err(e).context("write MessageFrame to wire");
            }

            state
                .store
                .outbox_mark_delivered(&row.sender, &row.channel_id, row.seq)
                .await
                .context("mark delivered in outbox")?;
            debug!(peer = %peer, channel = %row.channel, seq = row.seq, "delivered");
        }
    }
}

async fn send_inline_err(mut send: iroh::endpoint::SendStream, msg: &str) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    ciborium::into_writer(
        &IpcEvent::Err(IpcError::new(IpcErrorCode::Unauthorized, msg)),
        &mut buf,
    )?;
    framing::write_cbor_frame(&mut send, &buf).await?;
    let _ = send.finish();
    Ok(())
}

async fn ensure_no_other_daemon() -> Result<()> {
    let pid_path = paths::daemon_pid_path();
    let sock_path = paths::daemon_sock_path();
    if let Ok(content) = std::fs::read_to_string(&pid_path) {
        if let Ok(pid) = content.trim().parse::<i32>() {
            if pid_alive(pid) {
                anyhow::bail!("another wt-daemon is already running (pid {})", pid);
            }
        }
    }
    let _ = paths::unlink_if_exists(&pid_path);
    let _ = paths::unlink_if_exists(&sock_path);
    Ok(())
}

fn write_pidfile() -> std::io::Result<()> {
    let pid_path = paths::daemon_pid_path();
    let pid = std::process::id();
    std::fs::write(&pid_path, format!("{pid}\n"))
}

#[cfg(unix)]
fn pid_alive(pid: i32) -> bool {
    // SAFETY: kill(2) with signal 0 is a portable existence check.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(unix))]
fn pid_alive(_pid: i32) -> bool {
    false
}
