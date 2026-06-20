//! Unix-domain-socket IPC server. Frames are length-prefixed CBOR `IpcRequest` → `IpcEvent`.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use wt_core::auth;
use wt_core::paths;
use wt_core::store::{AgentRow, PeerSelectorBytes, SessionRow};
use wt_proto::ipc::{
    AgentInfo, FsMode, GroupInfo, IpcError, IpcErrorCode, IpcEvent, IpcRequest, PeerFilter,
    PeerInfo, PeerSelector, PeerSource, PeerState, SessionInfo, TokenInfo, WhoAmIInfo,
};
use wt_proto::token::SignedToken;
use wt_proto::NodeId;

use crate::state::{kind_from_str, DaemonState};

/// Build an `IpcEvent::Err` with a structured code.
fn err(code: IpcErrorCode, msg: impl Into<String>) -> IpcEvent {
    IpcEvent::Err(IpcError::new(code, msg))
}

pub async fn run_ipc_server(state: Arc<DaemonState>) {
    let sock_path = paths::daemon_sock_path();
    let _ = paths::unlink_if_exists(&sock_path);
    let listener = match UnixListener::bind(&sock_path) {
        Ok(l) => l,
        Err(e) => {
            error!(?e, "failed to bind IPC socket; daemon exiting");
            return;
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&sock_path) {
            let mut perm = meta.permissions();
            perm.set_mode(0o600);
            let _ = std::fs::set_permissions(&sock_path, perm);
        }
    }
    info!(path = %sock_path.display(), "IPC socket bound");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_ipc_conn(state, stream).await {
                        debug!(?e, "ipc conn ended with error");
                    }
                });
            }
            Err(e) => {
                warn!(?e, "ipc accept failed");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_ipc_conn(state: Arc<DaemonState>, mut stream: UnixStream) -> Result<()> {
    loop {
        let req = match read_frame(&mut stream).await? {
            Some(b) => b,
            None => return Ok(()),
        };
        let req: IpcRequest = match ciborium::from_reader(&req[..]) {
            Ok(r) => r,
            Err(e) => {
                write_event(
                    &mut stream,
                    &err(IpcErrorCode::BadRequest, format!("decode request: {e}")),
                )
                .await?;
                continue;
            }
        };
        debug!(?req, "ipc request");
        let want_stream = matches!(
            req,
            IpcRequest::RecvSubscribe { .. } | IpcRequest::AgentRecv { .. }
        );
        if let Err(e) = dispatch(&state, &req, &mut stream).await {
            // Any error escaping `dispatch` is an unexpected internal failure; user-facing
            // rejections are emitted inline with a specific code.
            write_event(
                &mut stream,
                &IpcEvent::Err(IpcError::internal(format!("{e:#}"))),
            )
            .await?;
        }
        if want_stream {
            // dispatch already drove the loop; once it returns, the subscription is done.
            return Ok(());
        }
    }
}

async fn dispatch(
    state: &Arc<DaemonState>,
    req: &IpcRequest,
    stream: &mut UnixStream,
) -> Result<()> {
    match req {
        IpcRequest::Status => {
            write_event(
                stream,
                &IpcEvent::StatusInfo {
                    nodeid: state.identity.nodeid(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    endpoint_bound: true,
                },
            )
            .await
        }
        IpcRequest::NodeId => {
            write_event(stream, &IpcEvent::NodeIdValue(state.identity.nodeid())).await
        }
        IpcRequest::Ticket => {
            let ticket = state.transport.local_ticket();
            let s = ticket.encode().map_err(|e| anyhow::anyhow!(e))?;
            write_event(stream, &IpcEvent::TicketValue(s)).await
        }
        IpcRequest::PeerAdd {
            nodeid,
            name,
            addr_blob,
        } => {
            state
                .store
                .peer_add(*nodeid, name, addr_blob.as_deref())
                .await?;
            write_event(stream, &IpcEvent::Ok).await
        }
        IpcRequest::PeerRm { selector } => {
            let sel = sel_to_bytes(selector);
            let n = state.store.peer_remove(&sel).await?;
            if n == 0 {
                write_event(stream, &err(IpcErrorCode::NotFound, "no such peer")).await
            } else {
                write_event(stream, &IpcEvent::Ok).await
            }
        }
        IpcRequest::PeerList { filter } => {
            let peers = state.store.peer_list().await?;
            let conns = state.list_conns().await;
            let mut emitted: std::collections::HashSet<NodeId> = Default::default();
            for p in peers {
                let conn_match = conns.iter().find(|c| c.nodeid == p.nodeid);
                let state_field = if let Some(c) = conn_match {
                    PeerState::Connected {
                        open_streams: c.streams.len() as u32,
                    }
                } else if p.last_seen_ms.is_some() {
                    PeerState::Idle
                } else {
                    PeerState::Offline
                };
                let info = PeerInfo {
                    nodeid: p.nodeid,
                    name: p.name,
                    source: PeerSource::Manual,
                    state: state_field,
                    last_seen_ms: p.last_seen_ms,
                };
                if filter_match(filter, &info) {
                    emitted.insert(info.nodeid);
                    write_event(stream, &IpcEvent::PeerListItem(info)).await?;
                }
            }
            // Add mDNS-discovered LAN peers (not yet in the durable peers table).
            if matches!(filter, PeerFilter::All | PeerFilter::Local) {
                if let Some(mdns) = state.mdns.as_ref() {
                    let lan = mdns.lan_peers();
                    let snapshot: Vec<_> = match lan.lock() {
                        Ok(m) => m.values().cloned().collect(),
                        Err(_) => vec![],
                    };
                    for lp in snapshot {
                        if emitted.contains(&lp.nodeid) {
                            continue;
                        }
                        let info = PeerInfo {
                            nodeid: lp.nodeid,
                            name: lp.instance.clone(),
                            source: PeerSource::Mdns,
                            state: PeerState::Idle,
                            last_seen_ms: Some(lp.last_seen_ms),
                        };
                        write_event(stream, &IpcEvent::PeerListItem(info)).await?;
                    }
                }
            }
            write_event(stream, &IpcEvent::PeerListEnd).await
        }
        IpcRequest::ConnList => {
            for c in state.list_conns().await {
                write_event(stream, &IpcEvent::ConnListItem(c)).await?;
            }
            write_event(stream, &IpcEvent::ConnListEnd).await
        }
        IpcRequest::ConnClose { selector: _ } => {
            write_event(
                stream,
                &err(
                    IpcErrorCode::Unimplemented,
                    "conn close not implemented yet",
                ),
            )
            .await
        }
        IpcRequest::TokenGrant {
            peer,
            caps,
            ttl_secs,
        } => {
            let sel = sel_to_bytes(peer);
            let peer_row = match state.store.peer_get(&sel).await? {
                Some(p) => p,
                None => {
                    return write_event(stream, &err(IpcErrorCode::NotFound, "peer not found"))
                        .await
                }
            };
            let (claims, signed) = auth::sign_token(
                state.identity.secret_key(),
                peer_row.nodeid,
                caps.clone(),
                *ttl_secs,
            )?;
            let token_row = auth::token_row(&claims, &signed)?;
            state.store.token_insert(&token_row).await?;
            let mut raw_buf = Vec::new();
            ciborium::into_writer(&signed, &mut raw_buf)?;
            let info = TokenInfo {
                id: claims.id,
                iss: claims.iss,
                sub: claims.sub,
                exp: claims.exp,
                caps: claims.caps,
                revoked: false,
            };
            write_event(stream, &IpcEvent::TokenIssued { raw: raw_buf, info }).await
        }
        IpcRequest::TokenImport { raw } => {
            let signed: SignedToken = match ciborium::from_reader(&raw[..]) {
                Ok(s) => s,
                Err(e) => {
                    return write_event(
                        stream,
                        &err(
                            IpcErrorCode::BadRequest,
                            format!("decode signed token: {e}"),
                        ),
                    )
                    .await
                }
            };
            let claims = match auth::verify_signed_claims(&signed) {
                Ok(c) => c,
                Err(e) => {
                    return write_event(
                        stream,
                        &err(
                            IpcErrorCode::Unauthorized,
                            format!("verify token signature: {e}"),
                        ),
                    )
                    .await
                }
            };
            // Basic sanity: claims.sub should be us. (Issuer is a peer that sent it.)
            if claims.sub != state.identity.nodeid() {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        "token subject is not this install",
                    ),
                )
                .await;
            }
            if state
                .store
                .peer_get(&PeerSelectorBytes::NodeId(claims.iss))
                .await?
                .is_none()
            {
                return write_event(
                    stream,
                    &err(IpcErrorCode::BadRequest, "token issuer is not a known peer"),
                )
                .await;
            }
            let row = auth::token_row(&claims, &signed)?;
            state.store.token_insert(&row).await?;
            let info = TokenInfo {
                id: claims.id,
                iss: claims.iss,
                sub: claims.sub,
                exp: claims.exp,
                caps: claims.caps,
                revoked: false,
            };
            write_event(stream, &IpcEvent::TokenListItem(info)).await?;
            write_event(stream, &IpcEvent::Ok).await
        }
        IpcRequest::TokenList => {
            for r in state.store.token_list().await? {
                let info = TokenInfo {
                    id: r.id,
                    iss: r.iss,
                    sub: r.sub,
                    exp: r.exp,
                    caps: r.caps,
                    revoked: r.revoked,
                };
                write_event(stream, &IpcEvent::TokenListItem(info)).await?;
            }
            write_event(stream, &IpcEvent::TokenListEnd).await
        }
        IpcRequest::TokenRevoke { id } => {
            let n = state.store.token_revoke(id).await?;
            if n == 0 {
                write_event(stream, &err(IpcErrorCode::NotFound, "no such token")).await
            } else {
                write_event(stream, &IpcEvent::Ok).await
            }
        }
        IpcRequest::Send {
            peer,
            channel,
            payload,
        } => {
            let sel = sel_to_bytes(peer);
            let peer_row = match state.store.peer_get(&sel).await? {
                Some(p) => p,
                None => {
                    return write_event(stream, &err(IpcErrorCode::NotFound, "peer not found"))
                        .await
                }
            };
            // Write to the durable outbox, then wake this peer's delivery task.
            state
                .store
                .outbox_enqueue(
                    state.identity.nodeid(),
                    peer_row.nodeid,
                    channel,
                    payload.clone(),
                )
                .await?;
            state.notify_peer(peer_row.nodeid).await;
            write_event(stream, &IpcEvent::Ok).await
        }
        IpcRequest::RecvSubscribe {
            peer,
            channel,
            since_ms,
            follow,
        } => {
            // Resolve peer selector to a NodeId (or None to match all).
            let want_peer = match peer {
                Some(PeerSelector::NodeId(n)) => Some(*n),
                Some(PeerSelector::Name(n)) => state
                    .store
                    .peer_get(&PeerSelectorBytes::Name(n.clone()))
                    .await
                    .ok()
                    .flatten()
                    .map(|r| r.nodeid),
                None => None,
            };
            let want_chan = channel.clone();

            // Subscribe to the live broadcast FIRST so we don't miss events between the backlog
            // drain and the live tail. (A message arriving in that narrow window may appear once
            // in both backlog and live; receiver-side dedup is by `(sender, channel, seq)` in the
            // store, so at worst `recv --follow` prints one duplicate line — acceptable.)
            let mut rx = state.recv_bcast.subscribe();

            // Drain backlog from the inbox.
            let backlog = state
                .store
                .inbox_backlog(want_peer.as_ref(), want_chan.as_deref(), *since_ms, 10_000)
                .await?;
            for row in backlog {
                let from_name = state
                    .store
                    .peer_get(&PeerSelectorBytes::NodeId(row.sender))
                    .await
                    .ok()
                    .flatten()
                    .map(|p| p.name);
                let ev = IpcEvent::RecvMsg {
                    from: row.sender,
                    from_name,
                    channel: row.channel,
                    payload: row.payload,
                    ts_ms: row.enqueued_at_ms,
                };
                write_event(stream, &ev).await?;
            }
            write_event(stream, &IpcEvent::RecvBacklogEnd).await?;
            if !follow {
                return Ok(());
            }

            // Live tail.
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let pass = match &ev {
                            IpcEvent::RecvMsg { from, channel, .. } => {
                                want_peer.is_none_or(|w| w == *from)
                                    && want_chan.as_ref().is_none_or(|c| c == channel)
                            }
                            _ => false,
                        };
                        if pass {
                            write_event(stream, &ev).await?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            Ok(())
        }

        // ===== v3 local orchestration: groups / sessions / agents / local bus =====
        IpcRequest::GroupNew { name } => {
            if !state.store.group_create(name).await? {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        format!("group '{name}' already exists"),
                    ),
                )
                .await;
            }
            // The caller becomes the group's prime agent; return its bearer token.
            let token = auth::new_agent_token();
            let prime = AgentRow {
                group_name: name.clone(),
                name: "prime".to_string(),
                token_hash: auth::agent_token_hash(&token),
                role: "prime".to_string(),
                dir: None,
                pid: None,
                status: "running".to_string(),
                created_at_ms: wt_core::store::unix_ms(),
                last_seen_ms: None,
            };
            state.store.agent_register(&prime).await?;
            write_event(
                stream,
                &IpcEvent::GroupCreated {
                    group: name.clone(),
                    token,
                },
            )
            .await
        }
        IpcRequest::GroupList => {
            for g in state.store.group_list().await? {
                let session_count = state.store.session_list(&g.name).await?.len() as u32;
                write_event(
                    stream,
                    &IpcEvent::GroupListItem(GroupInfo {
                        name: g.name,
                        created_at_ms: g.created_at_ms,
                        session_count,
                    }),
                )
                .await?;
            }
            write_event(stream, &IpcEvent::GroupListEnd).await
        }
        IpcRequest::SessionList { group } => {
            for s in state.store.session_list(group).await? {
                let child_status = state
                    .store
                    .agent_get(&s.group_name, &s.child_agent)
                    .await?
                    .map(|a| a.status);
                write_event(
                    stream,
                    &IpcEvent::SessionListItem(session_info(s, child_status)),
                )
                .await?;
            }
            write_event(stream, &IpcEvent::SessionListEnd).await
        }
        IpcRequest::AgentList { group, session } => {
            let rows = match (group.as_deref(), session.as_deref()) {
                // Both endpoints of a single session.
                (Some(g), Some(s)) => match state.store.session_get(g, s).await? {
                    Some(sess) => {
                        let mut v = Vec::new();
                        if let Some(a) = state.store.agent_get(g, &sess.prime_agent).await? {
                            v.push(a);
                        }
                        if let Some(a) = state.store.agent_get(g, &sess.child_agent).await? {
                            v.push(a);
                        }
                        v
                    }
                    None => {
                        return write_event(stream, &err(IpcErrorCode::NotFound, "no such session"))
                            .await
                    }
                },
                (g, _) => state.store.agent_list(g).await?,
            };
            for a in rows {
                write_event(stream, &IpcEvent::AgentListItem(agent_info(a))).await?;
            }
            write_event(stream, &IpcEvent::AgentListEnd).await
        }
        IpcRequest::AgentSend {
            token,
            session,
            kind,
            payload,
        } => {
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            let sess = match state.store.session_get(&me.group_name, session).await? {
                Some(s) => s,
                None => {
                    return write_event(stream, &err(IpcErrorCode::NotFound, "no such session"))
                        .await
                }
            };
            // Route to the other endpoint of this session.
            let to_agent = if me.name == sess.prime_agent {
                sess.child_agent.clone()
            } else if me.name == sess.child_agent {
                sess.prime_agent.clone()
            } else {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        "sender is not a participant of this session",
                    ),
                )
                .await;
            };
            state
                .bus_enqueue(
                    &me.group_name,
                    session,
                    &me.name,
                    &to_agent,
                    *kind,
                    payload.clone(),
                )
                .await?;
            write_event(stream, &IpcEvent::Ok).await
        }
        IpcRequest::AgentRecv {
            token,
            session,
            since_ms,
            follow,
            consume,
        } => {
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            // Subscribe first so we don't miss events between backlog drain and live tail.
            let mut rx = state.agent_bcast.subscribe();
            // Consume-on-read: default recv returns only undelivered messages and marks them read;
            // `--all` / `--since` (consume=false) give a non-destructive, time-filtered view.
            let (since, unconsumed_only) = if *consume {
                (None, true)
            } else {
                (*since_ms, false)
            };
            let backlog = state
                .store
                .agent_msg_backlog(
                    &me.group_name,
                    &me.name,
                    session.as_deref(),
                    since,
                    10_000,
                    unconsumed_only,
                )
                .await?;
            for row in backlog {
                let (sess, seq) = (row.session_name.clone(), row.seq);
                write_event(
                    stream,
                    &IpcEvent::AgentMsg {
                        session: row.session_name,
                        from_agent: row.from_agent,
                        kind: kind_from_str(&row.kind),
                        payload: row.payload,
                        ts_ms: row.enqueued_at_ms,
                    },
                )
                .await?;
                if *consume {
                    let _ = state
                        .store
                        .agent_msg_mark_consumed(&me.group_name, &sess, seq)
                        .await;
                }
            }
            write_event(stream, &IpcEvent::AgentBacklogEnd).await?;
            if !follow {
                return Ok(());
            }
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let session_match = session.as_ref().is_none_or(|s| s == &ev.session);
                        if ev.group == me.group_name && ev.to_agent == me.name && session_match {
                            let (sess, seq) = (ev.session.clone(), ev.seq);
                            write_event(
                                stream,
                                &IpcEvent::AgentMsg {
                                    session: ev.session,
                                    from_agent: ev.from_agent,
                                    kind: ev.kind,
                                    payload: ev.payload,
                                    ts_ms: ev.ts_ms,
                                },
                            )
                            .await?;
                            if *consume {
                                let _ = state
                                    .store
                                    .agent_msg_mark_consumed(&me.group_name, &sess, seq)
                                    .await;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            Ok(())
        }
        IpcRequest::WhoAmI { token } => {
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            let session = (me.role == "child").then(|| me.name.clone());
            write_event(
                stream,
                &IpcEvent::WhoAmIInfo(WhoAmIInfo {
                    group: me.group_name,
                    agent: me.name,
                    role: me.role,
                    session,
                }),
            )
            .await
        }
        IpcRequest::Spawn {
            token,
            group,
            session,
            base_dir,
            fs_mode,
            branch,
            label,
            prompt,
            harness_argv,
            idle_timeout_secs,
            permission_mode,
            skip_permissions,
            trace,
            coordinator,
        } => {
            let _ = (branch, label); // reserved; v1 derives the branch as wt/<group>/<session>
            // A coordinator is a prime-role harness (so it can spawn + command workers itself);
            // a normal worker is a child-role harness.
            let agent_role = if *coordinator { "prime" } else { "child" };
            let idle_timeout = (*idle_timeout_secs).map(std::time::Duration::from_secs);
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            if me.group_name != *group || me.role != "prime" {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::Unauthorized,
                        "spawn requires the group's prime token",
                    ),
                )
                .await;
            }
            if state.store.session_get(group, session).await?.is_some()
                || state.store.agent_get(group, session).await?.is_some()
            {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        format!("session '{session}' already exists in group '{group}'"),
                    ),
                )
                .await;
            }
            let base = std::path::PathBuf::from(base_dir);
            let ws = match wt_core::workspace::provision(group, session, &base, *fs_mode).await {
                Ok(w) => w,
                Err(e) => {
                    return write_event(
                        stream,
                        &err(
                            IpcErrorCode::BadRequest,
                            format!("workspace provisioning failed: {e}"),
                        ),
                    )
                    .await
                }
            };
            let child_token = auth::new_agent_token();
            let now = wt_core::store::unix_ms();
            // Atomic gate: INSERT OR IGNORE returns false if the name was taken (lost a race) —
            // roll back the freshly-provisioned workspace and error.
            if !state
                .store
                .agent_register(&AgentRow {
                    group_name: group.clone(),
                    name: session.clone(),
                    token_hash: auth::agent_token_hash(&child_token),
                    role: agent_role.to_string(),
                    dir: Some(base_dir.clone()),
                    pid: None,
                    status: "starting".to_string(),
                    created_at_ms: now,
                    last_seen_ms: None,
                })
                .await?
            {
                let _ = wt_core::workspace::teardown(
                    &ws.path,
                    Some(base.as_path()),
                    *fs_mode,
                    ws.branch.as_deref(),
                    true,
                )
                .await;
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        format!("session '{session}' already exists in group '{group}'"),
                    ),
                )
                .await;
            }
            let fs_mode_str = match fs_mode {
                FsMode::Worktree => "worktree",
                FsMode::New => "new",
            };
            if !state
                .store
                .session_create(&SessionRow {
                    group_name: group.clone(),
                    name: session.clone(),
                    prime_agent: me.name.clone(),
                    child_agent: session.clone(),
                    fs_mode: fs_mode_str.to_string(),
                    base_dir: Some(base_dir.clone()),
                    workspace_path: ws.path.to_string_lossy().to_string(),
                    branch: ws.branch.clone(),
                    status: "active".to_string(),
                    created_at_ms: now,
                })
                .await?
            {
                let _ = state
                    .store
                    .agent_set_status(group, session, "exited", None)
                    .await;
                let _ = wt_core::workspace::teardown(
                    &ws.path,
                    Some(base.as_path()),
                    *fs_mode,
                    ws.branch.as_deref(),
                    true,
                )
                .await;
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::BadRequest,
                        format!("session '{session}' already exists in group '{group}'"),
                    ),
                )
                .await;
            }
            // An explicit harness override wins verbatim; otherwise build the Claude command with
            // the requested permission posture (plan / skip-permissions / …) baked into argv.
            let argv = match harness_argv.clone().filter(|v| !v.is_empty()) {
                Some(a) => a,
                None => {
                    wt_core::harness::claude_argv(permission_mode.as_deref(), *skip_permissions)
                }
            };
            let spec = wt_core::harness::HarnessSpec {
                argv,
                cwd: ws.path.clone(),
                env: vec![
                    ("WT_TOKEN".to_string(), child_token.clone()),
                    ("WT_GROUP".to_string(), group.clone()),
                    ("WT_SESSION".to_string(), session.clone()),
                    ("WT_AGENT".to_string(), session.clone()),
                    (
                        "WT_HOME".to_string(),
                        paths::home().to_string_lossy().to_string(),
                    ),
                ],
                initial_prompt: if *coordinator {
                    coordinator_prompt(group, base_dir, prompt)
                } else {
                    prompt.clone()
                },
                stderr_log: Some(paths::logs_dir().join(format!("harness-{group}-{session}.log"))),
            };
            match crate::supervisor::start(
                state.clone(),
                group.clone(),
                session.clone(),
                me.name.clone(),
                spec,
                idle_timeout,
                *trace,
            )
            .await
            {
                Ok(_pid) => {
                    write_event(
                        stream,
                        &IpcEvent::Spawned {
                            group: group.clone(),
                            session: session.clone(),
                            token: child_token,
                            workspace: ws.path.to_string_lossy().to_string(),
                        },
                    )
                    .await
                }
                Err(e) => {
                    // Roll back: mark exited and discard the freshly-provisioned workspace.
                    let _ = state
                        .store
                        .agent_set_status(group, session, "exited", None)
                        .await;
                    let _ = wt_core::workspace::teardown(
                        &ws.path,
                        Some(base.as_path()),
                        *fs_mode,
                        ws.branch.as_deref(),
                        true,
                    )
                    .await;
                    write_event(
                        stream,
                        &err(IpcErrorCode::Internal, format!("harness spawn failed: {e}")),
                    )
                    .await
                }
            }
        }
        IpcRequest::AgentKill { token, agent } => {
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            if me.role != "prime" {
                return write_event(
                    stream,
                    &err(IpcErrorCode::Unauthorized, "only the prime may kill agents"),
                )
                .await;
            }
            if state
                .store
                .agent_get(&me.group_name, agent)
                .await?
                .is_none()
            {
                return write_event(stream, &err(IpcErrorCode::NotFound, "no such agent")).await;
            }
            state.kill_child(&me.group_name, agent).await;
            let _ = state
                .store
                .agent_set_status(&me.group_name, agent, "exited", None)
                .await;
            if state
                .store
                .session_get(&me.group_name, agent)
                .await?
                .is_some()
            {
                let _ = state.store.session_close(&me.group_name, agent).await;
            }
            write_event(stream, &IpcEvent::Ok).await
        }
        IpcRequest::SessionClose {
            token,
            session,
            discard,
        } => {
            let me = match resolve_agent(state, token).await? {
                Some(a) => a,
                None => {
                    return write_event(
                        stream,
                        &err(IpcErrorCode::Unauthorized, "unknown or invalid wt token"),
                    )
                    .await
                }
            };
            if me.role != "prime" {
                return write_event(
                    stream,
                    &err(
                        IpcErrorCode::Unauthorized,
                        "only the prime may close sessions",
                    ),
                )
                .await;
            }
            let sess = match state.store.session_get(&me.group_name, session).await? {
                Some(s) => s,
                None => {
                    return write_event(stream, &err(IpcErrorCode::NotFound, "no such session"))
                        .await
                }
            };
            state.kill_child(&me.group_name, &sess.child_agent).await;
            let _ = state
                .store
                .agent_set_status(&me.group_name, &sess.child_agent, "exited", None)
                .await;
            state.store.session_close(&me.group_name, session).await?;
            let mode = if sess.fs_mode == "worktree" {
                FsMode::Worktree
            } else {
                FsMode::New
            };
            let base = sess.base_dir.as_ref().map(std::path::PathBuf::from);
            let _ = wt_core::workspace::teardown(
                std::path::Path::new(&sess.workspace_path),
                base.as_deref(),
                mode,
                sess.branch.as_deref(),
                *discard,
            )
            .await;
            write_event(stream, &IpcEvent::Ok).await
        }
    }
}

/// Resolve the agent bound to a local bearer `token` (the v3 IPC auth check).
async fn resolve_agent(state: &Arc<DaemonState>, token: &str) -> Result<Option<AgentRow>> {
    let hash = auth::agent_token_hash(token);
    state.store.agent_by_token(&hash).await
}

fn session_info(s: SessionRow, child_status: Option<String>) -> SessionInfo {
    let fs_mode = if s.fs_mode == "worktree" {
        FsMode::Worktree
    } else {
        FsMode::New
    };
    SessionInfo {
        group: s.group_name,
        name: s.name,
        child_agent: s.child_agent,
        fs_mode,
        workspace_path: s.workspace_path,
        branch: s.branch,
        status: s.status,
        child_status,
    }
}

fn agent_info(a: AgentRow) -> AgentInfo {
    AgentInfo {
        group: a.group_name,
        name: a.name,
        role: a.role,
        status: a.status,
        dir: a.dir,
        pid: a.pid,
    }
}

fn filter_match(f: &PeerFilter, p: &PeerInfo) -> bool {
    match f {
        PeerFilter::All => true,
        PeerFilter::Remote => matches!(p.source, PeerSource::Manual),
        PeerFilter::Local => matches!(p.source, PeerSource::Mdns),
        PeerFilter::Connected => matches!(p.state, PeerState::Connected { .. }),
    }
}

fn sel_to_bytes(s: &PeerSelector) -> PeerSelectorBytes {
    match s {
        PeerSelector::Name(n) => PeerSelectorBytes::Name(n.clone()),
        PeerSelector::NodeId(n) => PeerSelectorBytes::NodeId(*n),
    }
}

async fn write_event(stream: &mut UnixStream, ev: &IpcEvent) -> Result<()> {
    let mut buf = Vec::new();
    ciborium::into_writer(ev, &mut buf).context("encode ipc event")?;
    let len = (buf.len() as u32).to_be_bytes();
    stream.write_all(&len).await.context("write ipc len")?;
    stream.write_all(&buf).await.context("write ipc body")?;
    stream.flush().await.context("flush ipc")?;
    Ok(())
}

async fn read_frame(stream: &mut UnixStream) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e).context("read ipc len"),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > 64 * 1024 * 1024 {
        anyhow::bail!("ipc frame too large: {len}");
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await.context("read ipc body")?;
    Ok(Some(buf))
}

/// Build the initial system+task prompt for a prime **coordinator** harness. Teaches it to drive
/// the `wt` CLI (already on PATH, with a prime token in its env) to spawn and command worker
/// sessions, then folds in the human's high-level goal. The coordinator delegates; it does not
/// write code itself. Its turn output is shown to the human in the dashboard.
fn coordinator_prompt(group: &str, base_dir: &str, goal: &str) -> String {
    format!(
        "You are the PRIME COORDINATOR of wt group `{group}`. A human supervises you from a \
dashboard: every message you receive is from them, and each turn's final output is shown back to \
them. You do NOT write code yourself — you delegate to worker agents and report progress.\n\n\
You have the `wt` CLI on your PATH and a prime token already in your environment (WT_TOKEN, \
WT_GROUP=`{group}`). Drive it with the Bash tool to manage workers. The base repository to spawn \
workers from is `{base_dir}`.\n\n\
Commands:\n\
- Spawn a worker (its own isolated git worktree, runs autonomously as a full Claude Code agent):\n\
    wt spawn --session <name> --dir {base_dir} --worktree --prompt \"<the worker's self-contained task>\"\n\
- Send a follow-up instruction to a worker:\n\
    wt send --session <name> --kind turn_input \"<text>\"\n\
- Read a worker's latest output (drains the backlog; add --follow to tail live):\n\
    wt recv --session <name>\n\
- List workers and their status:\n\
    wt ls --group {group}\n\n\
How to work:\n\
- Break the user's goal into a few worker sessions (e.g. `frontend`, `backend`) and spawn them.\n\
- Give each worker a clear, self-contained task; they cannot see this conversation.\n\
- After spawning, check on workers with `wt recv --session <name>` and summarize their progress \
to the user. Re-spawning a session name that already exists will fail — reuse `wt send` instead.\n\
- End every turn with a concise status the user will read in the dashboard.\n\n\
The user's goal:\n{goal}"
    )
}
