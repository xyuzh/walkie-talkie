//! Local HTTP + WebSocket gateway for the browser dashboard (Chrome new-tab extension).
//!
//! A browser page cannot speak the daemon's length-prefixed-CBOR Unix-socket protocol, so this
//! module exposes a small JSON surface over `127.0.0.1` that maps straight onto the existing
//! [`DaemonState`] / [`Store`](wt_core::store::Store). It does **not** re-implement orchestration:
//! reads hit the store, sends reuse [`DaemonState::bus_enqueue`], and the live feed subscribes to
//! the in-process `agent_bcast` broadcast that `bus_enqueue` already publishes to.
//!
//! Trust model: loopback bind + an `Origin` allowlist (extension or localhost) is the boundary —
//! the gateway acts on the prime's behalf locally and needs no bearer token. See the project plan.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::Request;
use axum::extract::{Path, Query, State};
use axum::http::{header::ORIGIN, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::state::{kind_from_str, kind_to_str, DaemonState};

/// Default loopback address for the web gateway; override with `WT_WEB_ADDR`.
const DEFAULT_ADDR: &str = "127.0.0.1:8787";

/// Serve the dashboard gateway until the process exits. Bind failures are logged and the task
/// returns (the daemon keeps running without the gateway) rather than taking the daemon down.
pub async fn run_web_server(state: Arc<DaemonState>) {
    let addr_str = std::env::var("WT_WEB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            warn!(%addr_str, ?e, "invalid WT_WEB_ADDR; web gateway disabled");
            return;
        }
    };

    let app = Router::new()
        .route("/api/groups", get(list_groups))
        .route("/api/groups/:g/sessions", get(list_sessions))
        .route("/api/groups/:g/sessions/:s/messages", get(list_messages))
        .route("/api/groups/:g/sessions/:s/send", post(send_message))
        .route("/api/ws", get(ws_handler))
        // Permissive CORS so the chrome-extension:// origin's fetch/WS is allowed by the browser;
        // the origin_guard below is the actual access control.
        .layer(CorsLayer::permissive())
        .layer(middleware::from_fn(origin_guard))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(%addr, ?e, "web gateway failed to bind; disabled");
            return;
        }
    };
    info!(%addr, "web gateway listening");
    if let Err(e) = axum::serve(listener, app).await {
        warn!(?e, "web gateway exited");
    }
}

/// Reject cross-origin calls from arbitrary websites (DNS-rebinding guard). A request with no
/// `Origin` (curl, same-origin) is allowed; an `Origin` is allowed only if it is a browser
/// extension or a loopback page.
async fn origin_guard(req: Request, next: Next) -> Response {
    if let Some(origin) = req.headers().get(ORIGIN).and_then(|v| v.to_str().ok()) {
        let allowed = origin.starts_with("chrome-extension://")
            || origin.starts_with("moz-extension://")
            || origin.starts_with("http://127.0.0.1")
            || origin.starts_with("http://localhost");
        if !allowed {
            return (StatusCode::FORBIDDEN, "forbidden origin").into_response();
        }
    }
    next.run(req).await
}

/// Map an `anyhow`/store error to a 500 JSON body.
fn err500(e: impl std::fmt::Display) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
        .into_response()
}

async fn list_groups(State(state): State<Arc<DaemonState>>) -> Response {
    match state.store.group_list().await {
        Ok(groups) => {
            let out: Vec<Value> = groups
                .into_iter()
                .map(|g| json!({ "name": g.name, "created_at_ms": g.created_at_ms }))
                .collect();
            Json(out).into_response()
        }
        Err(e) => err500(e),
    }
}

async fn list_sessions(
    State(state): State<Arc<DaemonState>>,
    Path(group): Path<String>,
) -> Response {
    let sessions = match state.store.session_list(&group).await {
        Ok(s) => s,
        Err(e) => return err500(e),
    };
    let mut out = Vec::with_capacity(sessions.len());
    for s in sessions {
        // Join the child agent's runtime status/pid for the progress badge.
        let agent = state
            .store
            .agent_get(&group, &s.child_agent)
            .await
            .ok()
            .flatten();
        out.push(json!({
            "name": s.name,
            "prime_agent": s.prime_agent,
            "child_agent": s.child_agent,
            "fs_mode": s.fs_mode,
            "workspace_path": s.workspace_path,
            "branch": s.branch,
            "session_status": s.status,
            "created_at_ms": s.created_at_ms,
            "agent_status": agent.as_ref().map(|a| a.status.clone()),
            // role of the child agent: "prime" => this is the interactive coordinator panel;
            // "child" => a read-only worker.
            "agent_role": agent.as_ref().map(|a| a.role.clone()),
            "pid": agent.as_ref().and_then(|a| a.pid),
            "last_seen_ms": agent.as_ref().and_then(|a| a.last_seen_ms),
        }));
    }
    Json(out).into_response()
}

#[derive(Deserialize)]
struct MessagesQuery {
    #[serde(default)]
    after_seq: u64,
}

async fn list_messages(
    State(state): State<Arc<DaemonState>>,
    Path((group, session)): Path<(String, String)>,
    Query(q): Query<MessagesQuery>,
) -> Response {
    match state
        .store
        .agent_msg_session_log(&group, &session, q.after_seq)
        .await
    {
        Ok(rows) => {
            let out: Vec<Value> = rows
                .into_iter()
                .map(|m| {
                    json!({
                        "seq": m.seq,
                        "from_agent": m.from_agent,
                        "to_agent": m.to_agent,
                        "kind": m.kind,
                        "text": String::from_utf8_lossy(&m.payload),
                        "ts_ms": m.enqueued_at_ms,
                    })
                })
                .collect();
            Json(out).into_response()
        }
        Err(e) => err500(e),
    }
}

#[derive(Deserialize)]
struct SendBody {
    /// "turn_input" (default) or "user".
    #[serde(default)]
    kind: Option<String>,
    text: String,
}

async fn send_message(
    State(state): State<Arc<DaemonState>>,
    Path((group, session)): Path<(String, String)>,
    Json(body): Json<SendBody>,
) -> Response {
    let sess = match state.store.session_get(&group, &session).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "no such session" })),
            )
                .into_response()
        }
        Err(e) => return err500(e),
    };
    // Default to a turn_input (prime → child reply). The dashboard always acts as the prime.
    let kind = kind_from_str(body.kind.as_deref().unwrap_or("turn_input"));
    match state
        .bus_enqueue(
            &group,
            &session,
            &sess.prime_agent,
            &sess.child_agent,
            kind,
            body.text.into_bytes(),
        )
        .await
    {
        Ok(seq) => Json(json!({ "ok": true, "seq": seq })).into_response(),
        Err(e) => err500(e),
    }
}

#[derive(Deserialize)]
struct WsQuery {
    group: String,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<DaemonState>>,
    Query(q): Query<WsQuery>,
) -> Response {
    ws.on_upgrade(move |socket| ws_loop(socket, state, q.group))
}

/// Forward every agent-bus event for `group` to the socket as JSON until the client disconnects.
async fn ws_loop(mut socket: WebSocket, state: Arc<DaemonState>, group: String) {
    let mut rx = state.agent_bcast.subscribe();
    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Ok(ev) if ev.group == group => {
                    let payload = json!({
                        "type": "message",
                        "session": ev.session,
                        "seq": ev.seq,
                        "from_agent": ev.from_agent,
                        "to_agent": ev.to_agent,
                        "kind": kind_to_str(ev.kind),
                        "text": String::from_utf8_lossy(&ev.payload),
                        "ts_ms": ev.ts_ms,
                    });
                    if socket.send(Message::Text(payload.to_string())).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {} // event for another group
                Err(RecvError::Lagged(_)) => continue, // dropped some; client re-polls /messages
                Err(RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                Some(Ok(_)) => {} // ignore client → server frames (pings handled by axum)
            },
        }
    }
}
