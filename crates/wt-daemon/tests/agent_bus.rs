//! In-process integration test for the v3 local agent bus (M1). Starts a daemon via
//! `start_for_test`, seeds a group + prime + child + session through the public `Store` API
//! (spawn arrives in M2), then drives `AgentSend` / `AgentRecv` / `WhoAmI` over the real Unix
//! socket and asserts routing, ordering, and token auth.

#![cfg(unix)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use wt_core::auth;
use wt_core::paths;
use wt_core::store::{unix_ms, AgentRow, SessionRow};
use wt_proto::ipc::{AgentMsgKind, IpcEvent, IpcRequest};

fn set_unique_home() -> std::path::PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let p = std::env::temp_dir().join(format!("wt-bus-{}-{nanos}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    std::env::set_var("WT_HOME", &p);
    p
}

async fn connect() -> UnixStream {
    let sock = paths::daemon_sock_path();
    for _ in 0..50 {
        if let Ok(s) = UnixStream::connect(&sock).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("daemon socket never came up at {}", sock.display());
}

async fn send_req(s: &mut UnixStream, req: &IpcRequest) {
    let mut buf = Vec::new();
    ciborium::into_writer(req, &mut buf).unwrap();
    s.write_all(&(buf.len() as u32).to_be_bytes())
        .await
        .unwrap();
    s.write_all(&buf).await.unwrap();
    s.flush().await.unwrap();
}

async fn read_ev(s: &mut UnixStream) -> IpcEvent {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await.unwrap();
    let n = u32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; n];
    s.read_exact(&mut body).await.unwrap();
    ciborium::from_reader(&body[..]).unwrap()
}

/// Drain an `AgentRecv` (follow=false) into `(from_agent, kind, payload)` tuples.
async fn drain_recv(token: &str, session: Option<&str>) -> Vec<(String, AgentMsgKind, Vec<u8>)> {
    let mut r = connect().await;
    send_req(
        &mut r,
        &IpcRequest::AgentRecv {
            token: token.to_string(),
            session: session.map(|s| s.to_string()),
            since_ms: None,
            follow: false,
            consume: false,
        },
    )
    .await;
    let mut got = Vec::new();
    loop {
        match read_ev(&mut r).await {
            IpcEvent::AgentMsg {
                from_agent,
                kind,
                payload,
                ..
            } => got.push((from_agent, kind, payload)),
            IpcEvent::AgentBacklogEnd => break,
            other => panic!("unexpected event during recv: {other:?}"),
        }
    }
    got
}

#[tokio::test]
async fn agent_bus_routes_send_recv_and_whoami() {
    let home = set_unique_home();
    let handle = wt_daemon::start_for_test().await.expect("start daemon");
    let store = handle.state.store.clone();

    // Seed a group with a prime + one child session (what `wt group new` + `wt spawn` will do).
    let tok_prime = "primetokenprimetokenprimetoken00";
    let tok_child = "childtokenchildtokenchildtoken000";
    store.group_create("myapp").await.unwrap();
    store
        .agent_register(&AgentRow {
            group_name: "myapp".into(),
            name: "prime".into(),
            token_hash: auth::agent_token_hash(tok_prime),
            role: "prime".into(),
            dir: None,
            pid: None,
            status: "running".into(),
            created_at_ms: unix_ms(),
            last_seen_ms: None,
        })
        .await
        .unwrap();
    store
        .agent_register(&AgentRow {
            group_name: "myapp".into(),
            name: "frontend".into(),
            token_hash: auth::agent_token_hash(tok_child),
            role: "child".into(),
            dir: Some("/proj".into()),
            pid: None,
            status: "running".into(),
            created_at_ms: unix_ms(),
            last_seen_ms: None,
        })
        .await
        .unwrap();
    store
        .session_create(&SessionRow {
            group_name: "myapp".into(),
            name: "frontend".into(),
            prime_agent: "prime".into(),
            child_agent: "frontend".into(),
            fs_mode: "worktree".into(),
            base_dir: Some("/proj".into()),
            workspace_path: "/tmp/ws".into(),
            branch: Some("wt/myapp/frontend".into()),
            status: "active".into(),
            created_at_ms: unix_ms(),
        })
        .await
        .unwrap();

    // prime → child (turn_input)
    {
        let mut c = connect().await;
        send_req(
            &mut c,
            &IpcRequest::AgentSend {
                token: tok_prime.into(),
                session: "frontend".into(),
                kind: AgentMsgKind::TurnInput,
                payload: b"do the thing".to_vec(),
            },
        )
        .await;
        assert!(matches!(read_ev(&mut c).await, IpcEvent::Ok));
    }

    // The child drains its inbox and sees the prime's turn_input.
    let to_child = drain_recv(tok_child, None).await;
    assert_eq!(to_child.len(), 1);
    assert_eq!(to_child[0].0, "prime");
    assert!(matches!(to_child[0].1, AgentMsgKind::TurnInput));
    assert_eq!(to_child[0].2, b"do the thing");

    // child → prime (turn_output)
    {
        let mut c = connect().await;
        send_req(
            &mut c,
            &IpcRequest::AgentSend {
                token: tok_child.into(),
                session: "frontend".into(),
                kind: AgentMsgKind::TurnOutput,
                payload: b"did the thing".to_vec(),
            },
        )
        .await;
        assert!(matches!(read_ev(&mut c).await, IpcEvent::Ok));
    }

    // The prime drains the frontend session and sees only the child's output.
    let to_prime = drain_recv(tok_prime, Some("frontend")).await;
    assert_eq!(to_prime.len(), 1);
    assert_eq!(to_prime[0].0, "frontend");
    assert!(matches!(to_prime[0].1, AgentMsgKind::TurnOutput));
    assert_eq!(to_prime[0].2, b"did the thing");

    // whoami resolves the child's identity from its token.
    {
        let mut c = connect().await;
        send_req(
            &mut c,
            &IpcRequest::WhoAmI {
                token: tok_child.into(),
            },
        )
        .await;
        match read_ev(&mut c).await {
            IpcEvent::WhoAmIInfo(w) => {
                assert_eq!(w.group, "myapp");
                assert_eq!(w.agent, "frontend");
                assert_eq!(w.role, "child");
                assert_eq!(w.session.as_deref(), Some("frontend"));
            }
            other => panic!("unexpected whoami event: {other:?}"),
        }
    }

    // An unknown token is rejected.
    {
        let mut c = connect().await;
        send_req(
            &mut c,
            &IpcRequest::WhoAmI {
                token: "bogus".into(),
            },
        )
        .await;
        assert!(matches!(read_ev(&mut c).await, IpcEvent::Err(_)));
    }

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&home);
}
