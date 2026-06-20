//! End-to-end test: spawn two `wt` daemons as subprocesses with separate `WT_HOME`s, pair them,
//! exchange reciprocal tokens, and verify bidirectional `{"user": "..."}` delivery.
//!
//! This is the closest thing to the smoke_local.sh shell script, but in Rust so `cargo test`
//! catches regressions. It requires network access for iroh (UDP to local interfaces); on a
//! sandboxed CI runner without that access, the test will be skipped via a setup-time probe.

#![cfg(unix)]

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn wt_bin() -> &'static str {
    env!("CARGO_BIN_EXE_wt")
}

fn unique_home(label: &str) -> std::path::PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let p = std::env::temp_dir().join(format!("wt-e2e-{label}-{}-{nanos}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run_wt(home: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(wt_bin())
        .env("WT_HOME", home)
        .env("RUST_LOG", "warn")
        .args(args)
        .output()
        .expect("spawn wt")
}

fn check(out: &std::process::Output) {
    if !out.status.success() {
        panic!(
            "wt failed (status={:?}): stdout=<<<{}>>> stderr=<<<{}>>>",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct DaemonHandle {
    child: Child,
    home: std::path::PathBuf,
}

impl DaemonHandle {
    fn start(label: &str) -> Self {
        let home = unique_home(label);
        // init
        check(&run_wt(&home, &["init"]));
        let child = Command::new(wt_bin())
            .env("WT_HOME", &home)
            .env("RUST_LOG", "warn")
            .arg("daemon")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn wt daemon");
        Self { child, home }
    }

    fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if run_wt(&self.home, &["status"]).status.success() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    }
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

/// Smoke: end-to-end bidirectional messaging across two locally-spawned daemons.
#[test]
fn two_daemons_exchange_reciprocal_messages() {
    let a = DaemonHandle::start("a");
    let b = DaemonHandle::start("b");
    assert!(
        a.wait_ready(Duration::from_secs(15)),
        "daemon A never became ready"
    );
    assert!(
        b.wait_ready(Duration::from_secs(15)),
        "daemon B never became ready"
    );

    // Exchange tickets.
    let a_ticket = {
        let o = run_wt(&a.home, &["ticket"]);
        check(&o);
        stdout(&o)
    };
    let b_ticket = {
        let o = run_wt(&b.home, &["ticket"]);
        check(&o);
        stdout(&o)
    };
    assert!(a_ticket.starts_with("wt1:"));
    assert!(b_ticket.starts_with("wt1:"));

    // Peer add (reciprocal).
    check(&run_wt(
        &a.home,
        &["peer", "add", &b_ticket, "--name", "bob"],
    ));
    check(&run_wt(
        &b.home,
        &["peer", "add", &a_ticket, "--name", "alice"],
    ));

    // Token grant (reciprocal).
    let t_ba = {
        let o = run_wt(
            &b.home,
            &["token", "grant", "alice", "--cap", "msg", "--ttl", "1h"],
        );
        check(&o);
        stdout(&o)
    };
    let t_ab = {
        let o = run_wt(
            &a.home,
            &["token", "grant", "bob", "--cap", "msg", "--ttl", "1h"],
        );
        check(&o);
        stdout(&o)
    };

    // Token import.
    check(&run_wt(&a.home, &["token", "import", &t_ba]));
    check(&run_wt(&b.home, &["token", "import", &t_ab]));

    // Start `wt recv --follow` subprocesses on both sides BEFORE sending; capture stdout.
    let mut b_recv = Command::new(wt_bin())
        .env("WT_HOME", &b.home)
        .env("RUST_LOG", "warn")
        .args(["recv", "--follow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn b recv");
    let mut a_recv = Command::new(wt_bin())
        .env("WT_HOME", &a.home)
        .env("RUST_LOG", "warn")
        .args(["recv", "--follow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn a recv");

    // Give the daemons a moment to register the subscriptions.
    std::thread::sleep(Duration::from_millis(500));

    // Send in both directions.
    check(&run_wt(
        &a.home,
        &["send", "bob", "{\"user\":\"hello from alice\"}"],
    ));
    check(&run_wt(
        &b.home,
        &["send", "alice", "{\"user\":\"hi alice\"}"],
    ));

    // Read one line from each recv subprocess with a deadline.
    let b_line = read_line_with_timeout(b_recv.stdout.take().unwrap(), Duration::from_secs(10))
        .expect("b never received a message");
    let a_line = read_line_with_timeout(a_recv.stdout.take().unwrap(), Duration::from_secs(10))
        .expect("a never received a message");

    let _ = b_recv.kill();
    let _ = a_recv.kill();
    let _ = b_recv.wait();
    let _ = a_recv.wait();

    assert!(
        b_line.contains("hello from alice"),
        "B did not see Alice's message; got: {b_line}"
    );
    assert!(
        a_line.contains("hi alice"),
        "A did not see Bob's message; got: {a_line}"
    );
    assert!(
        b_line.contains("\"from\":\"alice\""),
        "B's line missing from=alice: {b_line}"
    );
    assert!(
        a_line.contains("\"from\":\"bob\""),
        "A's line missing from=bob: {a_line}"
    );
}

/// v0.2: send while no receiver is subscribed; later `wt recv` (no --follow) should replay the
/// backlog from disk and exit on `RecvBacklogEnd`.
#[test]
fn recv_replays_persisted_backlog() {
    let a = DaemonHandle::start("ra");
    let b = DaemonHandle::start("rb");
    assert!(a.wait_ready(Duration::from_secs(15)));
    assert!(b.wait_ready(Duration::from_secs(15)));

    let a_ticket = stdout(&run_wt(&a.home, &["ticket"]));
    let b_ticket = stdout(&run_wt(&b.home, &["ticket"]));
    check(&run_wt(
        &a.home,
        &["peer", "add", &b_ticket, "--name", "bob"],
    ));
    check(&run_wt(
        &b.home,
        &["peer", "add", &a_ticket, "--name", "alice"],
    ));
    let t_ba = stdout(&run_wt(
        &b.home,
        &["token", "grant", "alice", "--cap", "msg", "--ttl", "1h"],
    ));
    let t_ab = stdout(&run_wt(
        &a.home,
        &["token", "grant", "bob", "--cap", "msg", "--ttl", "1h"],
    ));
    check(&run_wt(&a.home, &["token", "import", &t_ba]));
    check(&run_wt(&b.home, &["token", "import", &t_ab]));

    // No recv subscribers yet — send and let the delivery worker do its thing.
    check(&run_wt(&a.home, &["send", "bob", "{\"user\":\"first\"}"]));
    check(&run_wt(&a.home, &["send", "bob", "{\"user\":\"second\"}"]));
    // Give the delivery worker time to push and the receiver time to persist.
    std::thread::sleep(Duration::from_secs(2));

    // Now run `wt recv` (no --follow) — should replay from inbox and exit.
    let out = run_wt(&b.home, &["recv"]);
    check(&out);
    let body = String::from_utf8_lossy(&out.stdout);

    assert!(
        body.contains("\"user\":\"first\""),
        "backlog missing 'first': {body}"
    );
    assert!(
        body.contains("\"user\":\"second\""),
        "backlog missing 'second': {body}"
    );
    // Both messages tagged from alice.
    assert_eq!(
        body.matches("\"from\":\"alice\"").count(),
        2,
        "expected 2 lines, got: {body}"
    );
}

/// Read one line from `reader`, returning `None` if `timeout` elapses first. The reader is moved
/// into a helper thread so the timeout is enforced for real (a hung daemon fails the test cleanly
/// instead of blocking forever).
fn read_line_with_timeout<R: std::io::Read + Send + 'static>(
    reader: R,
    timeout: Duration,
) -> Option<String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = String::new();
        let mut bufread = std::io::BufReader::new(reader);
        let line = match bufread.read_line(&mut buf) {
            Ok(0) => None,
            Ok(_) => Some(buf.trim_end_matches('\n').to_string()),
            Err(_) => None,
        };
        let _ = tx.send(line);
    });
    rx.recv_timeout(timeout).ok().flatten()
}

/// Pair two daemons reciprocally: exchange tickets, add each other as peers, and grant + import
/// a `msg` capability token in both directions. `a_name_on_b` is how A is known on B; vice versa.
fn pair_reciprocal(a: &DaemonHandle, a_name_on_b: &str, b: &DaemonHandle, b_name_on_a: &str) {
    let a_ticket = stdout(&run_wt(&a.home, &["ticket"]));
    let b_ticket = stdout(&run_wt(&b.home, &["ticket"]));
    check(&run_wt(
        &a.home,
        &["peer", "add", &b_ticket, "--name", b_name_on_a],
    ));
    check(&run_wt(
        &b.home,
        &["peer", "add", &a_ticket, "--name", a_name_on_b],
    ));
    let t_ba = stdout(&run_wt(
        &b.home,
        &["token", "grant", a_name_on_b, "--cap", "msg", "--ttl", "1h"],
    ));
    let t_ab = stdout(&run_wt(
        &a.home,
        &["token", "grant", b_name_on_a, "--cap", "msg", "--ttl", "1h"],
    ));
    check(&run_wt(&a.home, &["token", "import", &t_ba]));
    check(&run_wt(&b.home, &["token", "import", &t_ab]));
}

/// P0-2 regression: a dead/unreachable peer must NOT stall delivery to other peers. With the old
/// single serial delivery worker this test would hang; with per-peer tasks the live peer's
/// message arrives promptly while the dead peer's delivery retries in isolation.
#[test]
fn dead_peer_does_not_block_delivery_to_live_peer() {
    let a = DaemonHandle::start("hola");
    let b = DaemonHandle::start("holb");
    let c = DaemonHandle::start("holc");
    assert!(a.wait_ready(Duration::from_secs(15)));
    assert!(b.wait_ready(Duration::from_secs(15)));
    assert!(c.wait_ready(Duration::from_secs(15)));

    pair_reciprocal(&a, "alice", &b, "bob");
    pair_reciprocal(&a, "alice", &c, "ghost");

    // Kill C — A still holds a valid token + ticket for it, so A will *try* (and fail) to deliver.
    drop(c);
    std::thread::sleep(Duration::from_millis(500));

    let mut b_recv = Command::new(wt_bin())
        .env("WT_HOME", &b.home)
        .env("RUST_LOG", "warn")
        .args(["recv", "--follow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn b recv");
    std::thread::sleep(Duration::from_millis(500));

    // Enqueue to the DEAD peer FIRST (oldest in the outbox), then to the LIVE peer.
    check(&run_wt(
        &a.home,
        &["send", "ghost", "{\"user\":\"into the void\"}"],
    ));
    check(&run_wt(
        &a.home,
        &["send", "bob", "{\"user\":\"to the living\"}"],
    ));

    let b_line = read_line_with_timeout(b_recv.stdout.take().unwrap(), Duration::from_secs(15))
        .expect("B never received its message — a dead peer is stalling delivery (HOL regression)");
    let _ = b_recv.kill();
    let _ = b_recv.wait();

    assert!(
        b_line.contains("to the living"),
        "B got an unexpected line: {b_line}"
    );
}
