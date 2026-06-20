//! End-to-end test for the M2 harness supervisor, driven through the real `wt` binary.
//!
//! Uses a tiny Python stub harness (via `$WT_HARNESS_CMD`) that speaks stream-json — so CI never
//! depends on a real `claude`. The stub echoes each user turn back as a `result` event. We assert
//! the full loop: `wt spawn` → the child's first turn output reaches the prime's `wt recv` →
//! `wt send` (turn_input) is fed back → the next turn output arrives → `wt ls --group` shows the
//! session → `wt agent kill` stops it.

#![cfg(unix)]

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn wt_bin() -> &'static str {
    env!("CARGO_BIN_EXE_wt")
}

fn unique_home(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    // A monotonic counter guarantees uniqueness even when the clock is coarse and several homes are
    // created in the same instant by parallel tests (a nanos-only name can collide → two daemons
    // share a WT_HOME and clobber each other's socket).
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!(
        "wt-spawn-{label}-{}-{nanos}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A stub harness: reads stream-json user turns on stdin, echoes each back as a `result` event.
const STUB: &str = r#"
import sys, json
def emit(o):
    sys.stdout.write(json.dumps(o) + "\n"); sys.stdout.flush()
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line)
        c = m["message"]["content"]
        text = "".join(b.get("text","") for b in c if b.get("type")=="text") if isinstance(c, list) else str(c)
    except Exception:
        text = line
    # Several frames per turn (two assistant lines + result) to stress cancel-safe framing.
    emit({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"thinking about "+text}]}})
    emit({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"still working"}]}})
    emit({"type":"result","subtype":"success","is_error":False,"result":"echo:"+text})
"#;

/// A stub that consumes turns but never produces output (a hung turn) — used for the idle test.
/// It keeps reading stdin so it exits cleanly when the daemon dies (no orphaned process).
const SILENT_STUB: &str = r#"
import sys
while sys.stdin.readline():
    pass
"#;

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

struct Daemon {
    child: Child,
    home: std::path::PathBuf,
    stub: std::path::PathBuf,
}

impl Daemon {
    fn start() -> Self {
        Self::start_with(STUB)
    }

    fn start_with(stub_body: &str) -> Self {
        let home = unique_home("d");
        let stub = home.join("stub.py");
        std::fs::write(&stub, stub_body).unwrap();
        let d = Self {
            child: Self::spawn_daemon(&home, &stub),
            home,
            stub,
        };
        d.wait_ready();
        d
    }

    fn spawn_daemon(home: &std::path::Path, stub: &std::path::Path) -> Child {
        Command::new(wt_bin())
            .env("WT_HOME", home)
            .env("RUST_LOG", "warn")
            // The daemon spawns the harness, so it reads $WT_HARNESS_CMD.
            .env("WT_HARNESS_CMD", format!("python3 {}", stub.display()))
            .arg("daemon")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn wt daemon")
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if self.wt(&["status"]).status.success() {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("daemon never became ready");
    }

    /// SIGKILL the daemon (ungraceful — no shutdown/kill_on_drop runs), simulating a crash.
    fn hard_kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Start a fresh daemon on the same WT_HOME — its `start()` runs orphan reconciliation.
    fn restart(&mut self) {
        self.child = Self::spawn_daemon(&self.home, &self.stub);
        self.wait_ready();
    }

    /// The displayed status of a session in `myapp` (the child agent's status), if listed.
    fn session_status(&self, session: &str) -> Option<String> {
        let out = self.wt(&["ls", "--group", "myapp"]);
        String::from_utf8_lossy(&out.stdout).lines().find_map(|l| {
            let cols: Vec<&str> = l.split_whitespace().collect();
            if cols.first().copied() == Some(session) {
                cols.get(3).map(|s| s.to_string())
            } else {
                None
            }
        })
    }

    fn wait_session_status(&self, session: &str, want: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.session_status(session).as_deref() == Some(want) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    }

    fn wt(&self, args: &[&str]) -> std::process::Output {
        Command::new(wt_bin())
            .env("WT_HOME", &self.home)
            .env("RUST_LOG", "warn")
            .args(args)
            .output()
            .expect("run wt")
    }

    /// Run `wt` as the prime (WT_TOKEN + WT_GROUP set).
    fn wt_prime(&self, token: &str, args: &[&str]) -> std::process::Output {
        Command::new(wt_bin())
            .env("WT_HOME", &self.home)
            .env("RUST_LOG", "warn")
            .env("WT_TOKEN", token)
            .env("WT_GROUP", "myapp")
            .args(args)
            .output()
            .expect("run wt")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.home);
        let _ = std::fs::remove_file(&self.stub);
    }
}

/// Stream every stdout line of a child into a channel, so the test can pull successive lines with
/// independent timeouts (one `wt recv --follow` emits many lines over its lifetime).
fn line_reader<R: std::io::Read + Send + 'static>(reader: R) -> std::sync::mpsc::Receiver<String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut br = std::io::BufReader::new(reader);
        loop {
            let mut buf = String::new();
            match br.read_line(&mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    if tx.send(buf.trim_end().to_string()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

#[test]
fn spawn_drives_turn_loop_through_the_prime() {
    let d = Daemon::start();

    // Prime token from `wt group new`.
    let out = d.wt(&["group", "new", "myapp"]);
    check(&out);
    let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(token.len(), 52, "expected a 52-char base32 token");

    // A base dir for the --new workspace.
    let base = unique_home("base");

    // Tail the prime's bus BEFORE spawning, so we never miss the first turn output.
    let mut recv = Command::new(wt_bin())
        .env("WT_HOME", &d.home)
        .env("WT_TOKEN", &token)
        .env("RUST_LOG", "warn")
        .args([
            "recv",
            "--group",
            "myapp",
            "--session",
            "worker",
            "--follow",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn wt recv");
    let lines = line_reader(recv.stdout.take().unwrap());
    std::thread::sleep(Duration::from_millis(500));

    // Spawn the child harness in a fresh-folder workspace with an initial prompt.
    let out = d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "worker",
            "--dir",
            base.to_str().unwrap(),
            "--new",
            "--prompt",
            "hello",
        ],
    );
    check(&out);
    let spawned: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("spawn prints a JSON summary");
    assert_eq!(spawned["session"], "worker");
    assert!(spawned["workspace"].as_str().unwrap().contains("worker"));

    // Turn 1: the stub echoes the initial prompt back to the prime.
    let line1 = lines
        .recv_timeout(Duration::from_secs(10))
        .expect("prime never received the child's first turn output");
    assert!(
        line1.contains("echo:hello") && line1.contains("\"from\":\"worker\""),
        "unexpected first turn: {line1}"
    );

    // Reply with a turn_input; the supervisor feeds it back to the stub as the next turn.
    check(&d.wt_prime(
        &token,
        &[
            "send",
            "--session",
            "worker",
            "--kind",
            "turn_input",
            "again please",
        ],
    ));

    // Turn 2: arrives live on the same recv stream.
    let line2 = lines
        .recv_timeout(Duration::from_secs(10))
        .expect("prime never received the child's second turn output");
    assert!(
        line2.contains("echo:again please"),
        "second turn did not echo the reply: {line2}"
    );

    // The session shows up in `wt ls --group`.
    let out = d.wt(&["ls", "--group", "myapp"]);
    check(&out);
    let listing = String::from_utf8_lossy(&out.stdout);
    assert!(listing.contains("worker"), "session not listed: {listing}");

    // Kill the agent.
    check(&d.wt_prime(&token, &["agent", "kill", "worker"]));

    let _ = recv.kill();
    let _ = recv.wait();
    let _ = std::fs::remove_dir_all(&base);
}

/// Robustness: two sessions run concurrently (the frontend + backend case). Their stdout streams
/// must never cross-contaminate, and a reply to one must advance only that one. This is the case
/// that the cancellation-safety fix exists for — with the old `read_line` it would corrupt under
/// the cross-session broadcast churn.
#[test]
fn concurrent_sessions_route_independently() {
    let d = Daemon::start();
    let token = String::from_utf8_lossy(&d.wt(&["group", "new", "myapp"]).stdout)
        .trim()
        .to_string();
    let base_fe = unique_home("fe");
    let base_be = unique_home("be");

    // Tail the prime's whole bus (all sessions).
    let mut recv = Command::new(wt_bin())
        .env("WT_HOME", &d.home)
        .env("WT_TOKEN", &token)
        .env("RUST_LOG", "warn")
        .args(["recv", "--group", "myapp", "--follow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn wt recv");
    let lines = line_reader(recv.stdout.take().unwrap());
    std::thread::sleep(Duration::from_millis(500));

    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "frontend",
            "--dir",
            base_fe.to_str().unwrap(),
            "--new",
            "--prompt",
            "FE",
        ],
    ));
    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "backend",
            "--dir",
            base_be.to_str().unwrap(),
            "--new",
            "--prompt",
            "BE",
        ],
    ));

    // Collect each child's first turn output; assert correct, non-crossed attribution.
    let mut fe: Option<String> = None;
    let mut be: Option<String> = None;
    let deadline = Instant::now() + Duration::from_secs(20);
    while (fe.is_none() || be.is_none()) && Instant::now() < deadline {
        let Ok(line) = lines.recv_timeout(Duration::from_secs(5)) else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        if v["kind"] != "turn_output" {
            continue;
        }
        let session = v["session"].as_str().unwrap();
        let payload = v["payload"].as_str().unwrap().to_string();
        // The child agent's name equals its session, so `from` must equal `session`.
        assert_eq!(
            v["from"].as_str().unwrap(),
            session,
            "from/session mismatch: {line}"
        );
        match session {
            "frontend" => fe = Some(payload),
            "backend" => be = Some(payload),
            other => panic!("unexpected session: {other}"),
        }
    }
    assert_eq!(
        fe.as_deref(),
        Some("echo:FE"),
        "frontend output wrong/missing"
    );
    assert_eq!(
        be.as_deref(),
        Some("echo:BE"),
        "backend output wrong/missing"
    );

    // Reply to frontend only; backend must not produce a turn.
    check(&d.wt_prime(
        &token,
        &[
            "send",
            "--session",
            "frontend",
            "--kind",
            "turn_input",
            "more",
        ],
    ));
    let mut fe_again = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !fe_again && Instant::now() < deadline {
        let Ok(line) = lines.recv_timeout(Duration::from_secs(5)) else {
            continue;
        };
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        if v["kind"] != "turn_output" {
            continue;
        }
        let session = v["session"].as_str().unwrap();
        assert_ne!(
            session, "backend",
            "backend produced a turn from a frontend-only reply"
        );
        if session == "frontend" && v["payload"] == "echo:more" {
            fe_again = true;
        }
    }
    assert!(fe_again, "frontend never processed the reply");

    let _ = recv.kill();
    let _ = recv.wait();
    let _ = std::fs::remove_dir_all(&base_fe);
    let _ = std::fs::remove_dir_all(&base_be);
}

/// Idle-turn timeout (notify-only): a turn that produces no output triggers one `control` message
/// to the prime, and the child is NOT killed.
#[test]
fn idle_turn_timeout_notifies_prime_without_killing() {
    let d = Daemon::start_with(SILENT_STUB);
    let token = String::from_utf8_lossy(&d.wt(&["group", "new", "myapp"]).stdout)
        .trim()
        .to_string();
    let base = unique_home("idle");

    // Observe the prime's bus non-destructively (--all ⇒ no consume).
    let mut recv = Command::new(wt_bin())
        .env("WT_HOME", &d.home)
        .env("WT_TOKEN", &token)
        .env("RUST_LOG", "warn")
        .args([
            "recv",
            "--group",
            "myapp",
            "--session",
            "stuck",
            "--follow",
            "--all",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn wt recv");
    let lines = line_reader(recv.stdout.take().unwrap());
    std::thread::sleep(Duration::from_millis(500));

    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "stuck",
            "--dir",
            base.to_str().unwrap(),
            "--new",
            "--prompt",
            "do work",
            "--idle-timeout",
            "2s",
        ],
    ));

    let mut got_idle = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    while !got_idle && Instant::now() < deadline {
        let Ok(line) = lines.recv_timeout(Duration::from_secs(6)) else {
            continue;
        };
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) {
            if v["kind"] == "control" && v["payload"].as_str().unwrap_or("").contains("idle") {
                got_idle = true;
            }
        }
    }
    assert!(got_idle, "prime never received an idle-turn notification");
    // Notify-only: the child must still be alive (not exited).
    assert_ne!(
        d.session_status("stuck").as_deref(),
        Some("exited"),
        "idle timeout must not kill the child"
    );

    let _ = recv.kill();
    let _ = recv.wait();
    let _ = std::fs::remove_dir_all(&base);
}

/// Recv cursor (consume-on-read): the first `wt recv` shows a turn's output and consumes it; an
/// immediate second `wt recv` shows nothing new; `wt recv --all` replays it.
#[test]
fn recv_consumes_then_all_replays() {
    let d = Daemon::start();
    let token = String::from_utf8_lossy(&d.wt(&["group", "new", "myapp"]).stdout)
        .trim()
        .to_string();
    let base = unique_home("cursor");

    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "worker",
            "--dir",
            base.to_str().unwrap(),
            "--new",
            "--prompt",
            "hello",
        ],
    ));
    assert!(
        d.wait_session_status("worker", "awaiting_input", Duration::from_secs(15)),
        "turn 1 never completed"
    );

    let first =
        String::from_utf8_lossy(&d.wt_prime(&token, &["recv", "--session", "worker"]).stdout)
            .into_owned();
    assert!(
        first.contains("echo:hello"),
        "first recv missing output: {first}"
    );

    let second =
        String::from_utf8_lossy(&d.wt_prime(&token, &["recv", "--session", "worker"]).stdout)
            .into_owned();
    assert!(
        !second.contains("echo:hello"),
        "second recv should be empty after consume: {second}"
    );

    let all = String::from_utf8_lossy(
        &d.wt_prime(&token, &["recv", "--session", "worker", "--all"])
            .stdout,
    )
    .into_owned();
    assert!(
        all.contains("echo:hello"),
        "--all should replay history: {all}"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// Orphan reconcile: after an ungraceful crash + restart on the same WT_HOME, the daemon's startup
/// reconciliation marks the stale child `exited` (so `ls` is accurate; no zombie "running" rows).
#[test]
fn orphan_reconcile_on_restart() {
    let mut d = Daemon::start();
    let token = String::from_utf8_lossy(&d.wt(&["group", "new", "myapp"]).stdout)
        .trim()
        .to_string();
    let base = unique_home("orphan");

    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "worker",
            "--dir",
            base.to_str().unwrap(),
            "--new",
            "--prompt",
            "hello",
        ],
    ));
    assert!(
        d.wait_session_status("worker", "awaiting_input", Duration::from_secs(15)),
        "turn 1 never completed"
    );

    d.hard_kill(); // SIGKILL — no graceful shutdown
    d.restart(); // fresh daemon on the same WT_HOME → start() reconciles

    let ls = String::from_utf8_lossy(&d.wt(&["ls", "--group", "myapp"]).stdout).into_owned();
    assert!(ls.contains("worker"), "session missing after restart: {ls}");
    assert_eq!(
        d.session_status("worker").as_deref(),
        Some("exited"),
        "stale child should be reconciled to exited after an ungraceful restart"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// Opt-in audit trace: `--trace` forwards the child's intermediate assistant text to the prime as
/// `kind:"trace"` messages (alongside the final `turn_output`); without it, only the output flows.
#[test]
fn trace_forwards_assistant_text_only_when_enabled() {
    let d = Daemon::start();
    let token = String::from_utf8_lossy(&d.wt(&["group", "new", "myapp"]).stdout)
        .trim()
        .to_string();

    // Traced session: prime should see the stub's "thinking about hello" assistant lines + result.
    let base = unique_home("trace");
    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "worker",
            "--dir",
            base.to_str().unwrap(),
            "--new",
            "--prompt",
            "hello",
            "--trace",
        ],
    ));
    assert!(d.wait_session_status("worker", "awaiting_input", Duration::from_secs(15)));
    let out = String::from_utf8_lossy(&d.wt_prime(&token, &["recv", "--session", "worker"]).stdout)
        .into_owned();
    assert!(
        out.contains("\"kind\":\"turn_output\"") && out.contains("echo:hello"),
        "missing turn output: {out}"
    );
    assert!(
        out.contains("\"kind\":\"trace\"") && out.contains("thinking about hello"),
        "expected trace messages with --trace: {out}"
    );

    // Untraced session: only the turn output, no trace messages.
    let base2 = unique_home("notrace");
    check(&d.wt_prime(
        &token,
        &[
            "spawn",
            "--session",
            "quiet",
            "--dir",
            base2.to_str().unwrap(),
            "--new",
            "--prompt",
            "hi",
        ],
    ));
    assert!(d.wait_session_status("quiet", "awaiting_input", Duration::from_secs(15)));
    let q = String::from_utf8_lossy(&d.wt_prime(&token, &["recv", "--session", "quiet"]).stdout)
        .into_owned();
    assert!(q.contains("echo:hi"), "missing turn output: {q}");
    assert!(
        !q.contains("\"kind\":\"trace\""),
        "no trace expected without --trace: {q}"
    );

    let _ = std::fs::remove_dir_all(&base);
    let _ = std::fs::remove_dir_all(&base2);
}
