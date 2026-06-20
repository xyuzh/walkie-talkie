//! Agent-harness seam (v3 orchestration). `wt` owns a child harness process and drives its turn
//! loop over stream-json stdio. v1 ships one implementation: **Claude Code** invoked as
//! `claude --print --input-format stream-json --output-format stream-json --verbose`.
//!
//! The supervisor (in `wt-daemon`) feeds user turns with [`Harness::send_turn`] and consumes
//! [`HarnessEvent`]s from [`Harness::next_event`]; a `TurnComplete` means the harness finished a
//! turn and is awaiting the next stdin message — that is when its output is queued to the prime.
//!
//! **Cancellation safety.** [`Harness::next_event`] is used inside a `tokio::select!` alongside the
//! message bus, so it must be cancel-safe. It reads with [`AsyncReadExt::read`] (which is cancel
//! safe) into a *persistent* line buffer owned by the `Harness`; a dropped `next_event` future
//! never loses bytes. (We deliberately avoid `read_line`/`Lines`, which tokio documents as *not*
//! cancellation safe.)
//!
//! `$WT_HARNESS_CMD` overrides the launch command (whitespace-split) — used by tests to inject a
//! scripted stub harness so CI never depends on a real `claude` binary.

use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// Guard against a runaway harness emitting an unbounded line with no newline.
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

/// What `wt` needs to launch a harness.
#[derive(Debug, Clone)]
pub struct HarnessSpec {
    /// Full command line; argv[0] is the program. See [`default_claude_argv`].
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    /// First user turn written to the harness on spawn (the task prompt).
    pub initial_prompt: String,
    /// If set, the harness's stderr is appended here (else discarded). Diagnostics for a failing
    /// `claude` (auth, bad flags, crashes) land here instead of vanishing.
    pub stderr_log: Option<PathBuf>,
}

/// A significant event read from the harness's stream-json stdout.
#[derive(Debug, Clone)]
pub enum HarnessEvent {
    /// Assistant text produced during a turn (may fire multiple times per turn).
    AssistantText(String),
    /// The turn finished; `result` is the harness's final text (`result` stream-json event).
    TurnComplete { result: String, is_error: bool },
    /// stdout closed / the process exited.
    Exited(Option<i32>),
}

/// Build the harness launch command. A `$WT_HARNESS_CMD` override is returned **verbatim** (so test
/// stubs / alternate harnesses are untouched); otherwise the real Claude Code command, with the
/// requested permission posture appended. Mode strings pass through (Claude validates them).
pub fn claude_argv(permission_mode: Option<&str>, skip_permissions: bool) -> Vec<String> {
    if let Ok(cmd) = std::env::var("WT_HARNESS_CMD") {
        if !cmd.trim().is_empty() {
            return cmd.split_whitespace().map(String::from).collect();
        }
    }
    let mut argv: Vec<String> = [
        "claude",
        "--print",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if let Some(mode) = permission_mode {
        argv.push("--permission-mode".to_string());
        argv.push(mode.to_string());
    }
    if skip_permissions {
        argv.push("--dangerously-skip-permissions".to_string());
    }
    argv
}

/// The default Claude Code launch command (no special permission posture), or a `$WT_HARNESS_CMD`
/// override (whitespace-split).
pub fn default_claude_argv() -> Vec<String> {
    claude_argv(None, false)
}

/// A running harness: owns the child process, its stdin, and a cancel-safe line reader over stdout.
pub struct Harness {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    /// Bytes read from stdout but not yet framed into a line. Persists across `next_event` calls,
    /// which is what makes `next_event` cancellation-safe.
    buf: Vec<u8>,
}

impl Harness {
    /// Spawn the harness and send the initial prompt (if any) as the first user turn.
    pub async fn spawn(spec: &HarnessSpec) -> Result<Self> {
        let (program, rest) = spec
            .argv
            .split_first()
            .ok_or_else(|| anyhow!("empty harness argv"))?;
        let stderr = match &spec.stderr_log {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    Ok(f) => Stdio::from(f),
                    Err(_) => Stdio::null(),
                }
            }
            None => Stdio::null(),
        };
        let mut cmd = Command::new(program);
        cmd.args(rest)
            .current_dir(&spec.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .kill_on_drop(true);
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn harness `{program}` in {}", spec.cwd.display()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("harness has no stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("harness has no stdout"))?;
        let mut h = Harness {
            child,
            stdin,
            stdout,
            buf: Vec::new(),
        };
        if !spec.initial_prompt.is_empty() {
            h.send_turn(&spec.initial_prompt).await?;
        }
        Ok(h)
    }

    /// OS pid of the child, if still live.
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Write one user turn to the harness as a stream-json `user` message.
    pub async fn send_turn(&mut self, text: &str) -> Result<()> {
        let msg = serde_json::json!({
            "type": "user",
            "message": { "role": "user", "content": [ { "type": "text", "text": text } ] }
        });
        let mut line = serde_json::to_vec(&msg).context("encode user turn")?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .context("write to harness stdin")?;
        self.stdin.flush().await.context("flush harness stdin")?;
        Ok(())
    }

    /// Read the next significant event, skipping stream-json frames we don't act on (system init,
    /// tool-result echoes, partial deltas). Returns `Exited` when stdout closes.
    ///
    /// Cancellation-safe: the only `.await` is a cancel-safe `read`, and unframed bytes live in
    /// `self.buf` across calls, so a dropped future loses nothing.
    pub async fn next_event(&mut self) -> Result<HarnessEvent> {
        loop {
            // Drain any complete lines already buffered.
            while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let raw: Vec<u8> = self.buf.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&raw);
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(ev) = parse_event(line) {
                    return Ok(ev);
                }
            }
            if self.buf.len() > MAX_LINE_BYTES {
                bail!("harness emitted a line larger than {MAX_LINE_BYTES} bytes");
            }
            let mut tmp = [0u8; 8192];
            let n = self
                .stdout
                .read(&mut tmp)
                .await
                .context("read harness stdout")?;
            if n == 0 {
                let code = self.child.wait().await.ok().and_then(|s| s.code());
                return Ok(HarnessEvent::Exited(code));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Terminate the child (used by `wt agent kill` / session close).
    pub async fn kill(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// Map one stream-json line to an event we act on, or `None` to skip it.
fn parse_event(line: &str) -> Option<HarnessEvent> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    match v.get("type").and_then(|t| t.as_str()) {
        Some("assistant") => {
            let text = extract_assistant_text(&v).unwrap_or_default();
            (!text.is_empty()).then_some(HarnessEvent::AssistantText(text))
        }
        Some("result") => {
            let result = v
                .get("result")
                .and_then(|r| r.as_str())
                .unwrap_or("")
                .to_string();
            let is_error = v.get("is_error").and_then(|b| b.as_bool()).unwrap_or(false);
            Some(HarnessEvent::TurnComplete { result, is_error })
        }
        _ => None,
    }
}

/// Join the `text` blocks of a stream-json `assistant` message.
fn extract_assistant_text(v: &serde_json::Value) -> Option<String> {
    let arr = v.get("message")?.get("content")?.as_array()?;
    let mut s = String::new();
    for block in arr {
        if block.get("type").and_then(|t| t.as_str()) == Some("text") {
            if let Some(t) = block.get("text").and_then(|t| t.as_str()) {
                s.push_str(t);
            }
        }
    }
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_assistant_text_joins_text_blocks_and_skips_others() {
        let v = serde_json::json!({
            "type": "assistant",
            "message": { "role": "assistant", "content": [
                { "type": "text", "text": "hello " },
                { "type": "tool_use", "name": "Bash" },
                { "type": "text", "text": "world" }
            ]}
        });
        assert_eq!(extract_assistant_text(&v).as_deref(), Some("hello world"));
    }

    #[test]
    fn parse_event_maps_result_and_assistant_and_skips_system() {
        match parse_event(
            r#"{"type":"result","subtype":"success","is_error":false,"result":"done"}"#,
        ) {
            Some(HarnessEvent::TurnComplete { result, is_error }) => {
                assert_eq!(result, "done");
                assert!(!is_error);
            }
            other => panic!("expected TurnComplete, got {other:?}"),
        }
        assert!(matches!(
            parse_event(r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#),
            Some(HarnessEvent::AssistantText(t)) if t == "hi"
        ));
        assert!(parse_event(r#"{"type":"system","subtype":"init"}"#).is_none());
        assert!(parse_event("not json").is_none());
    }

    #[test]
    fn claude_argv_honors_override_and_appends_mode_flags() {
        // A $WT_HARNESS_CMD override is returned verbatim — no mode flags injected.
        std::env::set_var("WT_HARNESS_CMD", "/bin/echo  hi   there");
        assert_eq!(
            claude_argv(Some("plan"), true),
            vec!["/bin/echo", "hi", "there"]
        );
        std::env::remove_var("WT_HARNESS_CMD");

        // The real Claude command: base flags, plus the requested posture.
        let base = claude_argv(None, false);
        assert_eq!(base.first().map(String::as_str), Some("claude"));
        assert!(base.iter().any(|a| a == "--input-format"));
        assert!(!base.iter().any(|a| a == "--permission-mode"));

        let plan = claude_argv(Some("plan"), false);
        let i = plan
            .iter()
            .position(|a| a == "--permission-mode")
            .expect("plan argv has --permission-mode");
        assert_eq!(plan[i + 1], "plan");
        assert!(!plan.iter().any(|a| a == "--dangerously-skip-permissions"));

        let skip = claude_argv(None, true);
        assert!(skip.iter().any(|a| a == "--dangerously-skip-permissions"));
        assert!(!skip.iter().any(|a| a == "--permission-mode"));
    }
}
