//! Per-child harness supervisor (v3 orchestration). One task per spawned session:
//! - feeds the child user turns (the prime's replies), and
//! - reads the child's stream-json output; on each completed turn it queues the result to the
//!   prime as a `turn_output` ("the harness finished responding → ask the prime to respond").
//!
//! **Turn-input delivery is DB-as-truth.** The prime's `turn_input`s are persisted on the bus;
//! the supervisor drains *unconsumed* ones (oldest first) and marks them consumed. The broadcast
//! and a periodic timer are only wakeups — so a dropped or lagged broadcast never strips a reply.
//!
//! **Idle-turn timeout (notify-only).** If a turn produces no harness output for `idle_timeout`,
//! the supervisor notifies the prime once (a `control` message) and leaves the child running — the
//! prime decides whether to nudge it or `agent kill` it. Disabled when `idle_timeout` is `None`.
//!
//! `next_event` is cancellation-safe (see `wt_core::harness`), so racing it against the bus in
//! `select!` cannot corrupt the child's stdout stream. The task owns the `Harness` (kill_on_drop),
//! so aborting it reaps the child.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tracing::{info, warn};
use wt_core::harness::{Harness, HarnessEvent, HarnessSpec};
use wt_proto::ipc::AgentMsgKind;

use crate::state::DaemonState;

/// Wakeup cadence for draining pending turn-inputs and checking idle (a fallback for missed
/// broadcasts); also bounds idle-detection granularity.
const POLL: Duration = Duration::from_secs(3);

/// Spawn the harness, mark the child `running`, and launch its supervisor task; returns the child
/// pid. A spawn failure propagates to the caller (the `Spawn` IPC handler) so `wt spawn` reports it
/// cleanly rather than leaving a half-live agent.
pub async fn start(
    state: Arc<DaemonState>,
    group: String,
    session: String,
    prime: String,
    spec: HarnessSpec,
    idle_timeout: Option<Duration>,
    trace: bool,
) -> anyhow::Result<Option<u32>> {
    let child = session.clone(); // child agent name == session name
    let initial_turn = !spec.initial_prompt.is_empty(); // spawn sends the prompt as turn 1
    let initial_prompt = spec.initial_prompt.clone();
    let harness = Harness::spawn(&spec).await?;
    let pid = harness.pid();
    state
        .store
        .agent_set_status(&group, &child, "running", pid.map(|p| p as i64))
        .await?;
    // Record the prime's opening prompt on the bus so observers (e.g. the web dashboard) see the
    // full two-way conversation, not just the child's reply. The harness already received it as
    // turn 1 via `Harness::spawn`, so mark it consumed immediately — otherwise the supervisor's
    // `drain_turn_inputs` would feed it a second time.
    if initial_turn {
        if let Ok(seq) = state
            .bus_enqueue(
                &group,
                &session,
                &prime,
                &child,
                AgentMsgKind::TurnInput,
                initial_prompt.into_bytes(),
            )
            .await
        {
            let _ = state.store.agent_msg_mark_consumed(&group, &session, seq).await;
        }
    }
    info!(%group, %session, ?pid, "harness supervised");
    let task = tokio::spawn(run(
        state.clone(),
        group.clone(),
        session,
        prime,
        harness,
        idle_timeout,
        initial_turn,
        trace,
    ));
    state
        .register_child(&group, &child, task.abort_handle())
        .await;
    Ok(pid)
}

#[allow(clippy::too_many_arguments)]
async fn run(
    state: Arc<DaemonState>,
    group: String,
    session: String,
    prime: String,
    mut harness: Harness,
    idle_timeout: Option<Duration>,
    initial_turn: bool,
    trace: bool,
) {
    let child = session.clone(); // child agent name == session name
    let mut bus = state.agent_bcast.subscribe();

    // The initial prompt (sent by `Harness::spawn`) is turn 1 already in flight.
    let mut turn_active = initial_turn;
    let mut last_activity = Instant::now();
    let mut idle_notified = false;

    macro_rules! drain_or_die {
        () => {
            match drain_turn_inputs(&state, &group, &child, &mut harness).await {
                Ok(fed) if fed > 0 => {
                    turn_active = true;
                    last_activity = Instant::now();
                    idle_notified = false;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(%group, %session, error = %e, "harness write failed");
                    finish(&state, &group, &session, &child, &prime, "harness write failed").await;
                    return;
                }
            }
        };
    }

    // Feed anything already queued before we subscribed (rare).
    drain_or_die!();

    loop {
        tokio::select! {
            ev = harness.next_event() => match ev {
                Ok(HarnessEvent::AssistantText(text)) => {
                    last_activity = Instant::now();
                    idle_notified = false;
                    // Opt-in audit: forward the child's intermediate reasoning to the prime.
                    if trace {
                        let _ = state
                            .bus_enqueue(
                                &group,
                                &session,
                                &child,
                                &prime,
                                AgentMsgKind::Trace,
                                text.into_bytes(),
                            )
                            .await;
                    }
                }
                Ok(HarnessEvent::TurnComplete { result, .. }) => {
                    turn_active = false;
                    last_activity = Instant::now();
                    idle_notified = false;
                    // Enqueue the output BEFORE flipping status, so any observer that sees
                    // `awaiting_input` is guaranteed the turn output is already on the bus.
                    let _ = state
                        .bus_enqueue(
                            &group,
                            &session,
                            &child,
                            &prime,
                            AgentMsgKind::TurnOutput,
                            result.into_bytes(),
                        )
                        .await;
                    let _ = state
                        .store
                        .agent_set_status(&group, &child, "awaiting_input", None)
                        .await;
                }
                Ok(HarnessEvent::Exited(code)) => {
                    finish(&state, &group, &session, &child, &prime,
                           &format!("child exited (code {code:?})")).await;
                    return;
                }
                Err(e) => {
                    warn!(%group, %session, error = %e, "harness read error");
                    finish(&state, &group, &session, &child, &prime, "harness read error").await;
                    return;
                }
            },
            r = bus.recv() => match r {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => drain_or_die!(),
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = tokio::time::sleep(POLL) => {
                // Heartbeat: prove this session's supervisor is still alive. A session whose
                // last_seen stops advancing is broken/orphaned — the dashboard flags it stale
                // rather than showing a misleading "running".
                let _ = state.store.agent_touch(&group, &child).await;
                drain_or_die!();
                // Idle-turn timeout (notify-only): a turn silent past the window ⇒ tell the prime
                // once; keep the child running (the prime decides to nudge or kill).
                if turn_active && !idle_notified {
                    if let Some(t) = idle_timeout {
                        let idle = last_activity.elapsed();
                        if idle >= t {
                            idle_notified = true;
                            let msg = format!(
                                "turn idle for {}s — harness still running",
                                idle.as_secs()
                            );
                            let _ = state
                                .bus_enqueue(
                                    &group,
                                    &session,
                                    &child,
                                    &prime,
                                    AgentMsgKind::Control,
                                    msg.into_bytes(),
                                )
                                .await;
                        }
                    }
                }
            }
        }
    }
    state.forget_child(&group, &child).await;
    // `harness` drops here → kill_on_drop reaps the child if it is still alive.
}

/// Feed every unconsumed `turn_input` for this child to the harness, in `seq` order, marking each
/// consumed. Returns the number fed. Errors propagate (a failed stdin write means the child is gone).
async fn drain_turn_inputs(
    state: &Arc<DaemonState>,
    group: &str,
    child: &str,
    harness: &mut Harness,
) -> anyhow::Result<usize> {
    let mut fed = 0;
    for row in state
        .store
        .agent_msg_pending(group, child, "turn_input")
        .await?
    {
        let text = String::from_utf8_lossy(&row.payload).to_string();
        harness.send_turn(&text).await?;
        let _ = state
            .store
            .agent_msg_mark_consumed(group, &row.session_name, row.seq)
            .await;
        let _ = state
            .store
            .agent_set_status(group, child, "running", None)
            .await;
        fed += 1;
    }
    Ok(fed)
}

/// Mark the child exited, notify the prime, and deregister. Called on every terminal path.
async fn finish(
    state: &Arc<DaemonState>,
    group: &str,
    session: &str,
    child: &str,
    prime: &str,
    reason: &str,
) {
    let _ = state
        .store
        .agent_set_status(group, child, "exited", None)
        .await;
    let _ = state
        .bus_enqueue(
            group,
            session,
            child,
            prime,
            AgentMsgKind::Control,
            reason.as_bytes().to_vec(),
        )
        .await;
    state.forget_child(group, child).await;
}
