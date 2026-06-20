//! `wt-daemon` — library that owns the iroh endpoint and serves the local CLI over a Unix socket.
//!
//! This is invoked from the `wt daemon` subcommand in `wt-cli`. v0.1: stale-socket cleanup, UDS
//! server, accept loop for `Msg` streams, graceful shutdown on SIGINT/SIGTERM.

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

pub mod ipc;
pub mod mdns;
pub mod state;
pub mod supervisor;
pub mod web;

pub use state::DaemonState;

/// Run the daemon until SIGINT/SIGTERM. Caller must have set up a tokio runtime and (optionally)
/// a tracing subscriber.
pub async fn run() -> Result<()> {
    let state = Arc::new(DaemonState::start().await.context("start daemon state")?);

    let ipc_task = tokio::spawn(ipc::run_ipc_server(state.clone()));
    let accept_task = tokio::spawn(state.clone().run_accept_loop());
    let web_task = tokio::spawn(web::run_web_server(state.clone()));

    // Resume delivery for any peers with a backlog from a previous run. Per-peer tasks are
    // spawned on demand (here and on each `Send`), so there is no standing delivery worker.
    state.resume_delivery().await;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c, shutting down");
        }
        _ = sigterm() => {
            info!("received SIGTERM, shutting down");
        }
    }

    state.shutdown().await; // signals per-peer tasks to exit
    ipc_task.abort();
    accept_task.abort();
    web_task.abort();
    Ok(())
}

/// Start the daemon state and return it along with handles to its background tasks. Useful
/// for integration tests where the test driver controls shutdown.
pub async fn start_for_test() -> Result<TestHandle> {
    let state = Arc::new(DaemonState::start().await.context("start daemon state")?);
    let ipc_task = tokio::spawn(ipc::run_ipc_server(state.clone()));
    let accept_task = tokio::spawn(state.clone().run_accept_loop());
    state.resume_delivery().await;
    Ok(TestHandle {
        state,
        ipc_task: Some(ipc_task),
        accept_task: Some(accept_task),
    })
}

/// In-process daemon handle for tests. Drop or call `shutdown()` to tear down.
pub struct TestHandle {
    pub state: Arc<DaemonState>,
    ipc_task: Option<tokio::task::JoinHandle<()>>,
    accept_task: Option<tokio::task::JoinHandle<()>>,
}

impl TestHandle {
    pub async fn shutdown(mut self) {
        self.state.shutdown().await; // signals per-peer delivery tasks to exit
        for t in [self.ipc_task.take(), self.accept_task.take()]
            .into_iter()
            .flatten()
        {
            t.abort();
        }
    }
}

impl Drop for TestHandle {
    fn drop(&mut self) {
        // Best-effort: signal per-peer tasks (they also hold an Arc<DaemonState>) to stop.
        self.state.shutdown_signal();
        for t in [self.ipc_task.take(), self.accept_task.take()]
            .into_iter()
            .flatten()
        {
            t.abort();
        }
    }
}

async fn sigterm() {
    #[cfg(unix)]
    {
        let mut s = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        s.recv().await;
    }
    #[cfg(not(unix))]
    {
        std::future::pending::<()>().await;
    }
}
