//! `wt` CLI — thin client that talks to `wt-daemon` over a Unix socket.

use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use wt_core::{auth, identity, paths};
use wt_proto::ipc::{
    AgentMsgKind, FsMode, IpcError, IpcErrorCode, IpcEvent, IpcRequest, PeerFilter, PeerSelector,
};
use wt_proto::token::{Cap, SignedToken, TokenId};
use wt_proto::NodeId;

#[derive(Debug, Parser)]
#[command(name = "wt", version, about = "walkie-talkie for AI agents")]
struct Cli {
    /// Optional: override `WT_HOME` for this invocation.
    #[arg(long, global = true)]
    home: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Initialize a new install: generate identity, init state.db.
    Init,
    /// Run the long-lived daemon in the foreground. Logs to stderr; honors `RUST_LOG`.
    Daemon,
    /// Print this install's NodeId (just the 32-byte pubkey).
    Nodeid,
    /// Print this install's full address ticket (NodeId + relay + direct addrs). This is what
    /// you share with a peer so they can dial you without DNS discovery.
    Ticket,
    /// Show daemon status.
    Status,
    /// Manage known peers.
    Peer {
        #[command(subcommand)]
        cmd: PeerCmd,
    },
    /// List peers (alias for `peer list` with filters), or sessions with `--group`.
    Ls {
        #[arg(long)]
        remote: bool,
        #[arg(long)]
        local: bool,
        #[arg(long)]
        connected: bool,
        /// List sessions in this orchestration group instead of peers.
        #[arg(long)]
        group: Option<String>,
    },
    /// List active connections.
    Conn,
    /// Manage capability tokens.
    Token {
        #[command(subcommand)]
        cmd: TokenCmd,
    },
    /// Send a message — to a peer, or onto the agent bus with `--session` (+ WT_TOKEN).
    Send {
        /// Peer name/NodeId (peer mode). In agent mode the message goes here (or via stdin).
        peer: Option<String>,
        /// Message body (peer mode). If omitted, reads UTF-8 from stdin.
        message: Option<String>,
        #[arg(short, long)]
        channel: Option<String>,
        /// Agent-bus target session (or set WT_SESSION). Presence selects agent mode.
        #[arg(long)]
        session: Option<String>,
        /// Agent token (or set WT_TOKEN).
        #[arg(long)]
        token: Option<String>,
        /// Agent-bus group (or set WT_GROUP). Informational; routing derives from the token.
        #[arg(long)]
        group: Option<String>,
        /// Agent message kind: user | turn_input | turn_output | control.
        #[arg(long, default_value = "user")]
        kind: String,
    },
    /// Receive messages. Drains backlog then exits; `--follow` tails live.
    /// With a wt token (--token / WT_TOKEN) this drains the agent bus instead of peer messages.
    Recv {
        /// Filter by peer name or NodeId hex (peer mode).
        #[arg(long)]
        from: Option<String>,
        /// Filter by channel name (peer mode).
        #[arg(long)]
        channel: Option<String>,
        /// Follow live messages after the backlog drains.
        #[arg(short, long)]
        follow: bool,
        /// Only include messages newer than this duration ago (e.g. `5m`, `1h`).
        #[arg(long)]
        since: Option<String>,
        /// Agent-bus session filter (agent mode).
        #[arg(long)]
        session: Option<String>,
        /// Agent token (or set WT_TOKEN). Presence selects agent mode.
        #[arg(long)]
        token: Option<String>,
        /// Agent-bus group (or set WT_GROUP). Informational; routing derives from the token.
        #[arg(long)]
        group: Option<String>,
        /// Agent mode: show all messages (incl. already-read), without consuming. Default recv
        /// returns only new messages and marks them read.
        #[arg(long)]
        all: bool,
    },
    /// Interactive chat with one peer (stdin → send, stdout ← recv).
    Chat {
        peer: String,
        #[arg(short, long, default_value = "default")]
        channel: String,
    },
    /// (v0.3+) Run a remote command. v0.1 stub: prints not-implemented.
    Exec {
        peer: String,
        /// Argv to run on the remote.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 1..)]
        argv: Vec<String>,
    },
    /// (v0.4+) Open an interactive PTY shell. v0.1 stub.
    Shell { peer: String },

    /// Manage orchestration groups (named agent swarms).
    Group {
        #[command(subcommand)]
        cmd: GroupCmd,
    },
    /// Manage agents within a group.
    Agent {
        #[command(subcommand)]
        cmd: AgentCmd,
    },
    /// Print the agent identity bound to a wt token (--token / WT_TOKEN).
    Whoami {
        #[arg(long)]
        token: Option<String>,
    },
    /// Spawn + supervise a child harness in an isolated per-session workspace.
    Spawn {
        /// Session name (e.g. frontend); also the child agent's name.
        #[arg(long)]
        session: String,
        /// Base directory to provision the workspace from.
        #[arg(long)]
        dir: String,
        /// Group (or set WT_GROUP).
        #[arg(long)]
        group: Option<String>,
        /// Prime token (or set WT_TOKEN).
        #[arg(long)]
        token: Option<String>,
        /// Provision a git worktree of the base repo (default when the base is a git repo).
        #[arg(long)]
        worktree: bool,
        /// Provision a fresh empty folder (default when the base is not a git repo).
        #[arg(long)]
        new: bool,
        /// Initial task prompt, sent to the child as its first turn.
        #[arg(long)]
        prompt: Option<String>,
        /// Notify the prime once if a turn stays idle this long (e.g. `5m`); off when unset.
        #[arg(long)]
        idle_timeout: Option<String>,
        /// Claude permission mode: default | plan | acceptEdits | auto | dontAsk | bypassPermissions.
        /// Applies to the built-in Claude harness; ignored if $WT_HARNESS_CMD overrides it.
        #[arg(long)]
        permission_mode: Option<String>,
        /// Shorthand for `--permission-mode plan` (read-only/explore — good for an auditor child).
        #[arg(long)]
        plan: bool,
        /// Launch the child with `--dangerously-skip-permissions` (autonomous edits/commands).
        #[arg(long)]
        skip_permissions: bool,
        /// Forward the child's intermediate assistant text to the prime as `trace` messages.
        #[arg(long)]
        trace: bool,
        /// Spawn as the group's prime **coordinator**: a prime-role harness you chat with that
        /// spawns + commands worker sessions itself (workers are read-only in the dashboard).
        #[arg(long)]
        coordinator: bool,
    },
    /// Manage sessions within a group.
    Session {
        #[command(subcommand)]
        cmd: SessionCmd,
    },
}

#[derive(Debug, Subcommand)]
enum PeerCmd {
    /// Add a peer. Accepts either a bare NodeId hex (no addressing info, relies on discovery)
    /// or a full `wt1:` ticket (direct dial).
    Add {
        id_or_ticket: String,
        #[arg(long)]
        name: String,
    },
    Rm {
        peer: String,
    },
    List,
}

#[derive(Debug, Subcommand)]
enum GroupCmd {
    /// Create a named group; prints the prime agent's token to stdout.
    New { name: String },
    /// List groups.
    Ls,
}

#[derive(Debug, Subcommand)]
enum AgentCmd {
    /// List agents (optionally scoped to a group and/or session).
    Ls {
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        session: Option<String>,
    },
    /// Kill a running agent (prime only).
    Kill {
        name: String,
        #[arg(long)]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum SessionCmd {
    /// Close a session: stop its child + tear down the workspace (branch kept unless --discard).
    Close {
        name: String,
        #[arg(long)]
        discard: bool,
        #[arg(long)]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCmd {
    Grant {
        peer: String,
        /// Capabilities (e.g. `msg`). Repeat flag for multiple.
        #[arg(long, num_args = 1.., default_value = "msg")]
        cap: Vec<String>,
        /// Optional TTL. Examples: `24h`, `30m`, `7d`. Omit for unlimited.
        #[arg(long)]
        ttl: Option<String>,
    },
    Import {
        token: String,
    },
    List,
    Revoke {
        id: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Some(h) = &cli.home {
        std::env::set_var("WT_HOME", h);
    }
    init_tracing();

    if let Err(e) = run_cmd(cli.cmd).await {
        // Structured daemon rejections map to specific exit codes so scripts can branch.
        if let Some(ipc) = e.downcast_ref::<IpcError>() {
            eprintln!("error: {}", ipc.message);
            std::process::exit(exit_code_for(ipc.code));
        }
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

/// Map a structured IPC error category to a process exit code.
fn exit_code_for(code: IpcErrorCode) -> i32 {
    match code {
        IpcErrorCode::Unauthorized
        | IpcErrorCode::Expired
        | IpcErrorCode::Revoked
        | IpcErrorCode::PeerNotKnown => 3,
        IpcErrorCode::NotFound => 4,
        IpcErrorCode::BadRequest => 2,
        IpcErrorCode::Unimplemented => 5,
        IpcErrorCode::Internal => 1,
    }
}

async fn run_cmd(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Init => cmd_init().await,
        Cmd::Daemon => cmd_daemon().await,
        Cmd::Nodeid => cmd_nodeid().await,
        Cmd::Ticket => cmd_ticket().await,
        Cmd::Status => cmd_status().await,
        Cmd::Peer { cmd } => match cmd {
            PeerCmd::Add { id_or_ticket, name } => cmd_peer_add(&id_or_ticket, &name).await,
            PeerCmd::Rm { peer } => cmd_peer_rm(&peer).await,
            PeerCmd::List => cmd_ls(PeerFilter::All).await,
        },
        Cmd::Ls {
            remote,
            local,
            connected,
            group,
        } => {
            if let Some(group) = group {
                cmd_session_ls(&group).await
            } else {
                let filter = if connected {
                    PeerFilter::Connected
                } else if remote {
                    PeerFilter::Remote
                } else if local {
                    PeerFilter::Local
                } else {
                    PeerFilter::All
                };
                cmd_ls(filter).await
            }
        }
        Cmd::Conn => cmd_conn().await,
        Cmd::Token { cmd } => match cmd {
            TokenCmd::Grant { peer, cap, ttl } => {
                cmd_token_grant(&peer, &cap, ttl.as_deref()).await
            }
            TokenCmd::Import { token } => cmd_token_import(&token).await,
            TokenCmd::List => cmd_token_list().await,
            TokenCmd::Revoke { id } => cmd_token_revoke(&id).await,
        },
        Cmd::Send {
            peer,
            message,
            channel,
            session,
            token,
            group,
            kind,
        } => {
            let _ = group; // accepted for symmetry; routing derives from the token
            let session = env_or(session, "WT_SESSION");
            let token = env_or(token, "WT_TOKEN");
            match (session, token) {
                (Some(session), Some(token)) => {
                    let body = match message.or(peer) {
                        Some(m) => m.into_bytes(),
                        None => read_stdin_bytes().await?,
                    };
                    cmd_agent_send(&token, &session, parse_agent_kind(&kind)?, body).await
                }
                _ => {
                    let peer = peer.ok_or_else(|| {
                        anyhow!("peer required (or use --session + WT_TOKEN for the agent bus)")
                    })?;
                    let payload = match message {
                        Some(m) => m.into_bytes(),
                        None => read_stdin_bytes().await?,
                    };
                    let ch = channel.unwrap_or_else(|| "default".to_string());
                    cmd_send(&peer, &ch, payload).await
                }
            }
        }
        Cmd::Recv {
            from,
            channel,
            follow,
            since,
            session,
            token,
            group,
            all,
        } => {
            let _ = group; // routing derives from the token
            match env_or(token, "WT_TOKEN") {
                Some(token) => {
                    // Default recv consumes new messages; --all / --since are non-destructive views.
                    let consume = !all && since.is_none();
                    let since_ms = if consume {
                        None
                    } else {
                        since_to_ms(since.as_deref())?
                    };
                    cmd_agent_recv(
                        &token,
                        env_or(session, "WT_SESSION"),
                        since_ms,
                        follow,
                        consume,
                    )
                    .await
                }
                None => cmd_recv(from.as_deref(), channel, follow, since.as_deref()).await,
            }
        }
        Cmd::Chat { peer, channel } => cmd_chat(&peer, &channel).await,
        Cmd::Exec { peer: _, argv: _ } => {
            eprintln!("`wt exec` is coming in v0.3 — not implemented yet.");
            std::process::exit(2);
        }
        Cmd::Shell { peer: _ } => {
            eprintln!("`wt shell` is coming in v0.4 — not implemented yet.");
            std::process::exit(2);
        }
        Cmd::Group { cmd } => match cmd {
            GroupCmd::New { name } => cmd_group_new(&name).await,
            GroupCmd::Ls => cmd_group_ls().await,
        },
        Cmd::Agent { cmd } => match cmd {
            AgentCmd::Ls { group, session } => {
                cmd_agent_ls(env_or(group, "WT_GROUP"), session).await
            }
            AgentCmd::Kill { name, token } => cmd_agent_kill(&name, &require_token(token)?).await,
        },
        Cmd::Whoami { token } => cmd_whoami(&require_token(token)?).await,
        Cmd::Spawn {
            session,
            dir,
            group,
            token,
            worktree,
            new,
            prompt,
            idle_timeout,
            permission_mode,
            plan,
            skip_permissions,
            trace,
            coordinator,
        } => {
            cmd_spawn(
                session,
                dir,
                group,
                token,
                worktree,
                new,
                prompt,
                idle_timeout,
                permission_mode,
                plan,
                skip_permissions,
                trace,
                coordinator,
            )
            .await
        }
        Cmd::Session { cmd } => match cmd {
            SessionCmd::Close {
                name,
                discard,
                token,
            } => cmd_session_close(&name, discard, &require_token(token)?).await,
        },
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

// ===== Commands =====

async fn cmd_daemon() -> Result<()> {
    wt_daemon::run().await
}

async fn cmd_init() -> Result<()> {
    let id = identity::Identity::load_or_create().context("create identity")?;
    let _ = wt_core::store::Store::open()?;
    println!("initialized wt at {}", paths::home().display());
    println!("nodeid: {}", id.nodeid());
    Ok(())
}

async fn cmd_nodeid() -> Result<()> {
    // Try daemon first; fall back to local key file.
    if let Ok(mut s) = connect_daemon().await {
        request_one(&mut s, &IpcRequest::NodeId).await?;
        match read_event(&mut s).await? {
            IpcEvent::NodeIdValue(n) => {
                println!("{n}");
                return Ok(());
            }
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected event from daemon: {other:?}"),
        }
    }
    let id = identity::Identity::load()
        .context("no daemon, and no local identity yet — run `wt init`")?;
    println!("{}", id.nodeid());
    Ok(())
}

async fn cmd_status() -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::Status).await?;
    match read_event(&mut s).await? {
        IpcEvent::StatusInfo {
            nodeid,
            version,
            endpoint_bound,
        } => {
            println!("nodeid: {nodeid}");
            println!("version: {version}");
            println!("endpoint_bound: {endpoint_bound}");
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_peer_add(id_or_ticket: &str, name: &str) -> Result<()> {
    let trimmed = id_or_ticket.trim();
    let (nodeid, addr_blob) = if trimmed.starts_with(wt_proto::ticket::TICKET_PREFIX) {
        let t = wt_proto::ticket::AddrTicket::decode(trimmed)
            .map_err(|e| anyhow!("decode ticket: {e}"))?;
        let mut buf = Vec::new();
        ciborium::into_writer(&t, &mut buf)?;
        (t.nodeid, Some(buf))
    } else {
        let nid: NodeId = trimmed
            .parse()
            .context("parse NodeId hex (or pass a wt1:… ticket)")?;
        (nid, None)
    };
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::PeerAdd {
            nodeid,
            name: name.to_string(),
            addr_blob,
        },
    )
    .await?;
    expect_ok(&mut s).await
}

async fn cmd_ticket() -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::Ticket).await?;
    match read_event(&mut s).await? {
        IpcEvent::TicketValue(t) => {
            println!("{t}");
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_peer_rm(peer: &str) -> Result<()> {
    let sel = peer_selector(peer);
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::PeerRm { selector: sel }).await?;
    expect_ok(&mut s).await
}

async fn cmd_ls(filter: PeerFilter) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::PeerList { filter }).await?;
    println!(
        "{:<12} {:<66} {:<8} {:<12} LAST SEEN",
        "NAME", "NODEID", "SOURCE", "STATE"
    );
    loop {
        match read_event(&mut s).await? {
            IpcEvent::PeerListItem(p) => {
                let source = match p.source {
                    wt_proto::ipc::PeerSource::Manual => "manual",
                    wt_proto::ipc::PeerSource::Mdns => "mdns",
                };
                let state = match p.state {
                    wt_proto::ipc::PeerState::Connected { open_streams } => {
                        format!("connected({open_streams})")
                    }
                    wt_proto::ipc::PeerState::Idle => "idle".into(),
                    wt_proto::ipc::PeerState::Offline => "offline".into(),
                };
                let last = p.last_seen_ms.map(ago).unwrap_or_else(|| "—".into());
                println!(
                    "{:<12} {:<66} {:<8} {:<12} {}",
                    p.name, p.nodeid, source, state, last
                );
            }
            IpcEvent::PeerListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_conn() -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::ConnList).await?;
    println!(
        "{:<12} {:<66} {:<10} {:<24} VIA",
        "PEER", "NODEID", "SINCE", "STREAMS"
    );
    loop {
        match read_event(&mut s).await? {
            IpcEvent::ConnListItem(c) => {
                let via = if c.via_relay { "relay" } else { "direct" };
                let streams = c.streams.join(",");
                println!(
                    "{:<12} {:<66} {:<10} {:<24} {}",
                    c.peer_name,
                    c.nodeid,
                    ago(c.since_ms),
                    streams,
                    via
                );
            }
            IpcEvent::ConnListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_token_grant(peer: &str, caps: &[String], ttl: Option<&str>) -> Result<()> {
    let caps_parsed: Result<Vec<Cap>> = caps
        .iter()
        .map(|c| c.parse::<Cap>().map_err(|e| anyhow!(e)))
        .collect();
    let caps_parsed = caps_parsed?;
    let ttl_secs = ttl.map(parse_duration).transpose()?;
    let sel = peer_selector(peer);
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::TokenGrant {
            peer: sel,
            caps: caps_parsed,
            ttl_secs,
        },
    )
    .await?;
    match read_event(&mut s).await? {
        IpcEvent::TokenIssued { raw, info } => {
            // Decode signed token then re-encode as base32 for paste-ability.
            let signed: SignedToken = ciborium::from_reader(&raw[..])?;
            let b32 = auth::token_encode_base32(&signed)?;
            eprintln!(
                "issued token id={}  iss={}  sub={}  exp={}  caps={:?}",
                hex::encode(info.id),
                info.iss,
                info.sub,
                info.exp,
                info.caps
            );
            println!("{b32}");
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_token_import(token_b32: &str) -> Result<()> {
    let signed = auth::token_decode_base32(token_b32)?;
    let mut raw = Vec::new();
    ciborium::into_writer(&signed, &mut raw)?;
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::TokenImport { raw }).await?;
    // First event: TokenListItem with the imported claims, second: Ok.
    let first = read_event(&mut s).await?;
    if let IpcEvent::TokenListItem(info) = &first {
        eprintln!(
            "imported token id={} from {} caps={:?} exp={}",
            hex::encode(info.id),
            info.iss,
            info.caps,
            info.exp,
        );
    } else if let IpcEvent::Err(e) = first {
        bail!(e);
    }
    expect_ok(&mut s).await
}

async fn cmd_token_list() -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::TokenList).await?;
    println!(
        "{:<34} {:<66} {:<66} {:<12} CAPS",
        "ID", "ISS", "SUB", "EXP"
    );
    loop {
        match read_event(&mut s).await? {
            IpcEvent::TokenListItem(t) => {
                let caps = t
                    .caps
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let exp = if t.revoked {
                    "REVOKED".into()
                } else {
                    t.exp.to_string()
                };
                println!(
                    "{:<34} {:<66} {:<66} {:<12} {}",
                    hex::encode(t.id),
                    t.iss,
                    t.sub,
                    exp,
                    caps
                );
            }
            IpcEvent::TokenListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_token_revoke(id_hex: &str) -> Result<()> {
    let bytes = hex::decode(id_hex).context("token id is not hex")?;
    if bytes.len() != 16 {
        bail!("token id must be 16 bytes (32 hex chars)");
    }
    let mut id: TokenId = [0u8; 16];
    id.copy_from_slice(&bytes);
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::TokenRevoke { id }).await?;
    expect_ok(&mut s).await
}

async fn cmd_send(peer: &str, channel: &str, payload: Vec<u8>) -> Result<()> {
    let sel = peer_selector(peer);
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::Send {
            peer: sel,
            channel: channel.to_string(),
            payload,
        },
    )
    .await?;
    expect_ok(&mut s).await
}

async fn cmd_recv(
    from: Option<&str>,
    channel: Option<String>,
    follow: bool,
    since: Option<&str>,
) -> Result<()> {
    let from_sel = from.map(peer_selector);
    let since_ms = match since {
        Some(s) => {
            let secs = parse_duration(s)?;
            let now = wt_core::store::unix_ms();
            Some(now.saturating_sub(secs.saturating_mul(1000)))
        }
        None => None,
    };
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::RecvSubscribe {
            peer: from_sel,
            channel,
            since_ms,
            follow,
        },
    )
    .await?;
    loop {
        match read_event(&mut s).await {
            Ok(IpcEvent::RecvMsg {
                from,
                from_name,
                channel,
                payload,
                ts_ms,
            }) => {
                let from_disp = from_name.unwrap_or_else(|| from.to_string());
                let payload_json = match serde_json::from_slice::<serde_json::Value>(&payload) {
                    Ok(v) => v,
                    Err(_) => {
                        serde_json::Value::String(String::from_utf8_lossy(&payload).to_string())
                    }
                };
                let event = serde_json::json!({
                    "from": from_disp,
                    "channel": channel,
                    "ts_ms": ts_ms,
                    "payload": payload_json,
                });
                println!("{}", serde_json::to_string(&event)?);
            }
            Ok(IpcEvent::RecvBacklogEnd) => {
                if !follow {
                    return Ok(());
                }
                // else: continue tailing live messages.
            }
            Ok(IpcEvent::Err(e)) => bail!(e),
            Ok(other) => bail!("unexpected: {other:?}"),
            Err(e) => return Err(e),
        }
    }
}

async fn cmd_chat(peer: &str, channel: &str) -> Result<()> {
    // Two halves over two separate connections to the daemon.
    let sel = peer_selector(peer);
    let mut recv_conn = connect_daemon().await?;
    // For chat: skip backlog (since_ms=now) and follow live only.
    let since_now = wt_core::store::unix_ms();
    request_one(
        &mut recv_conn,
        &IpcRequest::RecvSubscribe {
            peer: Some(sel.clone()),
            channel: Some(channel.to_string()),
            since_ms: Some(since_now),
            follow: true,
        },
    )
    .await?;
    let recv_task = tokio::spawn(async move {
        loop {
            match read_event(&mut recv_conn).await {
                Ok(IpcEvent::RecvMsg {
                    from_name,
                    from,
                    payload,
                    ..
                }) => {
                    let name = from_name.unwrap_or_else(|| from.to_string());
                    let body = String::from_utf8_lossy(&payload);
                    println!("[{name}] {body}");
                }
                _ => return,
            }
        }
    });

    let mut send_conn = connect_daemon().await?;
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    let mut line = String::new();
    loop {
        line.clear();
        let n = tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await?;
        if n == 0 {
            break;
        }
        let payload = line.trim_end_matches('\n').as_bytes().to_vec();
        let req = IpcRequest::Send {
            peer: sel.clone(),
            channel: channel.to_string(),
            payload,
        };
        request_one(&mut send_conn, &req).await?;
        let _ = read_event(&mut send_conn).await?; // Ok or Err
    }
    recv_task.abort();
    Ok(())
}

// ===== v3 orchestration commands =====

async fn cmd_group_new(name: &str) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::GroupNew {
            name: name.to_string(),
        },
    )
    .await?;
    match read_event(&mut s).await? {
        IpcEvent::GroupCreated { group, token } => {
            eprintln!("created group '{group}'; you are its prime agent. To act as it, export:");
            eprintln!("  export WT_GROUP={group} WT_TOKEN={token}");
            println!("{token}");
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_group_ls() -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::GroupList).await?;
    println!("{:<20} {:<10} CREATED", "GROUP", "SESSIONS");
    loop {
        match read_event(&mut s).await? {
            IpcEvent::GroupListItem(g) => {
                println!(
                    "{:<20} {:<10} {}",
                    g.name,
                    g.session_count,
                    ago(g.created_at_ms)
                )
            }
            IpcEvent::GroupListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_session_ls(group: &str) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::SessionList {
            group: group.to_string(),
        },
    )
    .await?;
    println!(
        "{:<14} {:<14} {:<10} {:<14} WORKSPACE",
        "SESSION", "CHILD", "FS", "STATUS"
    );
    loop {
        match read_event(&mut s).await? {
            IpcEvent::SessionListItem(si) => {
                let fs = match si.fs_mode {
                    FsMode::Worktree => "worktree",
                    FsMode::New => "new",
                };
                let status = si.child_status.unwrap_or(si.status);
                println!(
                    "{:<14} {:<14} {:<10} {:<14} {}",
                    si.name, si.child_agent, fs, status, si.workspace_path
                );
            }
            IpcEvent::SessionListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_agent_ls(group: Option<String>, session: Option<String>) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(&mut s, &IpcRequest::AgentList { group, session }).await?;
    println!(
        "{:<14} {:<12} {:<8} {:<14} PID/DIR",
        "AGENT", "GROUP", "ROLE", "STATUS"
    );
    loop {
        match read_event(&mut s).await? {
            IpcEvent::AgentListItem(a) => {
                let extra = a
                    .pid
                    .map(|p| format!("pid {p}"))
                    .or(a.dir)
                    .unwrap_or_default();
                println!(
                    "{:<14} {:<12} {:<8} {:<14} {}",
                    a.name, a.group, a.role, a.status, extra
                );
            }
            IpcEvent::AgentListEnd => break,
            IpcEvent::Err(e) => bail!(e),
            other => bail!("unexpected: {other:?}"),
        }
    }
    Ok(())
}

async fn cmd_whoami(token: &str) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::WhoAmI {
            token: token.to_string(),
        },
    )
    .await?;
    match read_event(&mut s).await? {
        IpcEvent::WhoAmIInfo(w) => {
            println!("group:   {}", w.group);
            println!("agent:   {}", w.agent);
            println!("role:    {}", w.role);
            println!("session: {}", w.session.as_deref().unwrap_or("—"));
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_agent_send(
    token: &str,
    session: &str,
    kind: AgentMsgKind,
    payload: Vec<u8>,
) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::AgentSend {
            token: token.to_string(),
            session: session.to_string(),
            kind,
            payload,
        },
    )
    .await?;
    expect_ok(&mut s).await
}

async fn cmd_agent_recv(
    token: &str,
    session: Option<String>,
    since_ms: Option<u64>,
    follow: bool,
    consume: bool,
) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::AgentRecv {
            token: token.to_string(),
            session,
            since_ms,
            follow,
            consume,
        },
    )
    .await?;
    loop {
        match read_event(&mut s).await {
            Ok(IpcEvent::AgentMsg {
                session,
                from_agent,
                kind,
                payload,
                ts_ms,
            }) => {
                let payload_json = match serde_json::from_slice::<serde_json::Value>(&payload) {
                    Ok(v) => v,
                    Err(_) => {
                        serde_json::Value::String(String::from_utf8_lossy(&payload).to_string())
                    }
                };
                let event = serde_json::json!({
                    "session": session,
                    "from": from_agent,
                    "kind": agent_kind_str(kind),
                    "ts_ms": ts_ms,
                    "payload": payload_json,
                });
                println!("{}", serde_json::to_string(&event)?);
            }
            Ok(IpcEvent::AgentBacklogEnd) => {
                if !follow {
                    return Ok(());
                }
            }
            Ok(IpcEvent::Err(e)) => bail!(e),
            Ok(other) => bail!("unexpected: {other:?}"),
            Err(e) => return Err(e),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn cmd_spawn(
    session: String,
    dir: String,
    group: Option<String>,
    token: Option<String>,
    worktree: bool,
    new: bool,
    prompt: Option<String>,
    idle_timeout: Option<String>,
    permission_mode: Option<String>,
    plan: bool,
    skip_permissions: bool,
    trace: bool,
    coordinator: bool,
) -> Result<()> {
    let group = env_or(group, "WT_GROUP")
        .ok_or_else(|| anyhow!("--group or WT_GROUP required for spawn"))?;
    let token = require_token(token)?;
    let fs_mode = resolve_fs_mode(&dir, worktree, new)?;
    let idle_timeout_secs = idle_timeout.map(|s| parse_duration(&s)).transpose()?;
    // `--plan` is sugar for `--permission-mode plan` (unless an explicit mode is given).
    let permission_mode = if plan && permission_mode.is_none() {
        Some("plan".to_string())
    } else {
        permission_mode
    };
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::Spawn {
            token,
            group,
            session,
            base_dir: dir,
            fs_mode,
            branch: None,
            label: None,
            prompt: prompt.unwrap_or_default(),
            harness_argv: None,
            idle_timeout_secs,
            permission_mode,
            skip_permissions,
            trace,
            coordinator,
        },
    )
    .await?;
    match read_event(&mut s).await? {
        IpcEvent::Spawned {
            group,
            session,
            token,
            workspace,
        } => {
            eprintln!("spawned session '{session}' in group '{group}'");
            eprintln!("workspace: {workspace}");
            eprintln!("child token: {token}");
            println!(
                "{}",
                serde_json::json!({
                    "group": group,
                    "session": session,
                    "workspace": workspace,
                    "token": token,
                })
            );
            Ok(())
        }
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

async fn cmd_agent_kill(name: &str, token: &str) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::AgentKill {
            token: token.to_string(),
            agent: name.to_string(),
        },
    )
    .await?;
    expect_ok(&mut s).await
}

async fn cmd_session_close(name: &str, discard: bool, token: &str) -> Result<()> {
    let mut s = connect_daemon().await?;
    request_one(
        &mut s,
        &IpcRequest::SessionClose {
            token: token.to_string(),
            session: name.to_string(),
            discard,
        },
    )
    .await?;
    expect_ok(&mut s).await
}

/// Pick a workspace mode: explicit `--worktree`/`--new`, else worktree iff the base is a git repo.
fn resolve_fs_mode(base: &str, worktree: bool, new: bool) -> Result<FsMode> {
    match (worktree, new) {
        (true, true) => bail!("--worktree and --new are mutually exclusive"),
        (true, false) => Ok(FsMode::Worktree),
        (false, true) => Ok(FsMode::New),
        (false, false) => Ok(if is_git_repo(base) {
            FsMode::Worktree
        } else {
            FsMode::New
        }),
    }
}

fn is_git_repo(base: &str) -> bool {
    std::process::Command::new("git")
        .arg("-C")
        .arg(base)
        .args(["rev-parse", "--is-inside-work-tree"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn parse_agent_kind(s: &str) -> Result<AgentMsgKind> {
    Ok(match s {
        "user" => AgentMsgKind::User,
        "turn_input" => AgentMsgKind::TurnInput,
        "turn_output" => AgentMsgKind::TurnOutput,
        "control" => AgentMsgKind::Control,
        "trace" => AgentMsgKind::Trace,
        other => {
            bail!("unknown message kind '{other}' (user|turn_input|turn_output|control|trace)")
        }
    })
}

fn agent_kind_str(k: AgentMsgKind) -> &'static str {
    match k {
        AgentMsgKind::TurnOutput => "turn_output",
        AgentMsgKind::TurnInput => "turn_input",
        AgentMsgKind::User => "user",
        AgentMsgKind::Control => "control",
        AgentMsgKind::Trace => "trace",
    }
}

/// First non-empty of an explicit flag or an environment variable.
fn env_or(opt: Option<String>, var: &str) -> Option<String> {
    opt.or_else(|| std::env::var(var).ok().filter(|v| !v.is_empty()))
}

fn require_token(opt: Option<String>) -> Result<String> {
    env_or(opt, "WT_TOKEN").ok_or_else(|| anyhow!("no wt token — pass --token or set WT_TOKEN"))
}

async fn read_stdin_bytes() -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    tokio::io::stdin().read_to_end(&mut buf).await?;
    Ok(buf)
}

fn since_to_ms(since: Option<&str>) -> Result<Option<u64>> {
    match since {
        Some(s) => {
            let secs = parse_duration(s)?;
            let now = wt_core::store::unix_ms();
            Ok(Some(now.saturating_sub(secs.saturating_mul(1000))))
        }
        None => Ok(None),
    }
}

// ===== Helpers =====

async fn connect_daemon() -> Result<UnixStream> {
    let sock = paths::daemon_sock_path();
    if !Path::new(&sock).exists() {
        bail!(
            "daemon socket not found at {} — start the daemon with `wt-daemon`",
            sock.display()
        );
    }
    let stream = tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(&sock))
        .await
        .map_err(|_| anyhow!("timed out connecting to daemon"))??;
    Ok(stream)
}

async fn request_one(stream: &mut UnixStream, req: &IpcRequest) -> Result<()> {
    let mut buf = Vec::new();
    ciborium::into_writer(req, &mut buf).context("encode ipc request")?;
    let len = (buf.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_event(stream: &mut UnixStream) -> Result<IpcEvent> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("read ipc len")?;
    let len = u32::from_be_bytes(len_buf);
    if len > 64 * 1024 * 1024 {
        bail!("ipc frame too large");
    }
    let mut body = vec![0u8; len as usize];
    stream
        .read_exact(&mut body)
        .await
        .context("read ipc body")?;
    let ev: IpcEvent = ciborium::from_reader(&body[..]).context("decode ipc event")?;
    Ok(ev)
}

async fn expect_ok(stream: &mut UnixStream) -> Result<()> {
    match read_event(stream).await? {
        IpcEvent::Ok => Ok(()),
        IpcEvent::Err(e) => bail!(e),
        other => bail!("unexpected: {other:?}"),
    }
}

fn peer_selector(s: &str) -> PeerSelector {
    if let Ok(n) = s.parse::<NodeId>() {
        PeerSelector::NodeId(n)
    } else {
        PeerSelector::Name(s.to_string())
    }
}

fn parse_duration(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty duration");
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num.parse().context("bad duration number")?;
    let secs = match unit {
        "s" => Some(n),
        "m" => n.checked_mul(60),
        "h" => n.checked_mul(3600),
        "d" => n.checked_mul(86400),
        other => bail!("unknown duration unit: {other}"),
    }
    .ok_or_else(|| anyhow!("duration is too large"))?;
    Ok(secs)
}

fn ago(ms: u64) -> String {
    let now = wt_core::store::unix_ms();
    let delta = now.saturating_sub(ms);
    if delta < 1000 {
        "now".into()
    } else if delta < 60_000 {
        format!("{}s ago", delta / 1000)
    } else if delta < 3_600_000 {
        format!("{}m ago", delta / 60_000)
    } else {
        format!("{}h ago", delta / 3_600_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_accepts_supported_units() {
        assert_eq!(parse_duration("0s").unwrap(), 0);
        assert_eq!(parse_duration("5s").unwrap(), 5);
        assert_eq!(parse_duration("2m").unwrap(), 120);
        assert_eq!(parse_duration("3h").unwrap(), 10_800);
        assert_eq!(parse_duration("4d").unwrap(), 345_600);
    }

    #[test]
    fn parse_duration_rejects_empty_unknown_and_overflow() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("10w").is_err());
        assert!(parse_duration("10ms").is_err());
        assert!(parse_duration("18446744073709551615d").is_err());
    }

    #[test]
    fn peer_selector_uses_nodeid_when_hex_is_valid() {
        let node = NodeId([0xab; 32]);
        match peer_selector(&node.to_string()) {
            PeerSelector::NodeId(parsed) => assert_eq!(parsed, node),
            other => panic!("expected nodeid selector, got {other:?}"),
        }

        match peer_selector("alice") {
            PeerSelector::Name(name) => assert_eq!(name, "alice"),
            other => panic!("expected name selector, got {other:?}"),
        }
    }
}
