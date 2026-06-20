# wt — walkie-talkie for AI agents

`wt` is an SSH-shaped primitive rebuilt for **agent-to-agent** communication: structured JSON messages, capability-scoped tokens, durable channels, and remote exec, all over a single peer-to-peer connection that survives NATs and mobile networks.

```
agent A ──┐                            ┌── agent B
          │ {"user": "hello"}          │
   ┌──────▼──────┐    iroh QUIC ┌──────▼──────┐
   │  wt daemon  │◀────────────▶│  wt daemon  │
   └─────────────┘  + n0 relay  └─────────────┘
        │                              │
        │ Unix socket IPC              │
   ┌────▼────┐                    ┌────▼────┐
   │ wt CLI  │                    │ wt CLI  │
   └─────────┘                    └─────────┘
```

This document describes how it's built. For tutorial-style usage see the CLI's `--help`.

---

## 1. Goals & non-goals

### What `wt` is
- A pipe for structured messages (`{"user": "..."}` JSON, but the daemon treats payloads as opaque bytes).
- A capability layer: each peer issues *tokens* to other peers that authorize specific actions.
- A NAT-transparent transport: peers reach each other by `NodeId` regardless of network topology.
- A durable channel: messages survive daemon restarts and offline gaps.

### What it isn't
- Not a chat app. No UI, no presence, no read-receipts beyond what the daemon emits.
- Not a CRDT or pub-sub broker. No cross-sender total ordering.
- Not a replacement for a real MQ. No exactly-once, no transactions across peers.
- Not multi-tenant *across identities*. One transport identity per install, one daemon per
  `WT_HOME`. (v0.3 adds many *local* agents under that one install for orchestration — see §16 —
  but the cross-machine identity stays one-per-install.)

### Why not just SSH + JSON-RPC?
SSH's auth is too coarse (single user@host scope), it has no native message buffering, and bolting structured channels onto it is fragile. A purpose-built primitive lets us optimize auth granularity, transport choice (QUIC vs TCP), and persistence semantics for the agent workload.

### Why not just raw `quinn` QUIC?
NAT traversal. The reason `wt` builds on `iroh` (which builds on `quinn`) is that iroh ships UDP hole-punching, relay fallback, and `NodeId = public_key` identity. Building those from scratch is a year of work.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ wt CLI (clap v4)                                                    │
│   init / daemon / nodeid / ticket / status                          │
│   peer add|rm|list  •  ls [--remote|--local|--connected]            │
│   token grant|import|list|revoke                                    │
│   send / recv [--follow] [--since] / chat                           │
│   conn  •  (v0.3+) exec / shell / @peer                             │
└────────────────────────────────┬────────────────────────────────────┘
                  Unix socket: ~/.wt/run/daemon.sock
                  Wire: u32-be length prefix + CBOR
┌────────────────────────────────▼────────────────────────────────────┐
│ wt-daemon (long-running tokio process)                              │
│                                                                     │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐ │
│  │  IPC server  │ │ accept loop  │ │ delivery wkr │ │ mDNS resp+ │ │
│  │   (UDS)      │ │  (iroh)      │ │ (outbox tail)│ │ browse     │ │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └─────┬──────┘ │
│         │                │                │                │        │
│         └────────┬───────┴────────┬───────┴────────────────┘        │
│                  ▼                ▼                                  │
│         ┌────────────────┐ ┌────────────────┐                       │
│         │  state.db      │ │  Identity (Ed25519)                    │
│         │  (rusqlite)    │ │  iroh Endpoint │                       │
│         └────────────────┘ └────────────────┘                       │
└────────────────────────────────┬────────────────────────────────────┘
                  Single ALPN: "wt/1"
                  QUIC streams: first-frame StreamOpen discriminator
┌────────────────────────────────▼────────────────────────────────────┐
│ iroh::Endpoint  (quinn + tls 1.3 + n0 relays + DNS discovery)       │
│   direct path or hole-punched UDP or relay-as-fallback              │
└─────────────────────────────────────────────────────────────────────┘
```

### Layer responsibilities

| Layer | Crate | Responsibility |
|---|---|---|
| Wire types | `wt-proto` | CBOR-serializable `StreamOpen`, `MessageFrame`, `Ack`, `IpcRequest`, `IpcEvent`, `CapabilityToken`, `AddrTicket`. **No I/O.** |
| Core | `wt-core` | `Identity` (Ed25519 key on disk), `Store` (SQLite), `auth` (sign/verify), `transport` (iroh wrapper), `services::msg` (stream loops), `framing` (length-prefixed CBOR), `paths` (XDG/`WT_HOME`). |
| Daemon | `wt-daemon` | Long-running process: state, accept loop, IPC server, delivery worker, mDNS. Exposes `run()` library entry. |
| CLI | `wt-cli` | clap subcommands. Thin IPC client over UDS. Single `wt` binary (incl. `wt daemon`). |

**Why this split**: `wt-proto` has zero runtime deps so it could power other clients/SDKs. `wt-core` is the reusable library; `wt-daemon` and `wt-cli` are concrete users.

---

## 3. Identity & addressing

### One identity per install

```rust
~/.wt/keys/id_ed25519       // 32 raw bytes, mode 0600
~/.wt/keys/id_ed25519.pub   // hex(public), human-readable
```

The same 32-byte secret seed is used for:
1. **iroh transport mTLS**: `Endpoint::builder(presets::N0).secret_key(sk)`. iroh derives an X25519 key for QUIC handshake and uses the Ed25519 pubkey as the connection identity.
2. **Token signing**: `auth::sign_token(&secret, sub, caps, ttl)` produces an Ed25519 signature over CBOR claims.

**Important**: there is exactly one identity per install. `NodeId == iroh::PublicKey == token issuer == token signer`. No separate "transport identity" vs "auth identity" — that distinction caused confusion in an early design and was deliberately collapsed.

### NodeId, ticket, and discovery

A peer is reachable two ways:

**By `NodeId` alone** — relies on iroh's DNS-based discovery (`presets::N0` includes `iroh-dns`). Works but the peer must have published its addressing info to a discovery service, which can take time after a fresh bind.

**By `AddrTicket`** — a self-describing blob containing the NodeId plus current relay URL plus direct socket addresses. Format:

```
wt1:<base32(cbor({nodeid, relay_url?, direct_addrs: [SocketAddr]}))>
```

Generated by `wt ticket` once the daemon's iroh Endpoint has finished its initial relay handshake (`endpoint.wait_online(5s)` with timeout). Pasted into `wt peer add <ticket> --name X` on the other side. The ticket's hints get stored in `peers.addr_blob` and used by `transport.connect_ticket(...)` for direct dialing without DNS lookup.

```rust
struct AddrTicket {
    nodeid: NodeId,
    relay_url: Option<String>,
    direct_addrs: Vec<SocketAddr>,
}
```

Tickets are the recommended pairing path. NodeId-only works once DNS publication has propagated (seconds to minutes).

---

## 4. Authentication & authorization

### Trust model

```
┌─────────────────────────────────────┐
│  Alice's install                    │
│  ─────────────                      │
│  ed25519 private key (the only      │
│  thing that proves "I am Alice")    │
│                                     │
│  peers table:                       │
│   bob = b32:abc... (NodeId)         │
│                                     │
│  tokens issued TO me:               │
│   from bob, expires 24h,            │
│   caps=[msg]                        │
└──────────────┬──────────────────────┘
               │
               │ when Alice opens a stream
               │ to Bob, she presents bob's
               │ token to bob
               ▼
┌─────────────────────────────────────┐
│  Bob's install                      │
│  ───────────                        │
│  peers table:                       │
│   alice = b32:def...                │
│                                     │
│  Bob verifies: iss == my own key,   │
│  sub == alice's NodeId (matches     │
│  iroh peer id), not expired, not    │
│  revoked, caps includes msg.        │
└─────────────────────────────────────┘
```

Two layers gate every operation:

1. **Transport mutual TLS** (iroh): both ends prove possession of the private key behind their advertised NodeId. Spoofing a NodeId requires breaking Ed25519.
2. **Capability token**: presented as the first frame on every new stream (`StreamOpen::Msg { token, channel }`). The receiver verifies the token was issued by *itself* to *the connecting peer*.

### Token model

```rust
// in wt-proto/src/token.rs
struct CapabilityToken {
    iss: NodeId,        // issuer (must equal verifier's own NodeId)
    sub: NodeId,        // subject (must equal initiator's NodeId)
    exp: u64,           // unix seconds; "unlimited" encoded as year 2100
    caps: Vec<Cap>,     // enum: { Msg, ... }
    id: [u8; 16],       // unique token id — revocation key, NOT a one-time nonce
}

struct SignedToken {
    claims_cbor: Vec<u8>,
    sig: Vec<u8>,       // 64-byte Ed25519 signature over claims_cbor
}
```

**Two distinct token types** are reserved for, even though only one is implemented now:
- `CapabilityToken` (implemented) — reusable, scoped authorization. The `id` is the revocation key; the token is *not* consumed on use. Valid until `exp` or explicit `wt token revoke <id>`.
- `InviteToken` (deferred to v0.5) — one-time pairing bootstrap. Carries reciprocal NodeIds + connection hints; the nonce is invalidated on first redemption.

### Verification

```rust
verify_token(signed, local_nodeid, initiator, required_cap, store)
    -> Result<CapabilityToken, AuthError>
```

Rules, in order:
1. CBOR-decode claims (else `BadClaims`).
2. `claims.iss == local_nodeid` (else `NotForUs` — we only accept tokens we issued).
3. Signature verifies against `claims.iss` pubkey (else `BadSignature`).
4. `claims.sub == initiator` where `initiator` is the iroh-verified peer id of the open connection (else `SubjectMismatch`).
5. `now - 5min ≤ claims.exp` (else `Expired`). ±5min skew tolerance.
6. `claims.id` not in `tokens` table with `revoked = 1` (else `Revoked`).
7. `required_cap ∈ claims.caps` (else `MissingCap`).

### Reciprocal authorization

Tokens authorize **the holder** to act on **the issuer**. For Alice ↔ Bob bidirectional messaging:

```
Bob issues T_AB (sub=Alice, caps=[msg])
    └─> grants Alice the right to open Msg streams to Bob
Alice issues T_BA (sub=Bob, caps=[msg])
    └─> grants Bob the right to open Msg streams to Alice
Each side `wt token import`s the inbound token.
```

Either party can revoke their side independently. v0.5's invite tokens will collapse this into a one-shot pairing ceremony.

### Future: biscuit caveats

v0.3's `wt exec` introduces the need for expressive predicates ("alice can exec but only `git *`"). Plan: replace the flat `Cap` enum with [`biscuit-auth`](https://www.biscuitsec.org/) tokens carrying caveats like `argv[0] == "git"` or `peer == "ci-runner-3"`. The current `Cap::Msg` becomes a preset that emits a biscuit.

---

## 5. Wire protocol

### Single ALPN, per-stream service

The iroh Endpoint negotiates a single ALPN: `"wt/1"`. ALPN is connection-level, so we don't burn a new ALPN for every service variant. Instead, the **first frame on every bidi stream is a `StreamOpen`** that names the logical service:

```rust
enum StreamOpen {
    Msg  { token: SignedToken, channel: String },
    // v0.3+: Exec { token, argv: Vec<String>, env }
    // v0.4+: Pty  { token, term, rows, cols }
    // later:  File { token, op }
}
```

This trades one CBOR frame per stream open for the freedom to add services without ALPN proliferation.

### Frame format

Every frame on a `wt/1` stream uses length-prefixed CBOR:

```
[ u32 BE length ][ <length> bytes of CBOR-encoded payload ]
```

`MAX_FRAME_BYTES = 16 MiB`. Encoded by `wt_core::framing::{read_cbor_frame, write_cbor_frame}` using iroh's `SendStream::write_all` / `RecvStream::read_exact` (which implement what they look like — pure `&mut self` byte sinks/sources, no tokio AsyncRead/Write traits needed).

### MessageFrame

After `StreamOpen::Msg`, each subsequent frame on the stream is:

```rust
struct MessageFrame {
    seq: u64,        // sender-stamped, monotonic per (sender, channel)
    ts_ms: u64,      // sender's enqueue time (unix ms)
    payload: Vec<u8>, // opaque; conventionally UTF-8 JSON {"user": "..."}
}
```

`Ack { seq }` is reserved for v0.2.1's strict at-least-once but not used by v0.2 — see §6.

### Stream direction (v0.2)

Each Msg stream is **one-way**: the initiator writes `MessageFrame`s, the receiver reads. To message both ways, both peers open their own outbound stream (which is natural under the reciprocal-token model). Splitting send/recv onto separate physical streams keeps the per-stream state minimal and avoids the deadlock hazard of trying to multiplex Acks on the reverse direction of an inbound stream we don't own the send half of.

### Encoding choice

CBOR (`ciborium`) everywhere — IPC, tokens, control frames, message frames. Forward-compatible by design (unknown fields ignored). One decoder, one mental model. The `payload` inside a `MessageFrame` is opaque bytes that `wt` does not parse; agents conventionally send UTF-8 JSON but anything works.

We deliberately chose **not** to add `postcard` for "10% smaller wire frames". One encoding is simpler than two, and we have no measurements showing that 10% matters at agent-message scales.

---

## 6. Persistence: outbox + inbox + delivery worker

### Single DB

```
~/.wt/data/state.db    (SQLite, WAL, synchronous=NORMAL)
```

One database, one schema. Tables created idempotently on daemon start.

```sql
CREATE TABLE meta    (key TEXT PRIMARY KEY, value BLOB);
CREATE TABLE peers   (nodeid BLOB PRIMARY KEY, name TEXT UNIQUE,
                      added_at_ms INTEGER, last_seen_ms INTEGER,
                      addr_blob BLOB);          -- CBOR(AddrTicket)
CREATE TABLE tokens  (id BLOB PRIMARY KEY, iss BLOB, sub BLOB, exp INTEGER,
                      caps TEXT, raw BLOB, revoked INTEGER);
CREATE TABLE messages (                          -- v2 schema; combined out+in log
    id_sender       BLOB NOT NULL,               -- 32-byte pubkey
    id_channel      BLOB NOT NULL,               -- 16-byte blake3-128 of channel name
    id_seq          INTEGER NOT NULL,            -- monotonic per (sender, channel)
    direction       INTEGER NOT NULL,            -- 0=out, 1=in
    channel         TEXT NOT NULL,
    peer_nodeid     BLOB NOT NULL,               -- counterparty
    payload         BLOB NOT NULL,
    enqueued_at_ms  INTEGER NOT NULL,
    delivered_at_ms INTEGER,                     -- NULL until on the wire
    PRIMARY KEY (id_sender, id_channel, id_seq)
) WITHOUT ROWID;
CREATE INDEX messages_outbox_pending
    ON messages(enqueued_at_ms)
    WHERE direction = 0 AND delivered_at_ms IS NULL;
```

The composite PK `(sender, channel_id, seq)` serves dual purpose: **receiver-side dedup** AND **sender-side monotonic sequence**. `channel_id` is `blake3(channel_name)[..16]` — stable across versions, ~128-bit collision-resistant.

### Send path: `wt send` → outbox → delivery worker → wire

```
┌─────────┐  IPC: Send  ┌───────────────┐
│ wt send │────────────▶│  IPC handler  │
└─────────┘             │               │
                        │ store.outbox_ │
                        │  enqueue()    │
                        │   - assigns   │
                        │     seq       │
                        │   - row.del   │
                        │     ivered_at │
                        │     = NULL    │
                        └───────┬───────┘
                                │ notify_one()
                                ▼
                        ┌───────────────┐
                        │ Notify        │
                        │ (1-bit)       │
                        └───────┬───────┘
                                │ wakes
                                ▼
                        ┌───────────────┐  ┌──────────────────────┐
                        │ delivery_     │  │ HashMap<(peer,chan), │
                        │ worker        │◀▶│   SendStream>        │
                        │ - drain       │  │  (owned by worker)   │
                        │ - mark        │  └──────────────────────┘
                        │   delivered   │
                        └───────┬───────┘
                                │ on success: UPDATE delivered_at_ms
                                │ on error: drop stream, retry
                                ▼
                          iroh write_all
```

The delivery worker (`run_delivery_worker` in `wt-daemon/src/state.rs`) is a single tokio task with private ownership of open `SendStream`s. Pseudocode:

```rust
let mut streams: HashMap<(NodeId, String), SendStream> = HashMap::new();
loop {
    while let Some(row) = store.outbox_next_pending()? {
        match deliver_one(state, &mut streams, &row).await {
            Ok(_) => { /* row.delivered_at_ms set inside deliver_one */ }
            Err(e) => {
                streams.remove(&(row.peer_nodeid, row.channel.clone()));
                tokio::time::sleep(250ms).await;
                // outer loop will retry — row is still NULL in DB
            }
        }
    }
    select! {
        _ = delivery_notify.notified() => {}
        _ = tokio::time::sleep(3s) => {}    // wake-up safety net
    }
}
```

### Receive path: wire → inbox → broadcast → `wt recv`

```
peer's send  ────▶ accept_bi() ──▶ StreamOpen::Msg verify_token()
                                          │
                                          ▼
                                  run_recv_loop:
                                  read MessageFrame
                                          │
                                          ▼
                                  inbox_record(sender, channel,
                                               seq, payload, ts_ms)
                                  ┌──────────────────────┐
                                  │ INSERT OR IGNORE     │
                                  │ ─ returns true if    │
                                  │   newly inserted     │
                                  │ ─ returns false if   │
                                  │   PK conflict (dup)  │
                                  └──────────┬───────────┘
                                             │ true
                                             ▼
                                  broadcast::send(RecvMsg{..})
                                             │
                                             ▼
                                  IPC subscribers (wt recv --follow)
```

Dedup is automatic via the composite PK. Duplicates from sender retries are silently dropped; only the first observation is emitted.

### `wt recv` backlog drain + live tail

```rust
IpcRequest::RecvSubscribe { peer, channel, since_ms, follow }
```

The daemon:
1. Subscribes to the live broadcast **first** (so we don't miss the seam).
2. Queries `inbox_backlog(peer, channel, since_ms, limit=10_000)` and emits each row as a `RecvMsg`.
3. Emits `RecvBacklogEnd` sentinel.
4. If `follow == false`, returns. If `true`, continues forwarding broadcast events.

The CLI maps:
- `wt recv` → `follow=false` (drain + exit)
- `wt recv --follow` → `follow=true` (drain + tail forever)
- `wt recv --since 5m` → `since_ms = now - 5min`
- `wt chat <peer>` → `since_ms = now` (skip backlog, live only)

### Delivery semantics

**v0.2's contract**: at-least-once *given that iroh successfully wrote the bytes to its QUIC send-stream*. Concretely:

- A message is durable from the moment `outbox_enqueue` returns (synchronous SQLite write).
- The delivery worker retries failed writes indefinitely against the same row (`delivered_at_ms IS NULL` is the retry predicate).
- On daemon restart: the worker picks up where it left off — any row with `delivered_at_ms IS NULL` is retried.
- The receiver dedups by `(sender, channel_id, seq)`. Duplicates are silent.

**What this does *not* guarantee**:
- True end-to-end ack. iroh's `write_all` returns when bytes are buffered for transmission, not when the peer's application has read them. If the connection dies between buffering and the peer's actual read, the bytes are likely lost but we marked the row delivered. v0.2.1's planned ack-on-reverse-stream upgrade closes this gap.
- Ordering across senders. Per-`(sender, channel)` FIFO is preserved; no cross-sender total order is offered or pursued.

### Backpressure

`SendStream::write_all` blocks when the peer's QUIC receive window is full. That propagates back into `deliver_one`, which blocks the worker, which blocks the outbox drain. New `wt send` calls keep enqueuing to SQLite (cheap) but the queue grows. There's currently no upper bound; for v0.2 this is acceptable because we'd rather accumulate than drop, and `wt send` is interactive so users notice.

A future enhancement is bounded outbox per peer with a high-water mark that returns `EAGAIN` to the IPC client.

---

## 7. Discovery: mDNS

Local-LAN discovery uses [`mdns-sd`](https://docs.rs/mdns-sd) 0.19. Service type: `_wt._udp.local.`. Each daemon both **announces** and **browses**.

```
Announce
  instance:    wt-<first 16 hex chars of nodeid>
  port:        iroh's bound UDP port (Endpoint::bound_sockets()[0])
  TXT records: nodeid=<full 64-hex>, v=0.2
  addresses:   auto-detected via enable_addr_auto()

Browse
  filters: skip own NodeId
  sink:    Arc<Mutex<HashMap<NodeId, LanPeer>>>
           where LanPeer = { nodeid, addresses, port, instance, last_seen_ms }
```

The shared map is read by the IPC `PeerList` handler. When the filter is `Local` (or `All`), mDNS-discovered peers not yet in the durable `peers` table are surfaced. The user explicitly runs `wt peer add` to promote a discovered peer into a paired one — discovery never auto-pairs. This separation is deliberate: mDNS announcements are unauthenticated and could be spoofed; pairing remains an explicit user action.

`wt ls --local` shows the LAN peers. `wt ls --remote` shows manually-added peers. `wt ls` shows both.

### Future remote discovery

v0.2 only does LAN discovery. Cross-internet, peers exchange tickets out of band. v0.5+ may add an optional registry server or DHT-based discovery (libp2p kademlia), but the threat model gets thornier off-LAN — a public registry needs rate limits, anti-enumeration guards, and a story for "I want to be discoverable by Alice but not the world."

---

## 8. Daemon process model

```
wt daemon
└─ tokio::main runtime
   ├─ DaemonState::start()
   │   ├─ load_or_create identity (~/.wt/keys/id_ed25519)
   │   ├─ Store::open() — creates tables, sets PRAGMA WAL
   │   ├─ Transport::bind() — iroh Endpoint with ALPN wt/1
   │   ├─ wait_online(5s) — best-effort relay handshake
   │   └─ Mdns::start() — best-effort, swallows errors
   ├─ tokio::spawn(ipc::run_ipc_server)
   ├─ tokio::spawn(state.run_accept_loop)   — iroh accept + dispatch
   ├─ tokio::spawn(state::run_delivery_worker)
   └─ select! {
        SIGINT  => shutdown
        SIGTERM => shutdown
      }
      └─ state.shutdown(): mdns.shutdown(); transport.close();
                            unlink socket + pidfile
```

### Lifecycle hygiene

- **Stale socket cleanup** on startup: if `daemon.pid` exists and `kill(pid, 0)` shows the process gone, unlink `daemon.sock` and `daemon.pid`. Refuse to start if another live daemon is detected.
- **Graceful shutdown** on SIGINT/SIGTERM: drop the iroh Endpoint (closes all streams cleanly), unlink socket + pidfile. SQLite's WAL checkpoint happens automatically via WAL's normal mode.
- **Test harness**: `wt_daemon::start_for_test() -> TestHandle` returns a struct that owns the daemon's tasks; dropping it aborts everything. Used by `crates/wt-cli/tests/e2e_two_daemons.rs`.

### IPC

```
┌─────────┐  AF_UNIX SOCK_STREAM  ┌──────────────────┐
│ wt CLI  │◀─────────────────────▶│ wt daemon (IPC)  │
└─────────┘   ~/.wt/run/daemon.   └──────────────────┘
              sock (mode 0600)
```

Wire format: `u32 BE length` + CBOR-encoded `IpcRequest` / `IpcEvent`. Each CLI invocation opens a fresh socket; for streaming (`wt recv --follow`, `wt chat`), the daemon writes events as long as the socket stays open.

```rust
enum IpcRequest {
    Status,
    NodeId,
    Ticket,
    PeerAdd { nodeid, name, addr_blob },
    PeerRm { selector },
    PeerList { filter },
    ConnList,
    ConnClose { selector },
    TokenGrant { peer, caps, ttl_secs },
    TokenImport { raw },
    TokenList,
    TokenRevoke { id },
    Send { peer, channel, payload },
    RecvSubscribe { peer, channel, since_ms, follow },
}

enum IpcEvent {
    Ok,
    Err(String),
    StatusInfo { nodeid, version, endpoint_bound },
    NodeIdValue(NodeId),
    TicketValue(String),
    PeerListItem(PeerInfo), PeerListEnd,
    ConnListItem(ConnInfo), ConnListEnd,
    TokenIssued { raw, info }, TokenListItem(TokenInfo), TokenListEnd,
    RecvMsg { from, from_name, channel, payload, ts_ms },
    RecvBacklogEnd,
}
```

---

## 9. CLI surface

The CLI is a thin IPC client. `wt daemon` is the special case that launches the long-running process in-band rather than talking to one.

| Command | Purpose |
|---|---|
| `wt init` | Generate `~/.wt/keys/id_ed25519`, init `state.db`. |
| `wt daemon` | Run the long-lived daemon (foreground; `RUST_LOG` honored). |
| `wt nodeid` | Print the 32-byte pubkey (hex). |
| `wt ticket` | Print the full `wt1:` ticket (NodeId + relay + direct addrs). |
| `wt status` | Daemon health + identity. |
| `wt peer add <nodeid\|wt1:…> --name X` | Add a peer. Auto-detects ticket vs raw NodeId. |
| `wt peer rm <name\|nodeid>` | Remove peer. |
| `wt ls [--remote\|--local\|--connected]` | List known + discovered peers. |
| `wt conn` | Live connections (peer, NodeId, streams, direct/relay). |
| `wt token grant <peer> --cap msg [--ttl 24h]` | Issue a signed capability token. |
| `wt token import <base32>` | Import an inbound token. |
| `wt token list` / `wt token revoke <id>` | Manage tokens. |
| `wt send <peer> [-c chan] [msg\|stdin]` | Enqueue a message. |
| `wt recv [--from p] [-c chan] [-f] [--since 5m]` | Drain inbox (+ tail with `-f`). |
| `wt chat <peer>` | Interactive stdin↔recv split. |
| `wt exec <peer> -- <argv...>` | (v0.3+) Remote command. v0.1 stub. |
| `wt shell <peer>` | (v0.4+) Interactive PTY. v0.1 stub. |
| `wt @<peer> <argv...>` | (v0.3+) Shorthand for `wt exec`. |
| **Orchestration (v0.3, §16)** | |
| `wt group new <name>` | Create a named group; prints the prime agent's token to stdout. |
| `wt group ls` | List groups. |
| `wt spawn --session <name> --dir <base> [--worktree\|--new] [--prompt …] [--idle-timeout D] [--plan\|--permission-mode M] [--skip-permissions] [--trace]` | Provision a session workspace and launch + supervise a child harness in it, in the chosen permission posture. |
| `wt ls --group <name>` | List the sessions in a group. |
| `wt agent ls [--group <name>] [--session <name>]` | List agents. |
| `wt agent kill <name>` | Stop a supervised child (prime only). |
| `wt session close <name> [--discard]` | Close a session + tear down its workspace. |
| `wt send --session <name> [--kind …] [msg]` | Send onto the agent bus (needs `WT_TOKEN`). |
| `wt recv [--session <name>] [-f] [--all]` | Drain the agent bus for this agent (needs `WT_TOKEN`). Default consumes new messages; `--all` replays history. |
| `wt whoami` | Print the agent identity bound to `WT_TOKEN`. |

### Conventions

- **Tickets** are paste-friendly: `wt1:` prefix + base32 no-padding (uppercase A-Z + 2-7).
- **Tokens** are paste-friendly: bare base32 no-padding (no prefix). They embed the issuer, so the receiver knows where they came from.
- **Stdin/stdout/stderr discipline**: `wt token grant` writes the parsed-info line to **stderr** and the base32 token to **stdout**. Scripts capturing tokens via `$(...)` get just the token. (Caveat: Daytona's `/process/execute` API merges streams, so when scripting through that, suppress stderr explicitly.)
- **Exit codes**: 0 = success, non-zero on any `Err` event from the daemon.

---

## 10. Workspace layout

```
wt/
├── Cargo.toml                       workspace root, shared deps
├── crates/
│   ├── wt-proto/                    wire & IPC types only (no I/O)
│   │   └── src/
│   │       ├── lib.rs               NodeId, exports
│   │       ├── ipc.rs               IpcRequest, IpcEvent, ConnInfo, PeerInfo, ...
│   │       ├── token.rs             CapabilityToken, SignedToken, Cap
│   │       ├── ticket.rs            AddrTicket, wt1: encoding
│   │       └── wire.rs              StreamOpen, MessageFrame, Ack
│   ├── wt-core/                     library: store + auth + transport + services
│   │   └── src/
│   │       ├── lib.rs               module exports, test_support
│   │       ├── identity.rs          load_or_create, secret key on disk
│   │       ├── paths.rs             ~/.wt resolution, WT_HOME override
│   │       ├── store.rs             rusqlite, migrations, outbox/inbox queries
│   │       ├── auth.rs              sign_token, verify_token, base32 helpers
│   │       ├── transport.rs         iroh Endpoint, ALPN, ticket-based dial
│   │       ├── framing.rs           u32-BE length-prefixed CBOR over QUIC streams
│   │       └── services/
│   │           └── msg.rs           run_recv_loop, run_send_loop, StreamOpen I/O
│   ├── wt-daemon/                   library: daemon process
│   │   └── src/
│   │       ├── lib.rs               run(), start_for_test(), TestHandle
│   │       ├── state.rs             DaemonState, accept loop, delivery worker
│   │       ├── ipc.rs               UDS server, dispatch
│   │       └── mdns.rs              announce + browse
│   └── wt-cli/                      binary: the `wt` command
│       ├── src/main.rs              clap, IPC client, command handlers
│       └── tests/
│           └── e2e_two_daemons.rs   subprocess-driven local e2e
└── scripts/
    └── smoke_local.sh               shell-based local two-daemon smoke
```

Compile output: a **single binary**, `target/release/wt`. `wt daemon` runs the daemon in-band; everything else is a thin client.

### Major dependencies

| Crate | Use |
|---|---|
| `tokio` | async runtime |
| `iroh` 0.98 | QUIC transport, NAT traversal, NodeId identity, n0 relays |
| `rusqlite` (bundled) | embedded SQLite, WAL |
| `ciborium` | CBOR encode/decode (wire + IPC + tokens) |
| `clap` v4 | CLI |
| `mdns-sd` 0.19 | LAN service discovery |
| `blake3` | channel_id hash |
| `tracing` + `tracing-subscriber` | structured logging |
| `serde` + `serde_json` | serialization; JSON only for CLI display |
| `thiserror` / `anyhow` | error types |
| `directories` | XDG home resolution |

iroh transitively provides rustls + aws-lc-rs + DNS-based discovery. We do not have a direct dep on `ring`, `quinn`, or `ed25519-dalek` — they're all reached via iroh.

---

## 11. Runtime artifacts

```
~/.wt/                       (or $WT_HOME if set)
├── config.toml              (reserved for future use)
├── keys/
│   ├── id_ed25519           32 raw bytes, mode 0600
│   └── id_ed25519.pub       hex(pubkey) + newline
├── data/
│   └── state.db             SQLite (peers, tokens, messages, meta;
│                             + v3: groups, agents, sessions, agent_messages)
├── run/
│   ├── daemon.sock          AF_UNIX, mode 0600
│   └── daemon.pid           ASCII pid + newline
├── sessions/                per-session workspaces (v0.3)
│   └── <group>/<session>/   the child harness's cwd (git worktree or fresh folder)
└── logs/
    └── daemon.log           (when run as `wt daemon > logs/daemon.log`)
```

Setting `WT_HOME=/tmp/foo` makes the entire tree live under `/tmp/foo`. This is how the e2e harness and `smoke_local.sh` run two daemons on the same host.

---

## 12. Building & running

```bash
# build
cargo build --release --workspace

# init + run daemon on machine A
wt init
wt daemon &

# init + run daemon on machine B (separate machine OR set WT_HOME)
WT_HOME=/tmp/wt-b wt init
WT_HOME=/tmp/wt-b wt daemon &

# pair (paste tickets cross-OOB)
A_TICKET=$(wt ticket)
B_TICKET=$(WT_HOME=/tmp/wt-b wt ticket)
wt peer add "$B_TICKET" --name bob
WT_HOME=/tmp/wt-b wt peer add "$A_TICKET" --name alice

# reciprocal capability grants
T_BA=$(WT_HOME=/tmp/wt-b wt token grant alice --cap msg --ttl 24h)
T_AB=$(wt token grant bob --cap msg --ttl 24h)
wt token import "$T_BA"
WT_HOME=/tmp/wt-b wt token import "$T_AB"

# talk
WT_HOME=/tmp/wt-b wt recv --follow &
wt send bob '{"user":"hello"}'
```

### Single-machine helper

`scripts/smoke_local.sh` runs the entire pair-and-exchange flow with two daemons in separate `WT_HOME` dirs. Exits non-zero on any failure. Useful as a release gate.

---

## 13. Verification

Three layers, all currently green:

### Unit tests (~25)
```bash
cargo test --workspace
```

Covers:
- `wt-proto`: NodeId parse/display roundtrips, ticket encode/decode + bad inputs, Cap parse-strictness, CBOR wire-type roundtrips, token-claims binary-ID preservation.
- `wt-core/identity`: load_or_create idempotency, pubkey file format, 0600 permissions on the secret key, rejection of truncated key files.
- `wt-core/store`: schema_version pinning, peer add/update preserves ticket on partial updates, peer name uniqueness, peer remove counts, token insert/revoke/find roundtrip, conflict-update revoked state, corrupt nodeid lengths rejected on read, channel_id stability, outbox seq monotonicity per channel, outbox_next_pending FIFO + delivered transitions, inbox dedup, inbox_backlog filters, outbox_pending_for_peer scoping.
- `wt-core/auth`: sign-verify roundtrip, base32 roundtrip + garbage rejection, sad paths (wrong iss / wrong sub / missing cap / revoked / expired / forgery / malformed payload).
- `wt-cli`: `parse_duration` (`5s`/`5m`/`5h`/`5d` + rejects empty/unknown/overflow), peer_selector NodeId-vs-name resolution.

### Subprocess-driven integration (Rust)
```bash
cargo test --workspace --test e2e_two_daemons
```

`crates/wt-cli/tests/e2e_two_daemons.rs` spawns two `wt` binaries with separate `WT_HOME`s, exchanges tickets + tokens, sends in both directions, asserts on the recv output. Two tests:
- `two_daemons_exchange_reciprocal_messages` — basic live exchange.
- `recv_replays_persisted_backlog` — send while no subscriber is listening, then `wt recv` (no `--follow`) drains the backlog from disk.

### Cross-internet (manual; reproducible)
A real two-machine test was run against a [Daytona](https://www.daytona.io/) cloud sandbox:

- Local: laptop on a residential network
- Remote: Daytona sandbox in `us` region (4 vCPU / 8 GB)
- Build time on sandbox: 2m 14s (rustup + `cargo build --release`)
- Result: bidirectional `{"user": "..."}` exchanged, `wt conn` showed `VIA: direct` (iroh hole-punched a direct path between residential NAT and the sandbox's container — no relay needed)

This validates: NAT traversal, ticket-based pairing across regions, cross-arch interop (macOS aarch64 ↔ Linux x86_64), and the v0.2 outbox→wire→inbox→drain path under realistic conditions.

### CI gates

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

All four pass on every commit.

---

## 14. Status & roadmap

| Version | Status | Highlights |
|---|---|---|
| **v0.1** | shipped | Single `wt` binary, daemon over Unix socket, iroh transport with ticket-based dial, reciprocal capability tokens, live `{user:...}` messaging. |
| **v0.2** | shipped | Single `state.db` with `messages` table, outbox + delivery worker (resumes after restart), receiver-side dedup, `wt recv` drains backlog + `--follow` tails, mDNS LAN announce/browse, `wt ls --local`. |
| **v0.3 — agents** | shipped | Local orchestration layer (§16): named groups/sessions/agents, in-daemon message bus, `wt spawn` + per-child harness supervisor (Claude Code stream-json), per-session git-worktree / new-folder workspaces. |
| v0.3 — exec | planned | `wt exec <peer> -- <argv>` (non-PTY remote command). New `StreamOpen::Exec`. Structured-argv biscuit caveats. |
| v0.4 | planned | `wt shell <peer>` interactive PTY via `portable-pty`, window resize, `pty:allocate` cap. |
| v0.5 | planned | `InviteToken` flow → `wt pair invite` / `wt pair accept`. Collapses ticket+reciprocal-token into one ceremony. QR rendering for in-person pairing. |
| v0.6+ | future | Key rotation via cross-cert, ack-on-reverse-stream for true end-to-end at-least-once, self-hosted relays, file transfer, perf pass (criterion benches), optional per-channel `redb` log. |

### Explicitly out of scope (probably forever)
- GUI / TUI client.
- Multi-tenant daemon (one identity per `WT_HOME`).
- Cross-sender total ordering or distributed consensus.
- Byzantine fault tolerance.
- Built-in encryption at rest for `state.db` (rely on filesystem-level encryption).

---

## 15. Design decisions log

A few non-obvious choices, recorded for future-you:

- **One identity per install, used for both transport mTLS and token signing.** Earlier drafts kept these separate; collapsing them eliminated a class of "transport peer vs token peer" bugs and one whole category of misconfiguration. Cost: rotating identity requires the cross-cert mechanism (v0.6+) since it's load-bearing in two places.
- **`iroh` over raw `quinn`.** Chose to depend on iroh's relay + discovery instead of reimplementing them. Cost: an opinionated transport with its own release cadence; if iroh disappears we have a non-trivial migration. Mitigation: the surface area we use is small (Endpoint, SecretKey, connect/accept, EndpointAddr) and could be re-targeted at quinn directly if needed.
- **Single `wt/1` ALPN with per-stream service discriminator.** Avoids ALPN proliferation as we add services. Cost: one CBOR frame of overhead per stream open. Worth it.
- **Each Msg stream is one-way in v0.2.** Reciprocal tokens make this natural — both peers open their own outbound stream. Cost: 2× streams for bidirectional flow. Benefit: no awkward Ack-on-reverse-direction multiplexing, no deadlock hazards. v0.2.1 may revisit this once acks are wired.
- **At-least-once "given iroh wrote the bytes" rather than "until peer acks".** v0.2 marks `delivered` on `write_all` success, not on application read. Pragmatic shortcut: closes the daemon-restart hole (the main motivation for persistence) without needing a reverse-stream ack protocol. Strict end-to-end ack is v0.2.1.
- **CBOR everywhere.** One encoding, one decoder. Rejected `postcard` (10–20% smaller wire frames) — saving 50 bytes per agent message isn't measurable in this workload.
- **mDNS announces but never auto-pairs.** Discovery is information; trust is an explicit user action. mDNS announcements are unauthenticated and could be spoofed.
- **`channel_id = blake3(name)[:16]` rather than `name` directly in the PK.** Stable across versions, ~128-bit collision-resistant, avoids variable-length keys in the index. Cost: channel ids are not human-readable in the DB.
- **Build inside the sandbox in the Daytona e2e.** Cross-compiling macOS aarch64 → Linux x86_64 is brittle (sysroot, libc, linker). Building on the target arch is ~3 minutes on a 4 vCPU sandbox and avoids the entire cross-compilation surface area.
- **Two token types, two trust domains (v0.3).** The orchestration layer authenticates *local* agents with a random bearer token over the 0600 Unix socket — deliberately **not** the Ed25519 `CapabilityToken` used for cross-peer authorization. Local agents are all the same human's processes under one daemon; the signed-capability machinery bought nothing there. This keeps "one identity per install" for transport while adding a local multi-agent namespace on a different axis (§16). Names identify; the token authenticates.
- **Daemon owns spawned harnesses (v0.3).** The supervisor runs *inside* the daemon and owns each child's stdio + lifecycle (kill_on_drop), rather than a separate process per child. Simpler and gives one place for `wt agent ls`/`kill` and shutdown reaping; the cost is that children die on daemon restart (acceptable — they are ephemeral workers). The supervisor is self-contained so it can move to a per-child process later if crash isolation is needed.

---

## 16. Local orchestration layer (groups · sessions · agents)

v0.3 adds a **local, daemon-mediated layer** for running and coordinating multiple agent harnesses
on one machine: a "prime" agent spawns child Claude Codes, supervises them, and exchanges messages
with them. It sits **beside** the peer transport (§2–§8) — reusing the daemon, `state.db`, and the
Unix-socket IPC — but its routing is **entirely local: no iroh, no peer tokens**. (Cross-machine
groups are future work; today a group lives under one daemon.)

### Model

```
group "myapp"  ── communication boundary (every agent in it can talk)
 ├ prime                              the controller (a wt CLI client)
 ├ session "frontend" → child harness (claude in its own workspace)
 └ session "backend"  → child harness
```

- **group** — a named swarm; the communication boundary. The prime creates one when a task benefits
  from several cooperating harnesses (e.g. a front-end and a back-end built separately).
- **session** — one **prime↔child** channel, named (`frontend`). Each spawned child is one session;
  the child agent's name equals the session name.
- **agent** — a participant (`prime`, or a child), identified by a human **name** within its group.
  Everything is addressed by name; ids are never random.
- **wt token** — a per-agent secret (32 random bytes, base32) that *authenticates* an agent over the
  0600 IPC socket. The daemon stores only `blake3(token)` and resolves it to an agent. Distinct from
  the Ed25519 `CapabilityToken` of §4 (peer→peer authorization): names identify, the token binds.

### Storage (schema v3)

Four name-keyed tables in the same `state.db`: `groups`, `agents`, `sessions`, and `agent_messages`
— the local bus: a durable per-`(group, session)` log with monotonic `seq`, mirroring the peer
inbox/outbox shape but delivered **in-process** (never over iroh).

### Message bus

`wt send` / `wt recv` carry a wt token (and a session). The daemon resolves the sender from the
token, routes within the session to the *other* endpoint, appends to `agent_messages`, and publishes
on an in-process broadcast. `wt recv` drains the durable backlog then (with `-f`) tails live — the
same drain-then-tail shape as peer `recv`. Message kinds: `turn_output` (child→prime), `turn_input`
(prime→child), `user` (free-form), `control` (lifecycle).

### Spawn + supervisor

`wt spawn --session <name> --dir <base> [--worktree|--new] --prompt <task>`, run by the prime:

1. Provisions an isolated **session workspace** (below) and mints a child token.
2. Registers the child agent + session, then launches the harness as a subprocess whose **stdio the
   daemon owns**, driving it over Claude Code stream-json
   (`claude --print --input-format stream-json --output-format stream-json --verbose`; override the
   command with `$WT_HARNESS_CMD`). The child's env carries `WT_TOKEN` / `WT_GROUP` / `WT_SESSION` /
   `WT_AGENT` / `WT_HOME`, so the child can itself run `wt` to message siblings.
3. A per-child **supervisor task** runs the loop: it feeds the initial prompt, and on each completed
   turn (a stream-json `result`) marks the child `awaiting_input` and queues the result to the prime
   as `turn_output` — *"the harness finished responding; ask the prime to respond."* The prime's
   reply (`wt send --kind turn_input`) is fed back as the child's next user turn.

The supervisor owns the `Harness` with `kill_on_drop`, so `wt agent kill <name>`, `wt session close`,
and daemon shutdown abort the task and reap the child. Children do not survive a daemon restart —
they are ephemeral workers in v0.3.

### Session filesystem

Each session runs in `~/.wt/sessions/<group>/<session>` (never the base dir), provisioned as either:

- **`--worktree`** — `git worktree add … -b wt/<group>/<session>` off a base repo: an isolated branch
  + working tree, diffable and mergeable. Default when the base is a git repo.
- **`--new`** — a fresh empty folder, for a brand-new component. Default otherwise.

`wt session close <name>` prunes the worktree but **keeps the branch** for merge-back; `--discard`
also deletes the branch / removes the folder.

### Typical flow

```bash
wt daemon &
export WT_TOKEN=$(wt group new myapp)   # become the prime
export WT_GROUP=myapp
wt spawn --session frontend --dir ~/app --worktree --prompt "scaffold the UI"
wt recv -f &                            # watch children's turn output
wt send --session frontend --kind turn_input "now add a login page"
wt ls --group myapp                     # frontend, backend, …
```

### Reliability (v0.4)

- **Idle-turn timeout (notify-only).** `wt spawn --idle-timeout <dur>` arms a watchdog: if a turn
  produces no harness output for that long, the supervisor sends the prime one `control` message and
  **leaves the child running** (the prime decides whether to nudge it or `wt agent kill` it). Off
  unless set.
- **Recv cursor (consume-on-read).** Default `wt recv` returns only messages you haven't seen and
  marks them delivered — the symmetric counterpart to how a child consumes its turn-inputs.
  `wt recv --all` replays full history; `wt recv --since <dur>` is a time-windowed view; neither
  consumes.
- **Orphan reconcile on restart.** On startup a fresh daemon marks any leftover `running`/`active`
  rows from a previous (crashed) daemon as `exited`/`closed`, so `wt ls` stays accurate. No processes
  are killed — orphaned children self-terminate when the dead daemon's stdio pipes break.
- **Bounded bus.** `agent_messages` is trimmed to the last `MAX_MSGS_PER_SESSION` (5000) rows per
  session, so a never-reading recipient or a chatty child can't grow the DB without bound.

**Why no MCP server?** `wt` already owns each child's stdio and drives its turn loop, and a child
already has `WT_TOKEN`/`WT_GROUP`/`WT_SESSION` in its env (so it can `wt send`/`recv` directly). An
MCP server would only turn those into model-visible tools to enable peer-to-peer, mid-turn
collaboration — a different topology from wt's prime-drives-children star, and intentionally out of
scope.

### Prime orchestration & per-child control (v0.5)

The **prime is a client** (a top-level agent or human holding the group's prime token) that audits
and drives several children concurrently — no extra transport needed:

```bash
# orchestration loop (an agent polls; a human can `wt recv -f` instead)
wt recv --group myapp            # only-new across ALL sessions, each tagged {session,from,kind}
wt send --group myapp --session frontend --kind turn_input "tighten the error states"
wt send --group myapp --session backend  --kind turn_input "add a /health check"
```

Each child runs in its own process, so this is concurrent + multi-turn by construction.
`wt agent kill <session>` is the reliable "interrupt" (re-spawn to restart).

**Per-child posture is chosen at spawn** (Claude sets permission modes only at launch):

```bash
wt spawn --session auditor --dir ~/app --plan                 # read-only/explore (an auditor)
wt spawn --session builder --dir ~/app --skip-permissions     # autonomous edits/commands
wt spawn --session api     --dir ~/app --permission-mode acceptEdits
```

`--plan` is sugar for `--permission-mode plan`; `--permission-mode <m>` passes through to Claude.
(These apply to the built-in Claude harness; a `$WT_HARNESS_CMD` override is launched verbatim.)

**Audit depth** is opt-in: `wt spawn --session x --trace` forwards the child's intermediate assistant
text to the prime as `kind:"trace"` messages (alongside the final `turn_output`), so the prime can
review *how* a child is working, not just its result. Off by default to avoid noise.

The prime only expresses intent (`--plan`, `wt send`, `wt agent kill`); the daemon/supervisor own
all stream-json framing and process control. Mid-session mode change / stdin interrupt are
undocumented in Claude's headless stream-json and intentionally not relied upon — re-spawn to change
a posture.

> **Operating manual:** an agent acting as a prime should follow [`AGENTS.md`](AGENTS.md) — the
> closed-loop discipline (decompose → dispatch with full context → validate in the workspace →
> correct/accept → integrate → escalate), the `WT_STATUS` child report protocol, and the rule that
> driving a child requires `--kind turn_input`.

### Verification

`cargo test -p wt-core` covers the schema, workspace provisioning, and harness parsing;
`crates/wt-daemon/tests/agent_bus.rs` drives the bus over a real socket; `crates/wt-cli/tests/
e2e_spawn.rs` drives the full spawn→turn→reply→next-turn loop through the `wt` binary with a stub
harness (`$WT_HARNESS_CMD`, no real `claude` needed). `scripts/smoke_agents.sh` is the shell gate.

---

## License

MIT OR Apache-2.0 (workspace default).
