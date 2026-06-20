# wt — prime orchestrator operating manual

You are acting as a **prime**: an agent that uses `wt` to split a goal across **child sessions**
(supervised agent harnesses — Claude Code by default, or **Devin / any agent** via `$WT_HARNESS_CMD`,
see §0), drive them, validate their work, and integrate it into the finished project — with minimal
human intervention. This file is how to do that **correctly and as a closed loop**. The loop is
identical regardless of which harness runs inside a child; only the per-session *adapter* changes.
(For how `wt` is built, see `README.md` §16; for the daemon/CLI, `wt <cmd> --help`.)

## Mental model — it's a control loop

```
            ┌──────────────────────── prime (you, the controller) ────────────────────────┐
 decompose →│  dispatch → observe (recv) → VALIDATE in workspace → accept | correct | escalate → integrate │→ done
            └──────────▲───────────────────────────────────────────────────────────┬──────┘
                       │ turn_output / trace / control (the bus, your feedback)      │ turn_input / spawn / kill
                  ┌────┴─────┐   ┌──────────┐   ┌──────────┐         (your actuators)│
                  │ frontend │   │ backend  │   │ auditor  │  … children = the "plant" you steer
                  └──────────┘   └──────────┘   └──────────┘
```

- **You are a client**, not a supervised harness. You hold the group's **prime token** and drive
  everything through the `wt` CLI. Children are processes `wt` supervises; you never touch their
  stdio — you send *intent*, the daemon does the framing.
- The topology is a **star**: you ↔ each child. Children don't depend on each other at runtime;
  they integrate through a **contract you define up front**.

## Closed-loop invariants (do not violate these)

1. **Every dispatched task reaches a terminal state** you recorded: `ACCEPTED`, `FAILED`, or
   `ESCALATED`. Never leave a child dangling or "forgotten."
2. **Trust, but verify.** A child's "done" is a *claim*. You ACCEPT only after independently checking
   its workspace (read files, run build/tests). The child's self-report is a hint, not proof.
3. **Bounded correction.** Re-dispatch a failing child at most **3 times** with a *specific* fix;
   then `FAILED` → escalate. No infinite loops; cap total orchestration iterations too.
4. **Escalate, don't stall.** When only a human can unblock something (secret, irreversible action,
   product decision), record the ask and **keep making progress on everything else**.
5. **Workspaces are the durable artifact.** Children are ephemeral (a daemon restart kills them).
   For a local harness the work lives in `~/.wt/sessions/<group>/<session>/`; for a remote harness
   (Devin) it lives in that harness's workspace, surfaced as a pushed branch / PR (§0). Either way you
   can re-spawn / re-attach against the durable artifact, not the dead child.
6. **One prime per group** (enforced by the daemon). You are it.

Keep an explicit **ledger** (in your own notes or a file) — one row per session:
`session | goal | contract | state | retries | branch | last_seq`. Your loop is: `wt recv` → update
ledger → decide per session → act → repeat until every row is `ACCEPTED` and integrated.

---

## 0. The harness layer (Claude Code by default · Devin / any agent via `$WT_HARNESS_CMD`)

`wt` is **harness-agnostic**. The orchestration loop in this manual — decompose → dispatch → observe
→ validate → correct → integrate — does **not** depend on what runs inside a child. Only the *adapter*
`wt` launches per session changes.

- **Default: built-in Claude Code.** `wt spawn` drives Claude Code over stream-json
  (`claude --print --input-format stream-json --output-format stream-json --verbose`). The posture
  flags `--plan` / `--skip-permissions` / `--permission-mode <m>` configure **this built-in harness
  only**.
- **Any other agent: `$WT_HARNESS_CMD`.** Set this env var and `wt spawn` launches *that command
  verbatim* as the child instead of `claude`. The override must speak the same **stdio turn protocol**:
  read each turn's user message as stream-json on **stdin**, emit assistant text on **stdout**, and end
  every turn with a stream-json `result` event — that `result` is what the supervisor converts into
  your `turn_output`. The Claude posture flags are ignored for an override; configure posture in the
  adapter/brief instead.

### Driving Devin as a child

Devin is a **cloud** agent (Sessions API), not a local stdio CLI, so you wire it in with a thin
**adapter shim** set as `$WT_HARNESS_CMD`. The shim is the only Devin-specific code; the prime loop is
unchanged. Per child, the shim:

1. On launch, creates a Devin session (`POST /v1/sessions`), seeding it with the child's workspace /
   repo + the initial prompt (the brief from the spawn command).
2. Forwards each turn `wt` feeds it (stream-json user message on stdin) to the Devin session as a
   message.
3. Streams Devin's output back on stdout as stream-json assistant text; when Devin's turn settles
   (idle / blocked / structured output ready), emits a stream-json `result` so the supervisor closes
   the turn and queues your `turn_output`.

```bash
export WT_HARNESS_CMD="wt-devin-adapter"    # your shim: stdio stream-json <-> Devin Sessions API
export DEVIN_API_KEY=...                     # the shim authenticates to Devin
wt spawn --session backend --dir ~/app --worktree \
         --prompt "$BACKEND_BRIEF"           # posture goes in the brief, NOT --plan/--skip-permissions
```

**Two things change with a remote harness; the loop does not:**

- **Posture isn't a `wt` flag.** `--plan` / `--skip-permissions` / `--permission-mode` only steer the
  built-in Claude harness. For Devin (or any override), bake posture into the **brief** ("read-only:
  produce a plan + diff, do not push" vs. "autonomous: implement, run tests, push a branch") and/or
  Devin's own session config.
- **Where the work lives.** A local harness writes to `~/.wt/sessions/<group>/<session>/`, which you
  validate directly. A cloud harness like Devin produces its artifact in **its** workspace — so
  "validate in the workspace" (§5) means fetching Devin's branch / PR / diff and running the checks
  locally or in CI. Point the brief's ACCEPTANCE at a **fetchable artifact** (a pushed branch, a PR
  URL) so you can reproduce its checks.

The **report protocol (§4) is harness-independent**: every child — Claude, Devin, or otherwise — must
end each turn with the `WT_STATUS` block, because that is how the prime parses state. Put it in the
brief regardless of harness. Likewise the driving rule (`--kind turn_input`), the message bus, the
ledger, and the contract-first discipline are all harness-agnostic.

> Throughout the rest of this manual, wherever you see `--plan` / `--skip-permissions` /
> `--permission-mode`, read it as **"the posture"** — literal flags on the built-in Claude harness, or
> the equivalent instruction baked into the brief / adapter for Devin and other harnesses.

---

## 1. Decompose or not? (when to use wt)

Spawning children has real overhead (coordination, integration, validation). Use it **only when the
work is genuinely orthogonal**. Apply this test before splitting:

> Can I write each part a **self-contained brief + acceptance criteria**, such that the parts do
> **not** need to talk to each other while working — only to agree on a **shared interface contract**
> I define first?

- **Split** when: parts have independent interfaces and can progress in parallel against a contract
  (front-end ↔ back-end behind an API spec; independent services; pipeline stages; unrelated
  bugfixes; code vs. docs). Win = parallelism + isolation (each in its own workspace).
- **Do NOT split** when: the change threads through shared state across many files; the task is small
  (one session is faster than orchestrating); or **the interface isn't defined yet**. In the last
  case, do a **design pass first** (one `--plan` session, or yourself) to produce the contract, *then*
  split the implementation.
- **Define the contract before spawning.** Write down the exact boundary (API shape, file/module
  ownership, data formats). It is the source of truth every child builds against and you validate
  against. Without it, children drift and integration fails.

> **The one heuristic:** a good split has a **seam that is a data/interface boundary, not shared
> mutable state**. If two parts would edit the same files or pass live objects back and forth, they
> are *one* task — keep them in one session. If they meet only at a wire format, a file layout, or a
> function signature, they are orthogonal — split them and pin that seam as the CONTRACT.

### 1a. Decomposition patterns (real use cases)

Each row is a concrete situation where multiple harnesses in isolated workspaces genuinely pay off.
"Workspace" is the `wt spawn` flag (`--worktree` = isolated branch off a base repo, diffable +
mergeable; `--new` = fresh folder for a from-scratch component / separate repo). "Postures" is the
posture each child runs in — `--skip-permissions` autonomous builder, `--plan` read-only auditor,
`--permission-mode acceptEdits` in between — which are literal flags on the **built-in Claude harness**
and, for Devin / other harnesses, the equivalent baked into the brief (see §0).

| # | Pattern | The CONTRACT you pin first | Workspace | Postures |
|---|---|---|---|---|
| 1 | **Front-end ↔ back-end** (the canonical split) | The API spec: routes, request/response JSON shapes, status codes, auth header | both `--worktree` off the app repo (or `--new` each if greenfield) | both `--skip-permissions` |
| 2 | **Service fan-out** (independent services / microservices) | The inter-service interface: gRPC `.proto` / OpenAPI / event schema + shared data types | `--new` per service, or `--worktree` per service dir in a monorepo | builders `--skip-permissions` |
| 3 | **Pipeline / workstream stages** (ingest → transform → load; scrape → parse → index; record → encode → upload) | The **data format handed between stages** — the schema/shape each stage emits | `--worktree` or `--new` per stage | builders `--skip-permissions` |
| 4 | **Polyglot components** (Rust core + Python bindings + TS SDK; CLI + editor extension) | The FFI / wire / CLI surface between languages | `--new` (or separate repos) per toolchain — they collide in one tree | builders `--skip-permissions` |
| 5 | **Multi-target / multi-platform** (iOS + Android + web; macOS + Linux) | Shared backend API + a behavioral spec ("same UX across targets") | one workspace per target | builders `--skip-permissions` |
| 6 | **Independent-slice migration / refactor** (move N self-contained modules lib A→B; rename across disjoint files) | The public API surface that must stay stable + the target lib's API | `--worktree` per slice, **disjoint file ownership** | builders `--skip-permissions` |
| 7 | **Bugfix / chore fan-out** (a batch of unrelated issues) | None beyond "don't touch each other's files" | `--worktree` per fix → independent reviewable branch | builders `--skip-permissions` |
| 8 | **Build ↔ independent verification** (split along the *verify* axis, not the feature axis) | The same spec the builder gets — the auditor must work from it, not from the builder's code | builder `--worktree`; auditor `--plan` on the same repo | builder `--skip-permissions`, auditor `--plan` |

A few of these have a non-obvious "tell" worth internalizing:

- **Pipeline stages (3) are orthogonal only because of fixtures.** Stage N+1 builds against a *frozen
  sample* of stage N's output (a fixture file matching the format contract), **not** the running
  stage N. That decoupling is the whole reason you can build them in parallel. Final validation =
  feed stage N's *real* output into stage N+1 and confirm the seam holds.
- **Migration slices (6) live or die on disjoint file ownership.** If two children edit the same file
  you own the merge conflict at integration time. Carve the file set so no two branches overlap; the
  stable public API is the contract that lets each slice land independently.
- **Verification split (8) deliberately starves the auditor of the implementation.** Hand the
  test-writer / reviewer the *contract and acceptance criteria only*, in a `--plan` (read-only)
  session. If it sees the implementation it will test-to-the-code and miss the bugs you care about.
- **Greenfield → `--new`, existing repo → `--worktree`.** A from-scratch service/SDK/component wants
  a clean folder (`--new`); a change inside an existing project wants an isolated branch
  (`--worktree`) so you can diff and merge it back.

### 1b. Two cases that look like splits but aren't (handle them differently)

- **Interface not defined yet → design pass *then* split.** If you can't yet write the CONTRACT, you
  cannot split safely. Spawn **one** `--plan` session (or do it yourself) to produce the
  interface/architecture, accept it, *then* fan out builders against it. Splitting before the seam
  exists guarantees drift.
- **Best-of-N exploration → isolation, not orthogonality.** When a task is **hard, risky, or
  uncertain** — no obvious right approach, a tricky perf/algorithm problem, a flaky bug with several
  candidate root causes, a "make it 2× faster" with multiple strategies — spawn several children on
  the **same** problem with **different** approaches, each in its own isolated `--worktree`. They run
  fully in parallel and can't clobber each other (separate branches). The split here is along
  *strategy*, not *labor*: there is no inter-child CONTRACT, only a shared **acceptance test** every
  attempt must pass, plus a comparison rubric the prime ranks winners by (benchmark number, diff
  size, passing tests, simplicity).

  ```bash
  # same brief + acceptance test, divergent strategies, isolated worktrees
  wt spawn --session approach-memo  --dir ~/proj --worktree --skip-permissions \
    --prompt "GOAL: cut p99 latency 2×. STRATEGY: memoize hot path. ACCEPTANCE: bench < 50ms; tests green. REPORT: WT_STATUS."
  wt spawn --session approach-index --dir ~/proj --worktree --skip-permissions \
    --prompt "GOAL: cut p99 latency 2×. STRATEGY: add the DB index. ACCEPTANCE: bench < 50ms; tests green. REPORT: WT_STATUS."
  wt spawn --session approach-cache --dir ~/proj --worktree --skip-permissions \
    --prompt "GOAL: cut p99 latency 2×. STRATEGY: front it with a cache. ACCEPTANCE: bench < 50ms; tests green. REPORT: WT_STATUS."
  # then: run the SAME benchmark/tests yourself in each workspace, rank, keep the winner's branch,
  #       and discard the losers so they don't litter the repo:
  wt session close approach-index --discard
  wt session close approach-cache --discard
  ```

  The prime's job is to **judge, not to merge them together** — pick one branch to integrate (or
  cherry-pick the best idea from each into a fresh attempt). Bound it: N is usually 2–4, and you
  still cap retries per child. Pair well with `--trace` on the riskier attempts so you can see *how*
  each one reasoned, and with a `--plan` auditor to score them against the rubric.

### 1c. Smell tests — do NOT split

- The change threads a single feature through **shared mutable state** across many files (one session).
- Parts must **talk to each other every turn** to make progress (the seam isn't a contract, it's a
  conversation — one session).
- The whole task is **smaller than the orchestration overhead** (spawn + brief + validate + integrate
  costs more than just doing it).
- You'd be **splitting the same files** across children (guaranteed merge conflicts — re-cut the
  boundary along file ownership or don't split).
- The "contract" keeps changing as you build (the interface isn't stable yet → do the design pass).

## 2. Bootstrap — register the prime (once)

First use creates the group **and** registers you as its single prime. Keep the token; it is your
identity.

```bash
wt daemon >/tmp/wt-daemon.log 2>&1 &          # one daemon per machine
export WT_GROUP=myproj
export WT_TOKEN="$(wt group new "$WT_GROUP" 2>/dev/null)"   # token → stdout; info → stderr
wt whoami                                      # → group=myproj agent=prime role=prime
```

`wt group new` fails if the group exists — there is exactly one prime. Resuming later? Reuse the
**same** saved `WT_TOKEN`; don't create a second group for the same project.

## 3. Dispatch with COMPLETE context (you own the shared context)

A child cannot easily ask you mid-turn — its **first prompt is turn 1**, so it must be self-contained.
Spawn each child in its own workspace with a full brief built from this template:

```
GOAL:        <the one outcome this child owns>
SCOPE:       <exactly what to build/change; what NOT to touch>
CONTRACT:    <the interface it must honor — API shape, file layout, formats, names>
CONSTRAINTS: <stack, style, perf, "no new deps", etc.>
INPUTS:      <facts it needs: paths, examples, the sibling contract>
ACCEPTANCE:  <machine-checkable "done": files exist, `cargo test` passes, endpoint matches CONTRACT>
REPORT:      End EVERY response with the WT_STATUS block (see below). Do NOT ask questions for things
             you can decide; if truly blocked, report state: blocked with a precise blocked_on.
```

Spawn it (pick the posture deliberately — it is fixed at launch). With the built-in Claude harness the
posture is a flag; with Devin / a `$WT_HARNESS_CMD` override it lives in the brief (§0):

```bash
# a builder that works autonomously in its own git worktree branch
wt spawn --session backend --dir ~/myproj --worktree --skip-permissions \
         --idle-timeout 10m --prompt "$BACKEND_BRIEF"
# a read-only auditor/reviewer (cannot edit) — great for validating a sibling
wt spawn --session review  --dir ~/myproj --worktree --plan \
         --trace --prompt "$REVIEW_BRIEF"
# same intent, Devin (or any) harness: posture is in the brief, not a flag
WT_HARNESS_CMD=wt-devin-adapter wt spawn --session backend --dir ~/myproj --worktree \
         --idle-timeout 10m --prompt "$BACKEND_BRIEF (POSTURE: autonomous — implement, test, push a branch)"
```

- `--worktree` → isolated branch `wt/<group>/<session>` off the base repo (diffable, mergeable);
  `--new` → a fresh folder for a from-scratch component. Each child's cwd is
  `~/.wt/sessions/<group>/<session>/` — **never** the base dir. (A cloud harness like Devin also works
  in *its* workspace; have it push a branch / PR you can fetch — see §0.)
- `--plan` (read-only/explore) for auditors; `--skip-permissions` (autonomous) for builders;
  `--permission-mode <m>` for anything in between. These steer the **built-in Claude harness**; for
  Devin / overrides, encode the same posture in the brief.
- `--idle-timeout <dur>` → you get a `control` ping if a turn goes silent that long (the child keeps
  running — your call to nudge or kill). `--trace` → you also receive the child's reasoning as
  `trace` messages (audit; off by default to avoid noise).
- `wt spawn` prints `{group, session, workspace, token}` — record `session` and `workspace`.

## 4. The report protocol (how the loop stays parseable)

Every child ends **every turn** with a fenced block you can parse deterministically — **regardless of
harness** (Claude, Devin, or any override). You put this rule in its brief; here it is verbatim:

```
<<<WT_STATUS>>>
state:      done | working | blocked | failed
summary:    <one line>
changed:    <files touched, or ->
checks:     <what you verified yourself, e.g. "cargo test: 12 passed">
blocked_on: <only if blocked — one of: SECRET:<NAME> | DECISION:<q> | ACCESS:<resource> | EXTERNAL:<thing>>
next:       <only if working — what you'll do next>
<<<END_WT_STATUS>>>
```

**Driving a child** (this is the #1 gotcha): to feed a child its next turn you **must** use
`--kind turn_input`. A plain `wt send` defaults to `--kind user`, which is **not** fed to the harness.

```bash
wt send --session backend --kind turn_input "Tests fail on empty input — handle it and re-run."
```

**Observing** — `wt recv` is consume-on-read: each call returns only **new** messages (and marks them
read), so a polling loop never re-processes. Messages are JSON: `{session, from, kind, payload}`.

```bash
wt recv --group "$WT_GROUP"     # new turn_output / trace / control across ALL sessions
wt recv --group "$WT_GROUP" --all   # replay full history (debugging; non-destructive)
wt ls --group "$WT_GROUP"       # per-session STATUS: running | awaiting_input | exited
```

`turn_output` = a child finished a turn (its result + WT_STATUS). `awaiting_input` status = same
signal. `control` = lifecycle (idle ping, "child exited"). `trace` = reasoning (if `--trace`).

## 5. Validate before you accept (point 3)

When a child reports `done`, **independently verify in its workspace** — do not accept on the report
alone:

- **Inspect**: read the files under `~/.wt/sessions/<group>/<session>/`.
- **Run the checks yourself** in that dir: `cargo test`, `npm run build`, `ruff`, `tsc`, etc. — the
  child's `checks:` line is a claim; reproduce it.
- **Check the contract**: confirm the produced interface matches what siblings expect (diff against
  the CONTRACT; e.g. curl the new endpoint and compare its shape).
- **Audit if risky**: review `trace` messages, or re-prompt the child to paste the diff / test output,
  then verify.

Outcome → ledger:
- **passes** → mark `ACCEPTED`; `wt session close <session>` (keeps the branch for merge).
- **fails** → send a `turn_input` naming the *specific* defect + the acceptance check it missed;
  `retries += 1`. After 3, mark `FAILED` and escalate.

## 6. Aggregate, integrate, converge (point 4)

You are the only one with the whole picture. Run this loop until done:

1. `wt recv` → for each new message update the ledger (parse `WT_STATUS`; note `control` exits/idles).
2. For each session decide: **validate** (if `done`), **correct** (`turn_input`), **escalate** (if
   `blocked`), **nudge/kill+respawn** (if idle/hung), or **wait**.
3. When a component is `ACCEPTED`, **integrate** it: merge its `wt/<group>/<session>` branch into your
   integration branch (or wire the `--new` projects together), then **validate the whole system**
   end-to-end (build + run + the project-level acceptance criteria). Integration is itself validated.
4. Stop when every component is `ACCEPTED` **and** integration passes. Close remaining sessions.
   Report the result (and any escalations) to the human.

Convergence guards: bounded retries per session; an overall iteration cap; if a child is `exited`
unexpectedly (`control`), inspect its workspace for partial work and decide re-spawn vs. escalate.

## 7. Human-in-the-loop — escalate precisely, don't stall (point 4)

Some things only a human can provide. Children signal them via `blocked_on`; you recognize the prefix
and **batch** them to the human in one precise ask, while other sessions keep working:

| `blocked_on` | Example | Your action |
|---|---|---|
| `SECRET:<NAME>` | `SECRET:OPENAI_API_KEY` | Ask the human for it; have them export it / write `.env` in the workspace; then **re-spawn** the child (env is fixed at launch) or `turn_input` "key is set, retry." Never invent or hardcode secrets. |
| `DECISION:<q>` | `DECISION:soft-delete or hard-delete?` | Surface the question + your recommendation; relay the answer via `turn_input`. |
| `ACCESS:<resource>` | `ACCESS:prod database` | Ask the human to grant/perform it; do not attempt to bypass. |
| `EXTERNAL:<thing>` | `EXTERNAL:domain DNS` | Surface; proceed on unblocked work meanwhile. |

Also escalate, regardless of children: **irreversible/destructive** actions (deleting data, force-push,
spending money, sending external messages) and genuinely **ambiguous product decisions**. Keep human
asks **few, batched, and specific** — that is "minimal intervention," not "never ask."

## 8. Failure handling (robustness quick-reference)

| Symptom | Signal | Response |
|---|---|---|
| Child hung (silent turn) | `control` idle ping (needs `--idle-timeout`) / `STATUS running` for too long | `turn_input` "status?"; if still dead, `wt agent kill` + re-spawn from the workspace. |
| Child exited unexpectedly | `control` "child exited" / `STATUS exited` | Inspect workspace for partial work; re-spawn or escalate. |
| Wrong/incomplete result | validation fails | Specific `turn_input` correction; bounded retries; then `FAILED`/escalate. |
| Contract drift | sibling integration mismatch | Contract is truth; re-align the offending child via `turn_input`. |
| Daemon restarted | sessions show `exited`/`closed` on `wt ls`; children gone | Workspaces persist — re-spawn the unfinished ones; resume from the ledger. |
| Noise / re-reads | — | Rely on consume-on-read `wt recv`; reserve `--trace` for risky/long children. |

## Command quick-reference

```bash
wt group new <g>                  # register prime; prints token (stdout). Export WT_GROUP/WT_TOKEN.
wt spawn --session <s> --dir <base> [--worktree|--new] [--plan|--permission-mode <m>] \
         [--skip-permissions] [--trace] [--idle-timeout <dur>] --prompt "<brief>"
wt send  --session <s> --kind turn_input "<msg>"   # DRIVE a child (turn_input is required)
wt recv  --group <g> [--session <s>] [-f] [--all] [--since <dur>]   # consume-on-read inbox
wt ls    --group <g>              # sessions + STATUS (running|awaiting_input|exited)
wt agent ls --group <g>          # agents (role, status, pid)
wt agent kill <s>                # stop a child (re-spawn to restart)
wt session close <s> [--discard] # close + tear down workspace (keeps branch unless --discard)
wt whoami                        # identity bound to $WT_TOKEN
```

Message kinds: `turn_input` (you→child, drives a turn) · `turn_output` (child→you, turn result) ·
`trace` (child→you, reasoning, opt-in) · `control` (lifecycle: idle, exited) · `user` (free-form, NOT
fed as a turn). Env a child inherits: `WT_TOKEN`/`WT_GROUP`/`WT_SESSION`/`WT_HOME` (so a child is
itself `wt`-capable). Workspaces: `~/.wt/sessions/<group>/<session>/`; worktree branch
`wt/<group>/<session>`.

Harness selection (env, read at `wt spawn`): unset → built-in Claude Code (stream-json; honors
`--plan`/`--skip-permissions`/`--permission-mode`). `WT_HARNESS_CMD=<cmd>` → launches `<cmd>` verbatim
as the child; it must speak the stdio stream-json turn protocol (stdin user-turn → stdout assistant +
terminal `result`). Use a thin adapter for cloud agents like **Devin** (§0); posture + `WT_STATUS`
go in the brief.

## Worked example — front-end + back-end

```bash
wt daemon & export WT_GROUP=shop
export WT_TOKEN="$(wt group new shop 2>/dev/null)"

# 0. Define the CONTRACT first (you own it):
#    GET /api/items -> [{id,name,cents}];  POST /api/cart {id} -> {count}

# 1. Spawn two orthogonal builders, each with the contract in its brief.
wt spawn --session backend  --dir ~/shop --worktree --skip-permissions --idle-timeout 10m \
  --prompt "GOAL: FastAPI implementing CONTRACT … ACCEPTANCE: uvicorn boots; pytest green; routes match CONTRACT. REPORT: end with WT_STATUS."
wt spawn --session frontend --dir ~/shop --worktree --skip-permissions --idle-timeout 10m \
  --prompt "GOAL: a page that lists items and adds to cart, calling CONTRACT … ACCEPTANCE: npm run build OK; calls match CONTRACT. REPORT: end with WT_STATUS."

# 2. Loop: observe → validate → correct.
wt recv --group shop                       # parse each WT_STATUS
# backend reports done → VALIDATE in its workspace:
( cd ~/.wt/sessions/shop/backend && pytest -q )   # reproduce its checks yourself
# frontend reports blocked_on: DECISION:framework? → reply:
wt send --session frontend --kind turn_input "Use plain React + fetch; no SSR. Proceed."

# 3. backend reports blocked_on: SECRET:STRIPE_KEY → ESCALATE to human (batched), keep frontend moving.
# 4. Both ACCEPTED → merge wt/shop/backend + wt/shop/frontend into an integration branch,
#    run the whole app, validate end-to-end, close sessions, report to the human.
```

## Hard rules

- **DO** define the contract + acceptance criteria **before** spawning. **DON'T** split work whose
  interface you haven't pinned down.
- **DO** drive children with `--kind turn_input`. A plain `wt send` will be ignored by the harness.
- **DO** validate in the workspace before accepting. **DON'T** trust a child's "done" on its word.
- **DO** keep a ledger and take every session to a terminal state. **DON'T** leave orphans.
- **DO** escalate secrets/irreversible/ambiguous decisions, batched and precise. **DON'T** invent
  secrets, guess product calls, or stall the whole project on one human ask.
- **DO** bound retries and total iterations. **DON'T** loop forever on a stuck child.
- **DO** keep the loop harness-agnostic: with Devin / any `$WT_HARNESS_CMD` override, put the posture
  and the `WT_STATUS` report rule **in the brief**, and validate the harness's pushed branch / PR.
  **DON'T** rely on `--plan`/`--skip-permissions` for anything but the built-in Claude harness.
