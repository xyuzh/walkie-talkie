#!/usr/bin/env bash
# Single-daemon smoke for the v0.3 orchestration layer:
#   group new -> spawn (per-session workspace + supervised harness) -> turn loop -> ls -> kill.
#
# Uses a stub harness that speaks stream-json (no real `claude` needed) unless $WT_HARNESS_CMD is
# already set in the environment. Run after `cargo build --release`.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WT="$ROOT/target/release/wt"

BASE="$(mktemp -d /tmp/wt-agents.XXXXXX)"   # short path keeps the AF_UNIX socket under SUN_LEN
export WT_HOME="$BASE/home"
WORKBASE="$BASE/proj"
mkdir -p "$WT_HOME" "$WORKBASE"
PIDS=()

cleanup() {
    set +e
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    sleep 0.3
    for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
    rm -rf "$BASE"
}
trap cleanup EXIT

# Stub harness: echo each stream-json user turn back as a `result` event.
STUB="$BASE/stub.py"
cat > "$STUB" <<'PY'
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line); c = m["message"]["content"]
        t = "".join(b.get("text","") for b in c if b.get("type")=="text") if isinstance(c, list) else str(c)
    except Exception:
        t = line
    o = "echo:" + t
    print(json.dumps({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":o}]}}), flush=True)
    print(json.dumps({"type":"result","subtype":"success","is_error":False,"result":o}), flush=True)
PY
export WT_HARNESS_CMD="${WT_HARNESS_CMD:-python3 $STUB}"

echo "--- start daemon ---"
RUST_LOG=warn "$WT" daemon >"$WT_HOME/daemon.log" 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 50); do "$WT" status >/dev/null 2>&1 && break; sleep 0.2; done
"$WT" status >/dev/null 2>&1 || { echo "FAIL: daemon not ready"; cat "$WT_HOME/daemon.log"; exit 1; }

echo "--- group new ---"
TOKEN=$("$WT" group new myapp 2>/dev/null)
export WT_TOKEN="$TOKEN" WT_GROUP=myapp
echo "prime token len: ${#TOKEN}"
"$WT" group ls

echo "--- spawn worker (--new workspace, prompt 'hello') ---"
"$WT" spawn --session worker --dir "$WORKBASE" --new --prompt "hello" >/dev/null
sleep 1

echo "--- turn 1: prime drains the worker's output ---"
"$WT" recv --session worker >"$BASE/turn1.out" 2>/dev/null || true
cat "$BASE/turn1.out"
grep -q "echo:hello" "$BASE/turn1.out" || { echo "FAIL: missing echo:hello"; exit 1; }

echo "--- reply (turn_input) -> worker's next turn ---"
"$WT" send --session worker --kind turn_input "do more"
sleep 1
"$WT" recv --session worker >"$BASE/turn2.out" 2>/dev/null || true
cat "$BASE/turn2.out"
grep -q "echo:do more" "$BASE/turn2.out" || { echo "FAIL: reply was not fed back to the harness"; exit 1; }

echo "--- ls --group (sessions) ---"
"$WT" ls --group myapp
"$WT" ls --group myapp | grep -q worker || { echo "FAIL: session not listed"; exit 1; }

echo "--- agent kill worker ---"
"$WT" agent kill worker

echo "--- SMOKE TEST PASSED ---"
