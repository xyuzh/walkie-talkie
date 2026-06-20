#!/usr/bin/env bash
# Local two-daemon loopback smoke test for wt v0.1.
# Both daemons run on the same host with separate WT_HOMEs; iroh handles peer addressing via NodeId.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release"
WT="$BIN/wt"

BASE="$(mktemp -d /tmp/wt-smoke.XXXXXX)"
A_HOME="$BASE/a"
B_HOME="$BASE/b"
PIDS=()

cleanup() {
    set +e
    echo "--- cleanup ---"
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 0.3
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$BASE"
}
trap cleanup EXIT

mkdir -p "$A_HOME" "$B_HOME"

echo "--- init A and B ---"
WT_HOME=$A_HOME $WT init
WT_HOME=$B_HOME $WT init

echo "--- start daemons ---"
WT_HOME=$A_HOME RUST_LOG=warn $WT daemon >"$A_HOME/daemon.log" 2>&1 &
A_PID=$!
PIDS+=("$A_PID")
WT_HOME=$B_HOME RUST_LOG=warn $WT daemon >"$B_HOME/daemon.log" 2>&1 &
B_PID=$!
PIDS+=("$B_PID")

echo "wait for daemons..."
READY=0
for i in {1..30}; do
    if WT_HOME=$A_HOME $WT status >/dev/null 2>&1 \
       && WT_HOME=$B_HOME $WT status >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.5
done
if [[ "$READY" != "1" ]]; then
    echo "FAIL: daemons did not become ready"
    echo "--- A daemon log ---"
    cat "$A_HOME/daemon.log" || true
    echo "--- B daemon log ---"
    cat "$B_HOME/daemon.log" || true
    exit 1
fi

WT_HOME=$A_HOME $WT status
WT_HOME=$B_HOME $WT status

A=$(WT_HOME=$A_HOME $WT ticket)
B=$(WT_HOME=$B_HOME $WT ticket)
echo "A ticket len: ${#A}"
echo "B ticket len: ${#B}"

echo "--- peer add (via tickets) ---"
WT_HOME=$A_HOME $WT peer add "$B" --name bob
WT_HOME=$B_HOME $WT peer add "$A" --name alice

echo "--- ls ---"
WT_HOME=$A_HOME $WT ls
WT_HOME=$B_HOME $WT ls

echo "--- token grant (reciprocal) ---"
T_BA=$(WT_HOME=$B_HOME $WT token grant alice --cap msg --ttl 1h 2>/dev/null)
T_AB=$(WT_HOME=$A_HOME $WT token grant bob   --cap msg --ttl 1h 2>/dev/null)
echo "T_BA len: ${#T_BA}"
echo "T_AB len: ${#T_AB}"

echo "--- token import ---"
WT_HOME=$A_HOME $WT token import "$T_BA"
WT_HOME=$B_HOME $WT token import "$T_AB"

echo "--- start recv on B (background) ---"
WT_HOME=$B_HOME $WT recv --follow >"$B_HOME/recv.out" 2>"$B_HOME/recv.err" &
RECV_PID=$!
PIDS+=("$RECV_PID")
sleep 1

echo "--- send A -> B ---"
WT_HOME=$A_HOME $WT send bob '{"user":"hello from alice"}'

echo "--- send B -> A (verify reciprocal works) ---"
WT_HOME=$A_HOME $WT recv --follow >"$A_HOME/recv.out" 2>"$A_HOME/recv.err" &
RECV_A_PID=$!
PIDS+=("$RECV_A_PID")
sleep 1
WT_HOME=$B_HOME $WT send alice '{"user":"hi alice"}'

sleep 2

echo "--- B's inbox ---"
cat "$B_HOME/recv.out"

echo "--- A's inbox ---"
cat "$A_HOME/recv.out"

echo "--- conn ---"
WT_HOME=$A_HOME $WT conn
WT_HOME=$B_HOME $WT conn

kill $RECV_PID 2>/dev/null || true
kill $RECV_A_PID 2>/dev/null || true
wait $RECV_PID 2>/dev/null || true
wait $RECV_A_PID 2>/dev/null || true

# Assert that B saw alice's message and A saw bob's message.
grep -q "hello from alice" "$B_HOME/recv.out" || { echo "FAIL: B did not receive alice's message"; exit 1; }
grep -q "hi alice"          "$A_HOME/recv.out" || { echo "FAIL: A did not receive bob's message"; exit 1; }

echo "--- SMOKE TEST PASSED ---"
