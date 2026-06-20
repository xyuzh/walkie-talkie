#!/usr/bin/env bash
# End-to-end edge-case checks for wt v0.1. This intentionally stays on one host and
# uses separate WT_HOME directories to exercise pairing, token, channel, and CLI boundaries.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WT="$ROOT/target/release/wt"
WTD="$ROOT/target/release/wt-daemon"

if [[ ! -x "$WT" || ! -x "$WTD" ]]; then
    echo "--- building release binaries ---"
    cargo build --release
fi

BASE="$(mktemp -d /tmp/wt-edge.XXXXXX)"
A_HOME="$BASE/a"
B_HOME="$BASE/b"
C_HOME="$BASE/c"
mkdir -p "$A_HOME" "$B_HOME" "$C_HOME"

PIDS=()

cleanup() {
    set +e
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

run_wt() {
    local home="$1"
    shift
    WT_HOME="$home" "$WT" "$@"
}

start_daemon() {
    local home="$1"
    WT_HOME="$home" RUST_LOG=warn "$WTD" --foreground >"$home/daemon.log" 2>&1 &
    PIDS+=("$!")
}

wait_daemon() {
    local home="$1"
    for _ in {1..40}; do
        if run_wt "$home" status >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "daemon did not become ready for $home"
    cat "$home/daemon.log" || true
    exit 1
}

expect_fail() {
    local desc="$1"
    shift
    if "$@" >"$BASE/last.out" 2>"$BASE/last.err"; then
        echo "FAIL: expected failure: $desc"
        cat "$BASE/last.out"
        cat "$BASE/last.err"
        exit 1
    fi
    echo "ok: $desc"
}

expect_contains() {
    local file="$1"
    local needle="$2"
    if ! grep -q "$needle" "$file"; then
        echo "FAIL: expected '$needle' in $file"
        echo "--- $file ---"
        cat "$file" || true
        exit 1
    fi
}

expect_not_contains() {
    local file="$1"
    local needle="$2"
    if grep -q "$needle" "$file"; then
        echo "FAIL: did not expect '$needle' in $file"
        echo "--- $file ---"
        cat "$file" || true
        exit 1
    fi
}

echo "--- init identities ---"
run_wt "$A_HOME" init >/dev/null
run_wt "$B_HOME" init >/dev/null
run_wt "$C_HOME" init >/dev/null

echo "--- start daemons ---"
start_daemon "$A_HOME"
start_daemon "$B_HOME"
start_daemon "$C_HOME"
wait_daemon "$A_HOME"
wait_daemon "$B_HOME"
wait_daemon "$C_HOME"

echo "--- invalid CLI inputs ---"
expect_fail "bad bare NodeId is rejected" run_wt "$A_HOME" peer add nothex --name bad
expect_fail "bad wt1 ticket is rejected" run_wt "$A_HOME" peer add wt1:not-valid --name bad
expect_fail "garbage token is rejected" run_wt "$A_HOME" token import not-a-token
expect_fail "bad token revoke id is rejected" run_wt "$A_HOME" token revoke nothex
expect_fail "grant to missing peer is rejected" run_wt "$A_HOME" token grant missing --cap msg --ttl 1h

echo "--- peer add and duplicate-name boundaries ---"
A_TICKET="$(run_wt "$A_HOME" ticket)"
B_TICKET="$(run_wt "$B_HOME" ticket)"
C_NODEID="$(run_wt "$C_HOME" nodeid)"
run_wt "$A_HOME" peer add "$B_TICKET" --name bob
run_wt "$B_HOME" peer add "$A_TICKET" --name alice
expect_fail "duplicate peer name for another NodeId is rejected" run_wt "$A_HOME" peer add "$C_NODEID" --name bob
expect_fail "unknown capability is rejected" run_wt "$A_HOME" token grant bob --cap exec --ttl 1h

echo "--- tokens: missing reciprocal, wrong subject, unknown issuer ---"
expect_fail "send without imported token is rejected" run_wt "$A_HOME" send bob '{"user":"no token"}'
T_BA="$(run_wt "$B_HOME" token grant alice --cap msg --ttl 1h 2>/dev/null)"
T_AB="$(run_wt "$A_HOME" token grant bob --cap msg --ttl 1h 2>/dev/null)"
expect_fail "token with wrong subject is rejected on import" run_wt "$A_HOME" token import "$T_AB"

run_wt "$C_HOME" peer add "$A_TICKET" --name alice
T_CA="$(run_wt "$C_HOME" token grant alice --cap msg --ttl 1h 2>/dev/null)"
expect_fail "token from unknown issuer is rejected on import" run_wt "$A_HOME" token import "$T_CA"

run_wt "$A_HOME" token import "$T_BA" >/dev/null 2>&1
expect_fail "reverse send without reciprocal token is rejected" run_wt "$B_HOME" send alice '{"user":"no reverse token"}'
run_wt "$B_HOME" token import "$T_AB" >/dev/null 2>&1

echo "--- channel filtering and opaque payload formatting ---"
run_wt "$B_HOME" recv --follow --channel alerts >"$B_HOME/alerts.out" 2>"$B_HOME/alerts.err" &
PIDS+=("$!")
sleep 0.5
run_wt "$A_HOME" send bob '{"user":"default should be filtered"}'
sleep 0.8
expect_not_contains "$B_HOME/alerts.out" "default should be filtered"
run_wt "$A_HOME" send bob --channel alerts '{"user":"alert message"}'
sleep 1
expect_contains "$B_HOME/alerts.out" "alert message"

run_wt "$B_HOME" recv --follow --channel raw >"$B_HOME/raw.out" 2>"$B_HOME/raw.err" &
PIDS+=("$!")
sleep 0.5
run_wt "$A_HOME" send bob --channel raw "plain text"
sleep 1
expect_contains "$B_HOME/raw.out" '"payload":"plain text"'

echo "--- from filtering and reciprocal messaging ---"
run_wt "$A_HOME" recv --follow --from bob >"$A_HOME/from-bob.out" 2>"$A_HOME/from-bob.err" &
PIDS+=("$!")
sleep 0.5
run_wt "$B_HOME" send alice '{"user":"hi alice"}'
sleep 1
expect_contains "$A_HOME/from-bob.out" "hi alice"

echo "--- v0.1 stubs stay explicit ---"
expect_fail "exec is a v0.1 stub" run_wt "$A_HOME" exec bob -- ls
expect_fail "shell is a v0.1 stub" run_wt "$A_HOME" shell bob

echo "--- EDGE CASE TESTS PASSED ---"
