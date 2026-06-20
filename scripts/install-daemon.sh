#!/usr/bin/env bash
# Install + run the wt daemon as a macOS LaunchAgent (auto-starts on login, restarts on crash).
#
#   scripts/install-daemon.sh          # build release, install to ~/.local/bin, load LaunchAgent
#
# Manage it afterwards:
#   launchctl kickstart -k gui/$(id -u)/com.wt.daemon   # restart now
#   launchctl bootout    gui/$(id -u)/com.wt.daemon      # stop + unload
#   tail -f ~/.wt/run/daemon.log                         # logs
#
# The web dashboard is then at http://127.0.0.1:8787 (load the extension/ folder in Chrome).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$HOME/.local/bin"
PLIST_SRC="$REPO/scripts/com.wt.daemon.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.wt.daemon.plist"
LABEL="com.wt.daemon"
DOMAIN="gui/$(id -u)"

echo "==> Building release binary"
( cd "$REPO" && cargo build --release -p wt-cli )

echo "==> Installing wt -> $BIN_DIR/wt"
mkdir -p "$BIN_DIR"
ln -sf "$REPO/target/release/wt" "$BIN_DIR/wt"

echo "==> Writing LaunchAgent -> $PLIST_DST"
mkdir -p "$HOME/Library/LaunchAgents" "$HOME/.wt/run"
sed "s|__HOME__|$HOME|g" "$PLIST_SRC" > "$PLIST_DST"

echo "==> (Re)loading LaunchAgent"
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST_DST"
launchctl enable "$DOMAIN/$LABEL"

echo "==> Waiting for the daemon to come up"
for _ in $(seq 1 20); do
  if curl -fsS http://127.0.0.1:8787/api/groups >/dev/null 2>&1; then
    echo "    daemon is up: http://127.0.0.1:8787"
    exit 0
  fi
  sleep 0.5
done
echo "    daemon did not respond yet; check: tail -f ~/.wt/run/daemon.log" >&2
exit 1
