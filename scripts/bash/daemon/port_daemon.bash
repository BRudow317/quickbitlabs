#!/usr/bin/env bash
# /usr/local/bin/port_daemon.sh
# Simple TCP daemon (via socat) that binds to a port and hands each connection to a bash handler.

set -euo pipefail

PORT="${1:-9000}"
BIND_ADDR="${2:-0.0.0.0}"

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || (( PORT < 1 || PORT > 65535 )); then
  echo "Invalid port: $PORT" >&2
  exit 2
fi

if ! command -v socat >/dev/null 2>&1; then
  echo "socat not found. Install on Ubuntu: sudo apt-get update && sudo apt-get install -y socat" >&2
  exit 3
fi

HANDLER="/usr/local/bin/port-daemon-handler.sh"
if [[ ! -x "$HANDLER" ]]; then
  echo "Handler not found/executable: $HANDLER" >&2
  exit 4
fi

# Note: port < 1024 requires root or special capability.
exec socat -d -d -ly \
  "TCP-LISTEN:${PORT},bind=${BIND_ADDR},reuseaddr,fork,keepalive" \
  "EXEC:${HANDLER},pipes,stderr"
