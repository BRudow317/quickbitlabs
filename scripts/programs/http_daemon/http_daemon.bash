#!/usr/bin/env bash
# /usr/local/bin/http-daemon.sh
# Binds to a TCP port and serves HTTP by forking /usr/local/bin/http-handler.sh per connection.

set -euo pipefail

PORT="${1:-9000}"
BIND_ADDR="${2:-0.0.0.0}"

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || (( PORT < 1 || PORT > 65535 )); then
  echo "Invalid port: $PORT" >&2
  exit 2
fi

command -v socat >/dev/null 2>&1 || {
  echo "socat not found. Install: sudo apt-get update && sudo apt-get install -y socat" >&2
  exit 3
}

HANDLER="/usr/local/bin/http-handler.sh"
[[ -x "$HANDLER" ]] || { echo "Handler not executable: $HANDLER" >&2; exit 4; }

# -T: connection idle timeout (seconds)
exec socat -T 30 -d -d -ly \
  "TCP-LISTEN:${PORT},bind=${BIND_ADDR},reuseaddr,fork,keepalive" \
  "EXEC:${HANDLER},pipes,stderr"
