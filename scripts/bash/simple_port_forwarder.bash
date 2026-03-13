#!/usr/bin/env bash
# setup_port_forward_service.sh
# Ubuntu: create a systemd service that forwards a local TCP port to another host:port using socat.
#
# Usage:
#   sudo ./setup_port_forward_service.sh <listen_port> <target_host> <target_port> [bind_addr]
#
# Examples:
#   sudo ./setup_port_forward_service.sh 8080 127.0.0.1 8081
#   sudo ./setup_port_forward_service.sh 80   127.0.0.1 8080 0.0.0.0
#   sudo ./setup_port_forward_service.sh 9000 10.0.0.25 9001 127.0.0.1   # listen only on localhost
#
# Logs:
#   sudo journalctl -u port-forward-<listen_port>.service -f

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: run with sudo/root." >&2
  exit 1
fi

LISTEN_PORT="${1:-}"
TARGET_HOST="${2:-}"
TARGET_PORT="${3:-}"
BIND_ADDR="${4:-0.0.0.0}"

if [[ -z "$LISTEN_PORT" || -z "$TARGET_HOST" || -z "$TARGET_PORT" ]]; then
  echo "Usage: sudo $0 <listen_port> <target_host> <target_port> [bind_addr]" >&2
  exit 2
fi

if ! [[ "$LISTEN_PORT" =~ ^[0-9]+$ ]] || (( LISTEN_PORT < 1 || LISTEN_PORT > 65535 )); then
  echo "ERROR: invalid listen_port: $LISTEN_PORT" >&2
  exit 3
fi
if ! [[ "$TARGET_PORT" =~ ^[0-9]+$ ]] || (( TARGET_PORT < 1 || TARGET_PORT > 65535 )); then
  echo "ERROR: invalid target_port: $TARGET_PORT" >&2
  exit 3
fi

# Ensure socat is installed (Ubuntu)
if ! command -v socat >/dev/null 2>&1; then
  apt-get update -y
  apt-get install -y socat
fi

SERVICE_NAME="port-forward-${LISTEN_PORT}.service"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"

cat >"$UNIT_PATH" <<EOF
[Unit]
Description=Port forward TCP ${BIND_ADDR}:${LISTEN_PORT} -> ${TARGET_HOST}:${TARGET_PORT}
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/socat -d -d -ly TCP-LISTEN:${LISTEN_PORT},fork,reuseaddr,bind=${BIND_ADDR} TCP:${TARGET_HOST}:${TARGET_PORT}
Restart=always
RestartSec=2
KillSignal=SIGINT
TimeoutStopSec=5

# Basic hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
RestrictAddressFamilies=AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"

echo "Created and started: $UNIT_PATH"
echo "Status:"
systemctl --no-pager --full status "$SERVICE_NAME" || true
echo
echo "Follow logs:"
echo "  sudo journalctl -u $SERVICE_NAME -f"
