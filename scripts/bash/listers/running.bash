#!/usr/bin/env bash
# Ubuntu: quick "what's running" view (services + top processes + listening ports)
set -euo pipefail

echo "=== Running systemd services ==="
systemctl list-units --type=service --state=running

echo
echo "=== Top processes by CPU ==="
ps -eo pid,user,comm,%cpu,%mem --sort=-%cpu | head -n 15

echo
echo "=== Listening ports (TCP/UDP) ==="
ss -tulpen
