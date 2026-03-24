#!/usr/bin/env bash
# /usr/local/bin/port-daemon-handler.sh
# Per-connection handler. Reads lines from the socket (stdin) and writes responses to the socket (stdout).

set -euo pipefail

# socat often exports peer info env vars; if not present, they’ll be blank.
PEER="${SOCAT_PEERADDR:-unknown}:${SOCAT_PEERPORT:-unknown}"

echo "connected ${PEER}"
echo "type 'quit' to close"

while IFS= read -r line; do
  [[ "${line,,}" == "quit" ]] && break
  printf 'you said: %s\n' "$line"
done

echo "bye"
