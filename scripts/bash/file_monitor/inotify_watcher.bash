#!/usr/bin/env bash
# file: /usr/local/bin/filewatch.sh
# Watches a path and runs a command when changes occur (systemd-friendly).
# Usage: filewatch.sh <watch_path> <command...>

set -euo pipefail

WATCH_PATH="${1:-}"
shift || true
if [[ -z "${WATCH_PATH}" || $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <watch_path> <command...>" >&2
  exit 2
fi

CMD=("$@")

if ! command -v inotifywait >/dev/null 2>&1; then
  echo "ERROR: inotifywait not found. Install: sudo dnf install -y inotify-tools" >&2
  exit 3
fi

# Debounce window (seconds) to avoid rapid duplicate triggers
DEBOUNCE="${DEBOUNCE:-1}"
last_run=0

inotifywait -m -r \
  -e close_write,create,modify,move,delete \
  --format '%w%f' \
  "$WATCH_PATH" | while IFS= read -r changed; do
    now=$(date +%s)
    if (( now - last_run < DEBOUNCE )); then
      continue
    fi
    last_run=$now

    # Pass changed path to the handler via env var
    CHANGED_PATH="$changed" "${CMD[@]}"
  done
