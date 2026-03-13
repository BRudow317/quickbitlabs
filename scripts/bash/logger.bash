#!/usr/bin/env bash
set -euo pipefail

# Log stdout to ./app/log/<scriptname>-YYYYmmdd-HHMMSS.log (relative to this script)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/app/log"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/$(basename "$0" .sh)-$(date +%Y%m%d-%H%M%S).log"

# Redirect ONLY stdout to the log file (stderr stays on the terminal)
exec 1>>"$LOG_FILE"

echo "Logging stdout to: $LOG_FILE"
echo "Example stdout line"
