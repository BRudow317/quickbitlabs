#!/usr/bin/env bash
set -euo pipefail

read -r -p "Enter python file name (e.g., script.py): " PYFILE
if [[ -z "$PYFILE" ]]; then
  echo "No file provided." >&2
  exit 1
fi

# If user entered just a name, look in current directory first
if [[ ! -f "$PYFILE" ]]; then
  if [[ -f "./$PYFILE" ]]; then
    PYFILE="./$PYFILE"
  else
    echo "File not found: $PYFILE" >&2
    exit 2
  fi
fi

# Prefer python3 on Ubuntu
if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "Python not found. Install: sudo apt-get update && sudo apt-get install -y python3" >&2
  exit 3
fi

# Execute and stream output to terminal
exec "$PY" "$PYFILE"
