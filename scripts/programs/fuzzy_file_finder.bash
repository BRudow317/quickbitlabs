#!/usr/bin/env bash
set -euo pipefail

read -r -p "Enter partial file/dir name to search for (case-insensitive): " TERM
if [[ -z "$TERM" ]]; then
  echo "No input provided." >&2
  exit 1
fi

# Case-insensitive partial match for files/dirs under current directory
find . -iname "*${TERM}*" -print
