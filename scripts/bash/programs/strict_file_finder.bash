#!/usr/bin/env bash
set -euo pipefail

read -r -p "Enter exact file/dir name to search for: " NAME
if [[ -z "$NAME" ]]; then
  echo "No input provided." >&2
  exit 1
fi

# Search current directory and all subdirectories for an exact name match
# Prints matching paths (files and/or directories), one per line.
find . -name "$NAME" -print
