#!/usr/bin/env bash
set -euo pipefail

read -r -p "Enter jar file name (e.g., app.jar): " JAR
if [[ -z "$JAR" ]]; then
  echo "No file provided." >&2
  exit 1
fi

# If user entered just a name, look in current directory first
if [[ ! -f "$JAR" ]]; then
  if [[ -f "./$JAR" ]]; then
    JAR="./$JAR"
  else
    echo "File not found: $JAR" >&2
    exit 2
  fi
fi

if ! command -v java >/dev/null 2>&1; then
  echo "Java not found. Install: sudo apt-get update && sudo apt-get install -y default-jre" >&2
  exit 3
fi

# Execute and stream output to terminal
exec java -jar "$JAR"
