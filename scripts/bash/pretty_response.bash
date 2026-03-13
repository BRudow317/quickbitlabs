#!/usr/bin/env bash
# Pretty-print input as JSON, XML, HTML, or CSV.
#
# Usage:
#   curl ... | ./pretty_response.sh
#   ./pretty_response.sh response.txt
#   ./pretty_response.sh --type json response.txt
#   curl ... | ./pretty_response.sh --type xml
#
# Dependencies
#   JSON: sudo apt-get install -y jq
#   XML : sudo apt-get install -y libxml2-utils
#   HTML: sudo apt-get install -y tidy
#   CSV : python3 (usually installed)

set -euo pipefail

TYPE=""
FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--type) TYPE="${2:-}"; shift 2 ;;
    *) FILE="$1"; shift ;;
  esac
done

tmp="$(mktemp)"
cleanup(){ rm -f "$tmp"; }
trap cleanup EXIT

if [[ -n "${FILE}" ]]; then
  [[ -f "$FILE" ]] || { echo "File not found: $FILE" >&2; exit 2; }
  cat -- "$FILE" >"$tmp"
else
  if [[ -t 0 ]]; then
    echo "Paste response, then press Ctrl+D to format:" >&2
  fi
  cat >"$tmp"
fi

# Auto-detect if type not provided
if [[ -z "$TYPE" ]]; then
  first="$(tr -d '\r' <"$tmp" | sed -e 's/^[[:space:]]*//' | head -c 200 || true)"
  if [[ "$first" == \{* || "$first" == \[* ]]; then
    TYPE="json"
  elif [[ "$first" == \<* ]]; then
    if grep -qiE '<!doctype html|<html' "$tmp"; then
      TYPE="html"
    else
      TYPE="xml"
    fi
  else
    TYPE="csv"
  fi
fi

case "${TYPE,,}" in
  json)
    command -v jq >/dev/null 2>&1 || { echo "jq not found. Install: sudo apt-get install -y jq" >&2; exit 3; }
    jq . <"$tmp"
    ;;
  xml)
    command -v xmllint >/dev/null 2>&1 || { echo "xmllint not found. Install: sudo apt-get install -y libxml2-utils" >&2; exit 3; }
    xmllint --format - <"$tmp"
    ;;
  html)
    command -v tidy >/dev/null 2>&1 || { echo "tidy not found. Install: sudo apt-get install -y tidy" >&2; exit 3; }
    # tidy writes diagnostics to stderr; keep output clean but show fatal errors
    tidy -q -i -wrap 0 -utf8 <"$tmp" 2>/dev/null || tidy -i -wrap 0 -utf8 <"$tmp"
    ;;
  csv)
    command -v python3 >/dev/null 2>&1 || { echo "python3 not found. Install: sudo apt-get install -y python3" >&2; exit 3; }
    python3 - <<'PY' <"$tmp"
import sys, csv
rows = list(csv.reader(sys.stdin))
if not rows:
    sys.exit(0)
cols = max(len(r) for r in rows)
for r in rows:
    r += [""] * (cols - len(r))
widths = [0]*cols
for r in rows:
    for i,v in enumerate(r):
        widths[i] = max(widths[i], len(v))
for idx,r in enumerate(rows):
    line = " | ".join((r[i] or "").ljust(widths[i]) for i in range(cols))
    print(line.rstrip())
    if idx == 0 and len(rows) > 1:
        print("-+-".join("-"*w for w in widths).rstrip())
PY
    ;;
  *)
    echo "Unknown type: $TYPE (use: json|xml|html|csv)" >&2
    exit 1
    ;;
esac
