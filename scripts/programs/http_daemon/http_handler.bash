#!/usr/bin/env bash
# /usr/local/bin/http-handler.sh
# Reads ONE HTTP request from stdin, calls /usr/local/bin/http-app.sh, and returns an HTTP response on stdout.
#
# App contract (CGI-lite):
#   App prints optional headers first (each "Key: Value"), then a blank line, then the body.
#   Supported headers from app:
#     Status: 200        (or "200 OK")
#     Content-Type: application/json
#
# The request body (if any) is provided to the app via stdin.
# Request metadata is provided via env vars:
#   REQUEST_METHOD, REQUEST_TARGET, REQUEST_PATH, QUERY_STRING, REMOTE_ADDR, REMOTE_PORT

set -euo pipefail

strip_cr() { printf '%s' "${1%$'\r'}"; }

status_text() {
  case "${1:-200}" in
    200) echo "OK" ;;
    201) echo "Created" ;;
    204) echo "No Content" ;;
    301) echo "Moved Permanently" ;;
    302) echo "Found" ;;
    400) echo "Bad Request" ;;
    401) echo "Unauthorized" ;;
    403) echo "Forbidden" ;;
    404) echo "Not Found" ;;
    405) echo "Method Not Allowed" ;;
    500) echo "Internal Server Error" ;;
    502) echo "Bad Gateway" ;;
    503) echo "Service Unavailable" ;;
    *)   echo "OK" ;;
  esac
}

# --- Read request line ---
IFS=' ' read -r METHOD TARGET VERSION || exit 0
METHOD="$(strip_cr "$METHOD")"
TARGET="$(strip_cr "$TARGET")"
VERSION="$(strip_cr "${VERSION:-HTTP/1.1}")"

# Parse path + query
REQUEST_PATH="${TARGET%%\?*}"
QUERY_STRING=""
[[ "$TARGET" == *\?* ]] && QUERY_STRING="${TARGET#*\?}"

# --- Read headers ---
CONTENT_LENGTH=0
while IFS= read -r line; do
  line="$(strip_cr "$line")"
  [[ -z "$line" ]] && break
  key="${line%%:*}"
  val="${line#*:}"
  val="${val#"${val%%[![:space:]]*}"}"  # ltrim
  key_lc="$(printf '%s' "$key" | tr '[:upper:]' '[:lower:]')"
  if [[ "$key_lc" == "content-length" ]]; then
    CONTENT_LENGTH="${val:-0}"
  fi
done

# --- Read body (if any) ---
BODY=""
if [[ "$CONTENT_LENGTH" =~ ^[0-9]+$ ]] && (( CONTENT_LENGTH > 0 )); then
  BODY="$(dd bs=1 count="$CONTENT_LENGTH" 2>/dev/null || true)"
fi

APP="/usr/local/bin/http-app.sh"

tmp_out="$(mktemp)"
tmp_hdr="$(mktemp)"
tmp_body="$(mktemp)"
cleanup(){ rm -f "$tmp_out" "$tmp_hdr" "$tmp_body"; }
trap cleanup EXIT

if [[ -x "$APP" ]]; then
  printf '%s' "$BODY" | \
    REQUEST_METHOD="$METHOD" \
    REQUEST_TARGET="$TARGET" \
    REQUEST_PATH="$REQUEST_PATH" \
    QUERY_STRING="$QUERY_STRING" \
    REMOTE_ADDR="${SOCAT_PEERADDR:-}" \
    REMOTE_PORT="${SOCAT_PEERPORT:-}" \
    "$APP" >"$tmp_out"
else
  printf 'Status: 500\nContent-Type: text/plain\n\nMissing app: %s\n' "$APP" >"$tmp_out"
fi

# Split app output into headers block + body (first blank line separates)
awk '
  BEGIN{h=1}
  h==1 {
    if ($0 ~ /^\r?$/) { h=0; next }
    print > hdr
    next
  }
  { print > body }
' hdr="$tmp_hdr" body="$tmp_body" "$tmp_out"

APP_STATUS="200"
APP_STATUS_LINE=""
APP_CTYPE="application/json"

if [[ -s "$tmp_hdr" ]]; then
  while IFS= read -r hline; do
    hline="$(strip_cr "$hline")"
    key="${hline%%:*}"
    val="${hline#*:}"
    val="${val#"${val%%[![:space:]]*}"}"
    key_lc="$(printf '%s' "$key" | tr '[:upper:]' '[:lower:]')"
    case "$key_lc" in
      status)
        # allow "200" or "200 OK"
        APP_STATUS="${val%% *}"
        APP_STATUS_LINE="$val"
        ;;
      content-type)
        APP_CTYPE="$val"
        ;;
    esac
  done <"$tmp_hdr"
fi

if [[ -z "$APP_STATUS_LINE" ]]; then
  APP_STATUS_LINE="${APP_STATUS} $(status_text "$APP_STATUS")"
fi

CONTENT_LEN="$(wc -c <"$tmp_body" | tr -d ' ')"

# --- Write HTTP response ---
printf 'HTTP/1.1 %s\r\n' "$APP_STATUS_LINE"
printf 'Content-Type: %s\r\n' "$APP_CTYPE"
printf 'Content-Length: %s\r\n' "$CONTENT_LEN"
printf 'Connection: close\r\n'
printf '\r\n'
cat "$tmp_body"
