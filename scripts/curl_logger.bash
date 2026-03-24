#!/usr/bin/env bash
# Logs a curl request + response to request.log and response.log.
# Each response append is separated by a timestamped separator.
#
# Usage:
#   ./curl_log.sh https://example.com/api
#   ./curl_log.sh -X POST -H 'Content-Type: application/json' -d '{"a":1}' https://example.com/api
#
# Notes:
# - Logs the constructed curl command (with args) to request.log.
# - Captures response body to response.log and headers to response.headers.log.
# - Also records status code and total time.

set -euo pipefail

REQ_LOG="${REQ_LOG:-request.log}"
RESP_LOG="${RESP_LOG:-response.log}"
HDR_LOG="${HDR_LOG:-response.headers.log}"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <curl-args...> <url>" >&2
  exit 1
fi

TS="$(date -Is)"
SEP="==================== ${TS} ===================="

mkdir -p "$(dirname -- "$REQ_LOG")" "$(dirname -- "$RESP_LOG")" "$(dirname -- "$HDR_LOG")" 2>/dev/null || true

# Log the request "command"
{
  echo "$SEP"
  echo "PWD: $(pwd)"
  echo -n "CMD: curl "
  printf '%q ' "$@"
  echo
} >> "$REQ_LOG"

# Append separator to response logs
{
  echo "$SEP"
} >> "$RESP_LOG"
{
  echo "$SEP"
} >> "$HDR_LOG"

# Temp files for this run
TMP_BODY="$(mktemp)"
TMP_HDRS="$(mktemp)"
TMP_META="$(mktemp)"
cleanup() { rm -f "$TMP_BODY" "$TMP_HDRS" "$TMP_META"; }
trap cleanup EXIT

# Run curl, capture:
# - body to TMP_BODY
# - headers to TMP_HDRS
# - meta (http_code/time/remote_ip/etc.) to TMP_META
curl -sS -D "$TMP_HDRS" -o "$TMP_BODY" \
  -w $'http_code=%{http_code}\ntime_total=%{time_total}\nremote_ip=%{remote_ip}\nurl_effective=%{url_effective}\n' \
  "$@" > "$TMP_META"

# Append results
{
  echo "--- META ---"
  cat "$TMP_META"
  echo
  echo "--- BODY ---"
  cat "$TMP_BODY"
  echo
} >> "$RESP_LOG"

{
  echo "--- HEADERS ---"
  cat "$TMP_HDRS"
  echo
} >> "$HDR_LOG"

echo "Wrote:"
echo "  Request log : $REQ_LOG"
echo "  Response log: $RESP_LOG"
echo "  Header log  : $HDR_LOG"
exit 0