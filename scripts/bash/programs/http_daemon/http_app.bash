#!/usr/bin/env bash
# /usr/local/bin/http-app.sh
# Example "business logic" script.
# Prints CGI-lite headers + blank line + body.

set -euo pipefail

REQ_BODY="$(cat || true)"

case "${REQUEST_PATH:-/}" in
  /health)
    printf 'Status: 200\nContent-Type: application/json\n\n{"status":"ok"}\n'
    ;;
  /hello)
    name="world"
    if [[ "${QUERY_STRING:-}" == name=* ]]; then
      name="${QUERY_STRING#name=}"
    fi
    printf 'Status: 200\nContent-Type: text/html\n\n'
    printf '<!doctype html><html><body><h1>Hello %s</h1><p>method=%s</p></body></html>\n' \
      "$name" "${REQUEST_METHOD:-}"
    ;;
  /echo)
    printf 'Status: 200\nContent-Type: text/plain\n\n'
    printf 'method=%s\npath=%s\nquery=%s\nremote=%s:%s\n\nbody:\n%s\n' \
      "${REQUEST_METHOD:-}" "${REQUEST_PATH:-}" "${QUERY_STRING:-}" \
      "${REMOTE_ADDR:-}" "${REMOTE_PORT:-}" "$REQ_BODY"
    ;;
  *)
    printf 'Status: 404\nContent-Type: text/plain\n\nNot found: %s\n' "${REQUEST_PATH:-}"
    ;;
esac
