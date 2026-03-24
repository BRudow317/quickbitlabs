#!/usr/bin/env bash
set -euo pipefail

ss -lntupeH | awk '
function proc(pidstr,  m) {
  # users:(("name",pid=1234,fd=3))
  if (match(pidstr, /"[^"]+".*pid=[0-9]+/, m)) return m[0];
  return pidstr;
}
{
  local=$5;
  # local address:port is the last ":" segment (works for IPv4; IPv6 shows [addr]:port in ss)
  port=local;
  sub(/^.*:/, "", port);
  gsub(/users:\(/, "", $0);
  # print: proto port pid/proc (best-effort)
  printf "%-4s %-6s %s\n", $1, port, proc($0);
}
'
