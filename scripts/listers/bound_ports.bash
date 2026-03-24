#!/usr/bin/env bash
# Ubuntu: list listening (bound) ports with process name + PID
set -euo pipefail

# -l = listening, -t/-u = tcp/udp, -p = process, -n = numeric, -e = extra, -H = no header
ss -lntupeH | awk '
  {
    # columns vary a bit; print the whole line but normalize spacing
    gsub(/[[:space:]]+/, " ");
    print
  }
' | sed 's/users:(/ users:(/'

echo
echo "Tip: to filter:  ss -lntupeH | grep -E \":80\\b|:443\\b\""
