#!/usr/bin/env bash
# List local users from /etc/passwd (Ubuntu)
set -euo pipefail

# username:uid:home:shell
awk -F: '{printf "%-20s uid=%-6s home=%-25s shell=%s\n",$1,$3,$6,$7}' /etc/passwd
