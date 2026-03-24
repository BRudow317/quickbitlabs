#!/usr/bin/env bash
# Ubuntu/Debian: list installed packages
set -euo pipefail

# Name + version (dpkg is the canonical source)
dpkg-query -W -f='${binary:Package}\t${Version}\n' | sort
