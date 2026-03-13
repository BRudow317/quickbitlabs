#!/usr/bin/env bash
# Ubuntu/Debian: list installed packages with their install status (verbose)
set -euo pipefail

apt list --installed 2>/dev/null
