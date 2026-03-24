#!/usr/bin/env bash
# List users known to NSS (includes LDAP/SSSD/etc if configured)
set -euo pipefail

getent passwd | cut -d: -f1
