#!/usr/bin/env bash
set -euo pipefail

read -r -p "Enter SQL file name (e.g., script.sql): " SQLFILE
if [[ -z "$SQLFILE" ]]; then
  echo "No file provided." >&2
  exit 1
fi

# If user entered just a name, look in current directory first
if [[ ! -f "$SQLFILE" ]]; then
  if [[ -f "./$SQLFILE" ]]; then
    SQLFILE="./$SQLFILE"
  else
    echo "File not found: $SQLFILE" >&2
    exit 2
  fi
fi

if ! command -v sqlplus >/dev/null 2>&1; then
  echo "sqlplus not found in PATH. Install Oracle Instant Client (sqlplus) and ensure sqlplus is on PATH." >&2
  exit 3
fi

# Absolute path helps SQL*Plus find the file reliably
ABS_SQLFILE="$(readlink -f "$SQLFILE" 2>/dev/null || realpath "$SQLFILE" 2>/dev/null || echo "$SQLFILE")"

read -r -p "DB connect identifier (e.g., //host:1521/service or TNS alias): " DBID
if [[ -z "$DBID" ]]; then
  echo "No connect identifier provided." >&2
  exit 4
fi

read -r -p "DB username: " DBUSER
if [[ -z "$DBUSER" ]]; then
  echo "No username provided." >&2
  exit 5
fi

read -r -s -p "DB password (hidden): " DBPASS
echo
if [[ -z "$DBPASS" ]]; then
  echo "No password provided." >&2
  exit 6
fi

read -r -p "Connect as SYSDBA? (y/N): " SYSDBA
CONNECT_CMD="connect ${DBUSER}/${DBPASS}@${DBID}"
if [[ "${SYSDBA,,}" == "y" ]]; then
  CONNECT_CMD="connect ${DBUSER}/${DBPASS}@${DBID} as sysdba"
fi

# Stream output to terminal (default). -S reduces banners; /nolog avoids password in process list.
printf '%s\n' \
  "whenever sqlerror exit 1" \
  "set define off" \
  "$CONNECT_CMD" \
  "@\"$ABS_SQLFILE\"" \
  "exit" \
| sqlplus -S -L /nolog
