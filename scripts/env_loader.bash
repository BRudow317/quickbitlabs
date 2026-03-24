#!/usr/bin/env bash
# Load variables from a .env file into the current shell session.
# example .env file:
#   VAR1=value1
# Use: source ./load_env.sh [path/to/.env]
# source ./load_env.sh ./app.env


set -euo pipefail

ENV_FILE="${1:-.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env file not found: $ENV_FILE" >&2
  return 1 2>/dev/null || exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

echo "Loaded env from: $ENV_FILE"
