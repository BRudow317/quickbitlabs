#!/usr/bin/env bash
# List available commands (external PATH commands + builtins + keywords + functions + aliases)
# Usage:
#   ./list_commands.sh                 # lists everything in this shell context
#   ./list_commands.sh ssh             # filter (case-insensitive)
#   source ./list_commands.sh          # to reflect functions/aliases in *current* shell session
#   ./list_commands.sh --source-bashrc # also load ~/.bashrc into this script before listing

set -euo pipefail

SOURCE_BASHRC=0
PATTERN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-bashrc) SOURCE_BASHRC=1; shift ;;
    *) PATTERN="${1:-}"; shift ;;
  esac
done

if (( SOURCE_BASHRC )); then
  shopt -s expand_aliases
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
fi

filter() {
  if [[ -n "${PATTERN}" ]]; then
    grep -i -- "${PATTERN}" || true
  else
    cat
  fi
}

print_section() {
  local title="$1"
  shift
  echo "=== ${title} ==="
  if [[ $# -gt 0 ]]; then
    printf '%s\n' "$@" | filter | column -x
  else
    echo "(none)"
  fi
  echo
}

mapfile -t ALL_CMDS      < <(compgen -c | sort -u)
mapfile -t ALIASES       < <(compgen -a | sort -u)
mapfile -t FUNCTIONS     < <(compgen -A function | sort -u)
mapfile -t BUILTINS      < <(compgen -b | sort -u)
mapfile -t KEYWORDS      < <(compgen -k | sort -u)

# External commands = all command names minus aliases/functions/builtins/keywords
mapfile -t NON_EXTERNAL  < <(printf '%s\n' "${ALIASES[@]}" "${FUNCTIONS[@]}" "${BUILTINS[@]}" "${KEYWORDS[@]}" | sort -u)
mapfile -t EXTERNAL_CMDS < <(comm -23 <(printf '%s\n' "${ALL_CMDS[@]}") <(printf '%s\n' "${NON_EXTERNAL[@]}"))

print_section "External commands in \$PATH" "${EXTERNAL_CMDS[@]}"
print_section "Bash builtins"               "${BUILTINS[@]}"
print_section "Shell keywords"              "${KEYWORDS[@]}"
print_section "Functions (current shell)"   "${FUNCTIONS[@]}"
print_section "Aliases (current shell)"     "${ALIASES[@]}"
print_section "All executable command names (combined)" "${ALL_CMDS[@]}"
