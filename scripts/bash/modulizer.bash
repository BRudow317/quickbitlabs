#!/usr/bin/env bash
# Generates/updates a "module import" file that adds a function per bash script in a directory.
# Each generated function name is based on the script filename and executes that script with passed args.
#
# Usage:
#   ./gen_bash_module.sh /path/to/scripts /path/to/module_imports.sh
#
# Then:
#   source /path/to/module_imports.sh
#   <function_name> arg1 arg2 ...
# Example:
# chmod +x ./gen_bash_module.sh
# ./gen_bash_module.sh ./scripts ./module_imports.sh

# source ./module_imports.sh
# # If you have scripts/backup-db.sh, it generates function: backup_db
# backup_db --help




set -euo pipefail

SRC_DIR="${1:-.}"
OUT_FILE="${2:-./module_imports.sh}"

if [[ ! -d "$SRC_DIR" ]]; then
  echo "ERROR: directory not found: $SRC_DIR" >&2
  exit 1
fi

OUT_DIR="$(cd -- "$(dirname -- "$OUT_FILE")" && pwd)"
mkdir -p "$OUT_DIR"

touch "$OUT_FILE"
chmod 0644 "$OUT_FILE"

# Create header once
if ! grep -q '^# GENERATED BASH MODULE IMPORTS' "$OUT_FILE" 2>/dev/null; then
  cat >>"$OUT_FILE" <<'HDR'
# GENERATED BASH MODULE IMPORTS
# This file was generated. It contains wrapper functions for scripts in a directory.
# Source it:  source /path/to/module_imports.sh
# Call wrappers: <scriptname> args...
HDR
  printf "\n" >>"$OUT_FILE"
fi

is_bash_script() {
  local f="$1"
  [[ -f "$f" ]] || return 1

  # If it ends with .sh, treat as bash script
  if [[ "$f" == *.sh ]]; then
    return 0
  fi

  # Otherwise detect bash shebang
  local first
  first="$(head -n 1 "$f" 2>/dev/null || true)"
  [[ "$first" =~ ^#!.*(bash|/usr/bin/env[[:space:]]+bash) ]] && return 0

  return 1
}

sanitize_func_name() {
  local name="$1"
  # replace non [a-zA-Z0-9_] with underscore
  name="${name//[^a-zA-Z0-9_]/_}"
  # can't start with a digit
  [[ "$name" =~ ^[0-9] ]] && name="_${name}"
  echo "$name"
}

append_wrapper_if_missing() {
  local script_path="$1"
  local base func rel marker

  base="$(basename "$script_path")"
  base="${base%.sh}"
  func="$(sanitize_func_name "$base")"

  # Make script path relative to OUT_FILE directory when possible
  if rel="$(realpath --relative-to="$OUT_DIR" "$script_path" 2>/dev/null)"; then
    : # ok
  else
    rel="$(realpath "$script_path" 2>/dev/null || echo "$script_path")"
  fi

  marker="# BEGIN WRAPPER: $func -> $rel"
  if grep -Fqx "$marker" "$OUT_FILE"; then
    return 0
  fi

  {
    printf "%s\n" "$marker"
    cat <<EOF
$func() {
  local _mod_dir
  _mod_dir="\$(cd -- "\$(dirname -- "\${BASH_SOURCE[0]}")" && pwd)"
  bash "\$_mod_dir/$rel" "\$@"
}
EOF
    printf "# END WRAPPER: %s\n\n" "$func"
  } >>"$OUT_FILE"
}

# Non-recursive by default. Set RECURSIVE=1 to include subdirectories.
if [[ "${RECURSIVE:-0}" == "1" ]]; then
  mapfile -d '' files < <(find "$SRC_DIR" -type f -print0)
else
  mapfile -d '' files < <(find "$SRC_DIR" -maxdepth 1 -type f -print0)
fi

for f in "${files[@]}"; do
  # Skip the output file if it's inside the same dir
  if [[ "$(realpath "$f" 2>/dev/null || echo "$f")" == "$(realpath "$OUT_FILE" 2>/dev/null || echo "$OUT_FILE")" ]]; then
    continue
  fi

  if is_bash_script "$f"; then
    append_wrapper_if_missing "$f"
  fi
done

echo "Updated: $OUT_FILE"
exit 0