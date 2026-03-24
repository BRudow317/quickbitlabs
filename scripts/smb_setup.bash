#!/usr/bin/env bash
# file: setup_smb_mount.sh
# Sets up a persistent SMB/CIFS mount using /etc/fstab + a credentials file.
#
# Usage:
#   sudo ./setup_smb_mount.sh //SERVER/SHARE /mnt/share smbuser 'smbpass' [DOMAIN]
#
# Notes:
# - Requires: cifs-utils
# - Creates: /etc/samba/creds-<mountname>
# - Adds:    an /etc/fstab entry (idempotent by match on //<server>/<share> + mountpoint)

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: run as root (use sudo)." >&2
  exit 1
fi

REMOTE="${1:-}"
MOUNTPOINT="${2:-}"
USER="${3:-}"
PASS="${4:-}"
DOMAIN="${5:-}"

if [[ -z "$REMOTE" || -z "$MOUNTPOINT" || -z "$USER" || -z "$PASS" ]]; then
  echo "Usage: sudo $0 //SERVER/SHARE /mnt/share smbuser 'smbpass' [DOMAIN]" >&2
  exit 2
fi

# Basic validation
if [[ "$REMOTE" != //*/* ]]; then
  echo "ERROR: REMOTE must look like //SERVER/SHARE" >&2
  exit 3
fi

# Ensure dependency
if ! rpm -q cifs-utils >/dev/null 2>&1; then
  echo "Installing cifs-utils..."
  dnf install -y cifs-utils
fi

# Create mountpoint
mkdir -p "$MOUNTPOINT"

# Choose a creds file name based on mountpoint basename (safe-ish)
MP_BASE="$(basename "$MOUNTPOINT")"
CREDS="/etc/samba/creds-${MP_BASE}"

mkdir -p /etc/samba
umask 077
{
  echo "username=${USER}"
  echo "password=${PASS}"
  if [[ -n "$DOMAIN" ]]; then
    echo "domain=${DOMAIN}"
  fi
} > "$CREDS"
chmod 600 "$CREDS"

# Determine uid/gid for the invoking non-root user if available; fallback to 0
REAL_USER="${SUDO_USER:-root}"
UID_VAL="$(id -u "$REAL_USER" 2>/dev/null || echo 0)"
GID_VAL="$(id -g "$REAL_USER" 2>/dev/null || echo 0)"

# Build options (tweak as needed)
# vers=3.0 works for most modern servers; change to 2.1/1.0 if required.
OPTS="credentials=${CREDS},iocharset=utf8,vers=3.0,_netdev,nofail,x-systemd.automount,uid=${UID_VAL},gid=${GID_VAL},file_mode=0664,dir_mode=0775"

FSTAB_LINE="${REMOTE} ${MOUNTPOINT} cifs ${OPTS} 0 0"

# Add/replace fstab entry (idempotent)
FSTAB="/etc/fstab"

# If an entry already exists for this mountpoint or remote, remove it first
TMP="$(mktemp)"
awk -v mp="$MOUNTPOINT" -v rem="$REMOTE" '
  BEGIN { removed=0 }
  $0 ~ "^[[:space:]]*#" { print; next }
  NF >= 2 && ($1 == rem || $2 == mp) { removed=1; next }
  { print }
' "$FSTAB" > "$TMP"
cat "$TMP" > "$FSTAB"
rm -f "$TMP"

echo "$FSTAB_LINE" >> "$FSTAB"

# Reload systemd mount units and mount
systemctl daemon-reload

echo "Mounting..."
mount "$MOUNTPOINT"

echo "OK."
echo "Remote:     $REMOTE"
echo "Mountpoint: $MOUNTPOINT"
echo "Creds:      $CREDS"
echo
echo "Verify:"
echo "  mount | grep -F \" $MOUNTPOINT \""
echo "  ls -la \"$MOUNTPOINT\""
