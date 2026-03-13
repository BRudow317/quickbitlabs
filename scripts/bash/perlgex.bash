#!/usr/bin/env bash
set -euo pipefail

# Usage: ./parse_9digits.sh /path/to/logfile
LOG_FILE="${1:-}"
if [[ -z "$LOG_FILE" || ! -f "$LOG_FILE" ]]; then
  echo "Usage: $0 /path/to/logfile" >&2
  exit 2
fi

# Find sequences that become 9 digits after removing spaces and dashes.
# Prints each 9-digit match to stdout (deduped), one per line.
perl -ne '
  my $line = $_;
  $line =~ s/[ -]//g;                 # remove spaces and dashes
  while ($line =~ /(\d{9})/g) {       # find 9 digits in a row
    print "$1\n";
  }
' "$LOG_FILE" | sort -u
