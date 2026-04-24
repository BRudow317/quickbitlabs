"""
CLI tool to create or update application credentials in the USER table.

Usage:
    python server/tools/create_user.py <username> <email> <password> [--env homelab] [--config Q:/.secrets/.env]

If the USERNAME already exists (e.g. a Salesforce-migrated user), this sets their
HASHED_PASSWORD so they can log into the application.  If the row does not exist yet,
a new one is inserted with the supplied email.

The tool loads the .env file and expands ${env} placeholders so it works standalone
without going through a boot loader.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# .env loader with ${env} substitution
# ---------------------------------------------------------------------------

def _load_env(config_path: str, env_name: str) -> dict[str, str]:
    raw = Path(config_path).read_text(encoding="utf-8")
    expanded = raw.replace("${env}", env_name)

    result: dict[str, str] = {}
    for line in expanded.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        result[key.strip()] = val.strip().strip('"').strip("'")

    def _resolve(v: str) -> str:
        # Handle ${VAR} style references
        resolved = re.sub(r'\$\{([^}]+)\}', lambda m: result.get(m.group(1), m.group(0)), v)
        # Handle bare indirect references: after ${env} substitution a value like
        # "oracle_homelab_port" is itself a key whose value is the real setting.
        if resolved in result:
            resolved = result[resolved]
        return resolved

    return {k: _resolve(v) for k, v in result.items()}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Create or update a user in the USER table")
    parser.add_argument("username")
    parser.add_argument("email")
    parser.add_argument("password")
    parser.add_argument("--env",    default="homelab",            help="Environment name (replaces ${env} in .env)")
    parser.add_argument("--config", default=r"Q:/.secrets/.env", help="Path to .env file")
    args = parser.parse_args()

    try:
        env_vars = _load_env(args.config, args.env)
    except FileNotFoundError:
        print(f"ERROR: config file not found: {args.config}", file=sys.stderr)
        return 1

    # Force-override so we replace any un-substituted template values already
    # present in the shell environment (e.g. ORACLE_PORT=oracle_${env}_port
    # set by the PowerShell profile before ${env} was resolved).
    for k, v in env_vars.items():
        os.environ[k] = v

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from server.db.ServerDatabase import OracleClient
    from server.core.security import get_password_hash

    try:
        client = OracleClient()
    except Exception as exc:
        print(f"ERROR: could not connect to Oracle: {exc}", file=sys.stderr)
        return 1

    hashed = get_password_hash(args.password)
    con = client.connect()

    # MERGE: update HASHED_PASSWORD if USERNAME exists, insert new row otherwise
    merge_sql = """
        MERGE INTO "USER" tgt
        USING (SELECT :username AS USERNAME FROM DUAL) src
        ON (tgt.USERNAME = src.USERNAME)
        WHEN MATCHED THEN
            UPDATE SET HASHED_PASSWORD = :hashed,
                       IS_ACTIVE       = 1
        WHEN NOT MATCHED THEN
            INSERT (USERNAME, EMAIL, HASHED_PASSWORD, IS_ACTIVE)
            VALUES (:username, :email, :hashed, 1)
    """
    with con.cursor() as cur:
        cur.execute(merge_sql, username=args.username, email=args.email, hashed=hashed)
    con.commit()

    print(f"User '{args.username}' ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
