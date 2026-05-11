#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
def main(username, email, password, role) -> int:

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from server.db.db import server_db
    from server.core.security import get_password_hash

    hashed = get_password_hash(password)
    con = server_db.connect()

    # MERGE: update password_hash if USERNAME exists, insert new row otherwise
    merge_sql = """
        MERGE INTO QBL_USERS tgt
        USING (SELECT :username AS USERNAME FROM DUAL) src
        ON (tgt.USERNAME = src.USERNAME)
        WHEN MATCHED THEN
            UPDATE SET password_hash = :hashed,
                       IS_ACTIVE     = 1
        WHEN NOT MATCHED THEN
            INSERT (USERNAME, EMAIL, password_hash, qbl_role, IS_ACTIVE)
            VALUES (:username, :email, :hashed, :role, 1)
    """
    with con.cursor() as cur:
        cur.execute(merge_sql, {"username": username, "email": email, "hashed": hashed, "role": role})
    con.commit()

    print(f"User '{username}' ready.")
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or update a user in the USER table")
    parser.add_argument("username")
    parser.add_argument("email")
    parser.add_argument("password")
    parser.add_argument("role")
    args = parser.parse_args()
    sys.exit(main(args.username, args.email, args.password, args.role))
