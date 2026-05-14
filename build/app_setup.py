from __future__ import annotations
import logging, os, subprocess, secrets
import argparse
import re
import sys
from pathlib import Path
from build.db_setup import sql_runner

logger = logging.getLogger(__name__)



def user_setup(
        username: str,
        email: str | None = None,
        password: str | None = None,
        role: str | None = None,
        is_active: bool = True,
        ) -> int:
    from server.core.security import get_password_hash
    if email is None and password is None and role is None:
        key = (username or '').upper()
        username_ = os.getenv(f"QBL_{key}_USER", key)
        email_ = os.getenv(f"QBL_{key}_EMAIL", '')
        password_ = os.getenv(f"QBL_{key}_PWD", '') or os.getenv(f"QBL_{key}_PASS", '')
        role_ = os.getenv(f"QBL_{key}_ROLE", '') or os.getenv(f"QBL_{key}_USER_ROLE", '')
    else:
        username_ = username
        email_ = email or ''
        password_ = password or secrets.token_urlsafe(16)
        role_ = role or ''

    if not username_:
        raise ValueError("user_setup: username could not be resolved")
    if not email_:
        raise ValueError(f"user_setup: email is required for '{username_}'")
    if not password_:
        raise ValueError(f"user_setup: password is required for '{username_}'")

    role_ = (role_ or 'USER').upper()
    hashed = get_password_hash(password_)
    is_active_ = 1 if is_active else 0

    merge_sql = f"""
        MERGE INTO QBL_USERS tgt
        USING (SELECT '{username_}' AS USERNAME FROM DUAL) src
        ON (tgt.USERNAME = src.USERNAME)
        WHEN MATCHED THEN
            UPDATE SET password_hash = '{hashed}',
                       IS_ACTIVE     = {is_active_}
        WHEN NOT MATCHED THEN
            INSERT (username, email, password_hash, qbl_role, IS_ACTIVE)
            VALUES ('{username_}', '{email_}', '{hashed}', '{role_}', {is_active_});
    COMMIT;
    """
    result = sql_runner(merge_sql, username="SYSDBA", schema="QBL")
    stdout_upper = result.stdout.upper()
    if result.returncode != 0 or "ORA-" in stdout_upper or "SP2-" in stdout_upper:
        logger.error("user_setup failed for '%s':\n%s", username_, result.stdout.strip())
        sys.exit(1)
    return result.returncode

