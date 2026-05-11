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
        email: str|None = None, 
        password: str|None = None, 
        role: str|None = None, 
        is_active: bool = True
        ) -> int:
    from server.core.security import get_password_hash
    if email is None and password is None and role is None:
        # If only username is provided, attempt to read all values from environment variables.
        key = (username or '').upper()
        username_ = os.getenv(f"QBL_{key}_USER", key)
        email_ = os.getenv(f"QBL_{key}_EMAIL", '')
        password_ = os.getenv(f"QBL_{key}_PWD", '')
        role_ = os.getenv(f"QBL_{key}_ROLE", "USER")
    else:
        username_ = username
        email_ = email
        password_: str = password or secrets.token_urlsafe(16)  # Generate a random password if not provided.
        role_ = role or "USER"

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
            INSERT (USERNAME, EMAIL, password_hash, qbl_role, IS_ACTIVE)
            VALUES ('{username_}', '{email_}', '{hashed}', '{role_}', {is_active_})
    """
    return sql_runner(merge_sql).returncode

