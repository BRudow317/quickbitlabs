from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

# Seed the minimum required env vars before any test module is collected.
# settings.py instantiates Settings() at module level, so these must be present
# before the first import.  Individual tests that exercise missing-var behavior
# use monkeypatch to remove them within the test scope, which is cleaned up
# automatically after each test.
os.environ.setdefault("JWT_SECRET", "_test_jwt_secret_")
os.environ.setdefault("UPLOAD_ENCRYPTION_KEY", "_test_upload_key_")


"""Oracle test bootstrap.

By default, use the real ``oracledb`` driver so live integration tests run
against an actual database connection.

Set ``ORACLE_TEST_USE_MOCKS=1`` to opt into global driver mocking for
isolated/offline runs.
"""
if os.getenv("ORACLE_TEST_USE_MOCKS", "0") == "1":
    _ORACLEDB_SUBMODULES = [
        "oracledb",
        "oracledb.arrow_impl",
        "oracledb.base",
        "oracledb.errors",
    ]

    for _mod in _ORACLEDB_SUBMODULES:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()