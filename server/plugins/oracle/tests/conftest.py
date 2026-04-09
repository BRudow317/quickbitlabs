"""Oracle test bootstrap.

By default, use the real ``oracledb`` driver so live integration tests run
against an actual database connection.

Set ``ORACLE_TEST_USE_MOCKS=1`` to opt into global driver mocking for
isolated/offline runs.
"""
import os
import sys
from unittest.mock import MagicMock

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
