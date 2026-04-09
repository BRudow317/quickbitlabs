"""
Stubs out oracledb before any oracle plugin modules are imported,
preventing connection attempts at class/module definition time.
"""
import sys
from unittest.mock import MagicMock

_ORACLEDB_SUBMODULES = [
    "oracledb",
    "oracledb.arrow_impl",
    "oracledb.base",
    "oracledb.errors",
]

for _mod in _ORACLEDB_SUBMODULES:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()
