"""
OracleService input-size mapping tests.

These tests validate that the service builds setinputsizes() hints aligned with
python-oracledb guidance for fast executemany() inserts.

Reference docs:
- https://python-oracledb.readthedocs.io/en/latest/user_guide/batch_statement.html
- https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html#reducing-the-sql-version-count
- https://python-oracledb.readthedocs.io/en/latest/api_manual/cursor.html#oracledb.Cursor.setinputsizes
"""

from __future__ import annotations

import oracledb

from server.plugins.PluginModels import Catalog, Column, Entity
from server.plugins.oracle.OracleServices import OracleService


def _service() -> OracleService:
    # Bypass __init__ because these tests only exercise _build_input_sizes.
    return OracleService.__new__(OracleService)


def _catalog_with_columns(*columns: Column) -> Catalog:
    return Catalog(entities=[Entity(name="T", qualified_name="T", columns=list(columns))])


def test_build_input_sizes_keeps_python_type_and_uses_numeric_string_hints() -> None:
    service = _service()
    col_id = Column(
        name="ID",
        raw_type="NUMBER",
        properties={"python_type": "integer"},
    )
    col_name = Column(
        name="NAME",
        raw_type="VARCHAR2",
        max_length=120,
        properties={"python_type": "string"},
    )

    sizes = service._build_input_sizes(_catalog_with_columns(col_id, col_name))

    assert sizes["ID"] == oracledb.DB_TYPE_NUMBER
    assert sizes["NAME"] == 120
    assert col_id.properties["python_type"] == "integer"
    assert col_name.properties["python_type"] == "string"
    assert col_name.properties["oracle_ddl"] == "VARCHAR2(120 CHAR)"


def test_build_input_sizes_uses_clob_for_large_string() -> None:
    service = _service()
    col = Column(
        name="DOC",
        raw_type="CLOB",
        max_length=10000,
        properties={"python_type": "string"},
    )

    sizes = service._build_input_sizes(_catalog_with_columns(col))

    assert sizes["DOC"] == oracledb.DB_TYPE_CLOB


def test_build_input_sizes_maps_temporal_binary_and_json() -> None:
    service = _service()
    cols = [
        Column(name="TS", raw_type="TIMESTAMP", properties={"python_type": "datetime"}),
        Column(name="D", raw_type="DATE", properties={"python_type": "date"}),
        Column(name="B", raw_type="BLOB", properties={"python_type": "binary"}),
        Column(name="J", raw_type="JSON", properties={"python_type": "json"}),
    ]

    sizes = service._build_input_sizes(_catalog_with_columns(*cols))

    assert sizes["TS"] == getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
    assert sizes["D"] == oracledb.DB_TYPE_DATE
    assert sizes["B"] == oracledb.DB_TYPE_BLOB
    assert sizes["J"] == getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)


def test_build_input_sizes_can_derive_python_type_from_raw_type() -> None:
    service = _service()
    col = Column(name="SCORE", raw_type="NUMBER", scale=0, properties={})

    sizes = service._build_input_sizes(_catalog_with_columns(col))

    assert col.properties["python_type"] == "integer"
    assert sizes["SCORE"] == oracledb.DB_TYPE_NUMBER
