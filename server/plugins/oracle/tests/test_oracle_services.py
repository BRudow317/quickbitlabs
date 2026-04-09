"""
Oracle Plugin - OracleService unit tests.
Tests the service layer in isolation using mocks.

Usage:
    python scripts/boot.py --env homelab --config "Q:/.secrets/.env" --log server/plugins/oracle/tests --exec pytest -v server/plugins/oracle/tests

    pytest -v server/plugins/oracle/tests/test_oracle_services.py
    
    pytest server/plugins/oracle/tests/test_oracle_services.py -v -s --tb=short
"""
from __future__ import annotations

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch, call

from server.plugins.PluginModels import Catalog, Entity, Column
from server.plugins.oracle.OracleServices import OracleService


#-----------------------------------------------------------------
# Factories
#-----------------------------------------------------------------

def make_column(name: str, raw_type: str = "string", primary_key: bool = False) -> Column:
    return Column(
        name=name,
        qualified_name=name,
        raw_type=raw_type,
        arrow_type_id="string",
        primary_key=primary_key,
        properties={"python_type": "string"},
    )


def make_entity(name: str, columns: list[Column]) -> Entity:
    return Entity(name=name, qualified_name=name, columns=columns)


def make_catalog(*entities: Entity) -> Catalog:
    return Catalog(entities=list(entities))


def make_stream(records: list[dict], schema: pa.Schema | None = None) -> pa.RecordBatchReader:
    if not records:
        s = schema or pa.schema([])
        return pa.RecordBatchReader.from_batches(s, iter([]))
    if schema is None:
        schema = pa.schema({k: pa.string() for k in records[0]})
    batch = pa.record_batch(
        {k: [r[k] for r in records] for k in schema.names},
        schema=schema,
    )
    return pa.RecordBatchReader.from_batches(schema, iter([batch]))


def make_service() -> tuple[OracleService, MagicMock, MagicMock]:
    """
    Bypass __init__ and wire mock engine + arrow_frame directly.
    Returns (service, mock_engine, mock_arrow_frame).
    """
    service = OracleService.__new__(OracleService)
    service.client = MagicMock()
    service.client.oracle_user = "TESTUSER"
    service.engine = MagicMock()
    service.arrow_frame = MagicMock()
    # Keep _build_input_sizes real unless a test needs to override it
    return service, service.engine, service.arrow_frame


# ---------------------------------------------------------------------------
# OracleService.get_data
# ---------------------------------------------------------------------------

class TestOracleServiceGetData:

    def test_raw_query_kwarg_calls_arrow_stream(self):
        """When query= is supplied, arrow_frame.arrow_stream is called with it directly."""
        service, _, mock_frame = make_service()
        catalog = make_catalog(make_entity("emp", [make_column("id")]))

        service.get_data(catalog, query="SELECT 1 FROM DUAL")

        mock_frame.arrow_stream.assert_called_once()
        args, kwargs = mock_frame.arrow_stream.call_args
        assert "SELECT 1 FROM DUAL" in (args + tuple(kwargs.values()))

    def test_catalog_path_calls_build_select_then_arrow_stream(self):
        """When no query= kwarg, build_select is called and its output drives arrow_frame.arrow_stream."""
        service, _, mock_frame = make_service()
        catalog = make_catalog(make_entity("emp", [make_column("id")]))

        with patch(
            "server.plugins.oracle.OracleServices.build_select",
            return_value=("SELECT id FROM emp", {}),
        ) as mock_build:
            service.get_data(catalog)

        mock_build.assert_called_once_with(catalog)
        mock_frame.arrow_stream.assert_called_once()

    def test_returns_arrow_stream_result(self):
        """get_data passes the arrow_stream result straight back to the caller."""
        service, _, mock_frame = make_service()
        expected = MagicMock()
        mock_frame.arrow_stream.return_value = expected

        result = service.get_data(
            make_catalog(make_entity("emp", [make_column("id")])),
            query="SELECT id FROM emp",
        )

        assert result is expected


# ---------------------------------------------------------------------------
# OracleService.insert_data
# ---------------------------------------------------------------------------

class TestOracleServiceInsertData:

    def test_calls_execute_many_once_for_single_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        catalog = make_catalog(make_entity("employees", [make_column("name")]))

        service.insert_data(catalog, make_stream([{"name": "Alice"}]))

        mock_frame.execute_many.assert_called_once()

    def test_calls_execute_many_once_per_entity(self):
        """Each entity in the catalog gets its own execute_many call."""
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        catalog = make_catalog(
            make_entity("emp", [make_column("name")]),
            make_entity("dept", [make_column("title")]),
        )

        service.insert_data(catalog, make_stream([{"name": "Alice", "title": "Eng"}]))

        assert mock_frame.execute_many.call_count == 2

    def test_insert_sql_targets_correct_entity(self):
        """The generated INSERT SQL references the entity name and column binds."""
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        entity = make_entity("employees", [make_column("name"), make_column("dept_id")])
        catalog = make_catalog(entity)

        service.insert_data(catalog, make_stream([{"name": "Alice", "dept_id": "10"}]))

        sql = mock_frame.execute_many.call_args[0][0]
        assert "employees" in sql
        assert ":name" in sql
        assert ":dept_id" in sql

    def test_passes_data_stream_to_execute_many(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        catalog = make_catalog(make_entity("employees", [make_column("name")]))
        stream = make_stream([{"name": "Alice"}])

        service.insert_data(catalog, stream)

        assert mock_frame.execute_many.call_args[0][1] is stream


# ---------------------------------------------------------------------------
# OracleService.update_data
# ---------------------------------------------------------------------------

class TestOracleServiceUpdateData:

    def test_returns_error_when_no_entities(self):
        service, _, _ = make_service()
        result = service.update_data(Catalog(entities=[]), make_stream([]))
        assert result.ok is False

    def test_calls_execute_many(self):
        service, _, mock_frame = make_service()
        entity = make_entity("employees", [make_column("name")])
        stream = make_stream([{"name": "Alice"}])

        service.update_data(make_catalog(entity), stream)

        mock_frame.execute_many.assert_called_once()

    def test_passes_stream_to_execute_many(self):
        service, _, mock_frame = make_service()
        entity = make_entity("employees", [make_column("name")])
        stream = make_stream([{"name": "Alice"}])

        service.update_data(make_catalog(entity), stream)

        kwargs = mock_frame.execute_many.call_args.kwargs
        assert kwargs.get("data") is stream

    def test_passes_one_sql_generator_entry_per_entity(self):
        """The statements generator yielded to execute_many has one entry per entity."""
        service, _, mock_frame = make_service()
        catalog = make_catalog(
            make_entity("emp", [make_column("name")]),
            make_entity("dept", [make_column("title")]),
        )
        service.update_data(catalog, make_stream([{"name": "Alice"}]))

        sql_arg = mock_frame.execute_many.call_args.kwargs.get("sql")
        # Consume the generator — build_update_dml runs here, but we just check length
        with patch("server.plugins.oracle.OracleServices.build_update_dml", return_value=("UPDATE t SET x=:x WHERE 1=1", {})):
            assert len(list(sql_arg)) == len(catalog.entities)

    def test_returns_success_response_on_no_exception(self):
        service, _, _ = make_service()
        entity = make_entity("employees", [make_column("name")])
        result = service.update_data(make_catalog(entity), make_stream([{"name": "Alice"}]))
        assert result.ok is True
        assert result.code == 200
        assert result.data is None

    def test_returns_error_response_on_exception(self):
        service, _, mock_frame = make_service()
        mock_frame.execute_many.side_effect = RuntimeError("connection lost")
        entity = make_entity("employees", [make_column("name")])

        result = service.update_data(make_catalog(entity), make_stream([{"name": "Alice"}]))

        assert result.ok is False
        assert "connection lost" in result.message


# ---------------------------------------------------------------------------
# OracleService.upsert_data
# ---------------------------------------------------------------------------

class TestOracleServiceUpsertData:

    def test_calls_execute_many_once_for_single_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        entity = make_entity("employees", [make_column("id", primary_key=True), make_column("name")])

        service.upsert_data(make_catalog(entity), make_stream([{"id": "1", "name": "Alice"}]))

        mock_frame.execute_many.assert_called_once()

    def test_calls_execute_many_once_per_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        catalog = make_catalog(
            make_entity("emp",  [make_column("id", primary_key=True), make_column("name")]),
            make_entity("dept", [make_column("id", primary_key=True), make_column("title")]),
        )

        service.upsert_data(catalog, make_stream([{"id": "1", "name": "Alice", "title": "Eng"}]))

        assert mock_frame.execute_many.call_count == 2

    def test_merge_sql_targets_correct_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        entity = make_entity("employees", [make_column("id", primary_key=True), make_column("name")])

        service.upsert_data(make_catalog(entity), make_stream([{"id": "1", "name": "Alice"}]))

        sql = mock_frame.execute_many.call_args[0][0]
        assert "MERGE" in sql.upper()
        assert "employees" in sql

    def test_raises_when_entity_has_no_primary_keys(self):
        service, _, _ = make_service()
        entity = make_entity("employees", [make_column("name", primary_key=False)])

        with pytest.raises(ValueError):
            service.upsert_data(make_catalog(entity), make_stream([{"name": "Alice"}]))


# ---------------------------------------------------------------------------
# OracleService.delete_data
# ---------------------------------------------------------------------------

class TestOracleServiceDeleteData:
    def test_calls_execute_many_once_for_single_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        entity = make_entity("employees", [make_column("id", primary_key=True)])

        with patch(
            "server.plugins.oracle.OracleServices.build_delete_dml",
            return_value=("DELETE FROM employees", {}),
        ):
            service.delete_data(make_catalog(entity), make_stream([{"id": "1"}]))

        mock_frame.execute_many.assert_called_once()

    def test_calls_execute_many_once_per_entity(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        catalog = make_catalog(
            make_entity("emp",  [make_column("id", primary_key=True)]),
            make_entity("dept", [make_column("id", primary_key=True)]),
        )

        with patch(
            "server.plugins.oracle.OracleServices.build_delete_dml",
            return_value=("DELETE FROM t", {}),
        ):
            service.delete_data(catalog, make_stream([{"id": "1"}]))

        assert mock_frame.execute_many.call_count == 2

    def test_passes_data_stream_to_execute_many(self):
        service, _, mock_frame = make_service()
        service._build_input_sizes = MagicMock(return_value={})
        entity = make_entity("employees", [make_column("id", primary_key=True)])
        stream = make_stream([{"id": "1"}])

        with patch(
            "server.plugins.oracle.OracleServices.build_delete_dml",
            return_value=("DELETE FROM employees", {}),
        ):
            service.delete_data(make_catalog(entity), stream)

        kwargs = mock_frame.execute_many.call_args.kwargs
        assert kwargs.get("data") is stream


# ---------------------------------------------------------------------------
# OracleService.get_catalog
# ---------------------------------------------------------------------------

class TestOracleServiceGetCatalog:

    def test_empty_catalog_hydrates_tables_and_columns(self):
        service, _, _ = make_service()
        catalog = Catalog()

        with patch.object(service, "_list_schema_tables", return_value=["EMP", "DEPT"]), patch.object(
            service,
            "_fetch_table_primary_keys",
            side_effect=[{"ID"}, {"ID"}],
        ), patch.object(
            service,
            "_fetch_table_columns",
            side_effect=[
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "CHAR_LENGTH": None,
                        "DATA_PRECISION": 38,
                        "DATA_SCALE": 0,
                        "NULLABLE": "N",
                        "COLUMN_ID": 1,
                    },
                    {
                        "COLUMN_NAME": "NAME",
                        "DATA_TYPE": "VARCHAR2",
                        "CHAR_LENGTH": 255,
                        "DATA_PRECISION": None,
                        "DATA_SCALE": None,
                        "NULLABLE": "Y",
                        "COLUMN_ID": 2,
                    },
                ],
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "CHAR_LENGTH": None,
                        "DATA_PRECISION": 38,
                        "DATA_SCALE": 0,
                        "NULLABLE": "N",
                        "COLUMN_ID": 1,
                    }
                ],
            ],
        ):
            out = service.get_catalog(catalog)

        assert out.name == "TESTUSER"
        assert out.qualified_name == "TESTUSER"
        assert len(out.entities) == 2
        assert out.entities[0].name == "EMP"
        assert out.entities[0].qualified_name == "TESTUSER.EMP"
        assert [c.name for c in out.entities[0].columns] == ["ID", "NAME"]
        assert out.entities[0].columns[0].primary_key is True

    def test_entities_without_columns_hydrate_full_columns(self):
        service, _, _ = make_service()
        catalog = Catalog(
            entities=[
                Entity(name="EMP", qualified_name="EMP", columns=[]),
            ]
        )

        with patch.object(service, "_fetch_table_primary_keys", return_value={"ID"}), patch.object(
            service,
            "_fetch_table_columns",
            return_value=[
                {
                    "COLUMN_NAME": "ID",
                    "DATA_TYPE": "NUMBER",
                    "CHAR_LENGTH": None,
                    "DATA_PRECISION": 38,
                    "DATA_SCALE": 0,
                    "NULLABLE": "N",
                    "COLUMN_ID": 1,
                },
                {
                    "COLUMN_NAME": "NAME",
                    "DATA_TYPE": "VARCHAR2",
                    "CHAR_LENGTH": 255,
                    "DATA_PRECISION": None,
                    "DATA_SCALE": None,
                    "NULLABLE": "Y",
                    "COLUMN_ID": 2,
                },
            ],
        ) as fetch_cols:
            out = service.get_catalog(catalog)

        assert out.entities[0].qualified_name == "TESTUSER.EMP"
        assert [c.name for c in out.entities[0].columns] == ["ID", "NAME"]
        fetch_cols.assert_called_once_with("TESTUSER", "EMP", None)

    def test_entities_with_columns_hydrate_only_requested_columns(self):
        service, _, _ = make_service()
        catalog = Catalog(
            entities=[
                Entity(
                    name="EMP",
                    qualified_name="EMP",
                    columns=[
                        Column(name="NAME", qualified_name="EMP.NAME"),
                    ],
                )
            ]
        )

        with patch.object(service, "_fetch_table_primary_keys", return_value=set()), patch.object(
            service,
            "_fetch_table_columns",
            return_value=[
                {
                    "COLUMN_NAME": "NAME",
                    "DATA_TYPE": "VARCHAR2",
                    "CHAR_LENGTH": 255,
                    "DATA_PRECISION": None,
                    "DATA_SCALE": None,
                    "NULLABLE": "Y",
                    "COLUMN_ID": 2,
                }
            ],
        ) as fetch_cols:
            out = service.get_catalog(catalog)

        assert len(out.entities[0].columns) == 1
        assert out.entities[0].columns[0].name == "NAME"
        fetch_cols.assert_called_once_with("TESTUSER", "EMP", {"NAME"})
