from __future__ import annotations
from typing import Any

from sqlalchemy import MetaData, Table as SATable, Column as SAColumn, text, inspect, Enum, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Session

from server.models.ConnectorStandard import Table, Column, Schema, DataStream
from server.connectors.postgres.postgres_utils.type_converter import pg_to_python_type, pg_source_type, PYTHON_TO_PG
from server.connectors.postgres.services.table_ops import PgObjType

import logging
logger = logging.getLogger(__name__)


class PgQuery:
    """Global Postgres operations: schema discovery, DDL, raw SQL, table factory."""

    def __init__(self, engine, metadata: MetaData):
        self._engine = engine
        self._metadata = metadata

    def __getattr__(self, name: str) -> PgObjType:
        """Dot-notation access to tables: pg.query.users.get('abc-123')"""
        if name.startswith('_'):
            return super().__getattribute__(name)
        return PgObjType(table_name=name, engine=self._engine, metadata=self._metadata)

    def table(self, name: str, pg_schema: str | None = None) -> PgObjType:
        """Schema-aware table accessor. Use this instead of __getattr__ when pg_schema matters."""
        return PgObjType(table_name=name, engine=self._engine, metadata=self._metadata, pg_schema=pg_schema)

    # ── Schema Discovery ──

    def list_tables(self, pg_schema: str | None = None) -> list[str]:
        """List all tables in a schema."""
        inspector = inspect(self._engine)
        return inspector.get_table_names(schema=pg_schema)

    def describe(self, table_name: str, pg_schema: str | None = None) -> Table:
        """Reflect a single table into the universal Table model."""
        sa_table = SATable(table_name, self._metadata, autoload_with=self._engine, schema=pg_schema)

        columns = []
        for col in sa_table.columns:
            columns.append(Column(
                source_name=col.name,
                datatype=pg_to_python_type(col.type),
                raw_type=pg_source_type(col.type),
                primary_key=col.primary_key,
                nullable=col.nullable if col.nullable is not None else True,
                unique=col.unique if col.unique is not None else False,
                length=getattr(col.type, 'length', None),
                precision=getattr(col.type, 'precision', None),
                scale=getattr(col.type, 'scale', None),
                source_description=col.comment,
                read_only=col.computed is not None,
                default_value=col.default.arg if col.default and not callable(col.default.arg) else None,
                timezone='UTC' if getattr(col.type, 'timezone', False) else None,
                array=isinstance(col.type, ARRAY),
                enum_values=list(col.type.enums) if isinstance(col.type, Enum) else None,
            ))

        return Table(
            source_name=table_name,
            columns=columns,
            source_description=sa_table.comment,
        )

    def describe_all(self, pg_schema: str = 'public') -> Schema:
        """Reflect an entire Postgres schema into a universal Schema."""
        inspector = inspect(self._engine)
        tables = inspector.get_table_names(schema=pg_schema)
        return Schema(
            source_name=pg_schema,
            tables=[self.describe(name, pg_schema=pg_schema) for name in tables],
        )

    # ── DDL ──

    def create_table(self, table: Table, pg_schema: str | None = None) -> None:
        """Create a Postgres table from a Table. Existing tables are left unchanged."""
        if pg_schema:
            with self._engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"'))

        columns = []
        for col in table.columns:
            sa_type_cls = PYTHON_TO_PG.get(col.datatype, String)
            col_name = col.target_name or col.source_name
            column_type = String(col.length) if sa_type_cls is String and col.length else sa_type_cls()
            columns.append(SAColumn(
                col_name,
                column_type,
                primary_key=col.primary_key,
                nullable=col.nullable,
                unique=col.unique,
            ))

        table_name = table.target_name or table.source_name
        meta = MetaData(schema=pg_schema) if pg_schema else self._metadata
        sa_table = SATable(table_name, meta, *columns, extend_existing=True)
        sa_table.create(self._engine, checkfirst=True)

    def drop_table(self, name: str, pg_schema: str | None = None) -> None:
        schema_prefix = f'"{pg_schema}".' if pg_schema else ''
        with self._engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {schema_prefix}"{name}"'))

    def drop_schema(self, name: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))

    def add_column(self, table_name: str, column: Column, pg_schema: str | None = None) -> None:
        """ALTER TABLE ADD COLUMN IF NOT EXISTS."""
        sa_type_cls = PYTHON_TO_PG.get(column.datatype, String)
        column_type = String(column.length) if sa_type_cls is String and column.length else sa_type_cls()
        col_name = column.target_name or column.source_name
        schema_prefix = f'"{pg_schema}".' if pg_schema else ''
        type_str = column_type.compile(dialect=self._engine.dialect)
        with self._engine.begin() as conn:
            conn.execute(text(
                f'ALTER TABLE {schema_prefix}"{table_name}" '
                f'ADD COLUMN IF NOT EXISTS "{col_name}" {type_str}'
            ))

    def drop_column(self, table_name: str, col_name: str, pg_schema: str | None = None) -> None:
        schema_prefix = f'"{pg_schema}".' if pg_schema else ''
        with self._engine.begin() as conn:
            conn.execute(text(
                f'ALTER TABLE {schema_prefix}"{table_name}" '
                f'DROP COLUMN IF EXISTS "{col_name}"'
            ))

    # ── Raw SQL ──

    def query(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute raw SQL and return results as dicts."""
        with Session(self._engine) as session:
            result = session.execute(text(sql), params or {})
            return [dict(row) for row in result.mappings()]

    def query_iter(self, sql: str, params: dict[str, Any] | None = None, batch_size: int = 10000) -> DataStream:
        """Lazily iterate over raw SQL results."""
        with Session(self._engine) as session:
            result = session.execute(
                text(sql).execution_options(yield_per=batch_size),
                params or {},
            )
            for row in result.mappings():
                yield dict(row)

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> int:
        """Execute a write statement, return rowcount."""
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), params or {})
            return result.rowcount
