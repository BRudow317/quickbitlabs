from __future__ import annotations
from typing import Any
from collections.abc import Iterator

from sqlalchemy import create_engine, MetaData, Table as SATable, text, inspect
from sqlalchemy.orm import Session

from server.models.ConnectorStandard import Table, Column, Schema, DataStream
from server.connectors.postgres.type_converter import pg_to_python_type, pg_source_type
from server.connectors.postgres.services.table_ops import PgObjType

import logging
logger = logging.getLogger(__name__)


class PgQuery:
    """Global Postgres operations: schema discovery, raw SQL, table factory."""

    def __init__(self, engine, metadata: MetaData):
        self._engine = engine
        self._metadata = metadata

    def __getattr__(self, name: str) -> PgObjType:
        """Dot-notation access to tables, mirroring sf.rest.Contact style.
        Example: pg.query.users.get('abc-123')
        """
        if name.startswith('_'):
            return super().__getattribute__(name)
        return PgObjType(table_name=name, engine=self._engine, metadata=self._metadata)

    # Schema Discovery 
    def list_tables(self) -> list[str]:
        """List all tables in the database."""
        inspector = inspect(self._engine)
        return inspector.get_table_names()

    def describe(self, table_name: str) -> Table:
        """Reflect a single table into the universal schema."""
        sa_table = SATable(table_name, self._metadata, autoload_with=self._engine)

        columns = []
        for col in sa_table.columns:
            columns.append(Column(
                source_name=col.name,
                datatype=pg_to_python_type(col.type),
                source_type=pg_source_type(col.type),
                primary_key=col.primary_key,
                nullable=col.nullable or True,
                unique=col.unique or False,
                length=getattr(col.type, 'length', None),
                precision=getattr(col.type, 'precision', None),
                scale=getattr(col.type, 'scale', None),
                description=col.comment,
            ))

        return Table(
            source_name=table_name,
            columns=columns,
            description=sa_table.comment,
        )

    def describe_all(self) -> Schema:
        """Reflect the entire database into a universal Schema."""
        tables = self.list_tables()
        return Schema(
            source_name=str(self._engine.url.database),
            tables=[self.describe(name) for name in tables],
        )

    # Raw SQL 
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