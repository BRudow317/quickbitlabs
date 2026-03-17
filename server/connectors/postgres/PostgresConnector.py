from __future__ import annotations
from typing import Any
import os
from collections.abc import Iterable

from sqlalchemy import create_engine, MetaData, Table as SATable, Column as SAColumn, text, String
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from more_itertools import chunked

from server.connectors.postgres.type_converter import PYTHON_TO_PG
from server.connectors.postgres.services.query import PgQuery
from server.models.StandardTemplate import Table, Column, Schema, DataStream

import logging
logger = logging.getLogger(__name__)


class PostgresConnector:
    """Entry point for all Postgres operations."""
    connection_string: str | None = None

    def __init__(self):
        con_str = self.connection_string or os.getenv("SUPABASE_CONNECTION_STRING") or ""
        self.engine = create_engine(con_str)
        self.metadata = MetaData()
        self.query = PgQuery(self.engine, self.metadata)

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to Postgres database.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Postgres database: {e}")
            return False

    # Schema: Universal -> Postgres 

    def apply_schema(self, table: Table, pg_schema: str | None = None) -> None:
        """Create/ensure a Postgres table from a universal Table."""
        if pg_schema:
            self._ensure_schema(pg_schema)

        logger.info(f"Applying schema for {table.source_name}...")

        columns = []
        for col in table.columns:
            sa_type_cls = PYTHON_TO_PG.get(col.datatype, String)
            col_name = col.target_name or col.source_name

            if sa_type_cls is String and col.length:
                column_type = String(col.length)
            else:
                column_type = sa_type_cls()

            columns.append(SAColumn(
                col_name,
                column_type,
                primary_key=col.primary_key,
                nullable=col.nullable,
                unique=col.unique,
            ))

        table_name = table.target_name or table.source_name
        meta = MetaData(schema=pg_schema) if pg_schema else self.metadata
        sa_table = SATable(table_name, meta, *columns, extend_existing=True)
        sa_table.create(self.engine, checkfirst=True)

    # Data: Write 

    def write_data(self, stream_name: str, records: DataStream, pg_schema: str | None = None, batch_size: int = 10000) -> None:
        """Bulk Postgres upserts in batches."""
        meta = MetaData(schema=pg_schema) if pg_schema else self.metadata
        table = SATable(stream_name, meta, autoload_with=self.engine)

        primary_keys = [key.name for key in table.primary_key]
        if not primary_keys:
            raise ValueError(f"Cannot upsert {stream_name}: no primary key defined.")

        logger.debug(f"Starting bulk upsert for {stream_name}...")

        with Session(self.engine) as session:
            for chunk_idx, record_batch in enumerate(chunked(records, batch_size)):
                insert_stmt = pg_insert(table).values(record_batch)
                update_dict = {
                    col.name: insert_stmt.excluded[col.name]
                    for col in table.columns
                    if col.name not in primary_keys
                }
                session.execute(
                    insert_stmt.on_conflict_do_update(
                        index_elements=primary_keys,
                        set_=update_dict,
                    )
                )
                session.commit()
                logger.info(f"Upserted batch {chunk_idx + 1} ({len(record_batch)} records) into {stream_name}.")

    # Internal 

    def _ensure_schema(self, pg_schema: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"'))
        logger.info(f"Ensured schema '{pg_schema}' exists.")