"""
python master.py --config ./.env -l ./logs --exec server/connectors/sf/repl.py
"""
from __future__ import annotations
from typing import Any
import os
from collections.abc import Iterable

from sqlalchemy import create_engine, MetaData, Table as SATable, Column, text
from sqlalchemy import String, Integer, Float, Boolean, DateTime, Date, Time, LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from more_itertools import chunked

# locals
from server.models.StandardTemplate import Table

import logging
logger = logging.getLogger(__name__)


class PostgresConnector():
    """Translates the Universal Schema into PostgreSQL tables and 
    performs bulk upserts into a Postgres/Supabase database.
    """
    connection_string: str | None = None
    def __init__(self):
        # e.g., "postgresql://postgres:password@db.xyz.supabase.co:5432/postgres"
        con_str: str = self.connection_string or os.getenv("SUPABASE_CONNECTION_STRING") or ""
        self.engine = create_engine(con_str)
        self.metadata = MetaData()
        # The Reverse Rosetta Stone: Universal Types -> Postgres Types
        self.type_map = {
            'string':   String,
            'integer':  Integer,
            'float':    Float,
            'boolean':  Boolean,
            'datetime': DateTime,
            'date':     Date,
            'time':     Time,
            'binary':   LargeBinary,
            'json':     JSONB,
        }
    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                cursor_result = conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to Postgres database.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Postgres database: {e}")
            return False

    def apply_schema(self, table: Table) -> None:
        """Translates a UniversalTable into a SQLAlchemy Table object and ensures it exists in the Postgres database.
        """
        logger.info(f"Applying schema for {table.source_name} to Postgres...")
        
        columns = []
        for col in table.columns:
            sa_type = self.type_map.get(col.datatype, String)
            col_name = col.target_name or col.source_name

            if sa_type == String and col.length:
                column_type = sa_type(col.length)
            elif sa_type == Float and col.precision is not None:
                column_type = sa_type(precision=col.precision)
            else:
                column_type = sa_type()

            sa_column = Column(
                name=col_name,
                type_=column_type,
                primary_key=col.primary_key,
                nullable=col.nullable,
                unique=col.unique,
            )
            columns.append(sa_column)

        table_name = table.target_name or table.source_name
        sa_table = SATable(table_name, self.metadata, *columns, extend_existing=True)

        # 5. Execute the DDL (CREATE TABLE IF NOT EXISTS)
        # Note: This handles creation. True schema evolution (ALTER TABLE) 
        # requires a migration tool like Alembic.
        sa_table.create(self.engine, checkfirst=True)

    def write_data(self, stream_name: str, records: Iterable[dict[str, Any]], batch_size: int = 10000) -> None:
        """Takes the stream of data and performs bulk Postgres upserts in batches"""
        
        # Load the SQLAlchemy Table object
        table = SATable(stream_name, self.metadata, autoload_with=self.engine)
        
        # Identify the primary keys for the Upsert conflict resolution
        primary_keys = [key.name for key in table.primary_key]
        if not primary_keys:
            raise ValueError(f"Cannot perform Upsert on {stream_name}: No Primary Key defined in schema.")

        logger.info(f"Starting bulk upsert for {stream_name}...")

        with Session(self.engine) as session:
            # Process the infinite iterator in safe, memory-friendly chunks
            for chunk_idx, record_batch in enumerate(chunked(records, batch_size)):
                
                # 1. Construct the Postgres-specific INSERT statement
                insert_stmt = pg_insert(table).values(record_batch)
                
                # 2. Build the ON CONFLICT DO UPDATE clause
                # We tell Postgres: "If the ID matches, update every other column with the new data"
                update_dict = {
                    col.name: insert_stmt.excluded[col.name] 
                    for col in table.columns 
                    if col.name not in primary_keys
                }
                
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=primary_keys,
                    set_=update_dict
                )
                
                # 3. Execute the batch
                session.execute(upsert_stmt)
                session.commit()
                
                logger.info(f"Upserted batch {chunk_idx + 1} ({len(record_batch)} records) into {stream_name}.")