from __future__ import annotations
from typing import Any
from collections.abc import Iterator

from sqlalchemy import MetaData, Table as SATable, select, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from more_itertools import chunked

from server.models.ConnectorStandard import DataStream

import logging
logger = logging.getLogger(__name__)


class PgObjType:
    """Interface to a specific Postgres table. Mirrors SfObjType's CRUD pattern."""

    def __init__(self, table_name: str, engine, metadata: MetaData):
        self._table_name = table_name
        self._engine = engine
        self._metadata = metadata
        self._sa_table = SATable(table_name, metadata, autoload_with=engine)
        self._pk_cols = [col.name for col in self._sa_table.primary_key]

    # Read 
    def get(self, record_id: Any) -> dict[str, Any] | None:
        """Fetch a single record by primary key.
        For composite keys, pass a dict: .get({'org_id': 1, 'user_id': 2})
        """
        filters = self._pk_filter(record_id)
        with Session(self._engine) as session:
            result = session.execute(
                select(self._sa_table).where(and_(*filters))
            )
            row = result.mappings().first()
            return dict(row) if row else None

    def read(self, batch_size: int = 10000) -> DataStream:
        """Yield all rows as dicts."""
        with Session(self._engine) as session:
            result = session.execute(
                self._sa_table.select().execution_options(yield_per=batch_size)
            )
            for row in result.mappings():
                yield dict(row)

    def filter(self, where: dict[str, Any], batch_size: int = 10000) -> DataStream:
        """Yield rows matching simple equality filters.
        Example: .filter({'status': 'active', 'region': 'US'})
        """
        conditions = [
            self._sa_table.c[k] == v for k, v in where.items()
        ]
        with Session(self._engine) as session:
            result = session.execute(
                select(self._sa_table)
                .where(and_(*conditions))
                .execution_options(yield_per=batch_size)
            )
            for row in result.mappings():
                yield dict(row)

    #  Write 
    def create(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Insert a single record, return it."""
        with self._engine.begin() as conn:
            result = conn.execute(
                self._sa_table.insert().values(**data).returning(*self._sa_table.c)
            )
            row = result.mappings().first()
            return {k: v for k, v in row.items()} if row else None

    def update(self, record_id: Any, data: dict[str, Any]) -> int:
        """Update a single record by primary key, return rowcount."""
        filters = self._pk_filter(record_id)
        with self._engine.begin() as conn:
            result = conn.execute(
                self._sa_table.update().where(and_(*filters)).values(**data)
            )
            return result.rowcount

    def upsert(self, records: DataStream, batch_size: int = 10000) -> None:
        """Bulk upsert via ON CONFLICT DO UPDATE."""
        if not self._pk_cols:
            raise ValueError(f"No primary key on {self._table_name} — cannot upsert.")

        with Session(self._engine) as session:
            for chunk_idx, batch in enumerate(chunked(records, batch_size)):
                stmt = pg_insert(self._sa_table).values(batch)
                update_cols = {
                    col.name: stmt.excluded[col.name]
                    for col in self._sa_table.columns
                    if col.name not in self._pk_cols
                }
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=self._pk_cols,
                        set_=update_cols,
                    )
                )
                session.commit()
                logger.info(f"Upserted batch {chunk_idx + 1} ({len(batch)} records) into {self._table_name}.")

    def delete(self, record_id: Any) -> int:
        """Delete a single record by primary key, return rowcount."""
        filters = self._pk_filter(record_id)
        with self._engine.begin() as conn:
            result = conn.execute(
                self._sa_table.delete().where(and_(*filters))
            )
            return result.rowcount

    def insert(self, records: DataStream, batch_size: int = 10000) -> None:
        """Bulk INSERT (no conflict handling)."""
        with Session(self._engine) as session:
            for chunk_idx, batch in enumerate(chunked(records, batch_size)):
                session.execute(self._sa_table.insert(), batch)
                session.commit()
                logger.info(f"Inserted batch {chunk_idx + 1} ({len(batch)} records) into {self._table_name}.")

    def update_many(self, records: DataStream) -> int:
        """Bulk UPDATE. Each record must contain PK fields plus columns to update."""
        count = 0
        with self._engine.begin() as conn:
            for record in records:
                pk_vals = {k: record[k] for k in self._pk_cols if k in record}
                update_vals = {k: v for k, v in record.items() if k not in self._pk_cols}
                if not pk_vals or not update_vals:
                    continue
                filters = [self._sa_table.c[k] == v for k, v in pk_vals.items()]
                result = conn.execute(
                    self._sa_table.update().where(and_(*filters)).values(**update_vals)
                )
                count += result.rowcount
        return count

    def delete_many(self, records: DataStream) -> int:
        """Delete records by primary key. Each record must contain PK fields."""
        count = 0
        with self._engine.begin() as conn:
            for record in records:
                pk_filter = [self._sa_table.c[k] == record[k] for k in self._pk_cols if k in record]
                if not pk_filter:
                    continue
                result = conn.execute(self._sa_table.delete().where(and_(*pk_filter)))
                count += result.rowcount
        return count

    #  Internals

    def _pk_filter(self, record_id: Any) -> list:
        """Build WHERE clauses from a scalar PK or composite dict."""
        if isinstance(record_id, dict):
            return [self._sa_table.c[k] == v for k, v in record_id.items()]
        if len(self._pk_cols) != 1:
            raise ValueError(f"Composite PK on {self._table_name} — pass a dict.")
        return [self._sa_table.c[self._pk_cols[0]] == record_id]