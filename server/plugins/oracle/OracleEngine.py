from __future__ import annotations

from typing import Any
import logging
import oracledb
from collections.abc import Iterator, Iterable
from .OracleClient import OracleClient

logger = logging.getLogger(__name__)


class OracleEngine:
    client: OracleClient
    default_schema: str
    schemas: list[OracleSchema]
    """
    The Oracle execution layer.
    Executes raw SQL/DDL and streams data through oracledb.
    Acts as the stateful bridge between OracleServices and OracleClient.

    Nothing in this layer should import anything above it.
    """
    def __init__(
        self,
        schema: str | list[str] | None = None,
        client: OracleClient | None = None,
    ):
        self.client = client or OracleClient()
        self.default_schema = (
            self.client.oracle_user.upper() if schema is None
            else (schema[0] if isinstance(schema, list) else schema.upper())
        )
        self.schemas = []

    def query(
        self, sql: str, binds: dict[str, Any] | None = None, fetch_size: int = 10000
    ) -> Iterator[dict[str, Any]]:
        """Execute a SELECT and yield rows as dicts."""
        binds = binds or {}
        with self.client.get_con().cursor() as cursor:
            try:
                cursor.arraysize = fetch_size
                cursor.execute(sql, binds)
                if cursor.description:
                    columns = [str(col[0]) for col in cursor.description]
                    for row in cursor:
                        yield dict(zip(columns, row))
            except oracledb.Error as e:
                logger.error("Oracle query failed: %s | %s", sql, e)
                raise

    def execute_many(
        self,
        sql: str,
        records: Iterable[dict[str, Any]],
        input_sizes: dict[str, Any],
        batch_size: int = 10000,
    ) -> Iterator[dict[str, Any]]:
        """Stream records into Oracle in batches. Yields rows tagged with __error on failure."""
        with self.client.get_con().cursor() as cursor:
            if input_sizes:
                typed = {k: v for k, v in input_sizes.items() if v is not None}
                if typed:
                    cursor.setinputsizes(**typed)
            batch: list[dict[str, Any]] = []
            for record in records:
                batch.append(record)
                if len(batch) >= batch_size:
                    yield from self._flush_batch(cursor, sql, batch)
                    batch = []
            if batch:
                yield from self._flush_batch(cursor, sql, batch)
        self.client.get_con().commit()

    def _flush_batch(
        self, cursor: Any, sql: str, batch: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        try:
            cursor.executemany(sql, batch, batcherrors=True)
            batch_errors = cursor.getbatcherrors()
            for error in batch_errors:
                batch[error.offset]["__error"] = error.message
            if batch_errors:
                messages = "; ".join(f"offset={e.offset}: {e.message}" for e in batch_errors)
                raise RuntimeError(f"Oracle batch errors [{sql}]: {messages}")
            return batch
        except oracledb.Error as e:
            logger.error("Oracle batch execution crashed: %s", e)
            raise

    def execute_ddl(self, sql: str) -> None:
        """Execute a structural statement (CREATE, ALTER, DROP, RENAME)."""
        with self.client.get_con().cursor() as cursor:
            try:
                cursor.execute(sql)
            except oracledb.Error as e:
                logger.error("Oracle DDL failed: %s | %s", sql, e)
                raise


class OracleSchema:
    client: OracleClient
    schema_name: str
    description: str | None

    def __init__(
        self,
        client: OracleClient | None = None,
        schema_name: str = "",
        description: str | None = None,
    ):
        self.client = client or OracleClient()
        self.schema_name = schema_name
        if not self.schema_name or self.schema_name.strip() == "":
            self.schema_name = str(self.client.oracle_user).upper()
        self.description = description

    def list_table_names(self) -> list[str]:
        sql = (
            "SELECT TABLE_NAME FROM ALL_TABLES "
            "WHERE OWNER = :owner ORDER BY TABLE_NAME"
        )
        with self.client.get_con().cursor() as cursor:
            cursor.execute(sql, {"owner": self.schema_name.upper()})
            return [str(row[0]) for row in cursor.fetchall()]


class OracleTable:
    table_name: str
    schema: OracleSchema
    _fetched_db_col: list[dict[str, Any]] | None

    def __init__(self, table_name: str, schema: OracleSchema):
        self.table_name = table_name
        self.schema = schema
        self._fetched_db_col = None

    @property
    def qualified_name(self) -> str:
        if not self.table_name:
            raise ValueError("table_name cannot be None")
        if not self.schema or not self.schema.schema_name:
            return self.table_name
        return f"{self.schema.schema_name}.{self.table_name}"

    @property
    def _fetch_tab_columns(self) -> list[dict[str, Any]] | None:
        if self._fetched_db_col is not None:
            return self._fetched_db_col
        sql = (
            "SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, CHAR_USED, "
            "       DATA_PRECISION, DATA_SCALE, COLUMN_ID, NULLABLE, DATA_DEFAULT "
            "FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = :owner AND TABLE_NAME = :table_name "
            "ORDER BY COLUMN_ID"
        )
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(
                sql,
                {"owner": self.schema.schema_name.upper(), "table_name": self.table_name.upper()},
            )
            col_names = [col[0] for col in (cursor.description or [])]
            cursor.rowfactory = lambda *args: dict(zip(col_names, args))
            res: list[dict[str, Any]] = cursor.fetchall()
        self._fetched_db_col = res if res else None
        return self._fetched_db_col

    def fetch_primary_keys(self) -> set[str]:
        sql = (
            "SELECT acc.COLUMN_NAME "
            "FROM ALL_CONSTRAINTS ac "
            "JOIN ALL_CONS_COLUMNS acc "
            "  ON ac.OWNER = acc.OWNER "
            " AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME "
            " AND ac.TABLE_NAME = acc.TABLE_NAME "
            "WHERE ac.OWNER = :owner "
            "  AND ac.TABLE_NAME = :table_name "
            "  AND ac.CONSTRAINT_TYPE = 'P'"
        )
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(
                sql,
                {"owner": self.schema.schema_name.upper(), "table_name": self.table_name.upper()},
            )
            return {str(row[0]) for row in cursor.fetchall()}

    def fetch_unique_columns(self) -> set[str]:
        sql = (
            "SELECT acc.COLUMN_NAME "
            "FROM ALL_CONSTRAINTS ac "
            "JOIN ALL_CONS_COLUMNS acc "
            "  ON ac.OWNER = acc.OWNER "
            " AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME "
            " AND ac.TABLE_NAME = acc.TABLE_NAME "
            "WHERE ac.OWNER = :owner "
            "  AND ac.TABLE_NAME = :table_name "
            "  AND ac.CONSTRAINT_TYPE = 'U' "
            "  AND ac.STATUS = 'ENABLED'"
        )
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(
                sql,
                {"owner": self.schema.schema_name.upper(), "table_name": self.table_name.upper()},
            )
            return {str(row[0]) for row in cursor.fetchall()}

    def fetch_foreign_keys(self) -> dict[str, dict[str, Any]]:
        sql = (
            "SELECT acc.COLUMN_NAME, rc.TABLE_NAME AS REF_TABLE, "
            "       racc.COLUMN_NAME AS REF_COLUMN, ac.STATUS "
            "FROM ALL_CONSTRAINTS ac "
            "JOIN ALL_CONS_COLUMNS acc "
            "  ON ac.OWNER = acc.OWNER "
            " AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME "
            " AND ac.TABLE_NAME = acc.TABLE_NAME "
            "JOIN ALL_CONSTRAINTS rc "
            "  ON ac.R_OWNER = rc.OWNER "
            " AND ac.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME "
            "JOIN ALL_CONS_COLUMNS racc "
            "  ON rc.OWNER = racc.OWNER "
            " AND rc.CONSTRAINT_NAME = racc.CONSTRAINT_NAME "
            " AND rc.TABLE_NAME = racc.TABLE_NAME "
            "WHERE ac.OWNER = :owner "
            "  AND ac.TABLE_NAME = :table_name "
            "  AND ac.CONSTRAINT_TYPE = 'R'"
        )
        result: dict[str, dict[str, Any]] = {}
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(
                sql,
                {"owner": self.schema.schema_name.upper(), "table_name": self.table_name.upper()},
            )
            for row in cursor.fetchall():
                result[str(row[0])] = {
                    "REF_TABLE": str(row[1]),
                    "REF_COLUMN": str(row[2]),
                    "STATUS": str(row[3]),
                }
        return result
