"""Apache Arrow: https://arrow.apache.org/docs/python/index.html
Oracle DataFrame Objects: https://python-oracledb.readthedocs.io/en/latest/api_manual/dataframe.html
Polars DataFrame: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html
Apache Arrow PyCapsule Interface: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
|-------------------------------------------------------|
| C Interface Type | PyCapsule Name |
|-------------------------------------------------------|
| ArrowSchema | arrow_schema |
| ArrowArray | arrow_array |
| ArrowArrayStream | arrow_array_stream |
| ArrowDeviceArray | arrow_device_array |
| ArrowDeviceArrayStream | arrow_device_array_stream |
|-------------------------------------------------------|
"""

from __future__ import annotations

import os
import logging
import oracledb
from collections.abc import Iterator, Iterable, Callable
from typing import Any
from oracledb import (
    LOB,
    Connection,
    Cursor,
    DataFrame,
    DbObjectType,
    DbType,
    Queue,
    AsyncQueue,
    DbObject,
    MessageProperties,
)
from oracledb.connection import Xid
import pyarrow as pa


logger: logging.Logger = logging.getLogger(__name__)

# TODO: Implement connection pooling for the server database to improve performance and resource management.
# This can be done using oracledb's ConnectionPool class, and the ServerDatabase can manage a pool of
# connections instead of a single connection. The connect() method would then acquire a connection from the
# pool, and there would be a corresponding method to release connections back to the pool when done.


class OracleClient:
    _oracle_user: str
    _oracle_pass: str
    _oracle_host: str
    _oracle_port: int
    _oracle_service: str
    _current_connection: Connection | None

    __slots__ = (
        "_oracle_user",
        "_oracle_pass",
        "_oracle_host",
        "_oracle_port",
        "_oracle_service",
        "_current_connection",
    )

    def __frozen__(self) -> bool:
        return True

    def __init__(
        self,
        oracle_user: str = '',
        oracle_pass: str = '',
        oracle_host: str = '',
        oracle_port: int | str = '0',
        oracle_service: str = '',
    ) -> None:
        self._oracle_user = oracle_user
        self._oracle_pass = oracle_pass
        self._oracle_host = oracle_host
        self._oracle_port = int(oracle_port)
        self._oracle_service = oracle_service
        self._current_connection = None
        if not self._oracle_pass:
            raise RuntimeError(f"No Values detected:\n{self.__repr__()}")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"user={self._oracle_user!r}, "
            f"host={self._oracle_host!r}, "
            f"port={self._oracle_port!r}, "
            f"service={self._oracle_service!r})"
        )

    def _new_connect(self) -> None:
        try:
            if (
                self._oracle_user == ''
                or self._oracle_pass == ''
                or self._oracle_host == ''
                or self._oracle_service == ''
            ):
                raise ValueError(
                    f"Missing required Oracle connection parameters: {self.__repr__()}"
                )

            self._current_connection = oracledb.connect(
                user=self._oracle_user,
                password=self._oracle_pass,
                host=self._oracle_host,
                port=self._oracle_port,
                service_name=self._oracle_service,
            )
            self._current_connection.autocommit = False

        except oracledb.Error as e:
            logger.error(f'Error connecting to Oracle: {self.__repr__()}\\nError: {e}')
            raise
        except Exception as e:
            logger.error(
                f'Unexpected error during Oracle connection: {self.__repr__()}\\nError: {e}'
            )
            raise

    def connect(self) -> oracledb.Connection:
        """Use this method for connections for database connection only"""
        if self._current_connection is not None and self._current_connection.is_healthy():
            return self._current_connection

        self._new_connect()

        if self._current_connection is None:
            raise RuntimeError(
                f"Failed to establish Oracle connection: {self.__repr__()}"
            )

        return self._current_connection

    def __call__(self) -> Connection:
        return self.connect()

    def close(self) -> None:
        if self._current_connection is None:
            return
        try:
            self._current_connection.close()
        except oracledb.Error as e:
            logger.error(f'Error closing Oracle connection: {self.__repr__()}\\nError: {e}')
            raise
        except Exception as e:
            logger.error(
                f'Unexpected error during Oracle connection close: {self.__repr__()}\\nError: {e}'
            )
            raise

    def __del__(self) -> None:
        try:
            self.close()
        except Exception as e:
            logger.warning(
                f'Error during OracleClient cleanup: {self.__repr__()}\\nError: {e}'
            )

    @property
    def con_str(self) -> str:
        return f"{self._oracle_user}/{self._oracle_pass}@{self._oracle_host}:{str(self._oracle_port)}/{self._oracle_service}"

    def cursor(self, scrollable: bool = False) -> Cursor:
        return Cursor(self.connect(), scrollable)

    @property
    def auto_commit(self) -> bool:
        return self.connect().autocommit

    @auto_commit.setter
    def auto_commit(self, auto_commit: bool) -> None:
        self.connect().autocommit = auto_commit

    @property
    def is_healthy(self) -> bool:
        return (
            self._current_connection is not None
            and self._current_connection.is_healthy()
        )

    @property
    def current_schema(self) -> str:
        return self.connect().current_schema

    @current_schema.setter
    def current_schema(self, schema_name: str) -> None:
        self.connect().current_schema = schema_name

    def commit(self) -> None:
        """Commits the current transaction."""
        self.connect().commit()

    def rollback(self) -> None:
        """Rolls back the current transaction."""
        self.connect().rollback()

    def createlob(
        self,
        lob_type: DbType,
        data: str | bytes | None = None,
    ) -> LOB:
        return self.connect().createlob(lob_type, data)

    """Apache PyArrow Methods that work with pyarrow schemas, tables, and arrow datatypes """

    def direct_path_load(
        self,
        schema_name: str,
        table_name: str,
        column_names: list[str],
        data: Any,
        *,
        batch_size: int = 2**32 - 1,
    ) -> None:
        self.connect().direct_path_load(
            schema_name,
            table_name,
            column_names,
            data,
            batch_size=batch_size,
        )

    def fetch_df_all(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        arraysize: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> DataFrame:
        return self.connect().fetch_df_all(
            statement,
            parameters,
            arraysize=arraysize,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    def fetch_df_batches(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        size: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> Iterator[DataFrame]:
        """https://python-oracledb.readthedocs.io/en/latest/api_manual/connection.html#oracledb.Connection.fetch_df_batches"""
        return self.connect().fetch_df_batches(
            statement,
            parameters,
            size=size,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    def odf_to_arrow(self, odf: oracledb.DataFrame) -> pa.Table:
        """Zero copy conversion to a PyArrow Table via the PyCapsule interface."""
        return pa.Table.from_arrays(odf.column_arrays(), names=odf.column_names())

    def arrow_query(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        size: int = 50_000,
        fetch_decimals: bool = True,
    ) -> pa.RecordBatchReader:
        """Returns a PyArrow RecordBatchReader"""
        raw_iter: Iterator[oracledb.DataFrame] = self.fetch_df_batches(
            statement=statement,
            parameters=parameters,
            size=size,
            fetch_decimals=fetch_decimals,
        )

        try:
            odf: oracledb.DataFrame = next(raw_iter)
            first_table: pa.Table = self.odf_to_arrow(odf)
        except StopIteration:
            return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))

        schema: pa.Schema = first_table.schema

        def batch_generator() -> Iterator[pa.RecordBatch]:
            yield from first_table.to_batches()
            for record_batch in raw_iter:
                yield from self.odf_to_arrow(record_batch).to_batches()

        return pa.RecordBatchReader.from_batches(schema, batch_generator())

    def execute_many(
        self,
        sql: str | Iterable[tuple[str, dict]],
        data: pa.RecordBatchReader,
        input_sizes: dict[str, Any] | None = None,
        *,
        batcherrors: bool = False,
    ) -> None:
        """Generic bulk DML over an Arrow stream. sql can be a single statement string, or an iterable of
        (statement, static_binds) tuples to execute multiple statements per batch like multiple updates.
        """
        statements: list[tuple[str, dict]] = [(sql, {})] if isinstance(sql, str) else list(sql)
        with self.connect().cursor() as cursor:
            if input_sizes:
                cursor.setinputsizes(**input_sizes)
            for batch in data:
                records = batch.to_pylist()
                if not records:
                    continue
                for stmt, binds in statements:
                    parameters = [{**r, **binds} for r in records] if binds else records
                    cursor.executemany(
                        statement=stmt,
                        parameters=parameters,
                        batcherrors=batcherrors,
                    )
            self.connect().commit()

    def get_table_def(self, schema, table) -> oracledb.DataFrame:
        odf: oracledb.DataFrame = self.fetch_df_all(
            """
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = :schema_name
              AND table_name = :table_name
            ORDER BY column_id
            """,
            {"schema_name": schema, "table_name": table},
        )
        return odf

    def create_table(self, schema, table, odf: oracledb.DataFrame) -> None:
        print("create_table ddl from odf...")
        cursor = self.connect().cursor()
        cursor.execute(
            """
            select distinct table_name
            from all_tab_columns
            where owner = :schema_name
              AND table_name = :table_name
            """,
            {"schema_name": schema, "table_name": table},
        )
        if cursor.fetchone():
            return

        arrow_table = pa.Table.from_arrays(odf.column_arrays(), names=odf.column_names())

        cols = []
        for row in arrow_table.to_pylist():
            col_name: str = row.get('COLUMN_NAME')
            dtype: str = row.get('DATA_TYPE')
            length: str = row.get('DATA_LENGTH')
            prec: str = row.get('DATA_PRECISION')
            scale: str = row.get('DATA_SCALE')
            nulls: str = row.get('NULLABLE')

            if dtype == "NUMBER":
                type_str = f"NUMBER({prec if prec else 38}, {scale if scale else 0})"
            elif "CHAR" in dtype:
                type_str = f"{dtype}({length})"
            else:
                type_str = dtype

            null_str = "NULL" if nulls == "Y" else "NOT NULL"
            cols.append(f"{col_name} {type_str} {null_str}")

        ddl = f"CREATE TABLE {schema}.{table} ({', '.join(cols)})"
        print(f"Create Table: {ddl}")
        self.cursor().execute(ddl)
        return

    def copy_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str | None = None,
    ) -> None:
        if not target_table:
            target_table = source_table

        odf = self.fetch_df_all(
            """
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM all_tab_columns
            WHERE owner = :owner
              AND table_name = :table
            ORDER BY column_id
            """,
            {"owner": source_schema, "table": source_table},
        )

        arrow_table = pa.Table.from_arrays(odf.column_arrays(), names=odf.column_names())
        cols = []
        for row in arrow_table:
            col_name, dtype, length, prec, scale, nulls = row

            if dtype == "NUMBER":
                type_str = f"NUMBER({prec if prec else 38}, {scale if scale else 0})"
            elif "CHAR" in dtype:
                type_str = f"{dtype}({length})"
            else:
                type_str = dtype

            null_str = "NULL" if nulls == "Y" else "NOT NULL"
            cols.append(f"{col_name} {type_str} {null_str}")

        ddl = f"CREATE TABLE {target_schema}.{target_table} ({', '.join(cols)})"
        self.cursor().execute(ddl)

    def plus_query(self, sql: str) -> tuple[int, str | None, str | None]:
        import sys, subprocess

        cmd = ["sqlplus", "-s", self.con_str]
        cmpl_prc = subprocess.run(
            cmd,
            input=sql,
            capture_output=True,
            check=False,
            text=True,
        )
        # 0=good 1=bad
        returned: tuple[int, str | None, str | None] = (
            cmpl_prc.returncode,
            cmpl_prc.stdout,
            cmpl_prc.stderr,
        )
        return returned

    def cancel(self) -> None:
        self.connect().cancel()

    def dbop(self, value: str) -> None:
        self.connect().dbop = value

    def action(self, value: str) -> None:
        self.connect().action = value

    def gettype(self, name: str) -> DbObjectType:
        return self.connect().gettype(name)

    def ping(self) -> None:
        """Throws an exception if the connection is not healthy. Otherwise, returns None."""
        self.connect().ping()

    """Oracle Advanced Queuing Features"""

    def msgproperties(
        self,
        payload: bytes | str | DbObject | None = None,
        correlation: str | None = None,
        delay: int | None = None,
        exceptionq: str | None = None,
        expiration: int | None = None,
        priority: int | None = None,
        recipients: list | None = None,
    ) -> MessageProperties:
        return self.connect().msgproperties(
            payload,
            correlation,
            delay,
            exceptionq,
            expiration,
            priority,
            recipients,
        )

    def queue(
        self,
        name: str,
        payload_type: DbObjectType | str | None = None,
    ) -> Queue:
        q: Queue | AsyncQueue = self.connect().queue(name, payload_type)
        if isinstance(q, Queue):
            return q
        else:
            raise TypeError(
                f"Expected Queue object from connection.queue(), got {type(q)}"
            )

    def subscribe(
        self,
        namespace: int = oracledb.SUBSCR_NAMESPACE_DBCHANGE,
        protocol: int = oracledb.SUBSCR_PROTO_CALLBACK,
        callback: Callable | None = None,
        timeout: int = 0,
        operations: int = oracledb.OPCODE_ALLOPS,
        port: int = 0,
        qos: int = oracledb.SUBSCR_QOS_DEFAULT,
        ip_address: str | None = None,
        grouping_class: int = oracledb.SUBSCR_GROUPING_CLASS_NONE,
        grouping_value: int = 0,
        grouping_type: int = oracledb.SUBSCR_GROUPING_TYPE_SUMMARY,
        name: str | None = None,
        client_initiated: bool = False,
    ) -> oracledb.Subscription:
        return self.connect().subscribe(
            namespace,
            protocol,
            callback,
            timeout,
            operations,
            port,
            qos,
            ip_address,
            grouping_class,
            grouping_value,
            grouping_type,
            name,
            client_initiated,
        )

    """Connection Properties"""

    @property
    def max_open_cursors(self) -> int:
        return self.connect().max_open_cursors

    @property
    def session_id(self) -> int:
        return self.connect().session_id

    @property
    def is_thin(self) -> bool:
        return self.connect().thin

    @property
    def username(self) -> str:
        return self.connect().username

    @property
    def version(self) -> str:
        return self.connect().version

    @property
    def ltxid(self) -> bytes:
        return self.connect().ltxid

    @property
    def dsn(self) -> str:
        return self.connect().dsn

    @property
    def internal_name(self) -> str:
        return self.connect().internal_name

    @internal_name.setter
    def internal_name(self, value: str) -> None:
        self.connect().internal_name = value

    @property
    def inputtypehandler(self) -> Callable:
        return self.connect().inputtypehandler

    @inputtypehandler.setter
    def inputtypehandler(self, value: Callable) -> None:
        self.connect().inputtypehandler = value

    @property
    def external_name(self) -> str:
        return self.connect().external_name

    @external_name.setter
    def external_name(self, value: str) -> None:
        self.connect().external_name = value

    @property
    def edition(self) -> str:
        return self.connect().edition

    @property
    def econtext_id(self) -> str:
        return self.connect().econtext_id

    @econtext_id.setter
    def econtext_id(self, value: str) -> None:
        self.connect().econtext_id = value

    @property
    def db_name(self) -> str:
        return self.connect().db_name

    @property
    def db_domain(self) -> str:
        return self.connect().db_domain

    @property
    def client_identifier(self) -> str:
        return self.connect().client_identifier

    @client_identifier.setter
    def client_identifier(self, value: str) -> None:
        self.connect().client_identifier = value

    @property
    def call_timeout(self) -> int:
        return self.connect().call_timeout

    @call_timeout.setter
    def call_timeout(self, value: int) -> None:
        self.connect().call_timeout = value