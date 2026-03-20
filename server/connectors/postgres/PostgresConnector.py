from __future__ import annotations
from typing import Any
import os

from sqlalchemy import create_engine, MetaData, text

from server.connectors.postgres.services.query import PgQuery
from server.models.ConnectorStandard import Table, Column, Schema, DataStream
from server.models.ConnectorResponse import ConnectorResponse

import logging
logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    Entry point for all Postgres operations.

    Thin facade: resolves inputs, delegates to PgQuery / PgObjType, wraps in ConnectorResponse.
    No SQL, no SQLAlchemy types, no engine references cross this boundary.
    """
    schema: Schema

    def __init__(
        self,
        schema: Schema | None = None,
        connection_string: str | None = None,
        **kwargs: Any,
    ):
        con_str = connection_string or os.getenv("SUPABASE_CONNECTION_STRING") or ""
        self.engine = create_engine(con_str)
        self.metadata = MetaData()
        self.query = PgQuery(self.engine, self.metadata)
        self.schema = schema if schema is not None else Schema(
            source_name=str(self.engine.url.database) or 'public'
        )

    #  Connection 

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Postgres connection failed: {e}")
            return False

    #  Schema 

    def create_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        name = schema.source_name if isinstance(schema, Schema) else schema
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{name}"'))
            return ConnectorResponse.success(
                data=schema if isinstance(schema, Schema) else Schema(source_name=name)
            )
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def get_schema(self, schema: Schema | str | None = None, **kwargs: Any) -> ConnectorResponse[Schema]:
        pg_schema = schema.source_name if isinstance(schema, Schema) else (schema or 'public')
        try:
            result = self.query.describe_all(pg_schema=pg_schema)
            if isinstance(schema, Schema):
                schema.tables = result.tables
                self.schema = schema
                return ConnectorResponse.success(data=schema)
            self.schema = result
            return ConnectorResponse.success(data=result)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def update_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Postgres schema rename is not supported via this connector")

    def upsert_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return self.create_schema(schema, **kwargs)

    def delete_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        name = schema.source_name if isinstance(schema, Schema) else schema
        try:
            self.query.drop_schema(name)
            return ConnectorResponse.success(
                data=schema if isinstance(schema, Schema) else Schema(source_name=name)
            )
        except Exception as e:
            return ConnectorResponse.error(str(e))

    #  Table 

    def create_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        if isinstance(table, str):
            result = self.get_table(table, **kwargs)
            if not result.ok or not result.data:
                return result
            table = result.data
        try:
            self.query.create_table(table, pg_schema=kwargs.get('pg_schema'))
            return ConnectorResponse.success(data=table)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def get_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        incoming = table if isinstance(table, Table) else None
        name = table.source_name if isinstance(table, Table) else table
        try:
            result = self.query.describe(name, pg_schema=kwargs.get('pg_schema'))
            result._schema = self.schema
            if incoming is not None:
                incoming.columns = result.columns
                incoming.source_description = result.source_description
                return ConnectorResponse.success(data=incoming)
            return ConnectorResponse.success(data=result)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def update_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Postgres table rename is not supported via this connector")

    def upsert_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return self.create_table(table, **kwargs)

    def delete_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        name = table.source_name if isinstance(table, Table) else table
        try:
            self.query.drop_table(name, pg_schema=kwargs.get('pg_schema'))
            return ConnectorResponse.success(
                data=table if isinstance(table, Table) else Table(source_name=name, columns=[])
            )
        except Exception as e:
            return ConnectorResponse.error(str(e))

    #  Column 

    def create_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        if isinstance(column, str):
            return ConnectorResponse.not_implemented("create_column requires a Column object, not a string")
        table_name = table.source_name if isinstance(table, Table) else table
        try:
            self.query.add_column(table_name, column, pg_schema=kwargs.get('pg_schema'))
            return ConnectorResponse.success(data=column)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def get_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        table_name = table.source_name if isinstance(table, Table) else table
        col_name = column.source_name if isinstance(column, Column) else column
        table_result = self.get_table(table_name, **kwargs)
        if not table_result.ok or not table_result.data:
            return ConnectorResponse.not_found(f"{table_name}.{col_name} not found")
        col = table_result.data.column_map.get(col_name)
        if not col:
            return ConnectorResponse.not_found(f"{table_name}.{col_name} not found")
        if isinstance(column, Column):
            column.datatype = col.datatype
            column.raw_type = col.raw_type
            column.primary_key = col.primary_key
            column.nullable = col.nullable
            column.unique = col.unique
            column.length = col.length
            column.precision = col.precision
            column.scale = col.scale
            column.source_description = col.source_description
            column.read_only = col.read_only
            column.default_value = col.default_value
            column.enum_values = col.enum_values
            return ConnectorResponse.success(data=column)
        return ConnectorResponse.success(data=col)

    def update_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Postgres column rename is not supported via this connector")

    def upsert_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return self.create_column(table, column, **kwargs)

    def delete_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        table_name = table.source_name if isinstance(table, Table) else table
        col_name = column.source_name if isinstance(column, Column) else column
        try:
            self.query.drop_column(table_name, col_name, pg_schema=kwargs.get('pg_schema'))
            return ConnectorResponse.success(
                data=column if isinstance(column, Column) else Column(source_name=col_name, datatype='string')
            )
        except Exception as e:
            return ConnectorResponse.error(str(e))

    # Records 

    def create_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).insert(records=records)
            return ConnectorResponse.success(data=iter([]))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def get_records(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        pg_schema = kwargs.get('pg_schema')
        table_ref = f'"{pg_schema}"."{name}"' if pg_schema else f'"{name}"'
        try:
            return ConnectorResponse.success(data=self.query.query_iter(f'SELECT * FROM {table_ref}'))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def update_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).update_many(records=records)
            return ConnectorResponse.success(data=iter([]))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def upsert_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).upsert(records=records)
            return ConnectorResponse.success(data=iter([]))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def delete_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).delete_many(records=records)
            return ConnectorResponse.success(data=iter([]))
        except Exception as e:
            return ConnectorResponse.error(str(e))
