from __future__ import annotations
from typing import Any
import os

from sqlalchemy import create_engine, MetaData, text

from server.Plugins.postgres.services.query import PgQuery
from server.models.PluginStandard import BaseTable, BaseColumn, BaseSchema, DataStream
from server.models.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


class PostgresPlugin:
    """
    Entry point for all Postgres operations.

    Thin facade: resolves inputs, delegates to PgQuery / PgObjType, wraps in PluginResponse.
    No SQL, no SQLAlchemy types, no engine references cross this boundary.
    """
    schema: BaseSchema

    def __init__(
        self,
        schema: BaseSchema | None = None,
        connection_string: str | None = None,
        **kwargs: Any,
    ):
        con_str = connection_string or os.getenv("SUPABASE_CONNECTION_STRING") or ""
        self.engine = create_engine(con_str)
        self.metadata = MetaData()
        self.query = PgQuery(self.engine, self.metadata)
        self.schema = schema if schema is not None else BaseSchema(
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

    #  BaseSchema 

    def create_schema(self, schema: BaseSchema | str, **kwargs: Any) -> PluginResponse[BaseSchema]:
        name = schema.source_name if isinstance(schema, BaseSchema) else schema
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{name}"'))
            return PluginResponse.success(
                data=schema if isinstance(schema, BaseSchema) else BaseSchema(source_name=name)
            )
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_schema(self, schema: BaseSchema | str | None = None, **kwargs: Any) -> PluginResponse[BaseSchema]:
        pg_schema = schema.source_name if isinstance(schema, BaseSchema) else (schema or 'public')
        try:
            result = self.query.describe_all(pg_schema=pg_schema)
            if isinstance(schema, BaseSchema):
                schema.tables = result.tables
                self.schema = schema
                return PluginResponse.success(data=schema)
            self.schema = result
            return PluginResponse.success(data=result)
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_schema(self, schema: BaseSchema | str, **kwargs: Any) -> PluginResponse[BaseSchema]:
        return PluginResponse.not_implemented("Postgres schema rename is not supported via this Plugin")

    def upsert_schema(self, schema: BaseSchema | str, **kwargs: Any) -> PluginResponse[BaseSchema]:
        return self.create_schema(schema, **kwargs)

    def delete_schema(self, schema: BaseSchema | str, **kwargs: Any) -> PluginResponse[BaseSchema]:
        name = schema.source_name if isinstance(schema, BaseSchema) else schema
        try:
            self.query.drop_schema(name)
            return PluginResponse.success(
                data=schema if isinstance(schema, BaseSchema) else BaseSchema(source_name=name)
            )
        except Exception as e:
            return PluginResponse.error(str(e))

    #  BaseTable 

    def create_table(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[BaseTable]:
        if isinstance(table, str):
            result = self.get_table(table, **kwargs)
            if not result.ok or not result.data:
                return result
            table = result.data
        try:
            self.query.create_table(table, pg_schema=kwargs.get('pg_schema'))
            return PluginResponse.success(data=table)
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_table(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[BaseTable]:
        incoming = table if isinstance(table, BaseTable) else None
        name = table.source_name if isinstance(table, BaseTable) else table
        try:
            result = self.query.describe(name, pg_schema=kwargs.get('pg_schema'))
            result._schema = self.schema
            if incoming is not None:
                incoming.columns = result.columns
                incoming.source_description = result.source_description
                return PluginResponse.success(data=incoming)
            return PluginResponse.success(data=result)
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_table(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[BaseTable]:
        return PluginResponse.not_implemented("Postgres table rename is not supported via this Plugin")

    def upsert_table(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[BaseTable]:
        return self.create_table(table, **kwargs)

    def delete_table(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[BaseTable]:
        name = table.source_name if isinstance(table, BaseTable) else table
        try:
            self.query.drop_table(name, pg_schema=kwargs.get('pg_schema'))
            return PluginResponse.success(
                data=table if isinstance(table, BaseTable) else BaseTable(source_name=name, columns=[])
            )
        except Exception as e:
            return PluginResponse.error(str(e))

    #  BaseColumn 

    def create_column(self, table: BaseTable | str, column: BaseColumn | str, **kwargs: Any) -> PluginResponse[BaseColumn]:
        if isinstance(column, str):
            return PluginResponse.not_implemented("create_column requires a BaseColumn object, not a string")
        table_name = table.source_name if isinstance(table, BaseTable) else table
        try:
            self.query.add_column(table_name, column, pg_schema=kwargs.get('pg_schema'))
            return PluginResponse.success(data=column)
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_column(self, table: BaseTable | str, column: BaseColumn | str, **kwargs: Any) -> PluginResponse[BaseColumn]:
        table_name = table.source_name if isinstance(table, BaseTable) else table
        col_name = column.source_name if isinstance(column, BaseColumn) else column
        table_result = self.get_table(table_name, **kwargs)
        if not table_result.ok or not table_result.data:
            return PluginResponse.not_found(f"{table_name}.{col_name} not found")
        col = table_result.data.column_map.get(col_name)
        if not col:
            return PluginResponse.not_found(f"{table_name}.{col_name} not found")
        if isinstance(column, BaseColumn):
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
            return PluginResponse.success(data=column)
        return PluginResponse.success(data=col)

    def update_column(self, table: BaseTable | str, column: BaseColumn | str, **kwargs: Any) -> PluginResponse[BaseColumn]:
        return PluginResponse.not_implemented("Postgres column rename is not supported via this Plugin")

    def upsert_column(self, table: BaseTable | str, column: BaseColumn | str, **kwargs: Any) -> PluginResponse[BaseColumn]:
        return self.create_column(table, column, **kwargs)

    def delete_column(self, table: BaseTable | str, column: BaseColumn | str, **kwargs: Any) -> PluginResponse[BaseColumn]:
        table_name = table.source_name if isinstance(table, BaseTable) else table
        col_name = column.source_name if isinstance(column, BaseColumn) else column
        try:
            self.query.drop_column(table_name, col_name, pg_schema=kwargs.get('pg_schema'))
            return PluginResponse.success(
                data=column if isinstance(column, BaseColumn) else BaseColumn(source_name=col_name, datatype='string')
            )
        except Exception as e:
            return PluginResponse.error(str(e))

    # Records 

    def create_records(self, table: BaseTable | str, records: DataStream, **kwargs: Any) -> PluginResponse[DataStream]:
        name = table.source_name if isinstance(table, BaseTable) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).insert(records=records)
            return PluginResponse.success(data=iter([]))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_records(self, table: BaseTable | str, **kwargs: Any) -> PluginResponse[DataStream]:
        name = table.source_name if isinstance(table, BaseTable) else table
        pg_schema = kwargs.get('pg_schema')
        table_ref = f'"{pg_schema}"."{name}"' if pg_schema else f'"{name}"'
        try:
            return PluginResponse.success(data=self.query.query_iter(f'SELECT * FROM {table_ref}'))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_records(self, table: BaseTable | str, records: DataStream, **kwargs: Any) -> PluginResponse[DataStream]:
        name = table.source_name if isinstance(table, BaseTable) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).update_many(records=records)
            return PluginResponse.success(data=iter([]))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_records(self, table: BaseTable | str, records: DataStream, **kwargs: Any) -> PluginResponse[DataStream]:
        name = table.source_name if isinstance(table, BaseTable) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).upsert(records=records)
            return PluginResponse.success(data=iter([]))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_records(self, table: BaseTable | str, records: DataStream, **kwargs: Any) -> PluginResponse[DataStream]:
        name = table.source_name if isinstance(table, BaseTable) else table
        pg_schema = kwargs.get('pg_schema')
        try:
            self.query.table(name, pg_schema=pg_schema).delete_many(records=records)
            return PluginResponse.success(data=iter([]))
        except Exception as e:
            return PluginResponse.error(str(e))
