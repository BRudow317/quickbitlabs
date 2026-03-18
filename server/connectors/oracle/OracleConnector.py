from __future__ import annotations
from typing import Any
import logging
from more_itertools import chunked

from server.models.ConnectorProtocol import Connector
from server.models.ConnectorStandard import Schema, Table, Column, DataStream
from server.models.ConnectorResponse import ConnectorResponse
from server.connectors.oracle.OracleClient import OracleClient
from server.connectors.oracle.utils.type_converter import get_python_type

logger = logging.getLogger(__name__)

class OracleConnector:
    """
    Oracle Connector bridge.
    Connects the universal ConnectorProtocol (program) to the OracleClient (database).
    """
    client: OracleClient
    schema: Schema

    def __init__(self, schema: Schema | None = None, **kwargs: Any):
        self.client = OracleClient(**kwargs)
        self.schema = schema if schema is not None else Schema(source_name='oracle')

    def test_connection(self) -> bool:
        return self.client.test_connection()

    # ── Schema ──

    def create_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Oracle does not support 'schema creation' via this API.")

    def get_schema(self, schema: Schema | str | None = None, **kwargs: Any) -> ConnectorResponse[Schema]:
        """Discover tables and columns in an Oracle schema."""
        try:
            # Determine which schema name to use
            oracle_schema = schema.source_name if isinstance(schema, Schema) else schema
            if not oracle_schema:
                oracle_schema = self.client.engine.url.username

            logger.info(f"Reflecting Oracle schema: {oracle_schema}")
            
            # Determine target tables
            target_tables = []
            if isinstance(schema, Schema) and schema.tables:
                target_tables = [t.source_name for t in schema.tables]
            else:
                target_tables = self.client.get_table_names(schema=oracle_schema)
            
            tables = []
            for tab_name in target_tables:
                table_res = self.get_table(tab_name, schema_name=oracle_schema)
                if table_res.ok and table_res.data:
                    tables.append(table_res.data)
            
            new_schema = Schema(source_name=oracle_schema or 'oracle', tables=tables)
            return ConnectorResponse.success(new_schema)
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            return ConnectorResponse.error(f"Discovery failed: {e}")

    def update_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("In-place schema update not supported.")

    def upsert_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Schema upsert not supported.")

    def delete_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Schema deletion not supported.")

    # ── Table ──

    def get_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        """Describe an Oracle table into a universal Table model."""
        try:
            name = table.source_name if isinstance(table, Table) else table
            schema_name = kwargs.get('schema_name')
            
            columns = []
            for col_meta in self.client.reflect_table_columns(name, schema=schema_name):
                columns.append(Column(
                    source_name=col_meta['name'],
                    datatype=get_python_type(col_meta['type']),
                    raw_type=str(col_meta['type']),
                    primary_key=col_meta['is_pk'],
                    nullable=col_meta.get('nullable', True),
                    length=col_meta.get('length'),
                    precision=col_meta.get('precision'),
                    scale=col_meta.get('scale'),
                ))
            
            new_table = Table(source_name=name, columns=columns)
            return ConnectorResponse.success(new_table)
        except Exception as e:
            logger.error(f"Failed to get table {table}: {e}")
            return ConnectorResponse.error(f"Table '{table}' not found.")

    def create_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        """Create an Oracle table from a universal Table model."""
        try:
            if isinstance(table, str):
                return ConnectorResponse.error("Cannot create table from string; need a Table model.")
            
            schema_name = kwargs.get('schema_name')
            col_defs = []
            for col in table.columns:
                col_defs.append({
                    'name': col.target_name or col.source_name,
                    'type_key': col.datatype,
                    'length': col.length,
                    'pk': col.primary_key,
                    'nullable': col.nullable
                })

            table_name = table.target_name or table.source_name
            self.client.create_table(table_name, col_defs, schema=schema_name)
            return ConnectorResponse.success(table)
        except Exception as e:
            return ConnectorResponse.error(f"Failed to create table: {e}")

    def update_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Table modification not yet implemented.")

    def upsert_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return self.create_table(table, **kwargs)

    def delete_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        try:
            name = table.source_name if isinstance(table, Table) else table
            schema_name = kwargs.get('schema_name')
            self.client.drop_table(name, schema=schema_name)
            return ConnectorResponse.success(table if isinstance(table, Table) else Table(source_name=name))
        except Exception as e:
            return ConnectorResponse.error(f"Failed to delete table: {e}")

    # ── Column ──

    def get_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Column-level discovery not implemented independently.")

    def create_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Column creation not implemented.")

    def update_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Column update not implemented.")

    def upsert_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Column upsert not implemented.")

    def delete_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Column deletion not implemented.")

    # ── Records ──

    def get_records(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[DataStream]:
        """Stream records from an Oracle table."""
        try:
            name = table.source_name if isinstance(table, Table) else table
            schema_name = kwargs.get('schema_name')
            return ConnectorResponse.success(self.client.stream_rows(name, schema=schema_name))
        except Exception as e:
            return ConnectorResponse.error(f"Failed to fetch records: {e}")

    def create_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        """Bulk insert records into an Oracle table."""
        try:
            name = table.target_name or (table.source_name if isinstance(table, Table) else table)
            schema_name = kwargs.get('schema_name')
            batch_size = kwargs.get('batch_size', 5000)

            for chunk in chunked(records, batch_size):
                self.client.insert_rows(name, list(chunk), schema=schema_name)
            
            return ConnectorResponse.success(records)
        except Exception as e:
            return ConnectorResponse.error(f"Failed to insert records: {e}")

    def update_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        return ConnectorResponse.not_implemented("Update records not implemented.")

    def upsert_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        return self.create_records(table, records, **kwargs)

    def delete_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        return ConnectorResponse.not_implemented("Delete records not implemented.")
