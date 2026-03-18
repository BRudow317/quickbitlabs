from __future__ import annotations

import csv
import io
from typing import Any

from server.connectors.sf.HttpClient import HttpClient
from server.connectors.sf.services.rest import SfRest
from server.connectors.sf.services.bulk2 import SfBulk2Handler
from server.connectors.sf.utils.type_converter import SF_TYPE_MAP, cast_record, prepare_record
from server.models.ConnectorStandard import Column, Table, Schema, DataStream
from server.models.ConnectorResponse import ConnectorResponse

import logging
logger = logging.getLogger(__name__)


class SalesforceConnector:
    """
    Single entry point for all Salesforce operations.

    Implements the Connector protocol where Salesforce supports it.
    SF is primarily a source — most metadata write operations
    return ConnectorResponse.not_implemented() since you can't DDL through the data API.
    """
    client: HttpClient
    schema: Schema
    _rest: SfRest | None = None
    _bulk2: SfBulk2Handler | None = None

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        org_url: str | None = None,
        schema: Schema | None = None,
    ):
        self.client = HttpClient(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            base_url=org_url,
        )
        self.schema = schema if schema is not None else Schema(source_name='salesforce')

    @property
    def rest(self) -> SfRest:
        if self._rest is None:
            self._rest = SfRest(self.client)
        return self._rest

    @property
    def bulk2(self) -> SfBulk2Handler:
        if self._bulk2 is None:
            self._bulk2 = SfBulk2Handler(self.client)
        return self._bulk2

    # ── Connection ──

    def test_connection(self) -> bool:
        try:
            self.rest.limits()
            return True
        except Exception as e:
            logger.error(f"SF connection failed: {e}")
            return False

# ============================ METADATA ============================
    
    # SCHEMA
    def get_schema(self, schema: Schema | str | None = None, **kwargs: Any) -> ConnectorResponse[Schema]:
        """
        Describe Salesforce objects and return a populated Schema.

        If a Schema is passed, its table list drives which objects are described
        (source_name on each Table is used as the SF object name).
        If a string is passed, it becomes the source_name of the returned Schema.
        If None, all migratable objects are fetched, or a ``streams`` kwarg
        (list[str]) can narrow the set.
        """
        try:
            if isinstance(schema, Schema):
                streams = [t.source_name for t in schema.tables]
                target = schema
            else:
                streams = kwargs.get('streams') or [obj['name'] for obj in self.rest.describe_migratable()]
                target = Schema(source_name=schema or 'salesforce')

            tables = []
            for s in streams:
                result = self.get_table(s)
                if result.ok and result.data:
                    tables.append(result.data)
                else:
                    logger.warning(f"Skipping {s}: {result.message}")

            target.tables = tables
            self.schema = target
            return ConnectorResponse.success(data=target)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def create_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Salesforce does not support schema creation via API")

    def update_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Salesforce does not support schema modification via API")

    def upsert_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        """SF as target: stamp target_* fields on the passed Schema."""
        if isinstance(schema, str):
            return ConnectorResponse.not_implemented("upsert_schema requires a Schema object, not a string")
        try:
            schema.target_name = schema.target_name or 'salesforce'
            for table in schema.tables:
                table.target_name = table.target_name or table.source_name
            return ConnectorResponse.success(data=schema)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def delete_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]:
        return ConnectorResponse.not_implemented("Salesforce does not support schema deletion via API")

    
    # TABLE
    def get_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        """Describe a Salesforce object into a Table (cached on self.schema).

        If a Table is passed, its source_name drives the describe call and its
        columns are populated in-place before returning it.
        """
        try:
            incoming = table if isinstance(table, Table) else None
            name = table.source_name if isinstance(table, Table) else table

            if name not in self.schema.table_map:
                describe = getattr(self.rest, name).describe()
                columns = []
                for f in describe.get('fields', []):
                    sf_type = f['type']
                    if sf_type in ('address', 'location'):
                        logger.debug(f"Skipping compound field {f['name']} ({sf_type}) on {name}")
                        continue
                    pv = f.get('picklistValues') or []
                    columns.append(Column(
                        source_name=f['name'],
                        datatype=SF_TYPE_MAP.get(sf_type, 'string'),
                        raw_type=sf_type,
                        primary_key=(f['name'] == 'Id'),
                        nullable=f.get('nillable', True),
                        unique=f.get('unique', False),
                        length=f.get('length') or None,
                        precision=f.get('precision') or None,
                        scale=f.get('scale') or None,
                        source_description=f.get('label'),
                        read_only=not f.get('updateable', True),
                        default_value=f.get('defaultValue'),
                        enum_values=[v['value'] for v in pv if v.get('active')] or None,
                    ))
                t = Table(source_name=name, columns=columns)
                self.schema.tables.append(t)
                t._schema = self.schema

            result = self.schema.table_map[name]
            if incoming is not None:
                incoming.columns = result.columns
                return ConnectorResponse.success(data=incoming)
            return ConnectorResponse.success(data=result)
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def create_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Salesforce does not support table creation via data API")

    def update_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Salesforce does not support table modification via data API")

    def upsert_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Salesforce does not support table creation via data API")

    def delete_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]:
        return ConnectorResponse.not_implemented("Salesforce does not support table deletion via data API")

    
    # COLUMN
    def get_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        name = table.source_name if isinstance(table, Table) else table
        col_name = column.source_name if isinstance(column, Column) else column

        table_result = self.get_table(name)
        if not table_result.ok or not table_result.data:
            return ConnectorResponse.not_found(f"Table {name} not found")

        col = table_result.data.column_map.get(col_name)
        if not col:
            return ConnectorResponse.not_found(f"{name}.{col_name} not found")

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

    def create_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Salesforce does not support column creation via data API")

    def update_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Salesforce does not support column modification via data API")

    def upsert_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Salesforce does not support column creation via data API")

    def delete_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]:
        return ConnectorResponse.not_implemented("Salesforce does not support column deletion via data API")

# ============================ DATA ============================
    
    # RECORDS
    def get_records(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[DataStream]:
        """Bulk2 full extract. The default read path."""
        try:
            name = table.source_name if isinstance(table, Table) else table
            table_result = self.get_table(table)
            if not table_result.ok or not table_result.data:
                return ConnectorResponse.not_found(f"Table {name} not found")
            fields = ', '.join(c.source_name for c in table_result.data.columns)
            return ConnectorResponse.success(data=self.bulk_query(f"SELECT {fields} FROM {name}", name))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def create_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        try:
            results = getattr(self.bulk2, name).insert(
                records=[prepare_record(r) for r in records]
            )
            return ConnectorResponse.success(data=iter(results))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def update_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        try:
            results = getattr(self.bulk2, name).update(
                records=[prepare_record(r) for r in records]
            )
            return ConnectorResponse.success(data=iter(results))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def upsert_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        external_id = kwargs.get('external_id_field', 'Id')
        try:
            results = getattr(self.bulk2, name).upsert(
                records=[prepare_record(r) for r in records],
                external_id_field=external_id,
            )
            return ConnectorResponse.success(data=iter(results))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    def delete_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]:
        name = table.source_name if isinstance(table, Table) else table
        try:
            results = getattr(self.bulk2, name).delete(
                records=[prepare_record(r) for r in records]
            )
            return ConnectorResponse.success(data=iter(results))
        except Exception as e:
            return ConnectorResponse.error(str(e))

    
    # Connector specifics
    def query(self, soql: str, object_name: str | None = None) -> DataStream:
        """REST SOQL query. Best for small, real-time result sets."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.source_name: c.raw_type for c in result.data.columns if c.raw_type}
        for record in self.rest.query_all_iter(soql):
            clean = {k: v for k, v in record.items() if k != 'attributes'}
            yield cast_record(clean, field_types) if field_types else clean

    def bulk_query(self, soql: str, object_name: str | None = None) -> DataStream:
        """Bulk2 SOQL query. Best for large exports."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.source_name: c.raw_type for c in result.data.columns if c.raw_type}
        for csv_page in self.bulk2.query(soql):
            for record in csv.DictReader(io.StringIO(csv_page)):
                yield cast_record(record, field_types) if field_types else dict(record)

    def get_limits(self) -> dict[str, Any]:
        return self.rest.limits()
