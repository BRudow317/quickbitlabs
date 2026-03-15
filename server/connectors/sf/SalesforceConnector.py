from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator
from typing import Any

# locals
from server.connectors.sf.HttpClient import HttpClient
from server.connectors.sf.services.rest import SfRest
from server.connectors.sf.services.bulk2 import SfBulk2Handler
from server.connectors.sf.utils.type_converter import SF_TYPE_MAP, cast_record, prepare_record
from server.models.StandardTemplate import Column, Table, Schema

# logging
import logging
logger = logging.getLogger(__name__)


class SalesforceConnector:
    """
    Single entry point for all Salesforce operations.
    In:  Python dicts (Iterable[dict[str, Any]])
    Out: Python dicts (Iterator[dict[str, Any]])
    All SF type conversion and format handling happens internally.
    """
    client: HttpClient
    _rest: SfRest | None = None
    _bulk2: SfBulk2Handler | None = None
    _table_cache: dict[str, Table]  # {object_name: Table}

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        org_url: str | None = None,
        access_token: str | None = None
    ):
        self.client = HttpClient(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            base_url=org_url,
            access_token=access_token
        )
        self._table_cache = {}

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

    #  Schema 

    def get_column(self, object_name: str, field_name: str) -> Column:
        """Return a StandardTemplate Column for a single Salesforce field."""
        table = self.get_table(object_name)
        for col in table.columns:
            if col.source_name == field_name:
                return col
        raise KeyError(f"{object_name}.{field_name} not found in schema")

    def get_table(self, object_name: str) -> Table:
        """Return a StandardTemplate Table for a Salesforce object (cached)."""
        if object_name not in self._table_cache:
            describe = getattr(self.rest, object_name).describe()
            columns = []
            for f in describe.get('fields', []):
                sf_type = f['type']
                # Bulk API 2.0 does not support compound fields (address, location).
                # Skip the compound wrapper — the individual sub-fields (Street, City, etc.) are already included.
                if sf_type in ('address', 'location'):
                    logger.debug(f"Skipping compound field {f['name']} ({sf_type}) on {object_name}")
                    continue
                columns.append(Column(
                    source_name=f['name'],
                    datatype=SF_TYPE_MAP.get(sf_type, 'string'),
                    source_type=sf_type,
                    primary_key=(f['name'] == 'Id'),
                    nullable=f.get('nillable', True),
                    unique=f.get('unique', False),
                    length=f.get('length') or None,
                    precision=f.get('precision') or None,
                    scale=f.get('scale') or None,
                    description=f.get('label'),
                ))
            self._table_cache[object_name] = Table(source_name=object_name, columns=columns)
        return self._table_cache[object_name]

    def get_schema(self, streams: list[str] | None = None) -> Schema:
        """Return a StandardTemplate Schema. Fetches all objects if streams is None."""
        if streams is None:
            streams = [obj['name'] for obj in self.rest.describe().get('sobjects', [])]
        tables = [self.get_table(s) for s in streams]
        return Schema(source_name='salesforce', tables=tables)

    def get_limits(self) -> dict[str, Any]:
        return self.rest.limits()

    #  Read 

    def query(self, soql: str, object_name: str | None = None) -> Iterator[dict[str, Any]]:
        """REST SOQL query. Best for small, real-time result sets."""
        field_types = {c.source_name: c.source_type for c in self.get_table(object_name).columns if c.source_type} if object_name else {}
        for record in self.rest.query_all_iter(soql):
            clean = {k: v for k, v in record.items() if k != 'attributes'}
            yield cast_record(clean, field_types) if field_types else clean

    def read_data(self, object_name: str) -> Iterator[dict[str, Any]]:
        """Bulk2 full extract of a Salesforce object. Yields one typed dict per record."""
        table = self.get_table(object_name)
        fields = ', '.join(c.source_name for c in table.columns)
        return self.bulk_query(f"SELECT {fields} FROM {object_name}", object_name)

    def bulk_query(self, soql: str, object_name: str | None = None) -> Iterator[dict[str, Any]]:
        """Bulk2 SOQL query. Best for large exports. Yields one typed dict per record."""
        field_types = {c.source_name: c.source_type for c in self.get_table(object_name).columns if c.source_type} if object_name else {}
        for csv_page in self.bulk2.query(soql):
            for record in csv.DictReader(io.StringIO(csv_page)):
                yield cast_record(record, field_types) if field_types else dict(record)

    #  Write (all Bulk2) 

    def insert(self, object_name: str, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
        return getattr(self.bulk2, object_name).insert(records=[prepare_record(r) for r in records])

    def upsert(self, object_name: str, records: Iterable[dict[str, Any]], external_id_field: str) -> dict[str, Any]:
        return getattr(self.bulk2, object_name).upsert(records=[prepare_record(r) for r in records], external_id_field=external_id_field)

    def update(self, object_name: str, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
        return getattr(self.bulk2, object_name).update(records=[prepare_record(r) for r in records])

    def delete(self, object_name: str, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
        return getattr(self.bulk2, object_name).delete(records=[prepare_record(r) for r in records])
