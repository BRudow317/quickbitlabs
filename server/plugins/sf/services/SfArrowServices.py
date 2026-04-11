from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TYPE_CHECKING

import pyarrow as pa

from server.plugins.PluginModels import Catalog, ArrowStream
from server.plugins.sf.engines.SfBulk2Engine import Bulk2, DEFAULT_QUERY_PAGE_SIZE
from server.plugins.sf.models.SfTypeMap import sf_to_python

if TYPE_CHECKING:
    from server.plugins.sf.engines.SfRestEngine import SfRest
    from server.plugins.sf.engines.SfBulk2Engine import Bulk2SObject

import logging
logger = logging.getLogger(__name__)

class SfArrowFrame:
    """
    Arrow conversion layer for the Salesforce plugin.
    The Catalog is the schema - pm_arrow_stream handles schema derivation,
    join nullability, and metadata embedding.
    Records are adapted to entity_column format before passing through.
    """

    def __init__(self, rest: SfRest, bulk2: Bulk2) -> None:
        self._rest = rest
        self._bulk2 = bulk2

    def rest_to_arrow_stream(
        self,
        soql: str,
        catalog: Catalog,
        include_deleted: bool = False,
    ) -> ArrowStream:
        """
        Execute SOQL via REST and return a lazily-paginated ArrowStream.
        Records are adapted from SF field names to entity_column format
        and passed through PluginModels.arrow_stream for schema + metadata.
        """
        entity = catalog.entities[0]
        field_types = {c.name: (c.raw_type or "anyType") for c in entity.columns}

        def _records() -> Iterator[dict[str, Any]]:
            result = self._rest.query(soql, include_deleted=include_deleted)
            while True:
                for r in result.get("records", []):
                    yield {
                        f"{entity.name}_{k}": sf_to_python(field_types.get(k, "anyType"), v)
                        for k, v in r.items() if k != "attributes"
                    }
                if result.get("done", True):
                    break
                result = self._rest.query_more(result["nextRecordsUrl"], identifier_is_url=True)

        return catalog.arrow_stream(_records())

    def bulk_to_arrow_stream(
        self,
        object_name: str,
        soql: str,
        catalog: Catalog,
        include_deleted: bool = False,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> ArrowStream:
        """
        Execute SOQL via Bulk 2.0 and return a lazily-paged ArrowStream.
        CSV pages are parsed with EXPLICIT schema mapping, renamed to entity_column format,
        and passed through PluginModels.arrow_stream for schema + metadata.
        """
        entity = catalog.entities[0]
        # Create a schema for the SF-specific CSV columns (no prefix yet)
        # Widen decimal precision to 38 to avoid "precision inferred" crashes
        def _widen_type(t: pa.DataType) -> pa.DataType:
            if pa.types.is_decimal(t):
                return pa.decimal128(38, t.scale)
            return t

        sf_schema = pa.schema([
            pa.field(c.name, _widen_type(c.arrow_type or pa.string()), nullable=True) 
            for c in entity.columns
        ])
        
        sf_obj: Bulk2SObject = getattr(self._bulk2, object_name)
        page_iter: Iterator[bytes] = (
            sf_obj.query_all(soql, max_records=max_records)
            if include_deleted
            else sf_obj.query(soql, max_records=max_records)
        )

        def _records() -> Iterator[dict[str, Any]]:
            for csv_bytes in page_iter:
                # Parse CSV with explicit schema to ensure correct types (Decimals, etc)
                table = sf_obj.csv_bytes_to_arrow(csv_bytes, schema=sf_schema)
                for row in table.to_pylist():
                    yield {f"{entity.name}_{k}": v for k, v in row.items()}

        return catalog.arrow_stream(_records())

    @staticmethod
    def stream_to_table(data: ArrowStream, keep_cols: set[str] | None = None) -> pa.Table:
        """Collect an ArrowStream into a pa.Table, optionally projecting to a column subset."""
        batches = list(data)
        if not batches:
            return pa.table({})
        table = pa.Table.from_batches(batches)
        if keep_cols:
            keep = [name for name in table.column_names if name in keep_cols]
            table = table.select(keep)
        return table

    @staticmethod
    def results_to_stream(results: list[dict[str, int]]) -> ArrowStream:
        """Wrap Bulk2 job result summaries into a minimal ArrowStream."""
        schema = pa.schema([
            pa.field("job_id",                 pa.string()),
            pa.field("numberRecordsProcessed", pa.int64()),
            pa.field("numberRecordsFailed",    pa.int64()),
            pa.field("numberRecordsTotal",     pa.int64()),
        ])
        rows = [{**r, "job_id": str(r.get("job_id", ""))} for r in results]
        table = pa.Table.from_pylist(rows, schema=schema)
        return pa.RecordBatchReader.from_batches(schema, table.to_batches())