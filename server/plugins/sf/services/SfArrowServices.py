from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TYPE_CHECKING

import pyarrow as pa

from server.plugins.PluginModels import Catalog, ArrowReader
from server.plugins.sf.engines.SfBulk2Engine import Bulk2, DEFAULT_QUERY_PAGE_SIZE
from server.plugins.sf.models.SfTypeMap import sf_to_python

if TYPE_CHECKING:
    from server.plugins.sf.engines.SfRestEngine import SfRest
    from server.plugins.sf.engines.SfBulk2Engine import Bulk2SObject

import logging
logger = logging.getLogger(__name__)

# TODO this is a bandaid fix for failures in the sniffing and describe phase. Salesforce is not accurately reporting decimal precision in the metadata, which leads to Arrow type inference picking a narrower type that then fails when we encounter higher-precision values in the data. To avoid this, we widen all decimals to the max precision of 38. The real fix is to implement proper sniffing and metadata describe logic that can detect the actual precision and scale of decimal fields and set the Column.arrow_type accordingly, but this is a quick mitigation to get things working without crashing or using this bandaid at the cost of memory and storage inefficiency.
def _widen_type(t: pa.DataType) -> pa.DataType:
    """Safety-widen decimal precision to 38 to avoid precision-mismatch crashes."""
    if pa.types.is_decimal(t):
        return pa.decimal128(38, t.scale)
    return t


class SfArrowFrame:
    """
    Arrow conversion layer for the Salesforce plugin.

    Data streams use bare column names (``FieldName``, not ``EntityName_FieldName``).
    The entity-qualified name is available on ``Column.qualified_name`` if callers
    need disambiguation in multi-entity contexts.
    """

    def __init__(self, rest: SfRest, bulk2: Bulk2) -> None:
        self._rest = rest
        self._bulk2 = bulk2

    def rest_to_arrow_stream(
        self,
        soql: str,
        catalog: Catalog,
        include_deleted: bool = False,
        chunk_size: int = 50_000,
    ) -> ArrowReader:
        """
        Execute SOQL via REST and return a lazily-paginated ArrowReader.
        Column names in the stream match the bare SF field names (``Name``, not
        ``Account_Name``). Type conversion is applied via the SF type map.
        """
        entity = catalog.entities[0]
        field_types = {c.name: (c.raw_type or "anyType") for c in entity.columns}

        schema = pa.schema([
            pa.field(c.name, _widen_type(c.arrow_type or pa.string()), nullable=True)
            for c in entity.columns
            if c.arrow_type is not None
        ])

        entity_name = catalog.entities[0].name or soql[:40]

        def _batches() -> Iterator[pa.RecordBatch]:
            try:
                buf: dict[str, list] = {name: [] for name in schema.names}
                count = 0
                result = self._rest.query(soql, include_deleted=include_deleted)
                while True:
                    for r in result.get("records", []):
                        for name in schema.names:
                            buf[name].append(sf_to_python(field_types.get(name, "anyType"), r.get(name)))
                        count += 1
                        if count == chunk_size:
                            yield pa.record_batch(
                                [pa.array(buf[f], type=schema.field(f).type) for f in schema.names],
                                schema=schema,
                            )
                            buf = {name: [] for name in schema.names}
                            count = 0
                    if result.get("done", True):
                        break
                    result = self._rest.query_more(result["nextRecordsUrl"], identifier_is_url=True)
                if count > 0:
                    yield pa.record_batch(
                        [pa.array(buf[f], type=schema.field(f).type) for f in schema.names],
                        schema=schema,
                    )
            except Exception as exc:
                if "INSUFFICIENT_ACCESS" in str(exc):
                    logger.warning(f"{entity_name}: INSUFFICIENT_ACCESS — skipping (no data returned)")
                    return
                raise

        return pa.RecordBatchReader.from_batches(schema, _batches())

    def bulk_to_arrow_stream(
        self,
        object_name: str,
        soql: str,
        catalog: Catalog,
        include_deleted: bool = False,
        max_records: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> ArrowReader:
        """
        Execute SOQL via Bulk 2.0 and return a lazily-paged ArrowReader.
        CSV pages are parsed with explicit schema mapping using bare field names.
        """
        entity = catalog.entities[0]
        schema = pa.schema([
            pa.field(c.name, _widen_type(c.arrow_type or pa.string()), nullable=True)
            for c in entity.columns
        ])

        sf_obj: Bulk2SObject = getattr(self._bulk2, object_name)
        page_iter: Iterator[bytes] = (
            sf_obj.query_all(soql, max_records=max_records)
            if include_deleted
            else sf_obj.query(soql, max_records=max_records)
        )

        def _batches() -> Iterator[pa.RecordBatch]:
            try:
                for csv_bytes in page_iter:
                    yield sf_obj.csv_bytes_to_arrow(csv_bytes, schema=schema)
            except Exception as exc:
                if "INSUFFICIENT_ACCESS" in str(exc):
                    logger.warning(f"{object_name}: INSUFFICIENT_ACCESS — skipping (no data returned)")
                    return
                raise

        return pa.RecordBatchReader.from_batches(schema, _batches())

    @staticmethod
    def stream_to_table(data: ArrowReader, keep_cols: set[str] | None = None) -> pa.Table:
        """Collect an ArrowReader into a pa.Table, optionally projecting to a column subset."""
        batches = list(data)
        if not batches:
            return pa.table({})
        table = pa.Table.from_batches(batches)
        if keep_cols:
            keep = [name for name in table.column_names if name in keep_cols]
            table = table.select(keep)
        return table

    @staticmethod
    def results_to_stream(results: list[dict[str, int]]) -> ArrowReader:
        """Wrap Bulk2 job result summaries into a minimal ArrowReader."""
        schema = pa.schema([
            pa.field("job_id",                 pa.string()),
            pa.field("numberRecordsProcessed", pa.int64()),
            pa.field("numberRecordsFailed",    pa.int64()),
            pa.field("numberRecordsTotal",     pa.int64()),
        ])
        rows = [{**r, "job_id": str(r.get("job_id", ""))} for r in results]
        table = pa.Table.from_pylist(rows, schema=schema)
        return pa.RecordBatchReader.from_batches(schema, table.to_batches())
