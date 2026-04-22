"""
Parquet engine for the Reader plugin.

Uses PyArrow modular encryption (AES_GCM_V1) when a master key is supplied.
Wrapped per-column and footer keys are stored inside the Parquet file itself —
no sidecar file is needed.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from server.plugins.readers.ReaderEncryption import (
    parquet_decryption_props,
    parquet_encryption_props,
)


class ParquetEngine:

    @staticmethod
    def read_schema(path: Path, master_key_b64: str | None = None) -> pa.Schema:
        """Read only the Parquet footer — no row data loaded."""
        kwargs: dict[str, Any] = {}
        if master_key_b64:
            kwargs["decryption_properties"] = parquet_decryption_props(master_key_b64)
        return pq.read_schema(str(path), **kwargs)

    @staticmethod
    def read(path: Path, master_key_b64: str | None = None) -> pa.RecordBatchReader:
        """Stream row groups as RecordBatches. Decrypts transparently if key is provided."""
        kwargs: dict[str, Any] = {}
        if master_key_b64:
            kwargs["decryption_properties"] = parquet_decryption_props(master_key_b64)
        pf = pq.ParquetFile(str(path), **kwargs)
        schema = pf.schema_arrow

        def _batches():
            for batch in pf.iter_batches():
                yield batch

        return pa.RecordBatchReader.from_batches(schema, _batches())

    @staticmethod
    def write(path: Path, stream: pa.RecordBatchReader, master_key_b64: str | None = None) -> None:
        """Write stream to Parquet with Snappy compression. Encrypts if key is provided."""
        schema = stream.schema
        kwargs: dict[str, Any] = {"compression": "snappy"}
        if master_key_b64:
            kwargs["encryption_properties"] = parquet_encryption_props(schema, master_key_b64)

        writer = pq.ParquetWriter(str(path), schema, **kwargs)
        try:
            for batch in stream:
                writer.write_batch(batch)
        finally:
            writer.close()
