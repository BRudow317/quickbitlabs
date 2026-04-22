"""
CSV engine for the Reader plugin.

Reads via pyarrow.csv.open_csv (streaming, schema inferred from header row).
Writes via polars (pyarrow has no CSV writer).

Encryption: AES-GCM via ReaderEncryption.encrypt/decrypt_bytes.
The wrapped per-file key is stored in a <file>.rkey sidecar alongside the CSV.
"""
from __future__ import annotations

import io
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv

from server.plugins.readers.ReaderEncryption import decrypt_bytes, encrypt_bytes


class CsvEngine:

    @staticmethod
    def _sidecar(path: Path) -> Path:
        return path.with_name(path.name + ".rkey")

    @staticmethod
    def read_schema(path: Path, master_key_b64: str | None = None) -> pa.Schema:
        """Return schema without consuming the full file."""
        return CsvEngine.read(path, master_key_b64=master_key_b64).schema

    @staticmethod
    def read(path: Path, master_key_b64: str | None = None) -> pa.RecordBatchReader:
        """Return a streaming RecordBatchReader. Decrypts first if a master key is provided."""
        if master_key_b64 is not None:
            sidecar = CsvEngine._sidecar(path)
            if not sidecar.exists():
                raise FileNotFoundError(f"Encryption sidecar not found: {sidecar}")
            raw = decrypt_bytes(path.read_bytes(), sidecar.read_bytes(), master_key_b64)
            return pa_csv.open_csv(io.BytesIO(raw))
        return pa_csv.open_csv(str(path))

    @staticmethod
    def write(path: Path, stream: pa.RecordBatchReader, master_key_b64: str | None = None) -> None:
        """Write stream to CSV. Encrypts and writes .rkey sidecar if a master key is provided."""
        table = stream.read_all()
        df = pl.from_arrow(table)

        if master_key_b64 is not None:
            buf = io.BytesIO()
            df.write_csv(buf)
            encrypted, sidecar = encrypt_bytes(buf.getvalue(), master_key_b64)
            path.write_bytes(encrypted)
            CsvEngine._sidecar(path).write_bytes(sidecar)
        else:
            df.write_csv(str(path))
