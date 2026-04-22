"""
Feather (Arrow IPC) engine for the Reader plugin.

Feather v2 is the Arrow IPC file format. PyArrow's ipc.open_file / ipc.new_file
are used directly for efficient streaming and schema-only reads.

Encryption: AES-GCM via ReaderEncryption.encrypt/decrypt_bytes (same as CsvEngine).
The wrapped per-file key is stored in a <file>.rkey sidecar alongside the Feather file.
"""
from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

from server.plugins.readers.ReaderEncryption import decrypt_bytes, encrypt_bytes


class FeatherEngine:

    @staticmethod
    def _sidecar(path: Path) -> Path:
        return path.with_name(path.name + ".rkey")

    @staticmethod
    def _open_reader(path: Path, master_key_b64: str | None) -> ipc.RecordBatchFileReader:
        """Open an IPC file reader, decrypting first if necessary."""
        if master_key_b64 is not None:
            sidecar = FeatherEngine._sidecar(path)
            if not sidecar.exists():
                raise FileNotFoundError(f"Encryption sidecar not found: {sidecar}")
            raw = decrypt_bytes(path.read_bytes(), sidecar.read_bytes(), master_key_b64)
            return ipc.open_file(io.BytesIO(raw))
        return ipc.open_file(str(path))

    @staticmethod
    def read_schema(path: Path, master_key_b64: str | None = None) -> pa.Schema:
        """Read schema from IPC file header without loading row data."""
        return FeatherEngine._open_reader(path, master_key_b64).schema_arrow

    @staticmethod
    def read(path: Path, master_key_b64: str | None = None) -> pa.RecordBatchReader:
        """Stream record batches from an IPC file. Decrypts if key is provided."""
        reader = FeatherEngine._open_reader(path, master_key_b64)
        schema = reader.schema_arrow

        def _batches():
            for i in range(reader.num_record_batches):
                yield reader.get_batch(i)

        return pa.RecordBatchReader.from_batches(schema, _batches())

    @staticmethod
    def write(path: Path, stream: pa.RecordBatchReader, master_key_b64: str | None = None) -> None:
        """Write stream to Feather IPC format. Encrypts and writes .rkey sidecar if key is provided."""
        schema = stream.schema

        if master_key_b64 is not None:
            buf = io.BytesIO()
            writer = ipc.new_file(buf, schema)
            try:
                for batch in stream:
                    writer.write_batch(batch)
            finally:
                writer.close()
            encrypted, sidecar = encrypt_bytes(buf.getvalue(), master_key_b64)
            path.write_bytes(encrypted)
            FeatherEngine._sidecar(path).write_bytes(sidecar)
        else:
            writer = ipc.new_file(str(path), schema)
            try:
                for batch in stream:
                    writer.write_batch(batch)
            finally:
                writer.close()
