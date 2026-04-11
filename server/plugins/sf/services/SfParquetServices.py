from __future__ import annotations

import base64
import datetime
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from server.plugins.PluginModels import Catalog

from server.plugins.sf.engines.SfBulk2Engine import (
    Bulk2,
    DEFAULT_QUERY_PAGE_SIZE,
    csv_bytes_to_arrow,
    csv_bytes_to_arrow_inferred,
)

import logging
logger = logging.getLogger(__name__)


@dataclass
class PqCacheEntry:
    """Tracks one encrypted Parquet file on disk plus in-memory key material."""
    object_name: str
    parquet_path: Path
    record_count: int
    created_at: datetime.datetime
    _key: bytearray = field(repr=False)


_FOOTER_KEY_ID = "sf_footer_key"
_COL_KEY_ID = "sf_col_key"


class _SessionKmsClient(pe.KmsClient):
    """In-memory KMS adapter backed by the per-session master key."""

    def __init__(self, config: pe.KmsConnectionConfig) -> None:
        super().__init__()
        self._master_keys: dict[str, bytes] = {
            k: base64.b64decode(v)
            for k, v in config.custom_kms_conf.items()
        }

    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str:
        master = self._master_keys[master_key_identifier]
        nonce = os.urandom(12)
        wrapped = AESGCM(master).encrypt(nonce, key_bytes, None)
        return base64.b64encode(nonce + wrapped).decode()

    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes:
        master = self._master_keys[master_key_identifier]
        raw = base64.b64decode(wrapped_key)
        nonce, ciphertext = raw[:12], raw[12:]
        return AESGCM(master).decrypt(nonce, ciphertext, None)


def _kms_config(key: bytes) -> pe.KmsConnectionConfig:
    b64 = base64.b64encode(key).decode()
    return pe.KmsConnectionConfig(custom_kms_conf={_FOOTER_KEY_ID: b64, _COL_KEY_ID: b64})


def _crypto_factory() -> pe.CryptoFactory:
    return pe.CryptoFactory(lambda kms_conn: _SessionKmsClient(kms_conn))


def _encryption_props(key: bytes, schema: pa.Schema):
    return _crypto_factory().file_encryption_properties(
        _kms_config(key),
        pe.EncryptionConfiguration(
            footer_key=_FOOTER_KEY_ID,
            column_keys={_COL_KEY_ID: [f.name for f in schema]},
            encryption_algorithm="AES_GCM_V1",
            plaintext_footer=False,
        ),
    )

def _decryption_props(key: bytes):
    return _crypto_factory().file_decryption_properties(_kms_config(key))

def stream_bulk_to_parquet(
    bulk2: Bulk2,
    object_name: str,
    soql: str,
    catalog: Catalog,
    max_records: int = DEFAULT_QUERY_PAGE_SIZE,
) -> PqCacheEntry:
    key = bytearray(AESGCM.generate_key(bit_length=256))
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    file_path = Path(tmp.name)
    tmp.close()

    nullable_schema = pa.schema([pa.field(f.name, f.type, nullable=True) for f in catalog.arrow_schema()])

    enc_props = _encryption_props(bytes(key), nullable_schema)
    writer = pq.ParquetWriter(
        str(file_path),
        nullable_schema,
        encryption_properties=enc_props,
        compression="snappy",
    )

    record_count = 0
    sf_obj = getattr(bulk2, object_name)

    try:
        for csv_bytes in sf_obj.query(soql, max_records=max_records):
            chunk = csv_bytes_to_arrow(csv_bytes, nullable_schema)
            writer.write_table(chunk)
            record_count += len(chunk)
            del chunk
    finally:
        writer.close()

    return PqCacheEntry(
        object_name=object_name,
        parquet_path=file_path,
        record_count=record_count,
        created_at=datetime.datetime.now(),
        _key=key,
    )

def write_parquet_encrypted(table: pa.Table, object_name: str) -> PqCacheEntry:
    """Write an Arrow table to encrypted Parquet and return cache metadata."""
    key = bytearray(AESGCM.generate_key(bit_length=256))
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    file_path = Path(tmp.name)
    tmp.close()

    enc_props = _encryption_props(bytes(key), table.schema)
    writer = pq.ParquetWriter(
        str(file_path),
        table.schema,
        encryption_properties=enc_props,
        compression="snappy",
    )
    try:
        writer.write_table(table)
    finally:
        writer.close()

    return PqCacheEntry(
        object_name=object_name,
        parquet_path=file_path,
        record_count=len(table),
        created_at=datetime.datetime.now(),
        _key=key,
    )

def iter_parquet_batches(
    entry: PqCacheEntry,
    batch_size: int = 100_000,
) -> Iterator[pl.DataFrame]:
    """Stream encrypted Parquet row groups as Polars DataFrames."""
    dec_props = _decryption_props(bytes(entry._key))
    pf = pq.ParquetFile(
        str(entry.parquet_path),
        decryption_properties=dec_props,
    )
    for record_batch in pf.iter_batches(batch_size=batch_size):
        pldf = pl.from_arrow(record_batch)
        if isinstance(pldf, pl.DataFrame):
            yield pldf


def open_parquet_arrow(entry: PqCacheEntry) -> pa.Table:
    """Read full encrypted Parquet contents into Arrow."""
    dec_props = _decryption_props(bytes(entry._key))
    return pq.read_table(
        str(entry.parquet_path),
        decryption_properties=dec_props,
    )


def teardown_cache(entry: PqCacheEntry) -> None:
    """Zero in-memory key bytes and unlink parquet cache file."""
    for i in range(len(entry._key)):
        entry._key[i] = 0

    try:
        entry.parquet_path.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Failed to unlink cache file {entry.parquet_path}: {e}")
