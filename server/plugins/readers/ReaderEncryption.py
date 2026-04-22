"""
Shared encryption utilities for the Reader plugin.

Parquet:  uses PyArrow modular encryption (AES_GCM_V1) — wrapped keys live in the
          Parquet footer, no sidecar file needed.

CSV / Feather: no native format encryption, so we AES-GCM the raw bytes and persist
               the wrapped per-file data key in a <filename>.rkey sidecar.

Sidecar layout (base64-encoded):
    key_nonce   (12 B)  — nonce used to wrap the data key with the master key
    wrapped_key (48 B)  — AES-GCM(master, data_key)  [32 B key + 16 B GCM tag]
Encrypted file layout (raw bytes):
    data_nonce  (12 B)  — nonce used to encrypt file data
    ciphertext  (rest)  — AES-GCM(data_key, plaintext)
"""
from __future__ import annotations

import base64
import os

import pyarrow as pa
import pyarrow.parquet.encryption as pe
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_FOOTER_KEY_ID = "reader_footer_key"
_COL_KEY_ID    = "reader_col_key"


# ---------------------------------------------------------------------------
# Parquet modular encryption (mirrors SfParquetServices._SessionKmsClient)
# ---------------------------------------------------------------------------

class _ReaderKmsClient(pe.KmsClient):
    """Session-scoped in-memory KMS adapter backed by a caller-supplied master key."""

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


def _kms_config(master_key_b64: str) -> pe.KmsConnectionConfig:
    return pe.KmsConnectionConfig(
        custom_kms_conf={_FOOTER_KEY_ID: master_key_b64, _COL_KEY_ID: master_key_b64}
    )


def _crypto_factory() -> pe.CryptoFactory:
    return pe.CryptoFactory(lambda cfg: _ReaderKmsClient(cfg))


def parquet_encryption_props(schema: pa.Schema, master_key_b64: str) -> pe.FileEncryptionProperties:
    return _crypto_factory().file_encryption_properties(
        _kms_config(master_key_b64),
        pe.EncryptionConfiguration(
            footer_key=_FOOTER_KEY_ID,
            column_keys={_COL_KEY_ID: [f.name for f in schema]},
            encryption_algorithm="AES_GCM_V1",
            plaintext_footer=False,
        ),
    )


def parquet_decryption_props(master_key_b64: str) -> pe.FileDecryptionProperties:
    return _crypto_factory().file_decryption_properties(_kms_config(master_key_b64))


# ---------------------------------------------------------------------------
# Generic AES-GCM byte-level encryption (CSV, Feather)
# ---------------------------------------------------------------------------

def encrypt_bytes(data: bytes, master_key_b64: str) -> tuple[bytes, bytes]:
    """
    Encrypt *data* with a fresh 256-bit per-file data key.

    Returns:
        encrypted_data — data_nonce (12 B) + ciphertext
        sidecar_blob   — base64(key_nonce (12 B) + wrapped_data_key (48 B))
    """
    master = base64.b64decode(master_key_b64)
    data_key = AESGCM.generate_key(bit_length=256)   # 32 B

    data_nonce = os.urandom(12)
    ciphertext = AESGCM(data_key).encrypt(data_nonce, data, None)

    key_nonce = os.urandom(12)
    wrapped_key = AESGCM(master).encrypt(key_nonce, data_key, None)  # 32 + 16 = 48 B

    sidecar = base64.b64encode(key_nonce + wrapped_key)
    return data_nonce + ciphertext, sidecar


def decrypt_bytes(encrypted_data: bytes, sidecar_blob: bytes, master_key_b64: str) -> bytes:
    """
    Decrypt *encrypted_data* using *sidecar_blob* and the master key.
    Inverse of encrypt_bytes.
    """
    master = base64.b64decode(master_key_b64)
    raw = base64.b64decode(sidecar_blob)

    key_nonce   = raw[:12]
    wrapped_key = raw[12:]           # 48 B: 32 B data key + 16 B GCM tag

    data_key   = AESGCM(master).decrypt(key_nonce, wrapped_key, None)
    data_nonce = encrypted_data[:12]
    ciphertext = encrypted_data[12:]
    return AESGCM(data_key).decrypt(data_nonce, ciphertext, None)
