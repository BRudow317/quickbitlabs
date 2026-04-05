from __future__ import annotations
import datetime, io, base64, tempfile, os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import polars as pl
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from server.plugins.sf.models.SfModels import (
    JobState,
    Operation,
    SfFieldMeta
)
from server.plugins.sf.models.SfTypeMap import SF_TYPE_TO_ARROW
from server.plugins.sf.engines.SfBulk2Engine import Bulk2, DEFAULT_QUERY_PAGE_SIZE
from server.plugins.sf.engines.SfRestEngine import SfRest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.plugins.sf.engines.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 1 - Schema data structures
# ---------------------------------------------------------------------------

@dataclass
class SfObjectSchema:
    """
    Cached result of a describeSObject call for one SF object.
    Built once by sniff_schema(), reused for every subsequent operation
    on that object within the session.
    """
    object_name: str
    queryable_fields: list[SfFieldMeta]          # SELECT list - read operations only
    writeable_fields: list[SfFieldMeta]           # safe for create / update payloads
    fk_fields: dict[str, SfFieldMeta]             # reference fields keyed by field name
    arrow_schema: pa.Schema                        # derived from queryable_fields

@dataclass
class SfCacheEntry:
    """
    Tracks one encrypted Parquet file on disk.
    Held by the session object. Teardown zeros the key and unlinks the file.

    Key is a bytearray (mutable) so teardown can zero it in place before deletion.
    The nonce is prepended to the ciphertext in the file - no separate storage needed.
    """
    object_name: str
    parquet_path: Path
    record_count: int
    created_at: datetime.datetime
    _key: bytearray = field(repr=False)           # AES-256 key - 32 bytes

async def sniff_schema(rest: SfRest, object_name: str) -> SfObjectSchema:
    """
    Call describeSObject and build the field lists and Arrow schema.
    Compound fields (address, location, compoundFieldName children) are excluded.
    Reference fields carry FK metadata in fk_fields.

    This is the foundation - call it once and pass the result through the session.
    """
    obj_type = getattr(rest, object_name)
    describe = await obj_type.describe()
    fields_raw: list[dict[str, Any]] = describe.get("fields", [])

    queryable: list[SfFieldMeta] = []
    writeable: list[SfFieldMeta] = []
    fk: dict[str, SfFieldMeta] = {}
    arrow_fields: list[pa.Field] = []

    for f in fields_raw:
        sf_type: str = f.get("type", "anyType")
        arrow_type: pa.DataType | None = SF_TYPE_TO_ARROW.get(sf_type)

        # Exclude compound types and compound children
        if arrow_type is None:
            continue
        if f.get("compoundFieldName") is not None:
            continue

        meta: SfFieldMeta = {
            "name":              f["name"],
            "sf_type":           sf_type,
            "arrow_type":        str(arrow_type),
            "filterable":         f.get("filterable", False), # aka queryable
            "createable":        f.get("createable", False),
            "updateable":        f.get("updateable", False),
            "nillable":          f.get("nillable", True),
            "reference_to":      f.get("referenceTo", []),
            "relationship_name": f.get("relationshipName"),
        }

        if f.get("filterable", False):
            queryable.append(meta)
            arrow_fields.append(
                pa.field(f["name"], arrow_type, nullable=f.get("nillable", True))
            )

        if f.get("createable", False) or f.get("updateable", False):
            writeable.append(meta)

        if sf_type == "reference":
            fk[f["name"]] = meta

    return SfObjectSchema(
        object_name=object_name,
        queryable_fields=queryable,
        writeable_fields=writeable,
        fk_fields=fk,
        arrow_schema=pa.schema(arrow_fields),
    )

# ---------------------------------------------------------------------------
# Step 2 - SOQL builder
# ---------------------------------------------------------------------------

@dataclass
class SfFilter:
    """
    One WHERE clause condition.
    operator: =, !=, <, >, <=, >=, LIKE, IN, NOT IN
    value: scalar or list (for IN / NOT IN)
    """
    field: str
    operator: str
    value: Any


def _escape_soql_value(value: Any) -> str:
    """
    Escape a scalar value for safe SOQL interpolation.
    Single quotes and backslashes are the injection surface.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    # String - escape single quotes and backslashes
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"

def _build_where_clause(filters: list[SfFilter]) -> str:
    parts: list[str] = []
    for f in filters:
        op = f.operator.upper()
        if op in ("IN", "NOT IN"):
            if not isinstance(f.value, (list, tuple)):
                raise ValueError(f"IN / NOT IN requires a list value, got {type(f.value)}")
            values = ", ".join(_escape_soql_value(v) for v in f.value)
            parts.append(f"{f.field} {op} ({values})")
        else:
            parts.append(f"{f.field} {op} {_escape_soql_value(f.value)}")
    return " AND ".join(parts)

def build_soql(
    schema: SfObjectSchema,
    filters: list[SfFilter] | None = None,
    order_by: str | None = None,
    limit: int | None = None,
    include_deleted: bool = False,
) -> str:
    """
    Build a SOQL SELECT from the cached queryable field list.
    Always uses queryable_fields - never writeable_fields.
    Filter values are escaped at build time.

    # TODO: SOQL injection via free-text field values - add stricter validation before prod.
    """
    if not schema.queryable_fields:
        raise ValueError(f"No queryable fields found for {schema.object_name} - run sniff_schema first.")

    field_list = ", ".join(f["name"] for f in schema.queryable_fields)
    soql = f"SELECT {field_list} FROM {schema.object_name}"

    if filters:
        soql += f" WHERE {_build_where_clause(filters)}"
    if order_by:
        soql += f" ORDER BY {order_by}"
    if limit:
        soql += f" LIMIT {limit}"

    return soql

# ---------------------------------------------------------------------------
# Step 3 - REST initial fetch (first page → Arrow table)
# ---------------------------------------------------------------------------

async def fetch_first_page(
    rest: SfRest,
    soql: str,
    schema: SfObjectSchema,
) -> tuple[pa.Table, str | None]:
    """
    Execute a SOQL query and return the first page as an Arrow table.
    Also returns nextRecordsUrl if more pages exist, or None if done.

    The caller decides whether to follow pagination synchronously
    or kick off a bulk job for the remainder.
    """
    result = await rest.query(soql)
    records: list[dict[str, Any]] = result.get("records", [])
    next_url: str | None = None if result.get("done") else result.get("nextRecordsUrl")

    table = _records_to_arrow(records, schema.arrow_schema)
    return table, next_url


async def fetch_next_page(
    rest: SfRest,
    next_records_url: str,
    schema: SfObjectSchema,
) -> tuple[pa.Table, str | None]:
    """Follow one nextRecordsUrl hop. Returns the page and the next URL if any."""
    result = await rest.query_more(next_records_url, identifier_is_url=True)
    records: list[dict[str, Any]] = result.get("records", [])
    next_url: str | None = None if result.get("done") else result.get("nextRecordsUrl")
    return _records_to_arrow(records, schema.arrow_schema), next_url


def _records_to_arrow(records: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """
    Convert SF REST JSON records to Arrow column by column.
    PyArrow infers the type from raw JSON values per column,
    then casts to the declared schema type at the C layer.
    No Python touches individual rows.
    """
    if not records:
        return pa.table(
            {f.name: pa.array([], type=f.type) for f in schema},
            schema=schema,
        )

    columns = {}
    for f in schema:
        raw_col = [r.get(f.name) for r in records]
        inferred = pa.array(raw_col)
        columns[f.name] = inferred.cast(f.type, safe=False) if inferred.type != f.type else inferred

    return pa.table(columns, schema=schema)

# ---------------------------------------------------------------------------
# Step 5 - Bulk 2.0 ingest job submission
# ---------------------------------------------------------------------------

async def submit_bulk_ingest(
    bulk2: Bulk2,
    object_name: str,
    table: pa.Table,
    operation: Operation = Operation.upsert,
    external_id_field: str | None = None,
    chunk_size: int | None = None,
    concurrency: int = 1,
) -> list[dict[str, int]]:
    """
    Serialize Arrow table to in-memory CSV bytes and submit as Bulk 2.0 ingest job(s).
    Returns job summary dicts with numberRecordsProcessed, numberRecordsFailed, job_id.

    Table must contain only writeable fields - strip read-only fields before calling.
    No CSV written to disk.
    """
    sf_obj = getattr(bulk2, object_name)
    method = getattr(sf_obj, operation.value)

    if operation == Operation.upsert:
        if not external_id_field:
            raise ValueError("external_id_field is required for upsert operations")
        return await method(table, external_id_field=external_id_field, chunk_size=chunk_size, concurrency=concurrency)

    return await method(table, chunk_size=chunk_size, concurrency=concurrency)

def strip_to_writeable(table: pa.Table, schema: SfObjectSchema) -> pa.Table:
    """
    Drop any columns from an Arrow table that are not in writeable_fields.
    Call this before any ingest or collections write to avoid SF errors
    on formula, rollup, and system-managed fields.
    """
    writeable_names = {f["name"] for f in schema.writeable_fields}
    keep = [name for name in table.column_names if name in writeable_names]
    return table.select(keep)

# ---------------------------------------------------------------------------
# Step 6 - Bulk job status poll (single check, not a wait loop)
# ---------------------------------------------------------------------------

async def poll_bulk_job(
    bulk2: Bulk2,
    job_id: str,
    is_query: bool = True,
) -> JobState:
    """
    Single status check for a bulk job.
    Returns the current JobState without blocking.

    The caller owns the polling loop and the frequency.
    For a blocking wait, use _Bulk2Client.wait_for_job() directly.
    """
    info = await bulk2._http.request(
        "GET",
        f"{bulk2.bulk2_url}{'query' if is_query else 'ingest'}/{job_id}",
    )
    state_str: str = info.json().get("state", "")
    return JobState(state_str)

async def fetch_ingest_results(
    bulk2: Bulk2,
    object_name: str,
    job_id: str,
    schema: SfObjectSchema,
) -> dict[str, pa.Table]:
    """
    Fetch successful, failed, and unprocessed result sets for a completed ingest job.
    All three are fetched concurrently and returned as Arrow tables.

    failed and unprocessed tables include SF's sf__Error and sf__Id columns
    alongside the original record fields - schema inference used for those extras.
    """
    sf_obj = getattr(bulk2, object_name)
    result_bytes = await sf_obj.get_all_ingest_results(job_id)

    return {
        "successfulRecords": _csv_bytes_to_arrow(
            result_bytes["successfulRecords"], schema.arrow_schema
        ),
        # Failed and unprocessed include SF error columns not in our schema
        # Use inference for those since we don't control the shape
        "failedRecords": _csv_bytes_to_arrow_inferred(
            result_bytes["failedRecords"]
        ),
        "unprocessedRecords": _csv_bytes_to_arrow_inferred(
            result_bytes["unprocessedRecords"]
        ),
    }

def _csv_bytes_to_arrow(data: bytes, schema: pa.Schema) -> pa.Table:
    """Parse raw CSV bytes into an Arrow table using an explicit schema."""
    if not data.strip():
        return pa.table({}, schema=schema)
    buf = io.BytesIO(data)
    return pa_csv.read_csv(
        buf,
        convert_options=pa_csv.ConvertOptions(column_types={
            schema.field(i).name: schema.field(i).type
            for i in range(len(schema))
        }),
    )

def _csv_bytes_to_arrow_inferred(data: bytes) -> pa.Table:
    """Parse raw CSV bytes with type inference - for result sets with unknown shape."""
    if not data.strip():
        return pa.table({})
    return pa_csv.read_csv(io.BytesIO(data))


# PME key IDs — logical names, not the key material itself
_FOOTER_KEY_ID = "sf_footer_key"
_COL_KEY_ID    = "sf_col_key"


# ---------------------------------------------------------------------------
# PME helpers — internal
# ---------------------------------------------------------------------------

class _SessionKmsClient(pe.KmsClient):
    """
    In-memory KMS client backed by the session key on SfCacheEntry.
    PyArrow PME generates DEKs internally — this wraps/unwraps them
    using AES-GCM with our master key. Master key is zeroed on teardown.
    """

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
    return pe.KmsConnectionConfig(
        custom_kms_conf={_FOOTER_KEY_ID: b64, _COL_KEY_ID: b64}
    )


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


# ---------------------------------------------------------------------------
# Step 6+7+8 combined — Bulk query streamed directly to encrypted Parquet
# ---------------------------------------------------------------------------

async def stream_bulk_to_parquet(
    bulk2: Bulk2,
    object_name: str,
    soql: str,
    schema: SfObjectSchema,
    max_records: int = DEFAULT_QUERY_PAGE_SIZE,
) -> SfCacheEntry:
    key = bytearray(AESGCM.generate_key(bit_length=256))
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    file_path = Path(tmp.name)
    tmp.close()

    # Normalize to all-nullable — CSV reader always produces nullable arrays
    # regardless of SF nillable flag. Writer schema must match chunk schema.
    nullable_schema = pa.schema([
        pa.field(f.name, f.type, nullable=True)
        for f in schema.arrow_schema
    ])

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
        async for csv_bytes in sf_obj.query(soql, max_records=max_records):
            chunk = _csv_bytes_to_arrow(csv_bytes, nullable_schema)
            writer.write_table(chunk)
            record_count += len(chunk)
            del chunk
    finally:
        writer.close()

    return SfCacheEntry(
        object_name=object_name,
        parquet_path=file_path,
        record_count=record_count,
        created_at=datetime.datetime.now(),
        _key=key,
    )


# ---------------------------------------------------------------------------
# Step 8 — Write a REST result table to encrypted Parquet
# ---------------------------------------------------------------------------

def write_parquet_encrypted(
    table: pa.Table,
    object_name: str,
) -> SfCacheEntry:
    """
    Write a single Arrow table (e.g. a REST first-page result) to encrypted Parquet.
    Uses PME — encrypts row groups as they're written, no double-buffer in memory.

    For bulk results use stream_bulk_to_parquet instead.
    """
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

    return SfCacheEntry(
        object_name=object_name,
        parquet_path=file_path,
        record_count=len(table),
        created_at=datetime.datetime.now(),
        _key=key,
    )


# ---------------------------------------------------------------------------
# Step 9 — Stream decrypted Parquet as Polars DataFrames
# ---------------------------------------------------------------------------

def iter_parquet_batches(
    entry: SfCacheEntry,
    batch_size: int = 100_000,
) -> Iterator[pl.DataFrame]:
    """
    Stream an encrypted Parquet file as Polars DataFrames, one row group at a time.
    Only one batch is decrypted and held in memory at any point.

    Replaces: open_parquet_lazy
    The caller processes each batch and discards it before the next arrives.

    Example:
        for batch in iter_parquet_batches(entry):
            result = batch.filter(pl.col("Status") == "Open")
            # process result, then batch goes out of scope
    """
    dec_props = _decryption_props(bytes(entry._key))
    pf = pq.ParquetFile(
        str(entry.parquet_path),
        decryption_properties=dec_props,
    )
    for record_batch in pf.iter_batches(batch_size=batch_size):
        pldf = pl.from_arrow(record_batch)
        if isinstance(pldf, pl.DataFrame):
            yield pldf


def open_parquet_arrow(entry: SfCacheEntry) -> pa.Table:
    """
    Read the full encrypted Parquet file into an Arrow table.
    Use only when you need the full dataset and have confirmed it fits in memory.
    For large files use iter_parquet_batches instead.
    """
    dec_props = _decryption_props(bytes(entry._key))
    return pq.read_table(
        str(entry.parquet_path),
        decryption_properties=dec_props,
    )


# Step 10 - CRUD write-back via sObject Collections API
async def collections_update(
    http: SfClient,
    object_name: str,
    records: list[dict[str, Any]],
    schema: SfObjectSchema,
    all_or_none: bool = False,
) -> list[dict[str, Any]]:
    """
    PATCH up to 200 records via the sObject Collections API.
    Each record must include an 'Id' field.
    Payload is stripped to writeable_fields - read-only fields dropped silently.

    Returns per-record result list from SF (id, success, errors).
    """
    if len(records) > 200:
        raise ValueError("sObject Collections supports max 200 records per call.")

    writeable_names = {f["name"] for f in schema.writeable_fields} | {"Id"}
    cleaned = [
        {"attributes": {"type": object_name}, **{k: v for k, v in r.items() if k in writeable_names}}
        for r in records
    ]

    response = await http.request(
        "PATCH",
        "composite/sobjects",
        json={"allOrNone": all_or_none, "records": cleaned},
    )
    return response.json()


async def collections_create(
    http: SfClient,
    object_name: str,
    records: list[dict[str, Any]],
    schema: SfObjectSchema,
    all_or_none: bool = False,
) -> list[dict[str, Any]]:
    """
    POST up to 200 records via the sObject Collections API.
    Payload stripped to createable fields only.

    Returns per-record result list (id, success, errors).
    """
    if len(records) > 200:
        raise ValueError("sObject Collections supports max 200 records per call.")

    createable_names = {f["name"] for f in schema.writeable_fields if f.get("createable")}
    cleaned = [
        {"attributes": {"type": object_name}, **{k: v for k, v in r.items() if k in createable_names}}
        for r in records
    ]

    response = await http.request(
        "POST",
        "composite/sobjects",
        json={"allOrNone": all_or_none, "records": cleaned},
    )
    return response.json()


async def collections_delete(
    http: SfClient,
    ids: list[str],
    all_or_none: bool = False,
) -> list[dict[str, Any]]:
    """
    DELETE up to 200 records by SF ID via the sObject Collections API.
    No CSV. No bulk job. Synchronous JSON response with per-record results.
    allOrNone=False means partial success - failed deletes don't roll back successes.
    """
    if len(ids) > 200:
        raise ValueError("sObject Collections supports max 200 records per call.")

    response = await http.request(
        "DELETE",
        "composite/sobjects",
        params={
            "ids": ",".join(ids),
            "allOrNone": str(all_or_none).lower(),
        },
    )
    return response.json()

# Step 12 - Teardown

def teardown_cache(entry: SfCacheEntry) -> None:
    """
    Zero the encryption key in place and unlink the Parquet file from disk.
    Call this when the session object that owns the entry is being destroyed.

    Safe to call multiple times - missing file is not an error.
    """
    # Zero the key bytes in place - bytearray is mutable
    for i in range(len(entry._key)):
        entry._key[i] = 0

    try:
        entry.parquet_path.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Failed to unlink cache file {entry.parquet_path}: {e}")