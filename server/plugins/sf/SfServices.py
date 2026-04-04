from __future__ import annotations
import datetime
import io, os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.ipc as pa_ipc
import polars as pl
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from server.plugins.sf.SfModels import (
    JobState,
    Operation,
    SfFieldMeta
)
from server.plugins.sf.SfTypeMap import SF_TYPE_TO_ARROW
from server.plugins.sf.Sfbulk2Engine import bulk2, DEFAULT_QUERY_PAGE_SIZE
from server.plugins.sf.SfRestEngine import SfRest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.plugins.sf.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)

# AES-GCM nonce length - 12 bytes is the GCM standard
_NONCE_LEN = 12

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

# ---------------------------------------------------------------------------
# Step 1 - Schema sniff
# ---------------------------------------------------------------------------

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
    Extracts each field as a Python list once per column, not once per row.
    Cast happens at the Arrow array level — no row-by-row Python type checking.
    """
    if not records:
        return pa.table(
            {f.name: pa.array([], type=f.type) for f in schema},
            schema=schema,
        )

    # Strip SF metadata field in one pass
    columns = {}
    for field in schema:
        raw_col = [r.get(field.name) for r in records]
        columns[field.name] = pa.array(raw_col, type=field.type, safe=False)

    return pa.table(columns, schema=schema)

# ---------------------------------------------------------------------------
# Step 5 - Bulk 2.0 ingest job submission
# ---------------------------------------------------------------------------

async def submit_bulk_ingest(
    bulk2: bulk2,
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
    bulk2: bulk2,
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

# ---------------------------------------------------------------------------
# Step 7 - Bulk result fetch → Arrow tables
# ---------------------------------------------------------------------------

async def fetch_bulk_query_results(
    bulk2: bulk2,
    object_name: str,
    soql: str,
    schema: SfObjectSchema,
    max_records: int = DEFAULT_QUERY_PAGE_SIZE,
) -> pa.Table:
    """
    Submit a Bulk 2.0 query job and collect all pages into a single Arrow table.
    Uses the explicit schema from sniff_schema - no type inference on CSV bytes.
    Blocks until the job completes (uses the internal wait loop).
    """
    sf_obj = getattr(bulk2, object_name)
    pages: list[pa.Table] = []

    async for csv_bytes in sf_obj.query(soql, max_records=max_records):
        page = _csv_bytes_to_arrow(csv_bytes, schema.arrow_schema)
        pages.append(page)

    return pa.concat_tables(pages) if pages else pa.table({}, schema=schema.arrow_schema)

async def fetch_ingest_results(
    bulk2: bulk2,
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

# ---------------------------------------------------------------------------
# Step 8 - Encrypt and write to Parquet
# ---------------------------------------------------------------------------

def write_parquet_encrypted(
    table: pa.Table,
    dir_path: Path,
    object_name: str,
) -> SfCacheEntry:
    """
    Serialize an Arrow table to Parquet bytes in memory,
    encrypt with AES-256-GCM, write to a temp file in dir_path.

    The encryption key lives on the returned SfCacheEntry.
    The nonce is prepended to the ciphertext in the file.
    No unencrypted bytes touch disk.

    Call teardown_cache(entry) to zero the key and unlink the file.
    """
    # Serialize to Parquet bytes in memory
    buf = io.BytesIO()
    import pyarrow.parquet as pq
    pq.write_table(table, buf, compression="snappy")
    parquet_bytes = buf.getvalue()

    # Encrypt
    key = bytearray(os.urandom(32))             # AES-256
    nonce = os.urandom(_NONCE_LEN)
    aesgcm = AESGCM(bytes(key))
    ciphertext = aesgcm.encrypt(nonce, parquet_bytes, None)

    # Write nonce + ciphertext
    file_path = dir_path / f"sf_{object_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}.enc"
    file_path.write_bytes(nonce + ciphertext)

    return SfCacheEntry(
        object_name=object_name,
        parquet_path=file_path,
        record_count=len(table),
        created_at=datetime.datetime.now(),
        _key=key,
    )



# Step 9 - Decrypt and scan with Polars

def open_parquet_lazy(entry: SfCacheEntry) -> pl.LazyFrame:
    """
    Decrypt the cached Parquet file into a memory buffer and return
    a Polars LazyFrame. The decrypted bytes never touch disk.

    The LazyFrame holds a reference to the in-memory buffer - collect()
    or streaming operations read from it. The buffer is released when
    the LazyFrame goes out of scope.
    """
    raw = entry.parquet_path.read_bytes()
    nonce, ciphertext = raw[:_NONCE_LEN], raw[_NONCE_LEN:]
    aesgcm = AESGCM(bytes(entry._key))
    parquet_bytes = aesgcm.decrypt(nonce, ciphertext, None)

    buf = io.BytesIO(parquet_bytes)
    # Read into Polars via PyArrow buffer - streaming=True defers materialization
    return pl.read_parquet(buf).lazy()


def open_parquet_arrow(entry: SfCacheEntry) -> pa.Table:
    """
    Decrypt and return as a PyArrow table.
    Use when you need Arrow interop rather than Polars.
    """
    raw = entry.parquet_path.read_bytes()
    nonce, ciphertext = raw[:_NONCE_LEN], raw[_NONCE_LEN:]
    aesgcm = AESGCM(bytes(entry._key))
    parquet_bytes = aesgcm.decrypt(nonce, ciphertext, None)

    import pyarrow.parquet as pq
    return pq.read_table(io.BytesIO(parquet_bytes))



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
            "ids":       ",".join(ids),
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