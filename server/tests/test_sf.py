from __future__ import annotations

import os
import uuid
import pytest
import pyarrow as pa

from server.plugins.sf.SfClient import SfClient
from server.plugins.sf.SfRestEngine import SfRest
from server.plugins.sf.Sfbulk2Engine import SfBulk2Handler
from server.plugins.sf.SfModels import Operation
from server.plugins.sf.SfServices import (
    SfObjectSchema,
    SfCacheEntry,
    SfFilter,
    sniff_schema,
    build_soql,
    fetch_first_page,
    fetch_bulk_query_results,
    submit_bulk_ingest,
    strip_to_writeable,
    collections_update,
    collections_delete,
    write_parquet_encrypted,
    open_parquet_lazy,
    teardown_cache,
)

from pathlib import Path
import tempfile

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Shared state across tests — holds IDs and objects created mid-run
# ---------------------------------------------------------------------------

state: dict = {}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def http():
    """
    Module-scoped async SfClient.
    Uses SF_ACCESS_TOKEN from env for dev (SF CLI token).
    Falls back to client credentials if not set.
    """
    client = await SfClient.create(
        access_token=os.getenv("SF_ACCESS_TOKEN") or None,
        consumer_key=os.getenv("SF_CONSUMER_KEY") or None,
        consumer_secret=os.getenv("SF_CONSUMER_SECRET") or None,
    )
    yield client
    await client.close()


@pytest.fixture(scope="module")
def rest(http):
    return SfRest(http)


@pytest.fixture(scope="module")
def bulk2(http):
    return SfBulk2Handler(http)


@pytest.fixture(scope="module")
def tmp_dir():
    """Temp directory for encrypted parquet files — cleaned up after module."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


# ---------------------------------------------------------------------------
# Test 1 — Schema sniff
# ---------------------------------------------------------------------------

async def test_sniff_case_schema(rest):
    """
    Verify describeSObject returns a usable schema for Case.
    Checks that we get queryable and writeable field lists,
    that compound fields are excluded, and FK fields are captured.
    """
    schema = await sniff_schema(rest, "Case")
    state["schema"] = schema

    assert isinstance(schema, SfObjectSchema)
    assert schema.object_name == "Case"

    queryable_names = [f["name"] for f in schema.queryable_fields]
    writeable_names = [f["name"] for f in schema.writeable_fields]

    # Core fields every Case has
    assert "Subject" in queryable_names
    assert "Status" in queryable_names
    assert "CaseNumber" in queryable_names

    # Subject is writeable, CaseNumber is auto-generated — not writeable
    assert "Subject" in writeable_names
    assert "CaseNumber" not in writeable_names

    # Compound address fields must be excluded
    assert "Address" not in queryable_names

    # OwnerId is a reference field — should be in fk_fields
    assert "OwnerId" in schema.fk_fields
    owner_fk = schema.fk_fields["OwnerId"]
    assert "User" in owner_fk["reference_to"]

    print(f"  queryable fields: {len(schema.queryable_fields)}")
    print(f"  writeable fields: {len(schema.writeable_fields)}")
    print(f"  FK fields: {list(schema.fk_fields.keys())}")


# ---------------------------------------------------------------------------
# Test 2 — SOQL builder + REST first page fetch
# ---------------------------------------------------------------------------

async def test_fetch_existing_cases(rest):
    """
    Build a SOQL query and fetch the first page of Cases.
    Verifies pagination metadata and Arrow schema.
    """
    schema = state["schema"]

    soql = build_soql(
        schema,
        filters=[SfFilter("Status", "!=", "Closed")],
        order_by="CreatedDate DESC",
        limit=5,
    )

    print(f"  SOQL: {soql}")
    assert "FROM Case" in soql
    assert "Status != 'Closed'" in soql
    assert "LIMIT 5" in soql

    table, next_url = await fetch_first_page(rest, soql, schema)
    state["first_page"] = table

    assert isinstance(table, pa.Table)
    assert len(table) > 0, "Dev org has no open Cases — seed some from Trailhead sample data"
    assert "Subject" in table.column_names
    assert "Status" in table.column_names

    print(f"  fetched {len(table)} records, next_url={'set' if next_url else 'none'}")


# ---------------------------------------------------------------------------
# Test 3 — REST create (insert a new Case)
# ---------------------------------------------------------------------------

async def test_rest_create_case(rest):
    """
    Create a fresh Case via single-record REST POST.
    Uses a unique subject so we can identify it for cleanup.
    """
    unique_tag = uuid.uuid4().hex[:8]
    subject = f"[SF-ENGINE-TEST] {unique_tag}"
    state["test_subject"] = subject
    state["test_tag"] = unique_tag

    obj = getattr(rest, "Case")
    result = await obj.create({
        "Subject":     subject,
        "Status":      "New",
        "Priority":    "Low",
        "Origin":      "Web",
        "Description": "Created by sf-engine integration test. Safe to delete.",
    })

    assert result.get("success") is True, f"Create failed: {result}"
    record_id = result["id"]
    assert record_id.startswith("500"), f"Expected Case ID (500...), got: {record_id}"

    state["created_id"] = record_id
    print(f"  created Case Id: {record_id}")


# ---------------------------------------------------------------------------
# Test 4 — REST update (single record PATCH)
# ---------------------------------------------------------------------------

async def test_rest_update_case(rest):
    """
    Update the created Case's Description via single-record PATCH.
    Verifies the update by re-fetching.
    """
    record_id = state["created_id"]
    obj = getattr(rest, "Case")

    status_code = await obj.update(record_id, {
        "Description": "Updated by sf-engine test — step 4.",
        "Priority":    "Medium",
    })

    # PATCH returns 204 No Content on success
    assert status_code == 204, f"Update returned unexpected status: {status_code}"

    # Re-fetch and verify
    refreshed = await obj.get(record_id)
    assert refreshed["Priority"] == "Medium"
    assert "step 4" in refreshed["Description"]
    print(f"  updated Case {record_id} — Priority now {refreshed['Priority']}")


# ---------------------------------------------------------------------------
# Test 5 — sObject Collections update (batch PATCH up to 200)
# ---------------------------------------------------------------------------

async def test_collections_update_case(http):
    """
    Update the same Case via the sObject Collections API.
    This is the preferred path for multi-record updates without a bulk job.
    """
    schema = state["schema"]
    record_id = state["created_id"]

    results = await collections_update(
        http,
        object_name="Case",
        records=[{
            "Id":          record_id,
            "Description": "Updated via Collections API — step 5.",
            "Priority":    "High",
        }],
        schema=schema,
        all_or_none=False,
    )

    assert len(results) == 1
    assert results[0]["success"] is True, f"Collections update failed: {results[0]}"
    print(f"  collections update result: {results[0]}")


# ---------------------------------------------------------------------------
# Test 6 — Bulk 2.0 query
# ---------------------------------------------------------------------------

async def test_bulk_query_case(bulk2, tmp_dir):
    """
    Run a Bulk 2.0 query for the test Case and write results to
    an encrypted Parquet file. Verify we can decrypt and read it back
    with Polars.
    """
    schema = state["schema"]
    tag = state["test_tag"]

    soql = build_soql(
        schema,
        filters=[SfFilter("Subject", "LIKE", f"%{tag}%")],
    )

    print(f"  bulk SOQL: {soql}")
    table = await fetch_bulk_query_results(bulk2, "Case", soql, schema)

    assert isinstance(table, pa.Table)
    assert len(table) >= 1, "Bulk query returned no results for test Case"
    print(f"  bulk query returned {len(table)} records")

    # Write encrypted parquet and verify roundtrip
    entry = write_parquet_encrypted(table, tmp_dir, "Case")
    state["cache_entry"] = entry

    assert entry.parquet_path.exists()
    assert entry.record_count == len(table)

    lf = open_parquet_lazy(entry)
    result = lf.filter(
        __import__("polars").col("Subject").str.contains(tag)
    ).collect()

    assert len(result) >= 1
    print(f"  encrypted parquet roundtrip ok — {entry.parquet_path.name}")


# ---------------------------------------------------------------------------
# Test 7 — Bulk 2.0 ingest upsert
# PREREQUISITE: Case must have a custom External ID field: External_Test_Id__c
# Skip this test if the field doesn't exist in your dev org.
# To add it: Setup → Object Manager → Case → Fields → New → Text, check External ID
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.getenv("SF_TEST_EXTERNAL_ID_FIELD"),
    reason="Set SF_TEST_EXTERNAL_ID_FIELD=External_Test_Id__c to run bulk upsert test",
)
async def test_bulk_upsert_case(bulk2):
    """
    Upsert the test Case via Bulk 2.0 using an external ID field.
    Requires a custom External ID field on Case configured in the dev org.
    """
    schema = state["schema"]
    record_id = state["created_id"]
    external_id_field = os.getenv("SF_TEST_EXTERNAL_ID_FIELD")
    external_id_value = f"TEST-{state['test_tag']}"

    # First stamp the external ID on the existing record via REST update
    obj = getattr(SfRest(bulk2._http), "Case")
    await obj.update(record_id, {external_id_field: external_id_value})

    # Now upsert via Bulk 2.0 with a Description change
    upsert_table = pa.table({
        external_id_field: [external_id_value],
        "Description":     ["Updated via Bulk 2.0 upsert — step 7."],
        "Priority":        ["Low"],
    })

    results = await submit_bulk_ingest(
        bulk2,
        object_name="Case",
        table=upsert_table,
        operation=Operation.upsert,
        external_id_field=external_id_field,
    )

    assert len(results) == 1
    assert results[0]["numberRecordsFailed"] == 0, f"Bulk upsert failures: {results[0]}"
    print(f"  bulk upsert result: {results[0]}")


# ---------------------------------------------------------------------------
# Test 8 — Collections delete (cleanup)
# ---------------------------------------------------------------------------

async def test_collections_delete_case(http):
    """
    Delete the test Case via sObject Collections DELETE.
    This is the teardown step — verifies delete returns success
    and the record is no longer fetchable.
    """
    record_id = state["created_id"]

    results = await collections_delete(
        http,
        ids=[record_id],
        all_or_none=False,
    )

    assert len(results) == 1
    assert results[0]["success"] is True, f"Delete failed: {results[0]}"
    print(f"  deleted Case {record_id}")

    # Verify it's gone — REST get should now 404, which SfClient raises as Exception
    obj = getattr(SfRest(http), "Case")
    with pytest.raises(Exception, match="404"):
        await obj.get(record_id)


# ---------------------------------------------------------------------------
# Test 9 — Cache teardown
# ---------------------------------------------------------------------------

async def test_teardown_cache():
    """
    Verify teardown zeros the key and unlinks the parquet file.
    """
    raw = state.get("cache_entry")
    if not raw:
        pytest.skip("No cache entry from bulk query test")

    entry: SfCacheEntry = raw  # linter now knows raw is not None

    path = entry.parquet_path
    assert path.exists()

    teardown_cache(entry)

    assert not path.exists(), "Parquet file still on disk after teardown"
    assert all(b == 0 for b in entry._key), "Encryption key not zeroed after teardown"
    print(f"  cache teardown ok — file unlinked, key zeroed")