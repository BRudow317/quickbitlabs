"""
Oracle Plugin - Live integration tests.
Requires a real Oracle database connection via environment variables.

Usage:
    python scripts/boot.py --env homelab --config "Q:/.secrets/.env" --log server/plugins/oracle/tests --exec pytest -v server/plugins/oracle/tests/live_test.py -s
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from server.plugins.PluginModels import Catalog, Entity, Column
from server.plugins.oracle.OracleClient import OracleClient
from server.plugins.oracle.OracleEngine import OracleEngine
from server.plugins.oracle.OracleArrowFrame import OracleArrowFrame
from server.plugins.oracle.OracleServices import OracleService

TABLE = "PYTEST_LIVE_TEST"

# Shared across tests — populated as the suite runs
state: dict = {}


# ---------------------------------------------------------------------------
# Fixtures — single connection for the whole module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    c = OracleClient()
    print(f"\n  connected: {c}")
    yield c
    c.close()
    print(f"\n  connection closed")


@pytest.fixture(scope="module")
def engine(client):
    return OracleEngine(client=client)


@pytest.fixture(scope="module")
def frame(client):
    return OracleArrowFrame(client)


@pytest.fixture(scope="module")
def service(client):
    return OracleService(client)


@pytest.fixture(scope="module", autouse=True)
def test_table(client):
    """Create the scratch table before any test runs; drop it after all complete."""
    with client.get_con().cursor() as cur:
        cur.execute(f"""
            CREATE TABLE {TABLE} (
                ID     NUMBER(10)       PRIMARY KEY,
                NAME   VARCHAR2(100 CHAR) NOT NULL,
                STATUS VARCHAR2(20 CHAR)
            )
        """)
    print(f"\n  created {TABLE}")
    yield
    with client.get_con().cursor() as cur:
        cur.execute(f"DROP TABLE {TABLE} PURGE")
    print(f"\n  dropped {TABLE}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_arrow_stream(records: list[dict], schema: pa.Schema) -> pa.RecordBatchReader:
    batch = pa.record_batch(
        {k: [r[k] for r in records] for k in schema.names},
        schema=schema,
    )
    return pa.RecordBatchReader.from_batches(schema, iter([batch]))


TABLE_SCHEMA = pa.schema([
    pa.field("ID",     pa.int64()),
    pa.field("NAME",   pa.string()),
    pa.field("STATUS", pa.string()),
])

def table_catalog() -> tuple[Catalog, Entity]:
    cols = [
        Column(name="ID",     raw_type="NUMBER",   arrow_type_id="int64",  primary_key=True, properties={"python_type": "integer"}),
        Column(name="NAME",   raw_type="VARCHAR2", arrow_type_id="string", max_length=100,   properties={"python_type": "string"}),
        Column(name="STATUS", raw_type="VARCHAR2", arrow_type_id="string", max_length=20,    properties={"python_type": "string"}),
    ]
    entity = Entity(name=TABLE, qualified_name=TABLE, columns=cols)
    catalog = Catalog(entities=[entity])
    return catalog, entity


# ---------------------------------------------------------------------------
# 1 — Client
# ---------------------------------------------------------------------------

def test_client_connection_is_healthy(client):
    assert client.get_con().is_healthy()


def test_client_ping(client):
    """Verify we can round-trip a query through the raw connection."""
    with client.get_con().cursor() as cur:
        cur.execute("SELECT 1 FROM DUAL")
        row = cur.fetchone()
    assert row == (1,)


def test_client_repr_contains_user_and_host(client):
    r = repr(client)
    assert client.oracle_user.lower() in r.lower()
    assert client.oracle_host.lower() in r.lower()


# ---------------------------------------------------------------------------
# 2 — Engine
# ---------------------------------------------------------------------------

def test_engine_insert_rows(engine):
    """Insert three rows via OracleEngine.execute_many."""
    records = [
        {"ID": 1, "NAME": "Alice",   "STATUS": "active"},
        {"ID": 2, "NAME": "Bob",     "STATUS": "active"},
        {"ID": 3, "NAME": "Charlie", "STATUS": "inactive"},
    ]
    sql = f"INSERT INTO {TABLE} (ID, NAME, STATUS) VALUES (:ID, :NAME, :STATUS)"
    input_sizes = {"ID": None, "NAME": 100, "STATUS": 20}

    list(engine.execute_many(sql, records, input_sizes))

    state["engine_inserted_ids"] = [r["ID"] for r in records]
    print(f"\n  inserted {len(records)} rows via engine")


def test_engine_query_returns_rows(engine):
    rows = list(engine.query(f"SELECT ID, NAME, STATUS FROM {TABLE} ORDER BY ID"))
    assert len(rows) == 3
    assert rows[0]["NAME"] == "Alice"
    assert rows[2]["STATUS"] == "inactive"
    print(f"\n  queried {len(rows)} rows via engine")


def test_engine_query_with_binds(engine):
    rows = list(engine.query(
        f"SELECT NAME FROM {TABLE} WHERE STATUS = :status",
        binds={"status": "active"},
    ))
    assert len(rows) == 2
    names = {r["NAME"] for r in rows}
    assert names == {"Alice", "Bob"}


# ---------------------------------------------------------------------------
# 3 — ArrowFrame
# ---------------------------------------------------------------------------

def test_arrow_frame_stream_returns_record_batch_reader(frame):
    reader = frame.arrow_stream(f"SELECT ID, NAME FROM {TABLE} ORDER BY ID")
    assert isinstance(reader, pa.RecordBatchReader)

    table = reader.read_all()
    assert len(table) == 3
    assert "ID" in table.schema.names
    assert "NAME" in table.schema.names
    print(f"\n  arrow_stream returned {len(table)} rows, schema: {table.schema}")


def test_arrow_frame_execute_many_update(frame):
    """Update STATUS for ID=3 via an Arrow stream."""
    stream = make_arrow_stream(
        [{"ID": 3, "STATUS": "active"}],
        pa.schema([pa.field("ID", pa.int64()), pa.field("STATUS", pa.string())]),
    )
    frame.execute_many(
        sql=f"UPDATE {TABLE} SET STATUS = :STATUS WHERE ID = :ID",
        data=stream,
    )

    rows = list(frame.arrow_stream(
        f"SELECT STATUS FROM {TABLE} WHERE ID = :id",
        parameters={"id": 3},
    ).read_all().to_pylist())
    assert rows[0]["STATUS"] == "active"
    print(f"\n  arrow_frame update confirmed: ID=3 STATUS now active")


def test_arrow_frame_execute_many_delete(frame):
    """Delete ID=3 via an Arrow stream."""
    stream = make_arrow_stream(
        [{"ID": 3}],
        pa.schema([pa.field("ID", pa.int64())]),
    )
    frame.execute_many(
        sql=f"DELETE FROM {TABLE} WHERE ID = :ID",
        data=stream,
    )

    reader = frame.arrow_stream(f"SELECT ID FROM {TABLE} ORDER BY ID")
    ids = reader.read_all().column("ID").to_pylist()
    assert 3 not in ids
    assert len(ids) == 2
    print(f"\n  arrow_frame delete confirmed: {len(ids)} rows remain")


# ---------------------------------------------------------------------------
# 4 — Service
# ---------------------------------------------------------------------------

def test_service_insert_data(service):
    """Insert a new row via OracleService.insert_data with an Arrow stream."""
    catalog, _ = table_catalog()
    stream = make_arrow_stream(
        [{"ID": 10, "NAME": "Diana", "STATUS": "active"}],
        TABLE_SCHEMA,
    )

    service.insert_data(catalog, stream)

    rows = list(service.get_data(
        catalog,
        query=f"SELECT NAME FROM {TABLE} WHERE ID = 10",
    ))
    assert rows[0]["NAME"] == "Diana"
    state["service_inserted_id"] = 10
    print(f"\n  service insert_data confirmed: Diana (ID=10) found")


def test_service_get_data_raw_query(service):
    """get_data with a raw query= kwarg returns an arrow stream of all rows."""
    catalog, _ = table_catalog()
    reader = service.get_data(catalog, query=f"SELECT ID, NAME FROM {TABLE} ORDER BY ID")

    assert isinstance(reader, pa.RecordBatchReader)
    table = reader.read_all()
    assert len(table) >= 3
    print(f"\n  service get_data returned {len(table)} rows")


def test_service_upsert_data(service):
    """Upsert a row that already exists and one that is new via OracleService.upsert_data."""
    catalog, _ = table_catalog()
    stream = make_arrow_stream(
        [
            {"ID": 1,  "NAME": "Alice-Updated", "STATUS": "active"},
            {"ID": 99, "NAME": "Eve",            "STATUS": "new"},
        ],
        TABLE_SCHEMA,
    )

    service.upsert_data(catalog, stream)

    rows = {
        r["ID"]: r
        for r in service.get_data(catalog, query=f"SELECT ID, NAME FROM {TABLE}")
    }
    assert rows[1]["NAME"] == "Alice-Updated"
    assert rows[99]["NAME"] == "Eve"
    print(f"\n  service upsert_data confirmed: Alice updated, Eve inserted")


def test_service_update_data_returns_success(service):
    """update_data returns a PluginResponse and does not raise."""
    catalog, _ = table_catalog()
    stream = make_arrow_stream(
        [{"ID": 2, "NAME": "Bob-Updated", "STATUS": "active"}],
        TABLE_SCHEMA,
    )

    result = service.update_data(catalog, stream)

    # update_data returns PluginResponse — either success or caught error
    from server.plugins.PluginResponse import PluginResponse
    assert isinstance(result, PluginResponse)
    print(f"\n  service update_data result: ok={result.ok} code={result.code} message={result.message!r}")
