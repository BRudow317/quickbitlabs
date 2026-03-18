# Server

The backend module for QuickBitLabs. Connects external data systems through a common contract so any source can talk to any target without either needing to know anything about the other.

---

## Directory Structure

```shell
server/
├── api/            # REST API layer — how the frontend and external callers reach services (to be implemented)
├── auth/           # Access control and authorization for the API
├── configs/        # High-level configuration via pydantic-settings
├── connectors/     # Integrations with external systems (Salesforce, Postgres, Oracle, CSV, etc.)
│   └── registry.py # Lazy-load connector lookup by name
├── core/           # Internal server utilities (security, etc.)
├── engine/         # Execution layer — runs the operations services define
├── models/         # Contracts, shapes, and schemas used throughout the program
│   ├── ConnectorProtocol.py  # Connector protocol — what every connector must implement
│   ├── ConnectorResponse.py  # Universal return type for all connector operations
│   └── ConnectorStandard.py  # Schema / Table / Column — the universal data contract
├── services/       # Actions available to external systems and users
│   └── MigrationService.py   # Source → target data migration orchestration
├── tests/          # Tests
└── utils/          # Shared utilities (logging, helpers, encryption)
```

---

## Core Concepts

### The Data Contract: `ConnectorStandard`

Every piece of metadata in the system lives as a node in this hierarchy:

```
Schema
  └── Table(s)
        └── Column(s)
              └── DataStream  (Iterable[dict[str, Any]])
```

`Schema`, `Table`, and `Column` are Pydantic models defined in `models/ConnectorStandard.py`. They carry both `source_*` and `target_*` fields so the same object describes both ends of a data movement without duplication. Parent references (`Column.table`, `Table.parent_schema`) are wired automatically on instantiation.

**`Column`** is where the real contract lives. Every column must declare:

| Field | Purpose |
|---|---|
| `source_name` / `target_name` | Name at each end — diverge during mapping |
| `datatype` | Python type the engine works with (`string`, `integer`, `datetime`, etc.) |
| `raw_type` | Raw type from the source system (`"currency"`, `"VARCHAR2"`, `"picklist"`) |
| `primary_key` | Used for upsert matching |
| `nullable` / `unique` | Constraint metadata |
| `length` / `precision` / `scale` | Sizing — used for DDL and validation |
| `read_only` | Skip on write — formula fields, auto-numbers, system timestamps |
| `default_value` | Value to use when source yields null for a non-nullable column |
| `enum_values` | Valid values for picklists / ENUMs — validated before send |
| `timezone` | IANA tz name of source (`"America/New_York"`, `"UTC"`); `None` = already UTC-aware |
| `array` | Disambiguates `json` datatype — `True` = list, `False` = dict |

All datetimes in a `DataStream` must be **UTC-aware**. The `timezone` field tells the engine what the source clock is so it can normalize before passing downstream. Targets never guess.

The dict keys in every `DataStream` record map directly to `Column.source_name`. The value for each key is guaranteed to be the Python type declared in `Column.datatype`. The Column definitions are the schema for the stream — there is no separate record model.

---

### The Response Contract: `ConnectorResponse`

All connector operations return `ConnectorResponse[T]`. No connector method raises for expected failures — the response always carries the reason.

```python
from server.models.ConnectorResponse import ConnectorResponse
```

| Field | Type | Description |
|---|---|---|
| `ok` | `bool` | `True` if the operation succeeded |
| `data` | `T \| None` | The result — `Schema`, `Table`, `Column`, `DataStream`, or `None` |
| `code` | `int` | HTTP-style status code |
| `message` | `str` | Always present — empty string on success, reason on failure |

**Status codes used:**

| Code | Meaning |
|---|---|
| `200` | OK |
| `403` | Forbidden — connector reached the system but lacks access |
| `404` | Not Found — resource doesn't exist in this system |
| `500` | Error — something went wrong internally |
| `501` | Not Implemented — this connector doesn't support this operation |

**Convenience constructors — use these in every connector implementation:**

```python
ConnectorResponse.success(data=table)
ConnectorResponse.not_found("Table 'Orders' not found in this schema")
ConnectorResponse.forbidden("Insufficient privileges on schema 'HR'")
ConnectorResponse.not_implemented("Salesforce does not support table creation via API")
ConnectorResponse.error("Query timed out after 30s")
```

**Record operations** return `ConnectorResponse[DataStream]`. The response is a preflight result — `ok` and `code` are set before the stream is consumed. Per-record failures flow back inside the stream as error-flagged dicts alongside successful records:

```python
result = connector.get_records('Account')

if not result.ok:
    logger.warning(f"[{result.code}] {result.message}")
else:
    for record in result.data:
        if record.get('__error'):
            logger.warning(f"Record failed: {record['__error']}")
        else:
            # process record
```

This means you build your per-record error handling once in the service layer, not per connector.

---

### The Connector Protocol: `ConnectorProtocol`

Every connector **must** implement `ConnectorProtocol.Connector`. This is a structural (duck-typed) interface — connectors do not inherit from it, they just implement it.

```python
from server.models.ConnectorProtocol import Connector

isinstance(my_connector, Connector)  # True if it walks the walk
```

**Required attribute:**

| Attribute | Type | Description |
|---|---|---|
| `schema` | `Schema` | Every connector owns its schema contract. Set at init, populated as data is fetched. |

**Required methods — five verbs × three nouns + one stream:**

```
create / get / update / upsert / delete
    × schema / table / column
    + create / get / update / upsert / delete records
```

Plus `test_connection() -> bool`.

**Rules:**
- Every method must be implemented. Use `ConnectorResponse.not_implemented(...)` for operations the system genuinely doesn't support — do not raise.
- Methods accept and return `ConnectorStandard` objects — never raw dicts or plain strings where a model is expected.
- `get_*` methods accept either a name (`str`) or a partial model. If a model is passed, use its `source_name` to drive the lookup and populate the object in-place before returning it.
- `upsert_*` methods stamp `target_name` fields on the passed object and return it. This is how the contract flows through the pipeline.
- All type conversion from source types to Python types happens inside the connector. Nothing upstream should ever see a raw Oracle `NUMBER` or Salesforce `currency` — only `float`.

---

### Connectors: `connectors/`

Each connector lives in its own subdirectory and exposes a single **facade class** whose name ends in `Connector` (e.g. `SalesforceConnector`, `PostgresConnector`, `OracleConnector`, `CsvConnector`). Everything the program needs from that system goes through the facade. Internal clients, handlers, and utilities are implementation details of that connector's subdirectory.

Connectors are registered in `connectors/registry.py`:

```python
CONNECTOR_REGISTRY = {
    'salesforce': ('server.connectors.sf.SalesforceConnector', 'SalesforceConnector'),
    'postgres':   ('server.connectors.postgres.PostgresConnector', 'PostgresConnector'),
    ...
}
```

Use `get_connector(name, **kwargs)` for lazy instantiation anywhere in the program. Do not import connector classes directly outside of their own module.

---

### Services: `services/`

Services are the program's public-facing actions. They orchestrate connectors and expose functionality to the API layer. Think of services as the program's facade — just as connectors are the facade for external systems, services are the facade for everything the connectors can do.

**`MigrationService`** is the reference implementation. A full source-to-target migration is three steps:

```python
svc = MigrationService(source_name='salesforce', target_name='postgres')

schema = svc.discover()   # source connector populates source_* on Schema/Table/Column
schema = svc.prepare()    # target connector stamps target_* and applies DDL
schema = svc.run()        # streams records table by table
```

The `Schema` object is the shared state that flows through every stage. It can be serialized, persisted, reloaded, and handed to a different connector pair. The orchestration is built once; connectors are plugins.

---

## Implementing a New Connector

1. **Create a directory** under `connectors/` (e.g. `connectors/mydb/`).

2. **Create the facade** — a class named `MyDbConnector`. It must implement `ConnectorProtocol.Connector` and have a `schema: Schema` attribute initialized in `__init__`. The constructor must accept an optional `schema` parameter.

3. **Implement every protocol method.** Return `ConnectorResponse.not_implemented(...)` for operations the system genuinely doesn't support. Never skip a method or raise unconditionally.

4. **Map all types** — every field coming out of your system must have a `datatype` set to one of the `PythonTypes` literals. Create a type map (`MY_TYPE_MAP: dict[str, PythonTypes]`) in your connector's utils.

5. **Honor the Column contract** — populate `read_only`, `enum_values`, `timezone`, and `default_value` wherever your source system exposes that information. These fields exist so downstream connectors don't have to guess.

6. **Register** in `connectors/registry.py`.

7. **Implement `get_schema()` carefully.** It is the most important method and must handle three cases:
   - **Passed a `Schema` object** — populate it as the target of metadata discovery. Use the tables already on the schema to drive which objects you describe.
   - **Passed a string** — the caller is asking you to populate a full schema by name. In Salesforce this is an org; in Postgres this is a database or schema. Return a fully populated `Schema` with all tables and columns you can discover.
   - **Passed `None`** — return your default schema. In Postgres this is `public`; in Oracle this is the connecting user's schema. This is how the system identifies and logs sources and targets.

### Minimal skeleton

```python
from server.models.ConnectorProtocol import Connector
from server.models.ConnectorStandard import Schema, Table, Column, DataStream
from server.models.ConnectorResponse import ConnectorResponse
from typing import Any


class MyDbConnector:
    schema: Schema

    def __init__(self, schema: Schema | None = None, **kwargs: Any):
        self.schema = schema if schema is not None else Schema(source_name='mydb')

    def test_connection(self) -> bool: ...

    # Schema
    def create_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]: ...
    def get_schema(self, schema: Schema | str | None = None, **kwargs: Any) -> ConnectorResponse[Schema]: ...
    def update_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]: ...
    def upsert_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]: ...
    def delete_schema(self, schema: Schema | str, **kwargs: Any) -> ConnectorResponse[Schema]: ...

    # Table
    def create_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]: ...
    def get_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]: ...
    def update_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]: ...
    def upsert_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]: ...
    def delete_table(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[Table]: ...

    # Column
    def create_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]: ...
    def get_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]: ...
    def update_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]: ...
    def upsert_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]: ...
    def delete_column(self, table: Table | str, column: Column | str, **kwargs: Any) -> ConnectorResponse[Column]: ...

    # Records — ok/code set before the stream is consumed; per-record failures
    # flow back inside the stream as dicts with an '__error' key
    def create_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]: ...
    def get_records(self, table: Table | str, **kwargs: Any) -> ConnectorResponse[DataStream]: ...
    def update_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]: ...
    def upsert_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]: ...
    def delete_records(self, table: Table | str, records: DataStream, **kwargs: Any) -> ConnectorResponse[DataStream]: ...
```

Verify protocol compliance at any time:

```python
assert isinstance(MyDbConnector(), Connector)
```
