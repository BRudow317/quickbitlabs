# Oracle Plugin Architecture Notes

This file defines the intended boundaries for the Oracle plugin so future edits (human or LLM) follow the same structure.

## Core Rule

The Catalog object is the operation envelope.

- It carries operation scope and metadata context.
- Empty `catalog.entities` means broad scope (for example: discover all entities in schema).
- Populated `catalog.entities` narrows scope to specific entities.
- Populated `entity.columns` narrows scope further to specific columns.

## Where Logic Goes

### Facade Layer: Oracle.py

File: `server/plugins/oracle/Oracle.py`

Responsibilities:
- Implement Plugin protocol surface.
- Delegate work to OracleService.
- Return PluginResponse envelopes at the plugin boundary.

Non-responsibilities:
- No raw cursor execution.
- No SQL/DDL string construction.
- No data-dictionary orchestration.

### Service Layer: OracleServices.py

File: `server/plugins/oracle/OracleServices.py`

Responsibilities:
- Orchestrate behavior using the Catalog contract.
- Decide operation strategy for create/get/update/upsert/delete.
- Coordinate dialect SQL builders, engine, and arrow frame.
- Apply plugin-level mapping rules (for example input-size construction).
- Service methods may also return PluginResponse where that keeps error handling localized.

This is the primary place for Catalog interpretation logic, including get_catalog behavior.

### SQL Builder Layer: OracleDialect.py

File: `server/plugins/oracle/OracleDialect.py`

Responsibilities:
- Build SQL statements and filter clauses.
- Translate catalog/operator structures into SQL + binds.

Non-responsibilities:
- No facade/service orchestration.
- No direct DB cursor usage.

### DB Execution Layer: OracleEngine.py and OracleClient.py

Files:
- `server/plugins/oracle/OracleEngine.py`
- `server/plugins/oracle/OracleClient.py`

Responsibilities:
- Execute SQL/DDL through cursors/connections.
- Hold low-level Oracle mechanics.
- Handle DB metadata queries (for example ALL_TAB_COLUMNS) when needed by service-level orchestration.

## CRUD by Object Type

The plugin protocol supports these nouns:
- Catalog
- Entity
- Column
- Data (ArrowStream)

Each noun supports these verbs:
- create
- get
- update
- upsert
- delete

The Oracle facade may return not_implemented for nouns/verbs not yet implemented, but orchestration decisions should still be made in the service layer.

## Testing Guidance

Oracle plugin tests should remain under:
- `server/plugins/oracle/tests`

Preferred test split:
- Service orchestration behavior: `test_oracle_services.py`
- SQL/bind generation behavior: `test_oracle_dialect_operators.py`
- Live integration behavior: `test_live.py`

When adding new behavior, prefer adding tests in existing oracle test modules before introducing new structure.
