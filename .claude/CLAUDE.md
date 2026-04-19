# QuickBitLabs Federation Framework

## Author: Blaine Rudow

---

## Current State (as of 2026-04-19)

Authentication: Oracle credential check (oracledb.connect) replaces old SQLite user table.
DataMart + Session layer implemented. Frontend has no plugin concept — it works with Catalog/Entity objects only.

### Completed work this session:
- `server/api/auth.py` — Oracle login replaces SQLite lookup; JWT-only get_current_user
- `server/api/users.py` — JWT-only, no DB
- `server/api/session.py` — `GET /api/session/` returns full Catalog from parquet cache; `GET /api/session/systems` lists cached system names
- `server/services/sync_systems.py` — full schema discovery via `get_catalog(Catalog())` on each plugin, writes one parquet per plugin to `server/metadata/`
- `server/services/new_session.py` — loads parquet cache, returns unified Catalog with all entities (column Locators embedded by plugins)
- `server/api/migration.py` — wraps FullMigration.run_all()
- `frontend/src/api/sessionApi.ts` — `getSession()`, `getData()` (Arrow IPC → QueryResult), Catalog/Entity/Column/Locator TypeScript mirrors
- `frontend/src/pages/DataMartPage.tsx` — entity browser (left) + query builder + results (right); builds Catalog from selected entities → POSTs to `/api/data/` → decodes Arrow IPC
- `frontend/src/layouts/AppLayout.tsx` — sticky nav: DataMart + Migration links
- `frontend/package.json` — added apache-arrow

### Next step:
1. `python server/services/sync_systems.py` — populate the metadata cache
2. `npm install` (from frontend/) — install apache-arrow
3. Start server + `npm run generate` — regenerate typed SDK
4. `npm run build`

---

## Core Architectural Vision (Restarted)

QuickBitLabs is a data integration platform designed to solve the **N x M integration nightmare** by reducing it to **N + M** through a **Universal Pydantic Contract**. This allows for hot-swapping different data sources (Oracle, Salesforce, CSV, etc.) without changing core application code.

The core vision is intended to be a system agnostic data & metadata engine built to bridge relationally complex systems like Salesforce and Oracle. This includes self discovery of metadata, data federation, agnostic querying, and orm style metadata management. 

- ### ArrowReader:
  - typed from pyarrow.RecordBatchReader
  - A lazy-loaded, memory-efficient stream of data batches. It is the "Package" that carries the actual data payload. The ArrowReader is designed to be consumed in a streaming fashion, allowing for efficient processing of large datasets without loading everything into memory at once.

### ipc
    - The Arrow IPC (Inter-Process Communication) format is used for serializing the ArrowReader stream when it needs to be passed over the network. This ensures that the data can be efficiently transmitted while preserving its structure and types.

- ### Catalog: 
  - A Pydantic metadata model. It defines the "Intent" (Request) and the "Reality" (Result). It is decoupled from the live data stream to ensure serializability. The Catalog is the "Envelope" that carries metadata and context, while the `ArrowReader` (`pyarrow.RecordBatchReader`) is the "Package" that carries the actual data. This separation allows for maximum flexibility and system-agnosticism.

- ### System Agnosticism:
  - The "High level Service Layer" (not to be confused with "plugin service layer") and the Interchange Objects (ArrowReader) must be blind to the source system. Only the Plugins handle the "Plugin Native <-> Universal" bridge.

## Prototypes

### ArrowFrame:
  - A system-agnostic data interchange container. It represents the "Clean Slate" where data is transitioned from system-specific native formats to a universal PyArrow stream. It does NOT own or carry system-specific metadata.
  - A *God Object* during prototyping, but the long-term vision is to have ArrowFrame adapted to the DataFrame Interchange Protocol, allowing it to be a true universal container that can be easily converted to/from Pandas, Spark, Dask, etc. without losing the Arrow-based performance benefits.
### DuckDB:
  - As a successor to the Catalog query engine, allowing for more complex transformations and in-memory operations on the ArrowReader without needing to regress to Python objects.

## Legacy Types:
### Records:
  - An older format represented as `Iterable[dict]`. It is less efficient and more memory-intensive than the ArrowReader. The Records format is being phased out in favor of the ArrowReader for all data operations. However I'm keeping it and it's type mappings to native python around for now.

---

## API Layer Rules (CRITICAL — read before touching any api/ or frontend/ file)

### What belongs where
- `server/api/catalog.py` — metadata discovery (POST Catalog → Catalog via federation)
- `server/api/data.py` — data retrieval (POST Catalog → Arrow IPC via federation)
- `server/api/session.py` — session bootstrap: loads parquet cache, returns full Catalog to frontend
- `server/api/migration.py` — migration orchestration (POST MigrationRequest → MigrationResult)
- `server/api/auth.py` + `server/api/users.py` — authentication only
- `server/services/sync_systems.py` — offline schema sync, writes `server/metadata/<plugin>.parquet`
- `server/services/new_session.py` — reads parquet cache, returns unified Catalog

### What MUST NOT be created
- Do NOT create plugin-specific API routes (`/api/plugins/...`). The Catalog's embedded Locators handle routing.
- Do NOT create custom request/response models for data operations. **Catalog is the contract.**
- Do NOT convert Arrow IPC to JSON rows for the network. Data travels as `application/vnd.apache.arrow.stream`.
- Do NOT bypass federation by calling plugin instances directly from API routes (except migration).
- Do NOT expose "plugin" as a concept on the frontend. The frontend knows Catalog, Entity, Column, Locator — not plugin names.

### The session → DataMart → federation flow
1. `sync_systems.py` calls `plugin.get_catalog(Catalog())` (blank = full discovery) for each plugin and writes entities (with column Locators already embedded) to parquet.
2. `GET /api/session/` loads parquet → returns unified Catalog. Frontend stores this.
3. User selects entities in the DataMart. Their Locators are already present (set by the plugin during step 1).
4. Frontend builds a sub-Catalog from selected entities + limit and POSTs to `POST /api/data/`.
5. Federation reads `entity.locator.plugin` (derived from `columns[0].locator`) and routes to the correct plugin.
6. Plugin returns ArrowReader → serialized to Arrow IPC → decoded in browser with `apache-arrow`.

### How entity.locator works
`Entity.locator` is a derived property: it returns `columns[0].locator`. For federation to route correctly, at least one column on each entity must have `locator.plugin` set. Oracle sets `locator=Locator(plugin='oracle', entity_name=...)` on every column it returns from `get_catalog`.

---

## Key Framework Rules:
- You cannot import functionality from a `plugin` into the "Higher Level Service Layer". The service layer is meant to be completely agnostic and unaware of the underlying systems. It should only interact with the universal contract (Catalog, ArrowReader) and the Plugin Protocol interface. This ensures that the core logic of the application remains decoupled from any specific data source, allowing for maximum flexibility and maintainability.
- The plugins are responsible for translating between their native data formats and the universal contract, typically in the <plugin>TypeMap.py file.
- You cannot modify the ArrowFrame prototype, or the four core plugin files without explicit permission to do so.
- You cannot build a high level service to parse for a kwarg or properties, or extra dictionary in the High level service layer for a specific plugin. Those are for plugins to return information to themselves if the object is passed back into the plugin.
  - For example, the Salesforce plugin may have specific information that's only relevant in the event the target system is another Salesforce org, the plugin would add that value to the properties dict or extra dict, and in turn be responsible for parsing and using that information in the event of a Sf->Sf migration. The high level service layer should not be concerned with or even aware of this information.
  - Another example could be a Polars plugin that adds a specific kwarg to the properties dict like `lazy_frame_preferred: bool=True` to indicate that the plugin prefers functionality to return the lazy frame version of the data when possible as it passes around the catalog internally. The high level service layer should not be concerned with this, it should be the responsibility of the plugin to parse this information and return the correct version of the data based on its own rules and logic.

---

## Key Project Files

### Core Framework (do not modify without permission)
- **server/plugins/PluginModels.py** — Catalog, Entity, Column, Locator, ArrowReader. Catalog has `serialize_arrow_stream` / `deserialize_arrow_stream` / `arrow_schema` / `arrow_reader` built in.
- **server/plugins/PluginProtocol.py** — Universal Plugin interface (5 verbs × 3 nouns + data ops).
- **server/plugins/PluginRegistry.py** — PLUGIN Literal, PLUGIN_REGISTRY, `get_plugin()`, `list_plugins()`.
- **server/plugins/PluginResponse.py** — Generic `PluginResponse[T]` envelope (ok/data/code/message).
- **server/core/ArrowFrame.py** — Universal data interchange (0 bytes, prototype location).
- **server/core/federation.py** — `resolve_catalog_plugins()`, `fanout_plan()`. Routes by `entity.locator.plugin`.

### API Layer
- **server/api/catalog.py** — `POST /api/catalog/` → fanout get_catalog via federation → JSON Catalog
- **server/api/data.py** — `POST /api/data/` → fanout get_data via federation → Arrow IPC bytes
- **server/api/session.py** — `GET /api/session/` → unified Catalog from parquet cache (auth required)
- **server/api/migration.py** — `POST /api/migration/run` → MigrationResult

### Services
- **server/services/sync_systems.py** — Full schema sync: `get_catalog(Catalog())` per plugin → `server/metadata/<plugin>.parquet`
- **server/services/new_session.py** — Reads parquet cache → returns unified `Catalog` with all entities
- **server/services/FullMigration.py** — Full schema+data migration (discover → prepare DDL → migrate data)

### Frontend
- **frontend/src/api/sessionApi.ts** — `getSession()`, `getData()` (Arrow IPC), Catalog/Entity/Column/Locator TS types
- **frontend/src/pages/DataMartPage.tsx** — Entity browser + Catalog query builder + results table
- **frontend/src/pages/MigrationPage.tsx** — Migration manager
- **frontend/src/layouts/AppLayout.tsx** — Authenticated nav layout

### Metadata cache (generated, not in git)
- **server/metadata/<plugin>.parquet** — One file per plugin. Schema: `entity_name (str)`, `entity_json (large_string)`. Column Locators are embedded inside entity_json.

## Successful Implementations:
- **server/services/FullMigration.py** A working full schema and data migration from Salesforce to Oracle.

## Existing Plugin Facades
- **server/plugins/oracle/Oracle.py** — Oracle plugin facade. Sets `locator=Locator(plugin='oracle', entity_name=table_name)` on every column.
- **server/plugins/sf/Salesforce.py** — Salesforce plugin facade.

## Additional Paths:
- server/ — core application code
- frontend/ — React frontend code
- server/ProjectTree.md — a visual map of the server codebase

---

## External Resources
### Core Resources:
- [Python 3.13](https://docs.python.org/3.13/py-modindex.html)
- [DataFrame Interchange Protocol](https://data-apis.org/dataframe-protocol/latest/API.html)
- [PyArrow Cookbook](https://arrow.apache.org/cookbook/py/)
- [PyArrow Interchange](https://arrow.apache.org/docs/python/interchange_protocol.html)
- [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Polars Python API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
- [Pandas API Reference:](https://pandas.pydata.org/docs/reference/index.html)

### Plugin Resources:
- [oracledb API Reference:](https://python-oracledb.readthedocs.io/en/latest/user_guide/dataframes.html)
- [Salesforce REST API Reference:](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)

### Maybe Later:
- [DuckDB Python API Reference:](https://duckdb.org/docs/current/clients/python/overview)
- [SQLGlot SQL Parser](https://github.com/tobymao/sqlglot)
- [Arrow Flight SQL:](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Arrow Flight Spec:](https://arrow.apache.org/docs/format/Flight.html)
- [Arrow IPC:](https://arrow.apache.org/docs/python/ipc.html)

### Much Much Later:
- [Alembic](https://alembic.sqlalchemy.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)

---

## Bugs & Edge Cases Found
- [ ] **Blob/Binary Support:** Exclude `base64`/`blob` fields from Bulk V2 CSV migrations (`Attachment`, `Document`).
- [ ] **Time Formatting:** Fix `time64[us]` conversion error for ISO-8601 strings with 'Z' suffix (`BusinessHours`).
- [ ] **Query Restrictions:** Add "skip-list" for objects requiring specific filters (`ContentFolderItem`, `IdeaComment`).
- [ ] **Oracle Constraints:** Handle `ORA-01400` (NULLs in NOT NULL columns) and `ORA-12899` (Value too large) for metadata-driven schema creation (`DuplicateRule`, `Group`, `ListView`).
- [ ] **Access/Permissions:** Gracefully handle `INSUFFICIENT_ACCESS` during bulk queries (`ConnectedApplication`).

### SF Compound Types Excluded from Migration ( Not a Bug )
Salesforce compound fields (`address`, `location`) have no Arrow type mapping and are excluded from `get_catalog` column lists. Sub-fields (e.g., `MailingStreet`, `MailingCity`) are migrated individually instead.

### Salesforce DDL (Create/Update/Delete) Not Implemented
The Salesforce plugin returns `not_implemented` for metadata mutation verbs. The standard REST/Bulk APIs do not support DDL; enabling this would require a separate implementation using the Salesforce Metadata API.

My long term vision is to turn the Salesforce plugin into a full "ORM-style" metadata manager that maintains its own internal state of the target org's schema. In the interim I intend to implement external schema management like utilizing parquet files to store the additionally created objects and fields, and applying the same copy-swap strategy as Oracle to handle DDL changes on Salesforce as well.

This means I'll need to maintain a relational graph of the Salesforce schema in order to know the correct order of operations for creating objects and fields with dependencies (e.g., creating a custom object before creating a field that references it).

## Last Run Error Summary (2026-04-11)

From `.logs/last run.log` (run summary reported 185 succeeded, 9 failed):

- Attachment -> ATTACHMENT: Bulk V2 Query with CSV does not support blob fields.
  - Fix: Improve SalesforceService to only select Bulk2 Queries when necessary, rest should be the default.
- BusinessHours -> BUSINESS_HOURS: CSV conversion error to `time64[us]` for value `00:00:00.000Z`.
  - Fix: Salesforce Type Mapping should properly convert Salesforce 'time' type to an appropriate arrow type.
- ConnectedApplication -> CONNECTED_APPLICATION: `INSUFFICIENT_ACCESS` on cross-reference id.
  - Fix: Insufficient access should be gracefully handled and logged but not cause a hard failure.
- ContentFolderItem -> CONTENT_FOLDER_ITEM: Query requires filter by `Id` or `ParentContentFolderId` (`=` or `IN`).
  - Fix: Gracefully handle these, log them as warnings and skip. 
- Document -> DOCUMENT: Bulk V2 Query with CSV does not support blob fields.
  - Fix: Improve SalesforceService to only select Bulk2 Queries when necessary, rest should be the default.
- DuplicateRule -> DUPLICATE_RULE: `ORA-01400` cannot insert NULL into `DUPLICATE_RULE.LANGUAGE`.
  - Further Research Needed.
- Group -> GROUP_COL: `ORA-01400` cannot insert NULL into `GROUP_COL.NAME`.
  - Further Research Needed.
- IdeaComment -> IDEA_COMMENT: Query requires restricted filter syntax (`CommunityId`, `Id`, or `IdeaId`, with single ID or `IN`).
  - Fix: Gracefully handle these, log them as warnings and skip.
- ListView -> LIST_VIEW: `ORA-12899` value too large for column `LIST_VIEW.NAME` (actual 44, max 40).
  - Further Research needed: to see if expanding available length is possible, or Oracle will need to be configured to truncate strings instead of erroring.

## Recent Migration Enhancements (SF -> Oracle)
The following core enhancements were implemented to support full automated schema and data migration:

### Oracle "Copy-Swap" Table Rebuild
To handle complex DDL changes on populated tables (such as adding `NOT NULL` columns which triggers `ORA-01758`), the Oracle plugin now utilizes an Alembic-style "Copy-Swap" strategy. It creates a temporary table with the new schema, transfers existing data using type-safe defaults, and then performs a `DROP/RENAME` to align the live table without data loss.

### Defensive Dialect Casting
Oracle's internal SQL engine is strict with type inference during `INSERT INTO ... SELECT` operations. The `OracleDialect` was enhanced to wrap **every** column in an explicit `CAST(col AS target_type)` expression. 
*   **Booleans:** Uses `CASE` expressions to safely convert Salesforce string-based booleans (`'true'`, `'false'`) to Oracle `NUMBER(1,0)`.
*   **Dates/LOBs:** Intelligent exclusion of `CAST` for `CLOB`, `BLOB`, `DATE`, and `TIMESTAMP` columns to avoid NLS formatting errors (`ORA-01843`) and LOB-inconsistency errors (`ORA-00932`).

### Global Precision Widening
Salesforce metadata often under-reports the precision of currency and calculated fields. To prevent PyArrow and Oracle from crashing on high-precision data (e.g., 19+ digits arriving for an 18-precision field), the integration engine now globally "safety-widens" all `decimal128` types to precision 38 while preserving the original scale.
