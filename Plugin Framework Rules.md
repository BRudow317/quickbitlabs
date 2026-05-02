# QuickBitLabs Federation Framework

## Author: Blaine Rudow

---

## Core Architectural Vision

QuickBitLabs is a data integration platform designed to solve the **N x M integration nightmare** by reducing it to **N + M** through a **Universal Pydantic Contract**. This allows for hot-swapping different data sources (Oracle, Salesforce, CSV, etc.) without changing core application code.

The core vision is intended to be a system agnostic data & metadata engine built to bridge relationally complex systems like Salesforce and Oracle. This includes self discovery of metadata, data federation, agnostic querying, and orm style metadata management. 

## Data Formats:
These data formats constitute the core data interchange formats of the system, and the contract that plugins must adhere to when returning data.

### ArrowReader:
  - typed from pyarrow.RecordBatchReader
  - A lazy-loaded, memory-efficient stream of data batches. It is the "Package" that carries the actual data payload. The ArrowReader is designed to be consumed in a streaming fashion, allowing for efficient processing of large datasets without loading everything into memory at once.
```python
ArrowReader: TypeAlias = pa.RecordBatchReader | RecordBatchStreamReader | RecordBatchFileReader
```

### ipc
  - The Arrow IPC (Inter-Process Communication) format is used for serializing the ArrowReader stream when it needs to be passed over the network. 
  - This is the format that should be returned by the API Layer to the frontend, and the frontend should decode it back into an ArrowReader for use in the browser. This allows us to maintain the performance benefits of Arrow's columnar format even when transmitting data between the backend and frontend.

---

## Federated Catalogs & Cross system / plugin operations:
### DuckDB:
  - DuckDB is the query system for cross plugin aka cross system data actions, allowing for more complex transformations and in-memory operations on the ArrowReader without needing to regress to Python objects.

### Catalog Fanout & Predicate Pushdown:
  - **Fanout (implemented)**: `Catalog.federate` splits a master Catalog into per-plugin child Catalogs using a connected-component algorithm on internal joins. Each child receives only the entities and `operator_groups` that belong to its plugin. This is the predicate pushdown — filters that can be resolved at the source are embedded in the child Catalog before the plugin is called, so each system only retrieves rows that match its own conditions.
  - **Reconstitution (not yet fully implemented)**: After each plugin returns its `ArrowReader`, the results must be joined back together in-memory by DuckDB to resolve cross-system joins and apply the master `limit`. `duckdb_orchestrator()` in `server/core/federation.py` is the planned home for this. Operator groups that reference columns across multiple plugins are held on the master Catalog and must be applied here rather than pushed down.
```python
class Catalog(BaseModel):
    ...all the class stuff...
    
    @property
    def federate(self) -> list[Catalog]:
        """
        Divides a federated master Catalog into plugin-specific child Catalogs.
        Each child carries only entities connected by internal joins within one system.
        Cross-system joins and operator groups remain on the master for DuckDB to resolve.
        """
        if not self.entities:
            return []

        # 1. Build internal adjacency list (only joins within the same plugin)
        internal_adj: dict[str, set[str]] = {e.name: set() for e in self.entities}
        for j in self.joins:
            if (j.left_entity.locator and j.right_entity.locator and 
                j.left_entity.locator.plugin == j.right_entity.locator.plugin):
                internal_adj[j.left_entity.name].add(j.right_entity.name)
                internal_adj[j.right_entity.name].add(j.left_entity.name)

        # 2. Find connected clusters within each plugin
        visited = set()
        children: list[Catalog] = []
        
        for root_entity in self.entities:
            if root_entity.name in visited:
                continue
            
            cluster_entities: list[Entity] = []
            stack = [root_entity.name]
            visited.add(root_entity.name)
            
            plugin_name = root_entity.locator.plugin if root_entity.locator else None
            
            while stack:
                curr_name = stack.pop()
                curr_entity = next(e for e in self.entities if e.name == curr_name)
                cluster_entities.append(curr_entity)
                for neighbor_name in internal_adj[curr_name]:
                    if neighbor_name not in visited:
                        visited.add(neighbor_name)
                        stack.append(neighbor_name)
            
            # 3. Build child catalog for this cluster
            child = self.model_copy(update={
                "source_type": plugin_name,
                "entities": cluster_entities,
                "joins": [
                    j for j in self.joins
                    if j.left_entity.name in [e.name for e in cluster_entities]
                    and j.right_entity.name in [e.name for e in cluster_entities]
                ],
                "sort_columns": [
                    s for s in self.sort_columns
                    if s.column.locator and s.column.locator.entity_name in [e.name for e in cluster_entities]
                ],
                "operator_groups": [
                    g for g in self.operator_groups
                    if _collect_plugins_from_group(g) == {plugin_name}
                    # TODO: refine to only include groups referencing columns in this cluster
                ],
                "limit": None,      # applied post-federation only
            })
            children.append(child)

        return children

## SystemDatabase & Catalog Registry:
  - The **SystemDatabase** (`server/db/ServerDatabase.py`) is the application's own persistence layer. It stores users, sessions, and other server-level state. It exists and is functional; ongoing work is focused on making it more robust.
  - The **Catalog Registry** is a component of the SystemDatabase that persists serialized Catalogs for fast retrieval and session reuse. It exists and is functional. A stored Catalog entry holds the full JSON-serialized Pydantic model and can be rehydrated with `Catalog.model_validate_json()`.
    - **Plugin caching**: The Catalog Registry also serves as the I/O cache for deep-hydrated plugin metadata — equivalent to Oracle's `ALL_TAB_COLUMNS` or Salesforce Tooling API describe results. Plugins write their discovered metadata here so that subsequent session loads do not require a live round-trip to the source system. This is critical for expensive discovery operations (Salesforce full describe can take 30–60 seconds).

## Infrastructure Database Rules (ServerDatabase)

These rules apply to **all** ServerDatabase (Oracle) DDL and DML written for the application's internal metadata, session, and user storage.

### Table structure (Managed Entities)
- Every internal table must have a surrogate primary key named `<table_name_singular>_ID` (e.g., `USER_SESSION_ID`, `USER_SIGN_IN_ID`). Use `NUMBER GENERATED BY DEFAULT AS IDENTITY` with an inline named constraint: `CONSTRAINT PK_<TABLE_NAME> PRIMARY KEY`.
- Every table must include all four audit columns:
  ```sql
  <table_name>_ID   NUMBER GENERATED BY DEFAULT AS IDENTITY CONSTRAINT PK_<TABLE_NAME> PRIMARY KEY,
  ... other columns...,
  CREATED_DATE  TIMESTAMP WITH TIME ZONE  DEFAULT CURRENT_TIMESTAMP  NOT NULL,
  CREATED_BY    VARCHAR2(100 CHAR)         DEFAULT 'SYSTEM'           NOT NULL,
  UPDATED_DATE  TIMESTAMP WITH TIME ZONE  DEFAULT CURRENT_TIMESTAMP  NOT NULL,
  UPDATED_BY    VARCHAR2(100 CHAR)         DEFAULT 'SYSTEM'           NOT NULL
  ```

### VARCHAR2 sizing
- Always size `VARCHAR2` columns with `CHAR` semantics, not `BYTE`. Write `VARCHAR2(100 CHAR)`, never `VARCHAR2(100)` or `VARCHAR2(100 BYTE)`.

### Foreign keys
- Define FK relationships for documentation and query-optimizer hints, but **never enforce them hard**. Always use the `RELY DISABLE NOVALIDATE` form:
  ```sql
  ALTER TABLE child_table
  ADD CONSTRAINT fk_<child>_<parent>
  FOREIGN KEY (parent_id)
  REFERENCES parent_table(parent_id)
  RELY DISABLE NOVALIDATE;
  ```

### Connections - Which Client to Use
These are **completely separate data sources** and must never be mixed. The ServerDatabase is the application's own persistence layer. OracleClient connects to a federated external Oracle system that the plugin manages — in production these are different databases, potentially on different hosts. Treat them as categorically different from a framework perspective even if they happen to point to the same Oracle instance during prototyping.

- **ServerDatabase** (`server/db/ServerDatabase.py`): The application's own persistence layer. Owns users, sessions, Catalog Registry, and all internal application state. Import as `from server.db.db import server_db`. Use anywhere in `server/` **outside** of plugin internals.
- **OracleClient** (`server/plugins/oracle/OracleClient.py`): Connects to a federated Oracle target. This is a plugin-scoped connector and is completely blind to the application database. Use **only** inside `server/plugins/oracle/`.
- **Rule**: Never use raw `oracledb.connect()`; always go through the designated `ServerDatabase` or `OracleClient` instance depending on the directory scope.

### Setup
- certain build steps, or DDL scripts need to run outside the plugin contract, they should be included in the server settings directory. 

---

## Federated Oracle Managed Entities (TODO)
While the Oracle Plugin is currently used for raw structural migrations, future iterations will enforce the same "Managed Entity" boilerplate (audit columns/system PKs) on migrated tables. For now, focus on the Infrastructure DB rules above.

---

## System Agnosticism:
  - The "High level Service Layer" (not to be confused with "plugin service layer") and the Interchange Objects (ArrowReader) must be blind to the source system. Only the Plugins handle the "Plugin Native <-> Universal" bridge.

## Prototypes

### ArrowFrame:
  - A *God Object* during prototyping, but the long-term vision is to have ArrowFrame adapted to the DataFrame Interchange Protocol, allowing it to be a true universal container that can be easily converted to/from Pandas, Spark, Dask, etc. without losing the Arrow-based performance benefits.

## Legacy Types:
### Records:
  - An older format represented as `Iterable[dict]`. It is less efficient and more memory-intensive than the ArrowReader. The Records format is being phased out in favor of the ArrowReader for all data operations. However I'm keeping it and it's type mappings to native python around for now.

---

## API Layer Rules (CRITICAL - read before touching any api/ or frontend/ file)

### What belongs where
- `server/api/catalog.py` - metadata discovery (POST Catalog → Catalog via federation)
- `server/api/data.py` - data retrieval (POST Catalog → Arrow IPC via federation)
- `server/api/session.py` - session bootstrap: loads plugin full schema cached catalog, returns full Catalog to frontend
- `server/api/migration.py` - migration orchestration (POST MigrationRequest → MigrationResult)
- `server/api/auth.py` + `server/api/users.py` - authentication only
- `server/services/sync_systems.py` - offline schema sync, writes to ServerDatabase Catalog Registry.
- `server/services/session_service.py` - reads Catalog Directory, returns unified Catalog

### What MUST NOT be created in the server/api layer
- Do NOT create plugin-specific API routes (`/api/plugins/...`). The Catalog's embedded Locators handle routing.
- Do NOT create custom request/response models for data operations. **Catalog is the contract.**
- Do NOT convert Arrow IPC to JSON rows for the network. Data travels as `application/vnd.apache.arrow.stream`.
- Do NOT bypass federation by calling plugin instances directly from API routes (except unique scenarios).
- Do NOT expose "plugin" as a concept on the frontend. The frontend knows Catalog, Entity, Column, Locator - not plugin names.

### The session → DataMart → federation flow
1. `sync_systems.py` calls `plugin.get_catalog(Catalog())` (blank = full discovery) for each plugin and writes entities (with column Locators already embedded) to parquet.
2. `GET /api/session/` loads parquet → returns unified Catalog. Frontend stores this.
3. User selects entities in the DataMart. Their Locators are already present (set by the plugin during step 1).
4. Frontend builds a sub-Catalog from selected entities + limit and POSTs to `POST /api/data/`.
5. Federation reads `entity.locator.plugin` (derived from `columns[0].locator`) and routes to the correct plugin.
6. Plugin returns ArrowReader → serialized to Arrow IPC → decoded in browser with `apache-arrow`.

---

## Key Framework Rules:

- **Plugin Import Rule**: You cannot import functionality from a `plugin` into the "Higher Level Service Layer". The service layer is meant to be completely agnostic and unaware of the underlying systems. It should only interact with the universal contract (Catalog, ArrowReader) and the Plugin Protocol interface. This ensures that the core logic of the application remains decoupled from any specific data source, allowing for maximum flexibility and maintainability.

- **Data Type Conversion**: The plugins are responsible for translating between their native data formats and the universal contract, typically in the <plugin>TypeMap.py file.

- **Core File Modification Rule**: You cannot modify the four core plugin files (`PluginModels.py`, `PluginProtocol.py`, `PluginRegistry.py`, `PluginResponse.py`) or the `ArrowFrame` prototype without explicit permission. `ArrowFrame` lives in `server/core/` and is a separate concern from the plugin contract files, but carries the same protection.

- **High Level Service Rule**: You cannot build a high level service to parse for a kwarg or properties, or extra dictionary in the High level service layer for a specific plugin. Kwargs are for plugins to return information to themselves if the object is passed back into the plugin.

- **PluginResponse Rule**: All operations should return PluginResponse[T] - never raises for expected failures.

- **Not Implemented Rule**: PluginResponse.not_implemented() so the caller stays in control.

- **Kwargs Rule**: kwargs are not for getting around framework rules, they are for a plugin to pass itself relevant context that isn't defined in the core contract. Higher level functionality outside the plugin should not be built on kwargs, nor should plugins rely on kwargs from higher level functionality or other plugins.
    
- **Catalog Rule**: The Catalog is the universal envelope. It carries all metadata, context, and query intent needed for an operation. Plugins consume it to determine what to do, execute against their native system, and return a `PluginResponse` with the result — never raising for expected failures.

- **Plugin Protocol Rule**: Any usage of a plugin should exhaust functionality using the `AST Query System`, and the `Plugin Protocol` verbs to complete external functionality before attempting any custom solution.

- **Plugin Contract Rule**: Plugins should never attempt to bypass the contract defined by the `Catalog` and the `Plugin Protocol`. This ensures that all plugins can be used interchangeably and that higher level services can rely on a consistent interface for interacting with any data source.

- **AST Query System Rule**: "=" is the standard SQL equality operator for `filters`. "==" is deprecated and should not be used. Assignments are handled via `catalog.assignments` and do not use an explicit operator field.

- **AST Upsert Rules**:
  1. Match Resolution (The 'ON' Clause):
     - Priority 1 (Explicit): If catalog.filters is populated, use it to build 
      the MERGE ON clause. This supports flexible identity matching (e.g. email, username).
     - Priority 2 (Metadata): If filters is empty, autonomously derive the ON 
      clause from the primary_key=True metadata in the Entity.columns, or a matching unique not null constraint column.
     - Priority 3 (Fallback): If no identity can be determined, degrade gracefully 
      to a standard INSERT (insert_data) to ensure "upsert" is never a hard failure.

  2. Managed Entity Alignment (Audit Boilerplate):
    - The implementation must recognize and automatically manage framework audit columns even if they are missing from the source catalog:
    - WHEN MATCHED: Update UPDATED_DATE (SYSTIMESTAMP) and UPDATED_BY (current session user).
    - WHEN NOT MATCHED: Initialize both CREATED_* and UPDATED_* boilerplate.

  3. System Identity (INTERNAL_ID):
    - Support for system-generated primary keys. If the target entity defines a managed PK (e.g. via identity column), the MERGE must allow Oracle to generate the value during the INSERT branch.

  4. Data Mutation:
    - The SET clause (mutation) is driven by `catalog.assignments`. If assignments are missing, the plugin may fall back to using all provided columns in the ArrowReader.

---

## Data Isolation & Domain Authority (The "Silo" Rules)

### Source Truth Principle:
- A Plugin is the absolute authority on its own domain, and ONLY its own domain.
- **Rule**: `get` operations (`get_catalog`, `get_entity`, `get_column`) must ONLY return objects owned by that specific plugin.
- **Predicate Pushdown Context**: If a Catalog containing external entities or joins is passed into a plugin, the plugin may analyze that metadata for **Predicate Pushdown** logic, but it MUST NOT return those external objects in its response.
- **Deterministic Existence**: 
    - If 0 of 1 requested entities exist in the target system -> return an empty Catalog.
    - If 1 of 2 requested entities exist -> return a Catalog with ONLY the 1 existing entity.

### Discovery & Hydration Behavior:
- **Replacement Rule**: When a plugin hydrates a Catalog, it REPLACES existing shallow objects with fresh, deep-hydrated versions. Do not attempt a partial merge.
- **Projection Integrity**: If an Entity in the input Catalog contains a subset of columns (an AST projection), the plugin must hydrate ONLY those specific columns. It must not expand the list to the full table unless the input was an empty Catalog (Full Discovery).
- **Metadata Contract**: All fields marked `# Hydration Required` in the code snippets below MUST be fully populated by the plugin before returning the Catalog.

---

---

## Key Project Files

### Core Framework (do not modify without permission)
- **server/plugins/PluginModels.py** - Catalog, Entity, Column, Locator, ArrowReader. Catalog has `serialize_arrow_stream` / `deserialize_arrow_stream` / `arrow_schema` / `arrow_reader` built in.
- **server/plugins/PluginProtocol.py** - Universal Plugin interface (5 verbs × 3 nouns + data ops).
- **server/plugins/PluginRegistry.py** - PLUGIN Literal, PLUGIN_REGISTRY, `get_plugin()`, `list_plugins()`.
- **server/plugins/PluginResponse.py** - Generic `PluginResponse[T]` envelope (ok/data/code/message).
- **server/core/ArrowFrame.py** - Universal data interchange (0 bytes, prototype location).
- **server/core/federation.py** - `resolve_catalog_plugins()`, `fanout_plan()`. Routes by `entity.locator.plugin`.

### API Layer
- **server/api/catalog.py** - `POST /api/catalog/` → fanout get_catalog via federation
- **server/api/data.py** - `POST /api/data/` → fanout get_data via federation → Arrow IPC bytes
- **server/api/session.py** - `GET /api/session/`
- **server/api/migration.py** - `POST /api/migration/run` → MigrationResult

### Services
- **server/services/sync_systems.py** - Full schema sync: `get_catalog(Catalog())` per plugin → `server/metadata/<plugin>.parquet`
- **server/services/CatalogMigration.py** - Full schema+data migration (discover → prepare DDL → migrate data) or just DDL generation (discover → prepare DDL) for review.

### Frontend
- **frontend/src/api/sessionApi.ts** - `getSession()`, `getData()` (Arrow IPC), Catalog/Entity/Column/Locator TS types
- **frontend/src/pages/DataMartPage.tsx** - Entity browser + Catalog query builder + results table
- **frontend/src/pages/MigrationPage.tsx** - Migration manager
- **frontend/src/layouts/AppLayout.tsx** - Authenticated nav layout


## Additional Paths:
- server/ - backend application code
- frontend/ - React frontend code
- server/ProjectTree.md - a visual map of the server codebase

---

# PluginModels.py

## Locator
Locator: Defines the absolute origin of a scalar value, including plugin source, URL, environment, namespace, and entity name.

```python
class Locator(BaseModel):
    """The strict contract defining the absolute origin of a scalar"""
    plugin: PLUGIN | None = None # 'oracle', 'salesforce', 'excel', 'parquet', 'feather', 'frontend', etc..
    url: str | None = None
    is_file: bool = False
    environment: str | None = None # 'dev01', 'sf-devint'
    namespace: str | None = None   # schema or namespace, etc. 'oradwh01', 'sobjects'
    entity_name: str | None = None # 'account', 'financials', tables, etc
    additional_locators: dict[str, Any] | None = None
```

## Column
Column represents a column or field in an entity, including metadata such as name, type, nullability, keys, and locators.
```python
class Column(BaseModel):
    name: str # Required, official system name, should be treated as immutable
    alias: str | None = None # Optional, overrides name in AST
    label: str | None = None # Optional, for UI display only
    locator: Locator | None = None # Hydration Required, but required for framework compliance to ensure traceability back to source
    raw_type: str | None = None # Hydration Required, the systems string representation of the original column type.
    arrow_type_id: arrow_type_literal | None = None # Hydration Required, the standardized type identifier that maps to an Apache Arrow type, used for type coercion and compatibility across systems. @See PluginModels.py for the ARROW_TYPE mapping and logic to handle parameterized types like decimal and timestamp.
    primary_key: bool = False # Required for framework compliance to ensure correct identity and join behavior
    is_unique: bool = False # Hydration Required, but required for framework compliance to ensure correct identity and join behavior
    is_nullable: bool = True # Hydration Required, but required for framework compliance to ensure correct null handling in queries and operations
    is_read_only: bool = False # Hydration Required, indicates if the column is read-only in the source system, which may impact how it can be used in queries and operations
    is_compound_key: bool = False # Hydration Required, indicates if the column is part of a compound key, which may impact how it can be used in joins and identity resolution
    is_foreign_key: bool = False # Hydration Required, indicates if the column is a foreign key, which requires additional metadata to identify the referenced entity and column for join operations
    foreign_key_entity: str | None = None # Hydration Required if is_foreign_key=True, the name of the referenced entity for a foreign key relationship, used to resolve joins and relationships between entities
    foreign_key_column: str | None = None # Hydration Required if is_foreign_key=True, the name of the referenced column for a foreign key relationship, used to resolve joins and relationships between entities
    is_foreign_key_enforced: bool = False # Hydration Required, indicates if the foreign key relationship is enforced by the source system, which may impact how it can be used in queries and operations
    max_length: int | None = None # Hydration Required for string types, 0 is unknown but None is not provided.
    precision: int | None = None # Hydration Required for numeric types, but optional for framework compliance as not all systems provide this metadata
    scale: int | None = None # Hydration Required for numeric types, but optional for framework compliance as not all systems provide this metadata
    serialized_null_value: str | None = None # Hydration Required, the serialized representation of a null value in the source system
    default_value: str | None = None # Hydration Required, the string representation of the default value of the column in the source system. ex) None, NULL, 0, '', 'Default' etc.
    enum_values: list[Any] = Field(default_factory=list) # Hydration Required for enum types, but optional for framework compliance as not all systems provide this metadata, the list is ordered and should be treated as ordered.
    timezone: str | None = None # Hydration Required for timestamp types, but optional for framework compliance as not all systems provide this metadata, should be in tz database format like 'America/Los_Angeles'
    properties: dict[str, Any] = Field(default_factory=dict) # Optional free-form metadata for plugins to use as needed
    ordinal_position: int | None = None # Hydration Required, the 1-based position of the column in the source system, used for ordering columns in queries and results
    is_computed: bool = False # Hydration Required, indicates if the column is a computed/generated column in the source system, which may impact how it can be used in queries and operations
    is_deprecated: bool = False # Hydration Required, indicates if the column is deprecated in the source system, which may impact how it can be used in queries and operations
    is_hidden: bool = False # Hydration Required, indicates if the column is hidden in the source system, which may impact how it can be used in queries and operations
    description: str | None = None # Optional, the description of the column in the source system, used for documentation and metadata purposes
    @property
    def arrow_type(self) -> pa.DataType | None:
        """Dynamically build the C++ object, accounting for parameterized types."""
        if not self.arrow_type_id: return None
        arrow_type = self.arrow_type_id
        if arrow_type == "decimal256":
            p = self.precision if self.precision is not None else 76
            s = self.scale if self.scale is not None else 18
            return pa.decimal256(p, s)
        if arrow_type.startswith("decimal"):
            p = self.precision if self.precision is not None else 38
            s = self.scale if self.scale is not None else 9
            return pa.decimal128(p, s)
        if arrow_type.startswith("timestamp"):
            unit = arrow_type.split("_")[1]
            return pa.timestamp(unit, tz=self.timezone)
        return ARROW_TYPE.get(arrow_type)
    @property
    def qualified_name(self) -> str:
        if self.locator and self.locator.entity_name:
            return ".".join([self.locator.entity_name, self.name])
        return self.name
```

## Entity
Entity represents a grouping of fields or columns limited to a single unit, such as a table, view, or other entity type, with its columns and metadata.
```python
class Entity(BaseModel):
    name: str # Required, official system name, should be treated as immutable
    alias: str | None = None # Optional, overrides name in AST
    namespace: str | None = None # Optional, used for qualified name
    description: str | None = None # Optional, for documentation purposes
    entity_type: Literal["table", "view", "materialized_view", "external", "api_endpoint", "procedure", "file", "unknown"] = "unknown"
    plugin: PLUGIN | None = None # Optional, the plugin that owns this entity (shorthand; authoritative routing comes from column Locators)
    row_count_estimate: int | None = None
    columns: list[Column] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)
    @property
    def primary_key_columns(self) -> list[Column]:
        return [f for f in self.columns if f.primary_key]
    @property
    def column_map(self) -> dict[str, Column]:
        return {f.name: f for f in self.columns}
    @property
    def qualified_name(self) -> str:
        if self.namespace:
            return ".".join([self.namespace, self.name])
        return self.name
    @property
    def locator_list(self) -> list[Locator] | None:
        result = [c.locator for c in self.columns if c.locator]
        return result if result else None
    @property
    def locator(self) -> Locator | None:
        """Convenience: returns the first column locator. Used by federation to route the entity to the correct plugin."""
        locs = self.locator_list
        return locs[0] if locs else None
```
### Entity Rules:
- An Entity must have a name.
- An Entity may have an optional namespace, which combined with the name forms a qualified name.
- Columns within an Entity must have unique names.
- Primary key columns must have `primary_key=True` and `is_unique=True`
- `entity.locator` is the authoritative routing key for federation — it must be non-None for any entity participating in a federated operation.

---

## Catalog
Catalog represents a collection of entities, joins, operator groups, and other metadata that defines a data source or schema. Catalog also represents an internal system query, or a cross system query. 

In Summary Catalog represents a collection of metadata and context. That collection can include 0, 1, or many entities (tables), and the relationships between them.

However the catalog is limited because it does not include the data, which can be needed to perform data operations, the data operations often require the catalog to be used in conjunction with the data. 

When included in conjunction with my ArrowReader aka a pyarrow.RecordBatchReader or an IPC stream, the data is supposed to have another catalog included in the data's schema metadata. The data's catalog is supposed to represent "what I have", while the Catalog object passed without the data is supposed to represent "what I want". The AST query in the unaccompanied Catalog might reference the data streams catalog.

```python
class Catalog(BaseModel):
    catalog_id: str | None = None # Optional unique identifier for the catalog, can be used for caching or reference purposes
    name: str | None = None # Required, official system name, should be treated as immutable
    alias: str | None = None # Optional, overrides name in AST
    label: str | None = None # Optional, for UI display only
    namespace: str | None = None # Hydration Required
    version: int = 1 # Required 
    description: str | None = None # Optional
    scope: Literal["SYSTEM", "TEAM", "USER"] = "USER" # Required
    source_type: PLUGIN | Literal["federation"] | None = None # Hydration Required
    entities: list[Entity] = Field(default_factory=list) # Hydration Required, but may be empty for framework compliance to trigger discovery
    filters: list[OperatorGroup] = Field(default_factory=list) # Optional
    assignments: list[Assignment] = Field(default_factory=list) # Optional
    joins: list[Join] = Field(default_factory=list)  # Optional
    joins: list[Join] = Field(default_factory=list)  # Optional
    sort_columns: list[Sort] = Field(default_factory=list)  # Optional
    limit: int | None = None # Optional
    offset: int | None = None # Optional, but required for pagination if limit is provided
    owner_username: str | None = None # Optional, but required for TEAM and USER scope
    team_id: str | None = None # Optional, but required for TEAM scope
    properties: dict[str, Any] = Field(default_factory=dict) # Optional free-form metadata for plugins to use as needed
    ...
```
Catalog is the top-level wrapper. It may be called schema, namespace, database, etc. in different systems, but the concept is the same: a container for entities/objects/tables sharing the same namespace.

The Catalog is the core contract of the plugin system. It may contain no entities, or one, or many. It may contain operator groups for filtering, sort fields for ordering, and a limit for constraining the result set. The Catalog is the universal way to pass along all the contextual information about what data you want to interact with, and how you want to interact with it.

It may be populated with all entities and columns, or just the relevant ones needed for a specific operation. 

### Catalog Usage Patterns:

- **empty catalog** = Provide an empty catalog without entities or columns to trigger a full discovery of all available metadata. This is also called **deep hydration**.
  - The catalog should return with a complete snapshot of entities and columns available in the target system.
- **targeted hydration** = Provide a catalog with one entity and one column to retrieve the metadata for that specific column. This is also called **projection**.
  - The catalog should return with only the requested entity and column, fully hydrated with metadata, while ignoring other entities and columns that may exist in the target system.
- **partial hydration** = Provide a catalog with multiple entities and columns to perform operations on those specific objects.
  - The catalog should return with only the requested entities and columns, fully hydrated with metadata, while ignoring other entities and columns that may exist in the target system.
- **envelope hydration** = Provide a catalog with relevant entities and columns as the envelope to carry the metadata needed to perform the requested operation.
- **Pydantic Serialization**
  - As a pydantic BaseModel you can serialize the entire working contract to JSON with 
    catalog.model_dump()
    - or dict with catalog.model_dump_json(),
    - and rehydrate with Catalog.model_validate()
    - or Catalog.model_validate_json() respectively


---

## AST Query System
1. **Sort**: Defines sorting criteria for query results.
2. **Join**: Represents a join between two entities based on specified columns and join type.
3. **Operation**: Represents a single operation in a query condition, including the independent column, operator, and dependent value.
4. **OperatorGroup**: Represents a group of operations combined with a logical condition (AND, OR).

```python
class Sort(BaseModel):
    column: Column
    direction: Literal["ASC", "DESC"] = "ASC"
    nulls_first: bool | None = None

class Join(BaseModel):
    left_entity: Entity
    left_column: Column
    right_entity: Entity
    right_column: Column
    join_type: Literal["INNER", "LEFT", "OUTER"] = "INNER" # removed "RIGHT", 

class Operation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    independent: Column
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None

class OperatorGroup(BaseModel):
    condition: Literal["AND", "OR", "NOT"]
    operation_group: list[Operation | OperatorGroup] = Field(default_factory=list)

class Assignment(BaseModel):
    """A scalar mutation: column = value. The operator is implicit — being in catalog.assignments means assignment."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    column: Column
    value: str | list[Any] | pa.Field | Column | None
```

### AST Rules:
- **Separation of Concerns**: Selection logic (WHERE) lives in `catalog.filters`. Mutation logic (SET) lives in `catalog.assignments`.
- **Operator Equality**: In `filters`, the single equal sign `"="` is the standard SQL equality operator.
- **Assignment Implicit**: `assignments` carry column-value pairs. No operator is needed because the intent is encoded in the model itself.

## Plugin Protocol Rules:
```python
@runtime_checkable
class Plugin(Protocol):
    def get_catalog(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Catalog]: ...
    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def get_entity(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Entity]: ...
    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def get_column(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Column]: ...
    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    # data protocols - and how data might be requested and passed
    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]: ...
    def create_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any
                    ) -> PluginResponse[ArrowReader]: ...
    def update_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any
                    ) -> PluginResponse[ArrowReader]: ...
    def upsert_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any
                    ) -> PluginResponse[ArrowReader]: ...
    def delete_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any
                    ) -> PluginResponse[None]: ...
```
PluginProtocol aka Plugin dictates the universal interface for any data system.
- Five verbs:  create, get, update, upsert, delete
- Three nouns: catalog, entity, column

---

## External Resources
### Core Resources:
- [Python 3.13](https://docs.python.org/3.13/py-modindex.html)
- [DuckDB Python API Reference:](https://duckdb.org/docs/current/clients/python/overview)
- [DataFrame Interchange Protocol](https://data-apis.org/dataframe-protocol/latest/API.html)
- [PyArrow Cookbook](https://arrow.apache.org/cookbook/py/)
- [PyArrow Interchange](https://arrow.apache.org/docs/python/interchange_protocol.html)
- [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Polars Python API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
- [Arrow IPC:](https://arrow.apache.org/docs/python/ipc.html)

### Plugin Resources:
- [oracledb API Reference:](https://python-oracledb.readthedocs.io/en/latest/user_guide/dataframes.html)
- [Salesforce REST API Reference:](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)

### Maybe Later:
- [SQLGlot SQL Parser](https://github.com/tobymao/sqlglot)
- [Arrow Flight SQL:](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Arrow Flight Spec:](https://arrow.apache.org/docs/format/Flight.html)
- [LanceDB](https://lancedb.com/)

### Being Removed:
- [Pandas API Reference:](https://pandas.pydata.org/docs/reference/index.html)
](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)

### Maybe Later:
- [SQLGlot SQL Parser](https://github.com/tobymao/sqlglot)
- [Arrow Flight SQL:](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Arrow Flight Spec:](https://arrow.apache.org/docs/format/Flight.html)
- [LanceDB](https://lancedb.com/)

### Being Removed:
- [Pandas API Reference:](https://pandas.pydata.org/docs/reference/index.html)
ocs/reference/index.html)
