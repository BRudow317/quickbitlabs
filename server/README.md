# QuickBitLabs Data Integration Engine

Welcome to the QuickBitLabs. This project is designed to solve the **N x M integration nightmare** by reducing it to **N + M**. 

Instead of writing custom pipelines between every single database, flat file, and SaaS API, this system utilizes a **Universal Pydantic Contract**. The business logic (API/Services aka server/services) never speaks directly to the databases. Instead, Data Streams are passed through a standardized `Plugin` interface, allowing us to hot-swap an Oracle database for a rest api, for a parquet file, for a excel file, for a postgres database, for an aws DynamoDB instance, etc with zero changes to the core application code.

## Core Architecture
- [ProjectTree.md](server\ProjectTree.md)

The platform is strictly divided into three layers:

1. **Backend (`/server`**: This handles FastAPI web requests, user authentication, and internal platform state.
2. **Frontend (`/frontend`)**: The Typescript/React UI.
3. **Plugins (`/server/plugins`)**: The Sandboxes. These are isolated, lazy-loaded modules that translate the universal platform language into system-specific execution (SQL, REST, File I/O).
   1. Plugins have their own Service layer for internal orchestration of the plugin. 
   2. Plugins usually have their own Dialect layer for translating the AST Catalog queries into native search language.
   3. Plugins likely have a type mapping layer for translating system types into the universal Catalog types.
   4. Plugin Engine layer is for execution of system-specific commands. To avoid import issues, this layer should rarely import anything else from the local plugin. 
   5. Plugins typically have a Client to manage connections and sessions. 
   6. Plugins must have a Facade that implements the Plugin Protocol.

## Data Transmission
Previously data transmission occured between plugins through the use of a shared `Records` object returned as an Iterable of dictionaries. However now there are new methods I am implementing such as the `Apache Arrow streaming`. 

Current implementations include:
- `Records` (Iterable[dict]): The deprecated universal boundary for data transmission.
- `ArrowStream` (Iterator[pa.RecordBatch]): A more efficient boundary for large data transfers, especially from databases or APIs that can natively produce Arrow RecordBatches. This allows for zero-copy data transmission and efficient in-memory processing.

## The Plugin Ecosystem (The Secret Sauce)

At the root of the `/server/plugins/` directory sit the four pillars of the platform's SDK. **Every plugin must abide by these files:**

- **`PluginModels.py` (The Nouns):** Defines `Catalog`, `Entity`, `Column`, `ArrowStream` and **Federated Querying Rules** and data transmission. This is the blueprint. Plugins never share proprietary schema objects; everything is wrapped in these Pydantic models.
  - ### Catalog
    One of the smallest, but most important parts of the entire project.

    All metadata at the higher levels exists as a subset of this object.
    Catalog -> Entities -> Columns -> Records(data, records, bytes, json, etc.)

    Catalog is the top-level wrapper. It may be called schema, namespace, database, etc. in different systems, but the concept is the same: a container for entities/objects/tables. 

    When implementing a plugin it is up to you to decide how to route the catalog, but the catalog, its entity, and the columns needed for operations should be passed along. 

    The intention is not to populate an entire catalog and every entity and every column every call, but to provide the relevant metadata needed to perform the requested operation. 

    An example might be implementing a csv plugin where the catalog is the source directory, the entity is the file, and the columns are the columns, and records, the rows.

    Another example might be a REST API plugin where the catalog is the base URL, the entity is the endpoint, and the columns are the query parameters or body parameters needed to perform the request.

    Another example might be a SQL plugin where the catalog is the database, the entity is the table, and the columns are the columns needed to perform the query or DML operation.

    Perhaps you only know the catalog to search for, the target plugin may accept an a catalog with an empty entity list and populate the entire catalog with its metadata, so the caller service can inspect and make relevant decisions about which entities and columns to operate on in subsequent calls.

    Or perhaps you want to know the type of a specific column, so you provide a catalog with one entity and one column, and the plugin returns the catalog with the column's metadata populated.

    This is how the protocols should be designed, and implemented. The catalog is the envelope that carries the metadata and context needed to perform the requested operation.
    
    As a pydantic BaseModel you can serialize the entire working contract to JSON with catalog.json() or dict with catalog.dict(), and rehydrate with CatalogModel.parse_raw() or CatalogModel.parse_obj() respectively.

    It can also serve as a base for implementing openapi schemas, ORM models, or any other structured representation of metadata you need.
  - ### Entity
    The noun for the object being operated on. This could be a table, file, API endpoint, etc. It has a name and a list of columns.
  - ### Column
    The noun for the attributes of the entity. This could be columns in a table, keys in a JSON object, etc. It has a name and a type.
  - ### Querying
    The universal JSON representation of a data request (AST). Instead of holding a raw SQL string, it structures the request abstractly with lists of entities, columns, and nested filter_groups (e.g., column: "status", operator: "==", value: "active"). This allows the FastAPI frontend to request data without knowing whether the target system speaks SQL, SOQL, or REST. (Note: It does include a native escape hatch for edge-case raw system queries).

- **`PluginProtocol.py` (The Verbs):** The strict Python `Protocol` defining the standard CRUD methods ex) (`get_data`, `create_data`, `update_catalog`, `upsert_entity`, `delete_column`). If a plugin doesn't support a verb, it returns a 501.

- **`PluginResponse.py` (The Envelope):** Every plugin method returns this wrapper. It maps outcomes to standard HTTP status codes (200, 404, 500) and safely carries data stream generators without exhausting them.

- **`PluginRegistry.py` (The Factory):** The lazy-loading engine. Services call `get_plugin('oracle')` to instantiate a facade. This prevents missing C-libraries (like `oracledb` or `psycopg2`) from crashing the whole server if they aren't currently being used.

### Existing Plugin Implementations
* `/oracle` - Heavy enterprise SQL and DDL generation.
* `/sf` - Salesforce bulk and REST API integration.

---

# [API of the __dataframe__ protocol](https://data-apis.org/dataframe-protocol/latest/API.html):
Specification for objects to be accessed, for the purpose of dataframe interchange between libraries, via the __dataframe__ method on a libraries’ data frame object.

For guiding requirements, see Protocol design requirements .

## Concepts in this design:
A Buffer class. A buffer is a contiguous block of memory - this is the only thing that actually maps to a 1-D array in a sense that it could be converted to NumPy, CuPy, et al.

## A Column class. 
A column has a single dtype. It can consist of multiple chunks . A single chunk of a column (which may be the whole column if num_chunks == 1 ) is modeled as again a Column instance, and contains 1 data buffer and (optionally) one mask for missing data.

## DataFrame
A data frame is an ordered collection of columns , which are identified with names that are unique strings. All the data frame’s rows are the same length. It can consist of multiple chunks . A single chunk of a data frame is modeled as again a DataFrame instance.

## mask concept
A mask of a single-chunk column is a buffer .

## chunk concept
A chunk is a sub-dividing element that can be applied to a data frame or a column .

Note that the only way to access these objects is through a call to __dataframe__ on a data frame object. This is NOT meant as public API; only think of instances of the different classes here to describe the API of what is returned by a call to __dataframe__ . They are the concepts needed to capture the memory layout and data access of a data frame.

## Design decisions
Use a separate column abstraction in addition to a dataframe interface.

Rationales:

This is how it works in R, Julia and Apache Arrow.

Semantically most existing applications and users treat a column similar to a 1-D array

We should be able to connect a column to the array data interchange mechanism(s)

Note that this does not imply a library must have such a public user-facing abstraction (ex. pandas.Series ) - it can only be accessed via __dataframe__ .

Use methods and properties on an opaque object rather than returning hierarchical dictionaries describing memory.

This is better for implementations that may rely on, for example, lazy computation.

No row names. If a library uses row names, use a regular column for them.

---

## Getting Started

```bash
# Activate your virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the project in editable mode
pip install -e .

# Start the FastAPI server
python server/start_server.py
```

---

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

## Known Issues & Limitations

### SF Compound Types Excluded from Migration
Salesforce compound fields (`address`, `location`) have no Arrow type mapping and are excluded from `get_catalog` column lists. Sub-fields (e.g., `MailingStreet`, `MailingCity`) are migrated individually instead.

### Salesforce DDL (Create/Update/Delete) Not Implemented
The Salesforce plugin returns `not_implemented` for metadata mutation verbs. The standard REST/Bulk APIs do not support DDL; enabling this would require a separate implementation using the Salesforce Metadata API.

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