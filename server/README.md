# QuickBitLabs Federation Framework

## Author: Blaine Rudow

## Core Architectural Vision (Restarted)

QuickBitLabs is a data integration platform designed to solve the **N x M integration nightmare** by reducing it to **N + M** through a **Universal Pydantic Contract**. This allows for hot-swapping different data sources (Oracle, Salesforce, CSV, etc.) without changing core application code.

The core vision is intended to be a system agnostic data & metadata engine built to bridge relationally complex systems like Salesforce and Oracle. This includes self discovery of metadata, data federation, agnostic querying, and orm style metadata management. 

- ### ArrowStream:
  - typed from pyarrow.RecordBatchReader
  - A lazy-loaded, memory-efficient stream of data batches. It is the "Package" that carries the actual data payload. The ArrowStream is designed to be consumed in a streaming fashion, allowing for efficient processing of large datasets without loading everything into memory at once.

- ### Catalog: 
  - A Pydantic metadata model. It defines the "Intent" (Request) and the "Reality" (Result). It is decoupled from the live data stream to ensure serializability. The Catalog is the "Envelope" that carries metadata and context, while the ArrowFrame is the "Package" that carries the actual data. This separation allows for maximum flexibility and system-agnosticism.

- ### System Agnosticism:
  - The "High level Service Layer" (not to be confused with "plugin service layer") and the Interchange Objects (ArrowFrame) must be blind to the source system. Only the Plugins handle the "Plugin Native <-> Universal" bridge.

## Prototypes

### ArrowFrame:
  - A system-agnostic data interchange container. It represents the "Clean Slate" where data is transitioned from system-specific native formats to a universal PyArrow stream. It does NOT own or carry system-specific metadata.
  - A *God Object* during prototyping, but the long-term vision is to have ArrowFrame adapted to the DataFrame Interchange Protocol, allowing it to be a true universal container that can be easily converted to/from Pandas, Spark, Dask, etc. without losing the Arrow-based performance benefits.
### DuckDB:
  - As a successor to the Catalog query engine, allowing for more complex transformations and in-memory operations on the ArrowFrame without needing to regress to Python objects.

## Legacy Types:
### Records:
  - An older format represented as `Iterable[dict]`. It is less efficient and more memory-intensive than the ArrowStream. The Records format is being phased out in favor of the ArrowStream for all data operations. However I'm keeping it and it's type mappings to native python around for now.

## Key Project Files

- **server/plugins/PluginModels.py** Pydantic models for Catalog, Entity, and Column metadata.
- **server/plugins/PluginProtocol.py** The universal interface defining CRUD operations for data and metadata.
- **server/plugins/PluginRegistry.py** Handles lazy-loading and instantiation of plugin facades.
- **server/plugins/PluginResponse.py** The generic envelope for all plugin returns, handling codes and errors.
- **server/plugins/ArrowFrame.py** (Current: 0 bytes) The intended location for the universal interchange class.

## Successful Implementations:
- **server/services/FullMigration.py** A working full schema and data migration from Salesforce to Oracle.

## Existing Plugin Facades
- **server/plugins/oracle/Oracle.py** The Oracle plugin facade.
- **server/plugins/sf/Salesforce.py** The Salesforce plugin facade.

## Additional Paths:
- server/ - core application code
- frontend/ - React frontend code
- server/ProjectTree.md - a visual map of the server codebase

## Core Resources:
- [Python 3.13](https://docs.python.org/3.13/py-modindex.html)
- [DataFrame Interchange Protocol](https://data-apis.org/dataframe-protocol/latest/API.html)
- [PyArrow Cookbook](https://arrow.apache.org/cookbook/py/)
- [PyArrow Interchange](https://arrow.apache.org/docs/python/interchange_protocol.html)
- [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
- [Polars Python API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
- [Pandas API Reference:](https://pandas.pydata.org/docs/reference/index.html)

## Plugin Resources:
- [oracledb API Reference:](https://python-oracledb.readthedocs.io/en/latest/user_guide/dataframes.html)
- [Salesforce REST API Reference:](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)
- 

## Maybe Later:
- [DuckDB Python API Reference:](https://duckdb.org/docs/current/clients/python/overview)
- [Arrow Flight SQL:](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/)
- [Arrow Flight Spec:](https://arrow.apache.org/docs/format/Flight.html)
- [Arrow IPC:](https://arrow.apache.org/docs/python/ipc.html)

## Much Much Later:
- [Alembic](https://alembic.sqlalchemy.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)

## Bugs & Edge Cases Found
- [ ] **Blob/Binary Support:** Exclude `base64`/`blob` fields from Bulk V2 CSV migrations (`Attachment`, `Document`).
- [ ] **Time Formatting:** Fix `time64[us]` conversion error for ISO-8601 strings with 'Z' suffix (`BusinessHours`).
- [ ] **Query Restrictions:** Add "skip-list" for objects requiring specific filters (`ContentFolderItem`, `IdeaComment`).
- [ ] **Oracle Constraints:** Handle `ORA-01400` (NULLs in NOT NULL columns) and `ORA-12899` (Value too large) for metadata-driven schema creation (`DuplicateRule`, `Group`, `ListView`).
- [ ] **Access/Permissions:** Gracefully handle `INSUFFICIENT_ACCESS` during bulk queries (`ConnectedApplication`).

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