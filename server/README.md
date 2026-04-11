# QuickBitLabs Data Integration Engine

Welcome to the QuickBitLabs backend. This project is designed to solve the **N x M integration nightmare** by reducing it to **N + M**. 

Instead of writing custom pipelines between every single database, flat file, and SaaS API, this system utilizes a **Universal Pydantic Contract**. The business logic (API/Services) never speaks directly to the databases. Instead, Data Streams are passed through a standardized `Plugin` interface, allowing us to hot-swap an Oracle database for a flat CSV file with zero changes to the core application code.

## Core Architecture

The platform is strictly divided into three layers:

1. **API & Models (`/server/api`, `/server/models`)**: The business domain. This handles FastAPI web requests, user authentication, and internal platform state.
2. **Services (`/server/services`)**: The Air Traffic Controllers. These scripts orchestrate data movement (e.g., `MigrationService.py`). They don't know *how* to talk to Oracle or Salesforce; they just juggle the universal data models between them.
3. **Plugins (`/server/plugins`)**: The Sandboxes. These are isolated, lazy-loaded modules that translate the universal platform language into system-specific execution (SQL, REST, File I/O).

## Data Transmission
Previously data transmission occured between plugins through the use of a shared `Records` object returned as an Iterable of dictionaries. However now there are new methods I am implementing such as the Apache Arrow streaming. 

Current implementations include:
- `Records` (Iterable[dict]): The original universal boundary for data transmission.
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