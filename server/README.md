# QuickBitLabs Data Integration Engine

Welcome to the QuickBitLabs backend. This project is designed to solve the **N x M integration nightmare** by reducing it to **N + M**. 

Instead of writing custom pipelines between every single database, flat file, and SaaS API, this system utilizes a **Universal Pydantic Contract**. The business logic (API/Services) never speaks directly to the databases. Instead, Data Streams are passed through a standardized `Plugin` interface, allowing us to hot-swap an Oracle database for a flat CSV file with zero changes to the core application code.

## Core Architecture

The platform is strictly divided into three layers:

1. **API & Models (`/server/api`, `/server/models`)**: The business domain. This handles FastAPI web requests, user authentication, and internal platform state (e.g., Tenant profiles, Users).
2. **Services (`/server/services`)**: The Air Traffic Controllers. These scripts orchestrate data movement (e.g., `MigrationService.py`). They don't know *how* to talk to Oracle or Salesforce; they just juggle the universal data models between them.
3. **Plugins (`/server/plugins`)**: The Sandboxes. These are isolated, lazy-loaded modules that translate the universal platform language into system-specific execution (SQL, REST, File I/O).

## The Plugin Ecosystem (The Secret Sauce)

At the root of the `/server/plugins/` directory sit the four pillars of the platform's SDK. **Every plugin must abide by these files:**

- **`PluginModels.py` (The Nouns):** Defines `CatalogModel`, `EntityModel`, `FieldModel`, and `QueryModel`. This is the blueprint. Plugins never share proprietary schema objects; everything is wrapped in these Pydantic models.
  - ### CatalogModel
    One of the smallest, but most important parts of the entire project.

    All metadata at the higher levels exists as a subset of this object.
    Catalog -> Entities -> Fields -> Records(data, records, bytes, json, etc.)

    Catalog is the top-level wrapper. It may be called schema, namespace, database, etc. in different systems, but the concept is the same: a container for entities/objects/tables. 

    When implementing a plugin it is up to you to decide how to route the catalog, but the catalog, its entity, and the fields needed for operations should be passed along. 

    The intention is not to populate an entire catalog and every entity and every field every call, but to provide the relevant metadata needed to perform the requested operation. 

    An example might be implementing a csv plugin where the catalog is the source directory, the entity is the file, and the fields are the columns, and records, the rows.

    Another example might be a REST API plugin where the catalog is the base URL, the entity is the endpoint, and the fields are the query parameters or body parameters needed to perform the request.

    Another example might be a SQL plugin where the catalog is the database, the entity is the table, and the fields are the columns needed to perform the query or DML operation.

    Perhaps you only know the catalog to search for, the target plugin may accept an a catalog with an empty entity list and populate the entire catalog with its metadata, so the caller service can inspect and make relevant decisions about which entities and fields to operate on in subsequent calls.

    Or perhaps you want to know the type of a specific field, so you provide a catalog with one entity and one field, and the plugin returns the catalog with the field's metadata populated.

    This is how the protocols should be designed, and implemented. The catalog is the envelope that carries the metadata and context needed to perform the requested operation.
    
    As a pydantic BaseModel you can serialize the entire working contract to JSON with catalog.json() or dict with catalog.dict(), and rehydrate with CatalogModel.parse_raw() or CatalogModel.parse_obj() respectively.

    It can also serve as a base for implementing openapi schemas, ORM models, or any other structured representation of metadata you need.
  - ### EntityModel
    The noun for the object being operated on. This could be a table, file, API endpoint, etc. It has a name and a list of fields.
  - ### FieldModel
    The noun for the attributes of the entity. This could be columns in a table, keys in a JSON object, etc. It has a name and a type.
  - ### QueryModel
    The noun for the query or operation being performed. This could be a SQL query, a REST API request body, etc. It has a query string and a list of parameters.


- **`PluginProtocol.py` (The Verbs):** The strict Python `Protocol` defining the standard CRUD methods (`get_records`, `insert_records`, `update_catalog`). If a plugin doesn't support a verb, it returns a 501.

- **`PluginResponse.py` (The Envelope):** Every plugin method returns this wrapper. It maps outcomes to standard HTTP status codes (200, 404, 500) and safely carries data stream generators without exhausting them.

- **`PluginRegistry.py` (The Factory):** The lazy-loading engine. Services call `get_plugin('oracle')` to instantiate a facade. This prevents missing C-libraries (like `oracledb` or `psycopg2`) from crashing the whole server if they aren't currently being used.

### Existing Plugin Implementations
* `/oracle` - Heavy enterprise SQL and DDL generation.
* `/postgres` - Standard relational data operations.
* `/sf` - Salesforce bulk and REST API integration.
* `/readers` - Flat file (CSV, Excel) and JSON parsing.

## Getting Started

```bash
# Activate your virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the project in editable mode
pip install -e .

# Start the FastAPI server
python server/start_server.py