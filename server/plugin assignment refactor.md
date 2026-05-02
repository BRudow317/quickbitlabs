# Intent-Based Catalog Refactor

## Objective
Refactor the Universal Pydantic Contract (`PluginModels.py`) to align with SQL-92 standards, cleanly separating data selection (filtering) from data mutation (assignments). This resolves the ambiguity that necessitated custom operators like `==` and eliminates the need for brittle translation layers between the UI (React Query Builder) and the backend.

## Background & Motivation
Currently, a single `operator_groups` list handles both filtering and assigning. To differentiate the intent, the system uses `=` for assignments and `==` for comparisons. This breaks standard SQL conventions, causes 422 errors when integrating industry-standard tools like React Query Builder, and makes complex federated updates (e.g., `UPDATE postgres.table SET col1 = 'val' WHERE oracle.table.col2 = 'val2'`) difficult to model securely.

## Proposed Solution (The Vision)

### 1. Refactoring `PluginModels.py`
Explicitly separate the intents within the `Catalog` model.

**Current `Catalog` Model snippet:**
```python
class Catalog(BaseModel):
    # ...
    operator_groups: list[OperatorGroup] = Field(default_factory=list)
```

**Proposed `Catalog` Model snippet:**
```python
class Catalog(BaseModel):
    # ...
    # Intent 1: Selection (The WHERE clause)
    filters: list[OperatorGroup] = Field(default_factory=list)
    
    # Intent 2: Mutation (The SET clause)
    assignments: list[Operation] = Field(default_factory=list)
```

### 2. Standardizing the `Operation` Model
Return to standard SQL operators. Because `filters` and `assignments` are now separate fields, the backend contextually knows the intent of the `=`.

**Proposed `Operation` Model snippet:**
```python
class Operation(BaseModel):
    independent: Column
    # Replaced "==" with "=" to match SQL and RQB.
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None
```

## How It Works in Practice: The Federated Update

This architecture is designed for the **high-level service layer (DuckDB)** to manage federation and actions, avoiding frontend serialization overhead and network transfer of massive datasets. The frontend only sends the *intent* (the Catalog JSON), and the backend orchestrates the execution.

### Example Payload
*Use Case: "Set status to 'Inactive' in Oracle where Salesforce customers have not purchased any products in Postgres in the last year."*

The frontend sends a single JSON payload defining the entire federated operation:

```json
{
  "source_type": "oracle",
  "entities": [
    { "name": "oracle_customers", "plugin": "oracle", "columns": [...] },
    { "name": "sf_customers", "plugin": "salesforce", "columns": [...] },
    { "name": "pg_invoices", "plugin": "postgres", "columns": [...] }
  ],
  "joins": [
    {
      "left_entity": {"name": "oracle_customers"},
      "left_column": {"name": "id"},
      "right_entity": {"name": "sf_customers"},
      "right_column": {"name": "oracle_id"},
      "join_type": "INNER"
    },
    {
      "left_entity": {"name": "sf_customers"},
      "left_column": {"name": "id"},
      "right_entity": {"name": "pg_invoices"},
      "right_column": {"name": "customer_id"},
      "join_type": "LEFT"
    }
  ],
  
  "assignments": [
    {
      "independent": { "name": "status", "locator": {"plugin": "oracle"} },
      "operator": "=",
      "dependent": "Inactive"
    }
  ],
  
  "filters": [
    {
      "condition": "AND",
      "operation_group": [
        {
          "independent": { "name": "purchase_date", "locator": {"plugin": "postgres"} },
          "operator": ">=",
          "dependent": "2025-05-02"
        },
        {
          "independent": { "name": "id", "locator": {"plugin": "postgres"} },
          "operator": "IS NULL",
          "dependent": null
        }
      ]
    }
  ]
}
```

## Implementation Plan

### Backend (`server/`)
1.  **`PluginModels.py`**: 
    -   Update `Catalog` to replace `operator_groups` with `filters` and `assignments`.
    -   Update `Operation` to use standard SQL operators (`=` instead of `==`).
2.  **`api/data.py`**:
    -   Update reading operations to process `catalog.filters`.
    -   Update writing operations (Upsert, Update) to process `catalog.filters` and `catalog.assignments`. The backend orchestration layer will use DuckDB to perform the federated join and predicate pushdown, generating an Arrow stream internally, and then piping that stream directly to the target plugin's write method.
3.  **`core/federation.py`**:
    -   Update the DuckDB query generation logic to parse the `filters` array to build the `WHERE` clause.
4.  **`plugins/`**:
    -   Update plugin-specific logic (e.g., `OracleServices.py`) to handle the new `Operation` operators and the separated `filters`/`assignments` structure when generating native SQL.

### Frontend (`frontend/`)
1.  **`api/sessionApi.ts`**:
    -   Update the TypeScript interfaces (`Catalog`, `Operation`) to reflect the backend changes.
2.  **`components/RQBQueryBuilder.tsx`**:
    -   Remove the operator translation logic (e.g., `=` to `==`).
    -   Pass the React Query Builder output directly to the `filters` property of the generated `Catalog` object.
