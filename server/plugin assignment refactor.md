# Intent-Based Catalog Refactor

## Objective
Refactor the Universal Pydantic Contract (`PluginModels.py`) to align with SQL-92 standards, cleanly separating data selection (filtering) from data mutation (assignments). This resolves the ambiguity that necessitated custom operators like `==` and eliminates the need for brittle translation layers between the UI (React Query Builder) and the backend.

## Background & Motivation
Currently, a single `operator_groups` list handles both filtering and assigning. To differentiate the intent, the system uses `=` for assignments and `==` for comparisons. This breaks standard SQL conventions, causes 422 errors when integrating industry-standard tools like React Query Builder, and makes complex federated updates (e.g., `UPDATE postgres.table SET col1 = 'val' WHERE oracle.table.col2 = 'val2'`) difficult to model securely.

### What the Current State Actually Looks Like
The `==` / `=` distinction isn't just a documentation convention — it is load-bearing code. `OracleDialect.py` has `_collect_assignments()` and `_strip_assignments()` functions plus an explicit runtime guard that throws if `"="` appears in a WHERE context. `RQBQueryBuilder.tsx:71` has a hardcoded `rule.operator === '=' ? '==' : rule.operator` translation to bridge the RQB-to-backend impedance mismatch. This is exactly the kind of "clever workaround that becomes the system" that needs to be unwound.

---

## Proposed Solution

### 1. New `Assignment` Model in `PluginModels.py`

Rather than reusing `Operation` with an implicit-only `"="` operator (which is redundant noise and an attack surface), introduce a dedicated minimal model:

```python
class Assignment(BaseModel):
    """A scalar mutation: column = value. The operator is implicit in the field name."""
    column: Column
    value: str | list[Any] | pa.Field | Column | None
```

This is self-documenting: being in `assignments` already encodes the intent. No operator field needed.

### 2. Refactoring `Catalog` in `PluginModels.py`

Explicitly separate the intents within the `Catalog` model.

**Current `Catalog` snippet:**
```python
class Catalog(BaseModel):
    # ...
    operator_groups: list[OperatorGroup] = Field(default_factory=list)
```

**Proposed `Catalog` snippet:**
```python
class Catalog(BaseModel):
    # ...
    # Intent 1: Selection (The WHERE clause)
    filters: list[OperatorGroup] = Field(default_factory=list)

    # Intent 2: Mutation (The SET clause)
    assignments: list[Assignment] = Field(default_factory=list)
```

### 3. Standardizing the `Operation` Model

Remove `"=="` from the operator literal. Because `filters` and `assignments` are now separate fields, the backend contextually knows the intent of every `"="`.

**Proposed `Operation` snippet:**
```python
class Operation(BaseModel):
    independent: Column
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None
```

*(The comment "The single equal sign '=' is an assignment operator" is removed — `=` is now a standard equality/comparison operator inside `filters`.)*

---

## How It Works in Practice: The Federated Update

This architecture is designed for the **high-level service layer (DuckDB)** to manage federation and actions, avoiding frontend serialization overhead and network transfer of massive datasets. The frontend only sends the *intent* (the Catalog JSON), and the backend orchestrates the execution.

### Example Payload
*Use Case: "Set status to 'Inactive' in Oracle where Salesforce customers have not purchased any products in Postgres in the last year."*

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
      "column": { "name": "status", "locator": {"plugin": "oracle"} },
      "value": "Inactive"
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

---

## Known Risks & Pre-Flight Checks

### Risk 1 — `Catalog.federate` must be updated before plugins
`Catalog.federate` currently fans out `operator_groups` by plugin:
```python
"operator_groups": [
    g for g in self.operator_groups
    if _collect_plugins_from_group(g) == {plugin_name}
],
```
After the split, **both** `filters` and `assignments` need independent fanout logic, and they aren't symmetric:
- A `filter` referencing `postgres.purchase_date` → only goes to the Postgres child catalog
- An `assignment` targeting `oracle.status` → only goes to the Oracle child catalog

**This must be designed before touching any plugin dialect.** Fan out `assignments` by `assignment.column.locator.plugin`. Fan out `filters` using the existing plugin-collection logic applied to each `OperatorGroup`.

### Risk 2 — Upsert identity resolution belongs in `filters`, not `assignments`
`CatalogMigration.py` builds `operator_groups` from primary key columns for MERGE ON matching. After the refactor, this is conceptually a `filter` — it's identity matching (WHERE), not a value assignment (SET). Update `CatalogMigration.py` and `OracleServices.py` upsert logic to place primary key identity operations into `filters`.

### Risk 3 — Persisted Catalog backward compatibility
The Catalog Registry stores full serialized Catalogs. If any cached Catalog has `operator_groups` populated and is rehydrated after the rename, Pydantic will silently drop those groups (unknown fields). This is probably low risk since `operator_groups` is typically built fresh per request, but:
- Audit the Catalog Registry contents before cutover.
- Optionally add a one-time migration pass or a `model_config = ConfigDict(extra='ignore')` note confirming the drop is intentional.

---

## Implementation Plan

### Phase 0 — Design (before any code changes)

- [ ] Sketch the updated `Catalog.federate` fanout logic for both `filters` and `assignments` on paper. Confirm the connected-component algorithm still works with two separate lists.
- [ ] Audit the Catalog Registry: run a query to check if any persisted Catalogs have non-empty `operator_groups`. Document and decide whether to migrate or drop them.

### Phase 1 — Core Contract (`PluginModels.py`)

> ⚠️ Requires explicit permission per the Core File Modification Rule.

- [ ] Add `Assignment` model above `OperatorGroup`.
- [ ] Remove `"=="` from `Operation.operator` literal union.
- [ ] Remove the `"The single equal sign '=' is an assignment operator"` comment from `Operation`.
- [ ] Replace `operator_groups: list[OperatorGroup]` with:
  - `filters: list[OperatorGroup]`
  - `assignments: list[Assignment]`
- [ ] Update `Catalog.federate` to fan out `filters` and `assignments` independently into child catalogs.
- [ ] Update the `operator_groups` filtering logic inside `federate` (currently uses `_collect_plugins_from_group`) to apply to `filters` only.

### Phase 2 — Backend Dialects & Services

- [ ] **`server/core/DuckDBDialect.py`**
  - Replace `_build_where(catalog.operator_groups)` with `_build_where(catalog.filters)`.

- [ ] **`server/plugins/oracle/OracleDialect.py`**
  - Delete `_collect_assignments()` and `_strip_assignments()` — no longer needed.
  - Delete the `"="` guard clause in `_parse_operation()`.
  - Replace all `catalog.operator_groups` WHERE-building calls with `catalog.filters`.
  - Replace all SET/MERGE-ON-building calls to use `catalog.assignments` (list of `Assignment`).

- [ ] **`server/plugins/oracle/OracleServices.py`**
  - Update `upsert_data()`: when no identity is provided, build identity ops into `catalog.filters` (not `operator_groups`).
  - Update any write path that previously injected `operator="="` operations into `operator_groups`.

- [ ] **`server/plugins/sf/models/SfDialect.py`**
  - Replace `catalog.operator_groups` SOQL building with `catalog.filters`.

- [ ] **`server/services/CatalogMigration.py`**
  - Update MERGE ON construction to put primary key identity operations into `catalog.filters`.

- [ ] **`server/api/data.py`**
  - Verify read/write paths reference `catalog.filters` and `catalog.assignments` appropriately (likely a pass-through — confirm no direct `operator_groups` access).

### Phase 3 — Frontend

- [ ] **`frontend/src/api/sessionApi.ts`**
  - Add `Assignment` interface: `{ column: Column; value: string | any[] | Column | null }`.
  - Remove `"=="` from `OperatorLiteral`.
  - Remove the `"= is assignment; == is comparison"` comment.
  - Replace `operator_groups?: OperatorGroup[]` on `Catalog` with:
    - `filters?: OperatorGroup[]`
    - `assignments?: Assignment[]`

- [ ] **`frontend/src/components/RQBQueryBuilder.tsx`**
  - Delete the `rule.operator === '=' ? '==' : rule.operator` translation (line ~71).
  - Wire RQB output directly to `catalog.filters`.

### Phase 4 — Documentation

- [ ] **`Q:/quickbitlabs/Plugin Framework Rules.md`**
  - Update `Catalog` model snippet: replace `operator_groups` with `filters` and `assignments`.
  - Update `Operation` model snippet: remove `"=="` from the operator literal.
  - Add `Assignment` model snippet alongside `Operation`.
  - Update the AST Query System section: remove the `"=" is assignment, "==" is comparison` rule. Replace with: `"=" is the standard SQL equality operator inside `filters`; `assignments` carry column+value pairs with no operator field."
  - Update the AST Upsert Rules section: match resolution via `catalog.filters`, SET clause via `catalog.assignments`.

- [ ] **`Q:/quickbitlabs/server/README.md`**
  - Update any mentions of `operator_groups`.
  - Update the session → DataMart → federation flow if filters/assignments are referenced.

- [ ] **`Q:/quickbitlabs/frontend/QBL Frontend Rules.md`** (if it exists)
  - Update Catalog/Operation type documentation.
  - Remove reference to the `=` → `==` translation hack.

- [ ] **`server/ProjectTree.md`**
  - Update if `PluginModels.py` description mentions `operator_groups` or the `==` operator.

- [ ] **`C:/Users/rmedi/.claude/projects/q--quickbitlabs/memory/`**
  - Update `project_datamart_session_arch.md` if it references `operator_groups`.
  - Save a new memory entry capturing the `filters` / `assignments` split as settled architecture.

- [ ] **Inline code comments**
  - Remove the `# The single equal sign '=' is an assignment operator` comment from `Operation` in `PluginModels.py` (done in Phase 1, document here as a doc concern too).
  - Remove the mirror comment in `sessionApi.ts` (done in Phase 3).
  - Update docstrings in `OracleDialect.py` for any method that referenced `operator_groups` or the `==` convention.

---

## Testing Strategy

### Unit Tests — Model Layer

- [ ] `Catalog` round-trips correctly with non-empty `filters` and `assignments` via `model_dump_json()` / `model_validate_json()`.
- [ ] `Catalog` with an old `operator_groups` field silently drops it (confirm Pydantic behavior; document as intentional).
- [ ] `Assignment` rejects unexpected fields (Pydantic strict mode check).
- [ ] `Operation` rejects `"=="` (now removed from the literal — confirm Pydantic validation error).
- [ ] `Catalog.federate` with cross-plugin filters routes each `OperatorGroup` to the correct child catalog.
- [ ] `Catalog.federate` with cross-plugin assignments routes each `Assignment` to the correct child catalog.
- [ ] `Catalog.federate` with a filter referencing two plugins leaves it on the master (not pushed down).

### Unit Tests — Dialect Layer

**OracleDialect:**
- [ ] `build_select()` with `catalog.filters` produces correct WHERE clause with standard `"="` as equality.
- [ ] `build_update()` with `catalog.assignments` produces correct SET clause.
- [ ] `build_merge()` with `catalog.filters` (identity) + `catalog.assignments` (SET) produces a correct MERGE statement.
- [ ] Passing a `Catalog` with empty `filters` produces no WHERE clause (not an error).
- [ ] Passing a `Catalog` with empty `assignments` to an update operation returns `not_implemented` or an appropriate error, not a silent no-op.

**DuckDBDialect:**
- [ ] `_build_where(catalog.filters)` with nested AND/OR groups produces correct SQL.
- [ ] Empty `catalog.filters` produces no WHERE clause fragment.

**SfDialect:**
- [ ] SOQL WHERE clause is built from `catalog.filters` correctly for string, numeric, and date columns.

### Integration Tests — API Layer

- [ ] `POST /api/data/` with `filters` returns the same filtered result set that `operator_groups` with `"=="` previously returned (regression test — run before and after).
- [ ] `POST /api/data/` with `assignments` + `filters` executes a write and confirms the correct rows were mutated and only those rows.
- [ ] A Catalog with both `filters` and `assignments` spanning two plugins routes each to the correct child and executes in the correct order (read filtered rows → write to target).
- [ ] A Catalog with `operator_groups` populated (simulating an old persisted Catalog) is silently handled without a 422 or 500 error.

### Frontend Tests

- [ ] `RQBQueryBuilder` produces a `Catalog.filters` array with standard `"="` (not `"=="`) when the user selects an equality rule.
- [ ] The generated Catalog payload passes TypeScript type checking with no `as any` casts.
- [ ] A round-trip fetch (build Catalog in UI → POST → receive data) works end-to-end with filtered results matching expectations.

### Regression Baseline (run before any code changes)

Before touching a single file, capture a baseline:
- [ ] Record current API responses for a known filtered query against Oracle (with `operator_groups` + `"=="`).
- [ ] Record current API responses for a known upsert against Oracle (with `operator_groups` + `"="`).
- [ ] Record the full Salesforce SOQL string generated for a known filtered query.
- [ ] Save these as fixture files. The post-refactor integration tests compare against these baselines.
