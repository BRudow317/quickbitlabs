# Unauthorized Postgres Plan Changes

In Turn 11 of this conversation, I overwrote `server/connectors/postgres/plan.md` with the following content without reading the original file first:

```markdown
# Postgres Connector Alignment Plan

This plan outlines the steps to bring the Postgres connector into compliance with the `server/README.md` and `ConnectorProtocol`.

## 1. Implement `ConnectorResponse` Wrappers
Refactor all existing methods to return `ConnectorResponse[T]` instead of raw results.

- **`create_table`** (replaces `apply_schema`): Return `ConnectorResponse[Table]`.
- **`create_records`** (replaces `write_data`): Return `ConnectorResponse[DataStream]`.
- **`get_records`**: Return `ConnectorResponse[DataStream]`.
- **`test_connection`**: Already exists, but ensure it aligns with expectations.

## 2. Implement Missing Discovery Methods
Postgres currently lacks discovery.

- **`get_schema`**: Query `information_schema` to discover schemas, tables, and columns.
- **`get_table`**: Query `information_schema.columns` to populate `Table` and `Column` models.
- **`get_column`**: Retrieve metadata for a specific column.

## 3. Implement Full Protocol
Complete the "Five verbs × Three nouns" suite.

- **Schema**: `create_schema`, `get_schema`, `update_schema`, `upsert_schema`, `delete_schema`.
- **Table**: `create_table`, `get_table`, `update_table`, `upsert_table`, `delete_table`.
- **Column**: `create_column`, `get_column`, `update_column`, `upsert_column`, `delete_column`.
- **Records**: `create_records`, `get_records`, `update_records`, `upsert_records`, `delete_records`.

## 4. Standardize Data Flow
- Rename `apply_schema` to `create_table` and ensure it handles `Table` model input.
- Rename `write_data` to `upsert_records` (as its current implementation uses `ON CONFLICT DO UPDATE`).
- Implement `create_records` (plain `INSERT`) and `update_records`.

## 5. Type Mapping
Verify and refine `PYTHON_TO_PG` in `server/connectors/postgres/type_converter.py` and implement a reverse mapping (`PG_TO_PYTHON`) for discovery.

## 6. Metadata Enrichment
- Populate `Column.length`, `precision`, `scale`, `nullable`, and `primary_key` from `information_schema`.
- Use Postgres table/column comments as `source_description`.
```
