"""
OracleService — orchestration layer for the Oracle plugin.

Responsibilities:
  - Translate between Catalog/Entity/Column objects and Oracle system catalogs
    (ALL_TAB_COLUMNS, ALL_CONSTRAINTS).
  - Route read/write operations through OracleArrowFrame (Arrow-native I/O).
  - Route DDL through OracleEngine (structural mutations).
  - Delegate all SQL generation to OracleDialect (pure builders, no I/O here).

Nothing in this file communicates with Oracle directly; all Oracle access goes
through self.client.get_con() (via OracleArrowFrame or OracleEngine).
"""
from __future__ import annotations

import logging
from typing import Any

from server.plugins.PluginModels import Catalog, Entity, Column, ArrowReader, Locator
from server.plugins.PluginResponse import PluginResponse
from server.plugins.oracle.OracleTypeMap import (
    map_oracle_to_arrow,
    map_oracle_to_python,
    map_python_to_oracledb_input_size,
    map_python_to_oracle_ddl,
    map_arrow_to_oracle_ddl,
    map_column_to_oracledb_input_size,
)
from .OracleEngine import OracleEngine, OracleSchema, OracleTable
from .OracleDialect import (
    build_select,
    build_insert_dml,
    build_update_dml,
    build_merge_dml,
    build_delete_dml,
    build_rebuild_select,
    _get_target_entity,
)
from .OracleArrowFrame import OracleArrowFrame
from .OracleClient import OracleClient

logger = logging.getLogger(__name__)


class OracleService:
    client: OracleClient
    engine: OracleEngine
    arrow_frame: OracleArrowFrame

    def __init__(self, client: OracleClient):
        self.client = client
        self.engine = OracleEngine(schema=self.client.oracle_user.upper(), client=client)
        self.arrow_frame = OracleArrowFrame(client)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_schema(self, catalog: Catalog) -> str:
        return (catalog.name or self.client.oracle_user).upper()

    def _input_sizes_for_entity(self, entity: Entity) -> dict[str, Any]:
        """Map an entity's columns to oracledb cursor.setinputsizes() hints."""
        sizes: dict[str, Any] = {}
        for col in entity.columns:
            if col.arrow_type_id:
                sizes[col.name] = map_column_to_oracledb_input_size(col)
            elif col.raw_type:
                if not col.properties.get("python_type"):
                    col.properties["python_type"] = map_oracle_to_python(
                        col.raw_type, col.scale
                    )
                sizes[col.name] = map_python_to_oracledb_input_size(col)
            else:
                sizes[col.name] = None
        return sizes

    def _input_sizes_for_catalog(self, catalog: Catalog) -> dict[str, Any]:
        sizes: dict[str, Any] = {}
        for entity in catalog.entities:
            sizes.update(self._input_sizes_for_entity(entity))
        return sizes

    # ------------------------------------------------------------------
    # Oracle system catalog queries (raw metadata)
    # ------------------------------------------------------------------

    def _list_tables(self, schema_name: str) -> list[str]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        return schema.list_table_names()

    def _fetch_primary_keys(self, schema_name: str, table_name: str) -> set[str]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        return table.fetch_primary_keys()

    def _fetch_columns_raw(
        self,
        schema_name: str,
        table_name: str,
        requested: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch ALL_TAB_COLUMNS rows for a table.

        When `requested` is provided, only rows whose COLUMN_NAME appears in
        that set (case-insensitive) are returned.
        """
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        rows: list[dict[str, Any]] = table._fetch_tab_columns or []
        if not requested:
            return rows
        upper = {c.upper() for c in requested}
        return [r for r in rows if str(r.get("COLUMN_NAME", "")).upper() in upper]

    def _column_from_row(
        self,
        table_name: str,
        row: dict[str, Any],
        pk_set: set[str],
    ) -> Column:
        """Hydrate a Column from an ALL_TAB_COLUMNS row dict."""
        name = str(row["COLUMN_NAME"])
        raw_type = str(row["DATA_TYPE"])
        scale = row["DATA_SCALE"]
        precision = row["DATA_PRECISION"]
        max_length = row["CHAR_LENGTH"]
        return Column(
            name=name,
            locator=Locator(plugin="oracle", entity_name=table_name),
            raw_type=raw_type,
            arrow_type_id=map_oracle_to_arrow(raw_type, scale),
            primary_key=name in pk_set,
            is_nullable=str(row["NULLABLE"]).upper() == "Y",
            max_length=int(max_length) if max_length is not None else None,
            precision=int(precision) if precision is not None else None,
            scale=int(scale) if scale is not None else None,
            properties={"python_type": map_oracle_to_python(raw_type, scale)},
        )

    # ------------------------------------------------------------------
    # READ: Metadata
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> Catalog:
        """Populate catalog.entities with full column metadata from Oracle.

        Empty catalog (no entities) → full schema discovery: all tables + all columns.
        Catalog with entities      → populate/filter each entity's columns.
        """
        schema_name = self._resolve_schema(catalog)

        if not catalog.entities:
            entities: list[Entity] = []
            for table_name in self._list_tables(schema_name):
                pk_set = self._fetch_primary_keys(schema_name, table_name)
                rows = self._fetch_columns_raw(schema_name, table_name)
                columns = [self._column_from_row(table_name, r, pk_set) for r in rows]
                entities.append(
                    Entity(name=table_name, namespace=schema_name, columns=columns)
                )
            catalog.entities = entities
            catalog.name = schema_name
            return catalog

        for idx, entity in enumerate(catalog.entities):
            table_name = (entity.name or "").upper()
            if not table_name:
                continue

            requested: set[str] | None = (
                {c.name.upper() for c in entity.columns if c.name}
                if entity.columns
                else None
            )
            pk_set = self._fetch_primary_keys(schema_name, table_name)
            rows = self._fetch_columns_raw(schema_name, table_name, requested)
            fetched = {
                str(r["COLUMN_NAME"]).upper(): self._column_from_row(table_name, r, pk_set)
                for r in rows
            }

            if entity.columns:
                entity.columns = [fetched.get(c.name.upper(), c) for c in entity.columns]
            else:
                entity.columns = list(fetched.values())

            entity.name = table_name
            entity.namespace = schema_name
            catalog.entities[idx] = entity

        catalog.name = schema_name
        return catalog

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """Hydrate the first entity in the catalog with full column metadata."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema(catalog)
        entity = catalog.entities[0]
        table_name = (entity.name or "").upper()
        pk_set = self._fetch_primary_keys(schema_name, table_name)
        rows = self._fetch_columns_raw(schema_name, table_name)
        columns = [self._column_from_row(table_name, r, pk_set) for r in rows]
        return Entity(name=table_name, namespace=schema_name, columns=columns)

    def get_column(self, catalog: Catalog, **kwargs: Any) -> Column:
        """Fetch metadata for the first column of the first entity."""
        if not catalog.entities or not catalog.entities[0].columns:
            raise ValueError("Catalog must specify an entity with at least one column.")
        schema_name = self._resolve_schema(catalog)
        entity = catalog.entities[0]
        col = entity.columns[0]
        table_name = (entity.name or "").upper()
        pk_set = self._fetch_primary_keys(schema_name, table_name)
        rows = self._fetch_columns_raw(schema_name, table_name, {col.name.upper()})
        if not rows:
            raise ValueError(
                f"Column '{col.name}' not found on {schema_name}.{table_name}."
            )
        return self._column_from_row(table_name, rows[0], pk_set)

    # ------------------------------------------------------------------
    # READ: Data
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog, **kwargs: Any) -> ArrowReader:
        """Stream data from Oracle as an Arrow RecordBatchReader.

        Pass statement= kwarg for a raw SQL override (e.g. from Oracle.query()).
        Otherwise, SQL is derived from the Catalog AST via build_select().
        """
        statement: str | None = kwargs.get("statement")
        binds: dict[str, Any] = kwargs.get("binds") or {}
        if statement:
            return self.arrow_frame.arrow_reader(statement, parameters=binds)
        sql, binds = build_select(catalog)
        return self.arrow_frame.arrow_reader(sql, parameters=binds)

    # ------------------------------------------------------------------
    # WRITE: Data DML (stream-based)
    # ------------------------------------------------------------------

    def insert_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> None:
        """INSERT rows from an Arrow stream into the single DML target entity."""
        entity = _get_target_entity(catalog)
        sql, static_binds = build_insert_dml(catalog, entity)
        input_sizes = self._input_sizes_for_entity(entity)
        self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)

    def update_data(
        self,
        catalog: Catalog,
        data: ArrowReader,
        **kwargs: Any,
    ) -> PluginResponse[None]:

        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for UPDATE.")
        try:
            target = _get_target_entity(catalog)
            sql, static_binds = build_update_dml(catalog, target)
            input_sizes = self._input_sizes_for_entity(target)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)
            return PluginResponse.success(None)
        except Exception as exc:
            return PluginResponse.error(str(exc))

    def upsert_data(
        self,
        catalog: Catalog,
        data: ArrowReader,
        **kwargs: Any,
    ) -> PluginResponse[None]:
        """
        [WORKING REQUIREMENTS] - Universal Upsert Lifecycle for Oracle:
        
        1. Match Resolution (The 'ON' Clause):
           - Priority 1 (Explicit): If catalog.operator_groups is populated, use it to build 
             the MERGE ON clause. This supports flexible identity matching (e.g. email, username).
           - Priority 2 (Metadata): If operator_groups is empty, autonomously derive the ON 
             clause from the primary_key=True metadata in the Entity.columns, or a matching unique not null constraint column.
           - Priority 3 (Fallback): If no identity can be determined, degrade gracefully 
             to a standard INSERT (insert_data) to ensure "upsert" is never a hard failure.

        2. Managed Entity Alignment (Audit Boilerplate):
           The implementation must recognize and automatically manage framework audit columns 
           even if they are missing from the source Arrow stream:
           - WHEN MATCHED: Update UPDATED_AT (SYSTIMESTAMP) and UPDATED_BY (current session user).
           - WHEN NOT MATCHED: Initialize both CREATED_* and UPDATED_* boilerplate.

        3. System Identity (INTERNAL_ID):
           - Support for system-generated primary keys. If the target entity defines a 
             managed PK (e.g. via identity column), the MERGE must allow Oracle to 
             generate the value during the INSERT branch.

        4. Execution:
           - Derive the DML via OracleDialect.build_merge_dml (or build_insert_dml).
           - Execute via arrow_frame.execute_many for high-performance batching.
           - Ensure transactional integrity (COMMIT on success, ROLLBACK on error).
        """
        if not catalog.entities:
            return PluginResponse.error("Catalog contains no entities.")
        try:
            target = _get_target_entity(catalog)
            sql, static_binds = build_merge_dml(catalog, target)
            input_sizes = self._input_sizes_for_entity(target)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("Oracle upsert_data failed for catalog '%s'", catalog.name)
            return PluginResponse.error(str(exc))

    def delete_data(
        self,
        catalog: Catalog,
        data: ArrowReader,
        **kwargs: Any,
    ) -> PluginResponse[None]:
        """DELETE rows from Oracle.

        With operator_groups: static DELETE executed once per entity.
        Without operator_groups: stream DELETE using entity columns as the
        WHERE binds — include only the columns you want to match on in the
        entity (e.g. just the ID column for a key-based stream delete).
        """
        if not catalog.entities:
            return PluginResponse.error("Catalog contains no entities for DELETE.")
        try:
            for entity in catalog.entities:
                sql, binds = build_delete_dml(catalog, entity)
                if binds:
                    with self.client.get_con().cursor() as cursor:
                        cursor.execute(sql, binds)
                    self.client.get_con().commit()
                else:
                    input_sizes = self._input_sizes_for_entity(entity)
                    self.arrow_frame.execute_many(sql, data, input_sizes)
            return PluginResponse.success(None)
        except Exception as exc:
            return PluginResponse.error(str(exc))

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def _column_ddl(self, column: Column) -> str:
        """Produce a single column DDL clause: NAME TYPE [NULL|NOT NULL]."""
        if column.arrow_type_id:
            ddl_type = map_arrow_to_oracle_ddl(column)
        elif column.raw_type:
            if not column.properties.get("python_type"):
                column.properties["python_type"] = map_oracle_to_python(
                    column.raw_type, column.scale
                )
            ddl_type = map_python_to_oracle_ddl(column)
        else:
            ddl_type = "VARCHAR2(255 CHAR)"
        nullable = "NULL" if column.is_nullable else "NOT NULL"
        return f"{column.name} {ddl_type} {nullable}"

    # ------------------------------------------------------------------
    # DDL: Entity (table) operations
    # ------------------------------------------------------------------

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """CREATE TABLE from the first entity in the catalog."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema(catalog)
        entity = catalog.entities[0]
        table_name = entity.name.upper()
        col_defs = [self._column_ddl(c) for c in entity.columns]
        pk_cols = entity.primary_key_columns
        constraint = ""
        if pk_cols:
            pk_names = ", ".join(c.name for c in pk_cols)
            constraint = f", CONSTRAINT {table_name}_PK PRIMARY KEY ({pk_names})"
        sql = (
            f"CREATE TABLE {schema_name}.{table_name} "
            f"({', '.join(col_defs)}{constraint})"
        )
        logger.info("DDL CREATE: %s", sql[:200])
        self.engine.execute_ddl(sql)
        return entity

    def _rebuild_copy_swap(
        self,
        catalog: Catalog,
        entity: Entity,
        existing_names: set[str],
    ) -> None:
        """Alembic-style Copy-Swap: create temp table → transfer → DROP/RENAME.

        Used when the existing table is missing columns (or has constraint
        changes) that cannot be expressed with a simple ALTER ADD COLUMN.
        """
        schema_name = self._resolve_schema(catalog)
        orig = entity.name.upper()
        tmp = f"{orig}_TMP"

        temp_entity = entity.model_copy(update={"name": tmp})
        temp_catalog = catalog.model_copy(update={"entities": [temp_entity]})
        self.create_entity(temp_catalog)

        target_cols = ", ".join(c.name for c in entity.columns)
        select_clause = build_rebuild_select(entity, existing_names)
        transfer_sql = (
            f"INSERT INTO {schema_name}.{tmp} ({target_cols}) "
            f"SELECT {select_clause} FROM {schema_name}.{orig}"
        )
        try:
            logger.info("Rebuild transfer: %s", transfer_sql[:200])
            self.engine.execute_ddl(transfer_sql)
            logger.info("Swapping %s → %s", orig, tmp)
            self.engine.execute_ddl(f"DROP TABLE {schema_name}.{orig}")
            self.engine.execute_ddl(f"RENAME {tmp} TO {orig}")
        except Exception:
            logger.exception("Copy-swap failed for %s — cleaning up temp table", orig)
            try:
                self.engine.execute_ddl(f"DROP TABLE {schema_name}.{tmp}")
            except Exception:
                pass
            raise

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """CREATE TABLE if it doesn't exist; rebuild via Copy-Swap if columns are missing."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema(catalog)
        entity = catalog.entities[0]
        table_name = entity.name.upper()

        existing_tables = {t.upper() for t in self._list_tables(schema_name)}
        if table_name not in existing_tables:
            logger.info("Table %s.%s does not exist — creating.", schema_name, table_name)
            return self.create_entity(catalog, **kwargs)

        existing_rows = self._fetch_columns_raw(schema_name, table_name) or []
        existing_names = {str(r["COLUMN_NAME"]).upper() for r in existing_rows}
        missing = [c for c in entity.columns if c.name.upper() not in existing_names]

        if missing:
            logger.info(
                "Table %s.%s is missing %d column(s) — triggering Copy-Swap rebuild.",
                schema_name, table_name, len(missing),
            )
            self._rebuild_copy_swap(catalog, entity, existing_names)
            logger.info("Table %s.%s rebuild complete.", schema_name, table_name)
        else:
            logger.info("Table %s.%s is already aligned — no DDL needed.", schema_name, table_name)

        return entity

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> Catalog:
        """Upsert every entity in the catalog (CREATE or Copy-Swap as needed)."""
        for entity in catalog.entities:
            self.upsert_entity(
                catalog.model_copy(update={"entities": [entity]}), **kwargs
            )
        return catalog

    # ------------------------------------------------------------------
    # DDL: Column operations
    # ------------------------------------------------------------------

    def create_column(self, catalog: Catalog, **kwargs: Any) -> Column:
        """ALTER TABLE ADD a single column."""
        if not catalog.entities or not catalog.entities[0].columns:
            raise ValueError("Catalog must specify an entity with at least one column.")
        schema_name = self._resolve_schema(catalog)
        entity = catalog.entities[0]
        col = entity.columns[0]
        table_name = (entity.name or "").upper()
        sql = f"ALTER TABLE {schema_name}.{table_name} ADD ({self._column_ddl(col)})"
        logger.info("DDL ALTER ADD: %s", sql)
        self.engine.execute_ddl(sql)
        return col
