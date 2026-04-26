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

import pyarrow as pa

from server.plugins.PluginModels import (
    ArrowReader, Catalog, Column, Entity, Locator, Operation, OperatorGroup,
)
from server.plugins.PluginResponse import PluginResponse
from server.plugins.oracle.OracleTypeMap import (
    map_arrow_to_oracle_ddl,
    map_oracle_to_arrow,
    map_oracle_to_python,
    map_python_to_oracledb_input_size,
    map_python_to_oracle_ddl,
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

_AUDIT_COLS = frozenset({"CREATED_DATE", "CREATED_BY", "UPDATED_DATE", "UPDATED_BY"})


def _empty_reader(entity: Entity) -> ArrowReader:
    fields = [
        pa.field(c.name, c.arrow_type or pa.null(), nullable=True)
        for c in entity.columns
        if c.arrow_type is not None
    ]
    schema = pa.schema(fields) if fields else pa.schema([])
    return pa.RecordBatchReader.from_batches(schema, iter([]))


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
        sizes: dict[str, Any] = {}
        for col in entity.columns:
            if col.arrow_type_id:
                sizes[col.name] = map_column_to_oracledb_input_size(col)
            elif col.raw_type:
                if not col.properties.get("python_type"):
                    col.properties["python_type"] = map_oracle_to_python(col.raw_type, col.scale)
                sizes[col.name] = map_python_to_oracledb_input_size(col)
            else:
                sizes[col.name] = None
        return sizes

    # ------------------------------------------------------------------
    # Oracle system catalog queries (raw metadata)
    # ------------------------------------------------------------------

    def _list_tables(self, schema_name: str) -> list[str]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        return schema.list_table_names()

    def _fetch_metadata(
        self, schema_name: str, table_name: str
    ) -> tuple[set[str], set[str], dict[str, dict[str, Any]]]:
        """Fetch pk_set, unique_set, fk_map for one table in a single pass."""
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        return (
            table.fetch_primary_keys(),
            table.fetch_unique_columns(),
            table.fetch_foreign_keys(),
        )

    def _fetch_columns_raw(
        self,
        schema_name: str,
        table_name: str,
        requested: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        rows: list[dict[str, Any]] = table._fetch_tab_columns or []
        if not requested:
            return rows
        upper = {c.upper() for c in requested}
        return [r for r in rows if str(r.get("COLUMN_NAME", "")).upper() in upper]

    def _column_from_row(
        self,
        schema_name: str,
        table_name: str,
        row: dict[str, Any],
        pk_set: set[str],
        unique_set: set[str],
        fk_map: dict[str, dict[str, Any]],
    ) -> Column:
        name = str(row["COLUMN_NAME"])
        raw_type = str(row["DATA_TYPE"])
        scale = row.get("DATA_SCALE")
        precision = row.get("DATA_PRECISION")
        max_length = row.get("CHAR_LENGTH")
        column_id = row.get("COLUMN_ID")
        data_default = row.get("DATA_DEFAULT")
        is_virtual = str(row.get("VIRTUAL_COLUMN") or "NO").upper() == "YES"
        is_hidden = str(row.get("HIDDEN_COLUMN") or "NO").upper() == "YES"
        is_pk = name in pk_set
        fk_info = fk_map.get(name)
        return Column(
            name=name,
            locator=Locator(plugin="oracle", namespace=schema_name, entity_name=table_name),
            raw_type=raw_type,
            arrow_type_id=map_oracle_to_arrow(raw_type, scale),
            primary_key=is_pk,
            is_unique=is_pk or name in unique_set,
            is_compound_key=is_pk and len(pk_set) > 1,
            is_nullable=str(row.get("NULLABLE", "Y")).upper() == "Y",
            is_read_only=is_virtual,
            is_computed=is_virtual,
            is_hidden=is_hidden,
            is_foreign_key=fk_info is not None,
            foreign_key_entity=fk_info["REF_TABLE"] if fk_info else None,
            foreign_key_column=fk_info["REF_COLUMN"] if fk_info else None,
            is_foreign_key_enforced=(
                str(fk_info.get("STATUS", "")).upper() == "ENABLED"
            ) if fk_info else False,
            max_length=int(max_length) if max_length is not None else None,
            precision=int(precision) if precision is not None else None,
            scale=int(scale) if scale is not None else None,
            ordinal_position=int(column_id) if column_id is not None else None,
            serialized_null_value="NULL",
            default_value=str(data_default).strip() if data_default is not None else None,
            properties={"python_type": map_oracle_to_python(raw_type, scale)},
        )

    def _inject_merge_audit(self, sql: str, present_audit: set[str]) -> str:
        """Inject SYSTIMESTAMP/USER audit expressions into a MERGE statement.

        Audit columns are excluded from the USING SELECT before build_merge_dml is called.
        This method re-inserts them as Oracle-computed expressions in both WHEN branches.
        """
        no_match_marker = "WHEN NOT MATCHED THEN INSERT"

        # Append UPDATED_* to WHEN MATCHED SET
        match_parts: list[str] = []
        if "UPDATED_DATE" in present_audit:
            match_parts.append("tgt.UPDATED_DATE = SYSTIMESTAMP")
        if "UPDATED_BY" in present_audit:
            match_parts.append("tgt.UPDATED_BY = USER")

        if match_parts and "WHEN MATCHED THEN UPDATE SET" in sql:
            idx = sql.index(no_match_marker)
            sql = sql[:idx].rstrip() + ", " + ", ".join(match_parts) + " " + sql[idx:]

        # Append audit cols to WHEN NOT MATCHED INSERT (cols) VALUES (vals)
        insert_cols: list[str] = []
        insert_vals: list[str] = []
        for col, expr in (
            ("CREATED_DATE", "SYSTIMESTAMP"),
            ("CREATED_BY", "USER"),
            ("UPDATED_DATE", "SYSTIMESTAMP"),
            ("UPDATED_BY", "USER"),
        ):
            if col in present_audit:
                insert_cols.append(col)
                insert_vals.append(expr)

        if insert_cols:
            try:
                nm_idx = sql.index(no_match_marker)
                ins_open = sql.index("(", nm_idx)
                ins_close = sql.index(")", ins_open)
                val_close = sql.rindex(")")
                extra_cols = ", ".join(insert_cols)
                extra_vals = ", ".join(insert_vals)
                sql = (
                    sql[:ins_close]
                    + ", " + extra_cols
                    + sql[ins_close:val_close]
                    + ", " + extra_vals
                    + sql[val_close:]
                )
            except ValueError:
                logger.warning("Could not inject audit columns into MERGE SQL — marker not found")

        return sql

    # ------------------------------------------------------------------
    # Catalog protocol
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            schema_name = self._resolve_schema(catalog)
            out = catalog.model_copy(update={"name": schema_name, "source_type": "oracle"})

            if not catalog.entities:
                # Full discovery: hydrate every table in the schema
                entities: list[Entity] = []
                for table_name in self._list_tables(schema_name):
                    sub = out.model_copy(update={"entities": [Entity(name=table_name)]})
                    resp = self.get_entity(sub, **kwargs)
                    if resp.ok and resp.data:
                        entities.append(resp.data)
                    else:
                        logger.warning("Failed to hydrate '%s': %s", table_name, resp.message)
                return PluginResponse.success(out.model_copy(update={"entities": entities}))

            # Targeted hydration — Silo Rule + Replacement Rule + Projection Integrity
            hydrated: list[Entity] = []
            for entity in catalog.entities:
                requested_cols = (
                    {c.name.upper() for c in entity.columns} if entity.columns else None
                )
                sub = out.model_copy(update={"entities": [entity]})
                resp = self.get_entity(sub, **kwargs)
                if not resp.ok or resp.data is None:
                    # Silo Rule: silently skip entities not owned by this plugin
                    logger.warning(
                        "Entity '%s' not found in '%s' — skipping (Silo Rule).",
                        entity.name, schema_name,
                    )
                    continue
                deep = resp.data
                # Projection Integrity: if input named specific columns, filter to only those
                if requested_cols:
                    deep = deep.model_copy(update={
                        "columns": [c for c in deep.columns if c.name.upper() in requested_cols]
                    })
                hydrated.append(deep)

            return PluginResponse.success(out.model_copy(update={"entities": hydrated}))
        except Exception as exc:
            logger.exception("get_catalog failed")
            return PluginResponse.error(str(exc))

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("create_catalog is not implemented for Oracle.")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("update_catalog is not implemented for Oracle.")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            for entity in catalog.entities:
                resp = self.upsert_entity(
                    catalog.model_copy(update={"entities": [entity]}), **kwargs
                )
                if not resp.ok:
                    return PluginResponse.error(
                        f"upsert_catalog failed on entity '{entity.name}': {resp.message}"
                    )
            return PluginResponse.success(catalog)
        except Exception as exc:
            logger.exception("upsert_catalog failed")
            return PluginResponse.error(str(exc))

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("delete_catalog is not implemented for Oracle.")

    # ------------------------------------------------------------------
    # Entity protocol
    # ------------------------------------------------------------------

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            if not catalog.entities:
                return PluginResponse.not_found("Catalog must contain at least one entity.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            table_name = (entity.name or "").upper()
            if not table_name:
                return PluginResponse.not_found("Entity name is required.")

            # Silo Rule: only return entities that exist in this schema
            existing = {t.upper() for t in self._list_tables(schema_name)}
            if table_name not in existing:
                return PluginResponse.not_found(
                    f"Table '{table_name}' not found in schema '{schema_name}'."
                )

            pk_set, unique_set, fk_map = self._fetch_metadata(schema_name, table_name)
            rows = self._fetch_columns_raw(schema_name, table_name)
            if not rows:
                return PluginResponse.not_found(
                    f"No columns found for '{schema_name}.{table_name}'."
                )
            columns = [
                self._column_from_row(schema_name, table_name, r, pk_set, unique_set, fk_map)
                for r in rows
            ]
            return PluginResponse.success(Entity(
                name=table_name,
                namespace=schema_name,
                entity_type="table",
                plugin="oracle",
                columns=columns,
            ))
        except Exception as exc:
            logger.exception("get_entity failed")
            return PluginResponse.error(str(exc))

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
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
            return PluginResponse.success(entity)
        except Exception as exc:
            logger.exception("create_entity failed")
            return PluginResponse.error(str(exc))

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("update_entity is not implemented for Oracle.")

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
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
                    "Table %s.%s missing %d column(s) — triggering Copy-Swap rebuild.",
                    schema_name, table_name, len(missing),
                )
                self._rebuild_copy_swap(catalog, entity, existing_names)
                logger.info("Table %s.%s rebuild complete.", schema_name, table_name)
            else:
                logger.info(
                    "Table %s.%s already aligned — no DDL needed.", schema_name, table_name
                )

            return PluginResponse.success(entity)
        except Exception as exc:
            logger.exception("upsert_entity failed")
            return PluginResponse.error(str(exc))

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("delete_entity is not implemented for Oracle.")

    # ------------------------------------------------------------------
    # Column protocol
    # ------------------------------------------------------------------

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.not_found(
                    "Catalog must specify an entity with at least one column."
                )
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            pk_set, unique_set, fk_map = self._fetch_metadata(schema_name, table_name)
            rows = self._fetch_columns_raw(schema_name, table_name, {col.name.upper()})
            if not rows:
                return PluginResponse.not_found(
                    f"Column '{col.name}' not found on {schema_name}.{table_name}."
                )
            return PluginResponse.success(
                self._column_from_row(schema_name, table_name, rows[0], pk_set, unique_set, fk_map)
            )
        except Exception as exc:
            logger.exception("get_column failed")
            return PluginResponse.error(str(exc))

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error(
                    "Catalog must specify an entity with at least one column."
                )
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            sql = f"ALTER TABLE {schema_name}.{table_name} ADD ({self._column_ddl(col)})"
            logger.info("DDL ALTER ADD: %s", sql)
            self.engine.execute_ddl(sql)
            return PluginResponse.success(col)
        except Exception as exc:
            logger.exception("create_column failed")
            return PluginResponse.error(str(exc))

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("update_column is not implemented for Oracle.")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("upsert_column is not implemented for Oracle.")

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("delete_column is not implemented for Oracle.")

    # ------------------------------------------------------------------
    # Data protocol
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            statement: str | None = kwargs.get("statement")
            binds: dict[str, Any] = kwargs.get("binds") or {}
            if statement:
                return PluginResponse.success(
                    self.arrow_frame.arrow_reader(statement, parameters=binds)
                )
            sql, binds = build_select(catalog)
            return PluginResponse.success(self.arrow_frame.arrow_reader(sql, parameters=binds))
        except Exception as exc:
            logger.exception("get_data failed")
            return PluginResponse.error(str(exc))

    def create_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        try:
            entity = _get_target_entity(catalog)
            sql, static_binds = build_insert_dml(catalog, entity)
            input_sizes = self._input_sizes_for_entity(entity)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)
            return PluginResponse.success(_empty_reader(entity))
        except Exception as exc:
            logger.exception("create_data failed")
            return PluginResponse.error(str(exc))

    def update_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for UPDATE.")
        try:
            target = _get_target_entity(catalog)
            sql, static_binds = build_update_dml(catalog, target)
            input_sizes = self._input_sizes_for_entity(target)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)
            return PluginResponse.success(_empty_reader(target))
        except Exception as exc:
            logger.exception("update_data failed")
            return PluginResponse.error(str(exc))

    def upsert_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        if not catalog.entities:
            return PluginResponse.error("Catalog contains no entities.")
        try:
            target = _get_target_entity(catalog)

            # Strip audit columns — Oracle computes them via SYSTIMESTAMP/USER in the MERGE.
            # They are re-injected by _inject_merge_audit after the SQL is built.
            present_audit = {c.name.upper() for c in target.columns} & _AUDIT_COLS
            if present_audit:
                target = target.model_copy(update={
                    "columns": [c for c in target.columns if c.name.upper() not in _AUDIT_COLS]
                })

            clean_catalog = catalog.model_copy(update={"entities": [
                target if e.name == target.name else e for e in catalog.entities
            ]})

            if not clean_catalog.operator_groups:
                # Priority 2: derive ON clause from PK columns
                pk_cols = target.primary_key_columns
                if not pk_cols:
                    # Priority 3: no identity available — degrade to insert
                    logger.warning(
                        "upsert_data: no match condition or PKs for '%s' — falling back to"
                        " create_data.",
                        target.name,
                    )
                    return self.create_data(catalog, data, **kwargs)
                clean_catalog = clean_catalog.model_copy(update={
                    "operator_groups": [
                        OperatorGroup(
                            condition="AND",
                            operation_group=[
                                Operation(
                                    independent=pk,
                                    operator="==",
                                    dependent=pa.field(pk.name),
                                )
                                for pk in pk_cols
                            ],
                        )
                    ]
                })

            sql, static_binds = build_merge_dml(clean_catalog, target)
            if present_audit:
                sql = self._inject_merge_audit(sql, present_audit)

            input_sizes = self._input_sizes_for_entity(target)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes)
            return PluginResponse.success(_empty_reader(target))
        except Exception as exc:
            logger.exception("upsert_data failed for catalog '%s'", catalog.name)
            return PluginResponse.error(str(exc))

    def delete_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[None]:
        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for DELETE.")
        try:
            for entity in catalog.entities:
                sql, binds = build_delete_dml(catalog, entity)
                if binds:
                    con = self.client.get_con()
                    with con.cursor() as cursor:
                        cursor.execute(sql, binds)
                    con.commit()
                else:
                    input_sizes = self._input_sizes_for_entity(entity)
                    self.arrow_frame.execute_many(sql, data, input_sizes)
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("delete_data failed")
            return PluginResponse.error(str(exc))

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def _column_ddl(self, column: Column) -> str:
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

    def _rebuild_copy_swap(
        self,
        catalog: Catalog,
        entity: Entity,
        existing_names: set[str],
    ) -> None:
        """Alembic-style Copy-Swap: create temp table → transfer data → DROP/RENAME."""
        schema_name = self._resolve_schema(catalog)
        orig = entity.name.upper()
        tmp = f"{orig}_TMP"

        temp_entity = entity.model_copy(update={"name": tmp})
        temp_catalog = catalog.model_copy(update={"entities": [temp_entity]})
        resp = self.create_entity(temp_catalog)
        if not resp.ok:
            raise RuntimeError(f"Failed to create temp table '{tmp}': {resp.message}")

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
