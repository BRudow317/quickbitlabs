"""
OracleService — orchestration layer for the Oracle plugin.

Responsibilities:
  - Translate Catalog/Entity/Column objects to/from Oracle system catalogs.
  - Route read/write operations through OracleArrowFrame (Arrow-native I/O).
  - Route DDL through OracleEngine (structural mutations).
  - Delegate SQL generation to OracleDialect (pure builders, no I/O).
  - Delegate pure helpers to OracleTools (type analysis, DDL fragments, model hydration).
"""
from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

from server.plugins.PluginModels import (
    ArrowReader, Catalog, Column, Entity, Operation, OperatorGroup,
)
from server.plugins.PluginResponse import PluginResponse
from .OracleEngine import OracleEngine, OracleSchema, OracleTable
from .OracleDialect import (
    build_select,
    build_insert_dml,
    build_update_dml,
    build_merge_dml,
    build_delete_dml,
    build_rebuild_select,
    _get_target_entity,
    _q,
)
from .OracleArrowFrame import OracleArrowFrame
from .OracleClient import OracleClient
from .OracleTools import (
    AUDIT_COLS, AUDIT_COL_DDLS, AUDIT_COL_DDL_MAP,
    managed_pk_name, managed_pk_ddl,
    type_change_action,
    column_ddl, column_from_row,
    input_sizes_for_entity, empty_reader, inject_merge_audit,
)

logger = logging.getLogger(__name__)


class OracleService:
    client: OracleClient
    engine: OracleEngine
    arrow_frame: OracleArrowFrame
    _connected_user: str | None

    def __init__(self, client: OracleClient):
        self.client = client
        self._connected_user = None
        self.engine = OracleEngine(schema=self.client.oracle_user.upper(), client=client)
        self.arrow_frame = OracleArrowFrame(client)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_schema(self, catalog: Catalog) -> str:
        if catalog.name:
            return catalog.name.upper()
        if self._connected_user is None:
            self._connected_user = self.client.get_current_user().upper()
        return self._connected_user

    def _schema(self, schema_name: str) -> OracleSchema:
        return OracleSchema(client=self.client, schema_name=schema_name)

    def _table(self, schema_name: str, table_name: str) -> OracleTable:
        return OracleTable(table_name=table_name, schema=self._schema(schema_name))

    # ------------------------------------------------------------------
    # Catalog protocol
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            schema_name = self._resolve_schema(catalog)
            out = catalog.model_copy(update={"name": schema_name, "source_type": "oracle"})

            if not catalog.entities:
                entities: list[Entity] = []
                for table_name in self._schema(schema_name).list_table_names():
                    sub = out.model_copy(update={"entities": [Entity(name=table_name)]})
                    resp = self.get_entity(sub, **kwargs)
                    if resp.ok and resp.data:
                        entities.append(resp.data)
                    else:
                        logger.warning("Failed to hydrate '%s': %s", table_name, resp.message)
                return PluginResponse.success(out.model_copy(update={"entities": entities}))

            hydrated: list[Entity] = []
            for entity in catalog.entities:
                requested_cols = (
                    {c.name.upper() for c in entity.columns} if entity.columns else None
                )
                sub = out.model_copy(update={"entities": [entity]})
                resp = self.get_entity(sub, **kwargs)
                if not resp.ok or resp.data is None:
                    logger.warning(
                        "Entity '%s' not found in '%s' — skipping (Silo Rule).",
                        entity.name, schema_name,
                    )
                    continue
                deep = resp.data
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
        # TODO: Oracle schemas are pre-provisioned (CREATE USER + GRANT). Not implementable
        # within the plugin contract without DBA privileges.
        return PluginResponse.not_implemented("create_catalog is not implemented for Oracle.")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        """Update column types on entities that already exist — skips entities not yet created."""
        try:
            schema_name = self._resolve_schema(catalog)
            existing = {t.upper() for t in self._schema(schema_name).list_table_names()}
            updated: list[Entity] = []
            for entity in catalog.entities:
                table_name = entity.name.upper()
                if table_name not in existing:
                    logger.info(
                        "update_catalog: '%s' not in '%s' — skipping.",
                        table_name, schema_name,
                    )
                    continue
                resp = self.update_entity(
                    catalog.model_copy(update={"entities": [entity]}), **kwargs
                )
                if not resp.ok:
                    return PluginResponse.error(
                        f"update_catalog failed on entity '{entity.name}': {resp.message}"
                    )
                if resp.data:
                    updated.append(resp.data)
            return PluginResponse.success(catalog.model_copy(update={"entities": updated}))
        except Exception as exc:
            logger.exception("update_catalog failed")
            return PluginResponse.error(str(exc))

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        # TODO: Catalog-level rollback strategy (partial failure leaves N-1 entities DDL'd).
        # TODO: Multi-system name collision resolution — merge column type conflicts with
        #       promote_arrow_types() before calling upsert_entity.
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
        try:
            schema_name = self._resolve_schema(catalog)
            existing = {t.upper() for t in self._schema(schema_name).list_table_names()}
            for entity in catalog.entities:
                table_name = entity.name.upper()
                if table_name not in existing:
                    logger.warning("delete_catalog: '%s' not in '%s' — skipping.", table_name, schema_name)
                    continue
                logger.info("DROP TABLE %s.%s", schema_name, table_name)
                self.engine.execute_ddl(f"DROP TABLE {_q(schema_name)}.{_q(table_name)}")
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("delete_catalog failed")
            return PluginResponse.error(str(exc))

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

            if table_name not in {t.upper() for t in self._schema(schema_name).list_table_names()}:
                return PluginResponse.not_found(
                    f"Table '{table_name}' not found in schema '{schema_name}'."
                )

            tbl = self._table(schema_name, table_name)
            rows = tbl._fetch_tab_columns or []
            if not rows:
                return PluginResponse.not_found(
                    f"No columns found for '{schema_name}.{table_name}'."
                )
            pk_set = tbl.fetch_primary_keys()
            unique_set = tbl.fetch_unique_columns()
            fk_map = tbl.fetch_foreign_keys()
            columns = [column_from_row(schema_name, table_name, r, pk_set, unique_set, fk_map) for r in rows]
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

            pk_name = managed_pk_name(table_name)
            managed = AUDIT_COLS | {pk_name}
            source_cols = [c for c in entity.columns if c.name.upper() not in managed]

            col_defs = (
                [managed_pk_ddl(table_name)]
                + [column_ddl(c) for c in source_cols]
                + list(AUDIT_COL_DDLS)
            )
            sql = f"CREATE TABLE {_q(schema_name)}.{_q(table_name)} ({', '.join(col_defs)})"
            logger.info("DDL CREATE: %s", sql[:400])
            self.engine.execute_ddl(sql)
            sub = catalog.model_copy(update={"name": schema_name, "entities": [Entity(name=table_name)]})
            resp = self.get_entity(sub)
            if resp.ok:
                return resp
            logger.warning("Post-DDL hydration failed for '%s.%s'.", schema_name, table_name)
            return PluginResponse.success(entity)
        except Exception as exc:
            logger.exception("create_entity failed")
            return PluginResponse.error(str(exc))

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        """Modify types of existing columns only — does not create missing columns or new tables."""
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            table_name = entity.name.upper()

            if table_name not in {t.upper() for t in self._schema(schema_name).list_table_names()}:
                return PluginResponse.not_found(
                    f"Table '{table_name}' not found in schema '{schema_name}'. "
                    "Use upsert_entity to create it."
                )

            tbl = self._table(schema_name, table_name)
            existing_rows = tbl._fetch_tab_columns or []
            existing_names = {str(r["COLUMN_NAME"]).upper() for r in existing_rows}
            existing_row_map = {str(r["COLUMN_NAME"]).upper(): r for r in existing_rows}

            pk_name = managed_pk_name(table_name)
            managed = AUDIT_COLS | {pk_name}
            source_cols = [c for c in entity.columns if c.name.upper() not in managed]

            modify_cols: list[tuple[Column, str]] = []
            rebuild_needed = False
            for c in source_cols:
                col_upper = c.name.upper()
                if col_upper not in existing_names:
                    continue  # update_entity only touches existing columns
                action, new_ddl = type_change_action(existing_row_map[col_upper], c)
                if action == "modify":
                    modify_cols.append((c, new_ddl))  # type: ignore[arg-type]
                elif action == "rebuild":
                    rebuild_needed = True
                    break

            if rebuild_needed:
                logger.info("Type collision in %s.%s — triggering Copy-Swap.", schema_name, table_name)
                self._rebuild_copy_swap(
                    catalog.model_copy(update={"name": schema_name}),
                    entity.model_copy(update={"columns": source_cols}),
                    existing_names,
                )
            elif modify_cols:
                for c, new_ddl in modify_cols:
                    sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} MODIFY ({c.name} {new_ddl})"
                    logger.info("DDL ALTER MODIFY: %s", sql)
                    self.engine.execute_ddl(sql)
            else:
                logger.info("Table %s.%s column types already aligned — no DDL needed.", schema_name, table_name)

            sub = catalog.model_copy(update={"name": schema_name, "entities": [Entity(name=table_name)]})
            resp = self.get_entity(sub)
            if resp.ok:
                return resp
            logger.warning("Post-DDL hydration failed for '%s.%s'.", schema_name, table_name)
            return PluginResponse.success(entity)
        except Exception as exc:
            logger.exception("update_entity failed")
            return PluginResponse.error(str(exc))

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            table_name = entity.name.upper()

            if table_name not in {t.upper() for t in self._schema(schema_name).list_table_names()}:
                logger.info("Table %s.%s does not exist — creating.", schema_name, table_name)
                return self.create_entity(catalog, **kwargs)

            tbl = self._table(schema_name, table_name)
            existing_rows = tbl._fetch_tab_columns or []
            existing_names = {str(r["COLUMN_NAME"]).upper() for r in existing_rows}
            existing_row_map = {str(r["COLUMN_NAME"]).upper(): r for r in existing_rows}

            pk_name = managed_pk_name(table_name)
            if pk_name not in existing_names:
                logger.info("Table %s.%s missing managed PK — adding %s.", schema_name, table_name, pk_name)
                self.engine.execute_ddl(
                    f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} ADD "
                    f"({pk_name} NUMBER GENERATED BY DEFAULT AS IDENTITY)"
                )
                self.engine.execute_ddl(
                    f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} ADD "
                    f"CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_name})"
                )
                existing_names.add(pk_name)

            missing_audit = [
                AUDIT_COL_DDL_MAP[col]
                for col in ("CREATED_DATE", "CREATED_BY", "UPDATED_DATE", "UPDATED_BY")
                if col not in existing_names
            ]
            if missing_audit:
                logger.info("Table %s.%s missing %d audit col(s) — adding.", schema_name, table_name, len(missing_audit))
                self.engine.execute_ddl(
                    f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} ADD ({', '.join(missing_audit)})"
                )
                existing_names.update(AUDIT_COLS)

            managed = AUDIT_COLS | {pk_name}
            source_cols = [c for c in entity.columns if c.name.upper() not in managed]
            missing = [c for c in source_cols if c.name.upper() not in existing_names]

            modify_cols: list[tuple[Column, str]] = []
            rebuild_needed = False
            for c in source_cols:
                col_upper = c.name.upper()
                if col_upper not in existing_names:
                    continue
                action, new_ddl = type_change_action(existing_row_map[col_upper], c)
                if action == "modify":
                    modify_cols.append((c, new_ddl))  # type: ignore[arg-type]
                elif action == "rebuild":
                    rebuild_needed = True
                    break

            if rebuild_needed:
                logger.info("Type collision in %s.%s — triggering Copy-Swap.", schema_name, table_name)
                self._rebuild_copy_swap(
                    catalog.model_copy(update={"name": schema_name}),
                    entity.model_copy(update={"columns": source_cols}),
                    existing_names,
                )
            else:
                for c, new_ddl in modify_cols:
                    sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} MODIFY ({c.name} {new_ddl})"
                    logger.info("DDL ALTER MODIFY: %s", sql)
                    self.engine.execute_ddl(sql)
                if missing:
                    col_defs = ", ".join(column_ddl(c) for c in missing)
                    sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} ADD ({col_defs})"
                    logger.info("DDL ALTER ADD: %s", sql[:400])
                    self.engine.execute_ddl(sql)
                elif not modify_cols:
                    logger.info("Table %s.%s already aligned — no DDL needed.", schema_name, table_name)

            sub = catalog.model_copy(update={"name": schema_name, "entities": [Entity(name=table_name)]})
            resp = self.get_entity(sub)
            if resp.ok:
                return resp
            logger.warning("Post-DDL hydration failed for '%s.%s'.", schema_name, table_name)
            return PluginResponse.success(entity)
        except Exception as exc:
            logger.exception("upsert_entity failed")
            return PluginResponse.error(str(exc))

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            table_name = entity.name.upper()
            if table_name not in {t.upper() for t in self._schema(schema_name).list_table_names()}:
                return PluginResponse.not_found(
                    f"Table '{table_name}' not found in schema '{schema_name}'."
                )
            logger.info("DROP TABLE %s.%s", schema_name, table_name)
            self.engine.execute_ddl(f"DROP TABLE {_q(schema_name)}.{_q(table_name)}")
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("delete_entity failed")
            return PluginResponse.error(str(exc))

    # ------------------------------------------------------------------
    # Column protocol
    # ------------------------------------------------------------------

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.not_found("Catalog must specify an entity with at least one column.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            tbl = self._table(schema_name, table_name)
            rows = tbl._fetch_tab_columns or []
            col_rows = [r for r in rows if str(r.get("COLUMN_NAME", "")).upper() == col.name.upper()]
            if not col_rows:
                return PluginResponse.not_found(
                    f"Column '{col.name}' not found on {schema_name}.{table_name}."
                )
            return PluginResponse.success(
                column_from_row(
                    schema_name, table_name, col_rows[0],
                    tbl.fetch_primary_keys(), tbl.fetch_unique_columns(), tbl.fetch_foreign_keys(),
                )
            )
        except Exception as exc:
            logger.exception("get_column failed")
            return PluginResponse.error(str(exc))

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error("Catalog must specify an entity with at least one column.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            col_upper = col.name.upper()
            pk_name = managed_pk_name(table_name)
            if col_upper in AUDIT_COLS or col_upper == pk_name:
                return PluginResponse.error(
                    f"Column '{col.name}' is plugin-managed and cannot be created manually."
                )
            sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} ADD ({column_ddl(col)})"
            logger.info("DDL ALTER ADD: %s", sql)
            self.engine.execute_ddl(sql)
            return self.get_column(catalog)
        except Exception as exc:
            logger.exception("create_column failed")
            return PluginResponse.error(str(exc))

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        """Modify a single column's type. Widens VARCHAR2 in-place; triggers Copy-Swap for
        cross-family type changes. Blocks managed columns."""
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error("Catalog must specify an entity with at least one column.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            col_upper = col.name.upper()
            pk_name = managed_pk_name(table_name)
            if col_upper in AUDIT_COLS or col_upper == pk_name:
                return PluginResponse.error(
                    f"Column '{col.name}' is plugin-managed and cannot be modified."
                )
            tbl = self._table(schema_name, table_name)
            rows = tbl._fetch_tab_columns or []
            row_map = {str(r["COLUMN_NAME"]).upper(): r for r in rows}
            if col_upper not in row_map:
                return PluginResponse.not_found(
                    f"Column '{col.name}' not found on {schema_name}.{table_name}."
                )
            action, new_ddl = type_change_action(row_map[col_upper], col)
            if action == "none":
                logger.info("Column %s.%s.%s already aligned — no DDL needed.", schema_name, table_name, col.name)
            elif action == "modify":
                sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} MODIFY ({col.name} {new_ddl})"
                logger.info("DDL ALTER MODIFY: %s", sql)
                self.engine.execute_ddl(sql)
            else:  # rebuild — cross-family type change
                existing_names = {r.upper() for r in row_map}
                sub = catalog.model_copy(update={"name": schema_name, "entities": [Entity(name=table_name)]})
                entity_resp = self.get_entity(sub)
                if not entity_resp.ok or not entity_resp.data:
                    return PluginResponse.error(
                        f"update_column: could not hydrate '{table_name}' for Copy-Swap."
                    )
                updated_cols = [
                    col if c.name.upper() == col_upper else c
                    for c in entity_resp.data.columns
                ]
                source_cols = [
                    c for c in updated_cols
                    if c.name.upper() not in (AUDIT_COLS | {pk_name})
                ]
                logger.info("Type collision on %s.%s.%s — triggering Copy-Swap.", schema_name, table_name, col.name)
                self._rebuild_copy_swap(
                    catalog.model_copy(update={"name": schema_name}),
                    entity_resp.data.model_copy(update={"columns": source_cols}),
                    existing_names,
                )
            return self.get_column(catalog)
        except Exception as exc:
            logger.exception("update_column failed")
            return PluginResponse.error(str(exc))

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        """Add the column if it doesn't exist; modify its type if it does. Blocks managed columns."""
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error("Catalog must specify an entity with at least one column.")
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            col_upper = col.name.upper()
            pk_name = managed_pk_name(table_name)
            if col_upper in AUDIT_COLS or col_upper == pk_name:
                return PluginResponse.error(
                    f"Column '{col.name}' is plugin-managed and cannot be modified."
                )
            tbl = self._table(schema_name, table_name)
            rows = tbl._fetch_tab_columns or []
            existing_names = {str(r["COLUMN_NAME"]).upper() for r in rows}
            if col_upper not in existing_names:
                return self.create_column(catalog, **kwargs)
            return self.update_column(catalog, **kwargs)
        except Exception as exc:
            logger.exception("upsert_column failed")
            return PluginResponse.error(str(exc))

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        """Drop a column. Requires confirm_drop=True in catalog.properties. Blocks managed columns."""
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error("Catalog must specify an entity with at least one column.")
            if not catalog.properties.get("confirm_drop"):
                return PluginResponse.error(
                    "delete_column requires confirm_drop=True in catalog.properties."
                )
            schema_name = self._resolve_schema(catalog)
            entity = catalog.entities[0]
            col = entity.columns[0]
            table_name = (entity.name or "").upper()
            col_upper = col.name.upper()
            pk_name = managed_pk_name(table_name)
            if col_upper in AUDIT_COLS or col_upper == pk_name:
                return PluginResponse.error(
                    f"Column '{col.name}' is plugin-managed and cannot be dropped."
                )
            sql = f"ALTER TABLE {_q(schema_name)}.{_q(table_name)} DROP COLUMN {_q(col_upper)}"
            logger.info("DDL ALTER DROP COLUMN: %s", sql)
            self.engine.execute_ddl(sql)
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("delete_column failed")
            return PluginResponse.error(str(exc))

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
            pk_name = managed_pk_name(entity.name)
            managed = AUDIT_COLS | {pk_name}
            if any(c.name.upper() in managed for c in entity.columns):
                entity = entity.model_copy(update={
                    "columns": [c for c in entity.columns if c.name.upper() not in managed]
                })
            sql, static_binds = build_insert_dml(catalog, entity)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes_for_entity(entity))
            return PluginResponse.success(empty_reader(entity))
        except Exception as exc:
            logger.exception("create_data failed")
            return PluginResponse.error(str(exc))

    def update_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        # TODO: Strip managed columns from SET clause; inject SYSTIMESTAMP/USER for audit cols.
        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for UPDATE.")
        try:
            target = _get_target_entity(catalog)
            sql, static_binds = build_update_dml(catalog, target)
            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes_for_entity(target))
            return PluginResponse.success(empty_reader(target))
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
            pk_name = managed_pk_name(target.name)
            managed = AUDIT_COLS | {pk_name}
            if {c.name.upper() for c in target.columns} & managed:
                target = target.model_copy(update={
                    "columns": [c for c in target.columns if c.name.upper() not in managed]
                })

            clean_catalog = catalog.model_copy(update={"entities": [
                target if e.name == target.name else e for e in catalog.entities
            ]})

            if not clean_catalog.filters:
                pk_cols = target.primary_key_columns
                if not pk_cols:
                    logger.warning(
                        "upsert_data: no match condition or PKs for '%s' — falling back to create_data.",
                        target.name,
                    )
                    return self.create_data(catalog, data, **kwargs)
                clean_catalog = clean_catalog.model_copy(update={
                    "filters": [
                        OperatorGroup(
                            condition="AND",
                            operation_group=[
                                Operation(independent=pk, operator="=", dependent=pa.field(pk.name))
                                for pk in pk_cols
                            ],
                        )
                    ]
                })

            sql, static_binds = build_merge_dml(clean_catalog, target)
            sql = inject_merge_audit(sql)

            self.arrow_frame.execute_many([(sql, static_binds)], data, input_sizes_for_entity(target))
            return PluginResponse.success(empty_reader(target))
        except Exception as exc:
            logger.exception("upsert_data failed for catalog '%s'", catalog.name)
            return PluginResponse.error(str(exc))

    def delete_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[None]:
        # TODO: Document the stream vs predicate contract (supply just the PK column for
        #       WHERE {PK} = :value semantics; all columns = full composite match).
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
                    self.arrow_frame.execute_many(sql, data, input_sizes_for_entity(entity))
            return PluginResponse.success(None)
        except Exception as exc:
            logger.exception("delete_data failed")
            return PluginResponse.error(str(exc))

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def _rebuild_copy_swap(
        self,
        catalog: Catalog,
        entity: Entity,
        existing_names: set[str],
    ) -> None:
        """Copy-Swap: build new table → transfer data → archive original → promote new.

        Used when ALTER TABLE MODIFY is insufficient (e.g. VARCHAR2 → CLOB, NUMBER → VARCHAR2).
        The source table is renamed to {TABLE}_OLD — never dropped. Verify data, then DROP manually.
        """
        # TODO: Oracle RENAME may not accept quoted identifiers in all versions.
        #       Safe alternative: ALTER TABLE {schema}.{TMP} RENAME TO {ORIG} (unquoted new name).
        # TODO: Re-create secondary indexes and grants post-rename.
        schema_name = self._resolve_schema(catalog)
        orig = entity.name.upper()
        tmp = f"{orig}_TMP"
        old = f"{orig}_OLD"

        orig_pk_name = managed_pk_name(orig)
        managed = AUDIT_COLS | {orig_pk_name}
        source_cols = [c for c in entity.columns if c.name.upper() not in managed]
        temp_col_defs = (
            [managed_pk_ddl(orig)]
            + [column_ddl(c) for c in source_cols]
            + list(AUDIT_COL_DDLS)
        )
        temp_create_sql = (
            f"CREATE TABLE {_q(schema_name)}.{_q(tmp)} "
            f"({', '.join(temp_col_defs)})"
        )
        logger.info("Rebuild CREATE temp: %s", temp_create_sql[:400])
        self.engine.execute_ddl(temp_create_sql)

        transfer_cols = [c for c in entity.columns if c.name.upper() not in managed]
        target_col_list = ", ".join(c.name for c in transfer_cols)
        select_clause = build_rebuild_select(
            entity.model_copy(update={"columns": transfer_cols}), existing_names
        )
        transfer_sql = (
            f"INSERT INTO {_q(schema_name)}.{_q(tmp)} ({target_col_list}) "
            f"SELECT {select_clause} FROM {_q(schema_name)}.{_q(orig)}"
        )

        try:
            logger.info("Rebuild transfer: %s", transfer_sql)
            self.engine.execute_ddl(transfer_sql)
            logger.info("Archiving %s → %s, promoting %s → %s", orig, old, tmp, orig)
            self.engine.execute_ddl(f"RENAME {_q(orig)} TO {_q(old)}")
            self.engine.execute_ddl(f"RENAME {_q(tmp)} TO {_q(orig)}")
            logger.info(
                "Swap complete. Original archived as '%s.%s' — verify, then DROP manually.",
                schema_name, old,
            )
        except Exception:
            logger.exception("Copy-swap failed for %s — temp table left for inspection", orig)
            raise
