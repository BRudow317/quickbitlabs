"""
Full migration service: Source -> Target with schema mapping, DDL, and data transfer.

python ./scripts/boot.py -v -l ./.logs --env homelab --config ../.secrets/.env --exec ./main.py
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from server.plugins.PluginRegistry import get_plugin
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import ArrowStream, Catalog, Entity, Column
from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


def _rename_stream(stream: ArrowStream, name_map: dict[str, str]) -> ArrowStream:
    """Rename columns in an ArrowStream according to a name mapping.
    Keys not in the map are kept as-is."""
    schema = stream.schema
    if not schema:
        return stream
    new_names = [name_map.get(f.name, f.name) for f in schema]
    new_fields = [
        pa.field(new_names[i], schema.field(i).type, schema.field(i).nullable)
        for i in range(len(schema))
    ]
    new_schema = pa.schema(new_fields)

    def _batches() -> Iterator[pa.RecordBatch]:
        for batch in stream:
            yield batch.rename_columns(new_names)

    return pa.RecordBatchReader.from_batches(new_schema, _batches())


class FullMigration:
    """Orchestrates a full schema + data migration between two Plugin implementations.

    Flow:
        1. discover()      — introspect source and target schemas
        2. prepare()       — map source schema to target naming/types, execute DDL
        3. migrate_data()  — extract per entity, rename columns, MERGE into target
    """
    source: Plugin
    target: Plugin
    source_catalog: Catalog
    target_catalog: Catalog

    def __init__(
        self,
        source_plugin: str,
        target_plugin: str,
        source_catalog_name: str | None = None,
        target_catalog_name: str | None = None,
        entities: list[str] | None = None,
        source_kwargs: dict[str, Any] | None = None,
        target_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.source = get_plugin(source_plugin, **(source_kwargs or {}))
        self.target = get_plugin(target_plugin, **(target_kwargs or {}))
        self.source_catalog = Catalog(name=source_catalog_name)
        self.target_catalog = Catalog(name=target_catalog_name)
        self.entity_filter: set[str] | None = {e.upper() for e in entities} if entities else None
        self.migration_catalog: Catalog | None = None
        self._column_maps: dict[str, dict[str, str]] = {}  # src_entity_name -> {stream_col: target_col}

    # ------------------------------------------------------------------
    # Step 1: Discovery
    # ------------------------------------------------------------------

    def discover(self) -> None:
        """Discover source schema (entity list + columns) and target schema (what already exists)."""
        logger.info("Step 1: Discovering source schema...")

        # First call: entity list (no columns)
        resp = self.source.get_catalog(catalog=self.source_catalog)
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Source discovery failed [{resp.code}]: {resp.message}")
        self.source_catalog = resp.data

        # Filter to requested entities
        if self.entity_filter:
            self.source_catalog.entities = [
                e for e in self.source_catalog.entities
                if (e.name or "").upper() in self.entity_filter
            ]

        logger.info(f"  Found {len(self.source_catalog.entities)} source entities.")

        # Second call: populate columns per entity (triggers per-entity describe)
        if self.source_catalog.entities and not self.source_catalog.entities[0].columns:
            resp = self.source.get_catalog(catalog=self.source_catalog)
            if not resp.ok or not resp.data:
                raise RuntimeError(f"Source column discovery failed [{resp.code}]: {resp.message}")
            self.source_catalog = resp.data

        # Discover existing target schema
        logger.info("  Discovering target schema...")
        resp = self.target.get_catalog(catalog=self.target_catalog)
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Target discovery failed [{resp.code}]: {resp.message}")
        self.target_catalog = resp.data
        logger.info(f"  Found {len(self.target_catalog.entities)} existing target entities.")

    # ------------------------------------------------------------------
    # Step 2: Schema Mapping + DDL
    # ------------------------------------------------------------------

    def prepare(self) -> Catalog:
        """Map source schema to target naming conventions and execute DDL to align target."""
        from server.plugins.oracle.OracleTools import to_oracle_snake

        logger.info("Step 2: Preparing target schema...")
        migration_entities: list[Entity] = []

        for src_entity in self.source_catalog.entities:
            target_table = to_oracle_snake(src_entity.name)
            name_map: dict[str, str] = {}
            target_columns: list[Column] = []

            for col in src_entity.columns:
                if col.arrow_type_id is None:
                    continue  # skip unmappable columns (compound types)

                oracle_col = to_oracle_snake(col.name)
                # get_data ArrowStream names columns as "EntityName_ColumnName"
                stream_col = f"{src_entity.name}_{col.name}"
                name_map[stream_col] = oracle_col

                target_columns.append(col.model_copy(update={
                    "name": oracle_col,
                    "qualified_name": f"{target_table}.{oracle_col}",
                }))

            migration_entities.append(Entity(
                name=target_table,
                qualified_name=f"{self.target_catalog.name}.{target_table}" if self.target_catalog.name else target_table,
                columns=target_columns,
            ))
            self._column_maps[src_entity.name] = name_map
            logger.info(f"  Mapped {src_entity.name} -> {target_table} ({len(target_columns)} columns)")

        self.migration_catalog = Catalog(
            name=self.target_catalog.name,
            qualified_name=self.target_catalog.qualified_name,
            entities=migration_entities,
        )

        # Execute DDL
        logger.info(f"  Upserting {len(migration_entities)} entities on target...")
        resp = self.target.upsert_catalog(self.migration_catalog)
        if not resp.ok:
            raise RuntimeError(f"Target DDL failed [{resp.code}]: {resp.message}")

        logger.info("  Target schema prepared.")
        return self.migration_catalog

    # ------------------------------------------------------------------
    # Step 3: Data Migration
    # ------------------------------------------------------------------

    def migrate_data(self) -> list[dict[str, Any]]:
        """Extract from source entity by entity, rename columns, MERGE into target."""
        if not self.migration_catalog:
            raise RuntimeError("Call prepare() before migrate_data().")

        logger.info("Step 3: Migrating data...")
        results: list[dict[str, Any]] = []

        for i, src_entity in enumerate(self.source_catalog.entities):
            target_entity = self.migration_catalog.entities[i]
            name = src_entity.name
            logger.info(f"  {name} -> {target_entity.name}...")

            try:
                # Extract from source
                src_sub = self.source_catalog.model_copy(update={"entities": [src_entity]})
                resp = self.source.get_data(src_sub)
                if not resp.ok or not resp.data:
                    msg = f"get_data failed for {name}: [{resp.code}] {resp.message}"
                    logger.error(f"    {msg}")
                    results.append({"entity": name, "status": "error", "message": msg})
                    continue

                stream: ArrowStream = resp.data

                # Rename stream columns: "Account_Id" -> "ID"
                name_map = self._column_maps.get(name, {})
                if name_map:
                    stream = _rename_stream(stream, name_map)

                # Load into target
                target_sub = self.migration_catalog.model_copy(update={"entities": [target_entity]})
                resp = self.target.upsert_data(target_sub, stream)
                if not resp.ok:
                    msg = f"upsert_data failed for {target_entity.name}: [{resp.code}] {resp.message}"
                    logger.error(f"    {msg}")
                    results.append({"entity": name, "status": "error", "message": msg})
                else:
                    logger.info(f"    {name} -> {target_entity.name} complete.")
                    results.append({"entity": name, "target": target_entity.name, "status": "ok"})

            except Exception as e:
                logger.exception(f"    Migration failed for {name}")
                results.append({"entity": name, "status": "exception", "message": str(e)})

        return results

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def run_all(self) -> list[dict[str, Any]]:
        """Run discover -> prepare -> migrate_data in sequence."""
        self.discover()
        self.prepare()
        return self.migrate_data()


def run() -> int:
    """Entry point for the bootloader."""
    migration = FullMigration(
        source_plugin="Salesforce",
        target_plugin="Oracle",
        source_catalog_name="homelab",
        target_catalog_name="brudow",
        # Specify entities or set to None for all migratable entities
        entities=["Account", "Contact", "Opportunity", "Lead"],
    )
    try:
        results = migration.run_all()
        ok = sum(1 for r in results if r.get("status") == "ok")
        fail = len(results) - ok
        for r in results:
            status = r.get("status", "unknown")
            entity = r.get("entity", "?")
            if status == "ok":
                logger.info(f"  OK: {entity} -> {r.get('target')}")
            else:
                logger.error(f"  FAIL: {entity} -- {r.get('message', '')}")
        logger.info(f"Migration complete: {ok} succeeded, {fail} failed.")
        return 0 if fail == 0 else 1
    except Exception as e:
        logger.exception(f"Migration aborted: {e}")
        return 1
