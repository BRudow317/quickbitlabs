"""
CatalogMigration service: Source -> Target with schema mapping, DDL, and data transfer.

python Q:/scripts/boot.py -v -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./main.py
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from server.plugins.PluginRegistry import get_plugin, PLUGIN
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import ArrowReader, Catalog, Entity, Column, Locator
from server.plugins.PluginResponse import PluginResponse


import logging
logger = logging.getLogger(__name__)

class CatalogMigration:
    """Orchestrates a full schema + data migration between two Plugin implementations.
    Input:
    ---
        - source_plugin: PLUGIN name for the source system (e.g. "salesforce")
        - target_plugin: PLUGIN name for the target system (e.g. "oracle")
        - source_catalog: Optional Catalog to scope the discovery on the source side. If not provided, the source plugin will be queried with an empty Catalog (i.e. full discovery).
        - target_catalog: Optional Catalog to scope the DDL and data migration on the target side. If not provided, the target plugin will be queried with an empty Catalog (i.e. no pre-existing schema knowledge, full DDL and data migration).
        - source_kwargs: Optional dict of additional parameters to pass to the source plugin. This can include things like parallelism settings, filters to scope the discovery or migration, or any other plugin-specific parameters.
        - target_kwargs: Optional dict of additional parameters to pass to the target plugin. This can include things like parallelism settings, conflict resolution strategies, or any other plugin-specific parameters.
    Output:
    ---
        - A report of the migration results, including any errors encountered during discovery, DDL, or data migration, and a summary of the entities and records migrated.
    """
    source: Plugin
    target: Plugin
    source_catalog: Catalog
    target_catalog: Catalog
    source_kwargs: dict[str, Any]  # for extensibility (e.g. parallelism settings, conflict resolution strategy, etc.)
    target_kwargs: dict[str, Any]  # for extensibility (e.g. parallelism settings, conflict resolution strategy, etc.)

    def __init__(
        self,
        source_plugin: PLUGIN,
        target_plugin: PLUGIN,
        source_catalog: Catalog | None = None,
        target_catalog: Catalog | None = None,
        source_kwargs: dict[str, Any] | None = None,
        target_kwargs: dict[str, Any] | None = None
    ):
        self.source = get_plugin(source_plugin, **(source_kwargs or {}))
        self.target = get_plugin(target_plugin, **(target_kwargs or {}))
        self.source_catalog = source_catalog or Catalog(
            properties=source_kwargs or {}
        )
        self.target_catalog = target_catalog or Catalog(
            properties=target_kwargs or {}
        )
        self.source_kwargs = source_kwargs or {}
        self.target_kwargs = target_kwargs or {}
    # ------------------------------------------------------------------
    # Step 1: Discovery
    # ------------------------------------------------------------------

    def get_catalog(self) -> None:
        """Discover source schema (entity list + columns) and target schema (what already exists)."""
        logger.info("Step 1: Schema Discovery...")

        # First call: entity list (no columns)
        logger.debug("  Discovering source schema...")
        resp = self.source.get_catalog(catalog=self.source_catalog)
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Source discovery failed [{resp.code}]: {resp.message}")
        self.source_catalog = resp.data

    # ------------------------------------------------------------------
    # Step 2: Schema Mapping + DDL
    # ------------------------------------------------------------------

    def upsert_catalog(self) -> Catalog:
        """Map source schema to target and execute DDL to align target."""
        logger.info("Step 2: Target schema checks and DDL...")
        ddl_catalog: Catalog = self.source_catalog.model_copy(update={"entities": []})

        for src_entity in self.source_catalog.entities:
            target_entity: Entity = src_entity.model_copy(update={"columns": []})
            for col in src_entity.columns:
                if col.arrow_type_id is None:
                    continue  # skip unmappable columns (salesforce compound types for example)
                else: 
                    target_entity.columns.append(col)
            ddl_catalog.entities.append(target_entity)

        self.target_catalog = ddl_catalog

        # Execute DDL
        # Any name collisions are handled target side. The target plugin can rename, or update the existing schema to fit the new data, or reject if the resolution is not handled. 
        resp = self.target.upsert_catalog(self.target_catalog)
        if not resp.ok:
            raise RuntimeError(f"Target DDL failed [{resp.code}]: {resp.message}")
        return self.target_catalog

    # ------------------------------------------------------------------
    # Step 3: Data Migration
    # ------------------------------------------------------------------

    def upsert_data(self) -> list[dict[str, Any]]:
        """Extract from source entity by entity, rename columns, MERGE into target."""
        if not self.target_catalog.entities:
            raise RuntimeError("No Target Entities Detected. Call prepare() before migrate_data().")
        results: list[dict[str, Any]] = []
        
        # Create a single entity loop to avoid a cartesian product.
        for entity in self.target_catalog.entities:
            # Load into target
            loader_catalog = Catalog(
                name=self.source_catalog.name, 
                entities=[entity],
                properties=self.source_catalog.properties
            )

            source_resp = self.source.get_data(loader_catalog)
            if not source_resp.ok:
                msg = f"Data extraction failed [{source_resp.code}]: {source_resp.message}"
                logger.error(msg)
                results.append({"entity": entity.name, "status": "error", "message": msg})
                continue
            
            upsert_resp = self.target.upsert_data(loader_catalog, source_resp.data)
            
            if not upsert_resp.ok:
                msg = f"upsert_data failed for {entity.name}: [{upsert_resp.code}] {upsert_resp.message}"
                logger.error(f"    {msg}")
                results.append({"entity": entity.name, "status": "error", "message": msg})
            else:
                logger.info(f"    {entity.name} -> {entity.name} complete.")
                results.append({"entity": entity.name, "target": entity.name, "status": "ok"})

        return results

def run_job() -> None:
    """Entry point for the bootloader."""
    catalog_migration = CatalogMigration(
        source_plugin="salesforce",
        target_plugin="oracle",
    )
    catalog_migration.get_catalog()
    catalog_migration.upsert_catalog()
    catalog_migration.upsert_data()