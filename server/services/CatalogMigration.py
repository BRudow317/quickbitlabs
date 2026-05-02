from __future__ import annotations
from typing import Any

import pyarrow as pa

from server.plugins.PluginRegistry import get_plugin, PLUGIN
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import ArrowReader, Catalog, Entity, Column, Locator, Operation, OperatorGroup

import logging
logger = logging.getLogger(__name__)


class CatalogMigration:
    """Orchestrates a full schema + data migration between two Plugin implementations.

    Input:
    ---
        - source_plugin: PLUGIN name for the source system (e.g. "salesforce")
        - target_plugin: PLUGIN name for the target system (e.g. "oracle")
        - source_catalog: Optional Catalog to scope the discovery on the source side.
          If not provided, the source plugin will be queried with an empty Catalog (full discovery).
        - target_catalog: Optional Catalog to scope the DDL and data migration on the target side.
          If not provided, the target plugin will be queried with an empty Catalog (no pre-existing
          schema knowledge, full DDL and data migration).
        - source_kwargs: Optional dict of additional parameters to pass to the source plugin.
        - target_kwargs: Optional dict of additional parameters to pass to the target plugin.

    Output:
    ---
        - A report of the migration results, including any errors encountered during discovery,
          DDL, or data migration, and a summary of the entities and records migrated.
    """
    source: Plugin
    target: Plugin
    source_plugin: PLUGIN
    target_plugin: PLUGIN
    source_catalog: Catalog
    target_catalog: Catalog
    source_kwargs: dict[str, Any]
    target_kwargs: dict[str, Any]

    def __init__(
        self,
        source_plugin: PLUGIN,
        target_plugin: PLUGIN,
        source_catalog: Catalog | None = None,
        target_catalog: Catalog | None = None,
        source_kwargs: dict[str, Any] | None = None,
        target_kwargs: dict[str, Any] | None = None,
    ):
        self.source_plugin = source_plugin
        self.target_plugin = target_plugin
        self.source = get_plugin(source_plugin, **(source_kwargs or {}))
        self.target = get_plugin(target_plugin, **(target_kwargs or {}))
        self.source_catalog = source_catalog or Catalog(properties=source_kwargs or {})
        self.target_catalog = target_catalog or Catalog(properties=target_kwargs or {})
        self.source_kwargs = source_kwargs or {}
        self.target_kwargs = target_kwargs or {}

    # ------------------------------------------------------------------
    # Step 1: Discovery
    # ------------------------------------------------------------------

    def get_catalog(self) -> None:
        """Discover source schema (entity list + columns)."""
        logger.info("Step 1: Schema Discovery...")
        resp = self.source.get_catalog(catalog=self.source_catalog)
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Source discovery failed [{resp.code}]: {resp.message}")
        self.source_catalog = resp.data

    # ------------------------------------------------------------------
    # Step 2: Schema Mapping + DDL
    # ------------------------------------------------------------------

    def upsert_catalog(self) -> Catalog:
        """Map source schema to target-native entities and execute DDL to align target.

        Each source column is retargeted to the target plugin so the target receives
        entities it owns — not Salesforce-locator columns masquerading as Oracle rows.
        The target plugin's upsert_entity handles create-or-Copy-Swap alignment.
        """
        logger.info("Step 2: Target schema checks and DDL...")
        schema_name = self.target_catalog.name
        ddl_catalog = self.target_catalog.model_copy(update={"entities": []})

        for src_entity in self.source_catalog.entities:
            table_name = src_entity.name.upper()
            target_cols = [
                col.model_copy(update={
                    "locator": Locator(
                        plugin=self.target_plugin,
                        namespace=schema_name,
                        entity_name=table_name,
                    ),
                    "raw_type": None,       # target plugin derives DDL type from arrow_type_id
                    "primary_key": False,   # target system manages its own PKs
                    "is_unique": False,     # don't impose source uniqueness constraints
                })
                for col in src_entity.columns
                if col.arrow_type_id is not None
            ]
            ddl_catalog.entities.append(Entity(
                name=table_name,
                namespace=schema_name,
                entity_type="table",
                plugin=self.target_plugin,
                columns=target_cols,
            ))

        self.target_catalog = ddl_catalog

        resp = self.target.upsert_catalog(self.target_catalog)
        if not resp.ok:
            raise RuntimeError(f"Target DDL failed [{resp.code}]: {resp.message}")
        return self.target_catalog

    # ------------------------------------------------------------------
    # Step 3: Data Migration
    # ------------------------------------------------------------------

    def upsert_data(self) -> list[dict[str, Any]]:
        """Extract from source entity by entity, upsert into target.

        Source fetch uses source-native catalog (source name + source entity with source locators)
        so the source plugin resolves columns correctly against its own system.

        Target upsert uses target catalog (target name + target entity with target locators)
        so the target plugin resolves its schema and MERGE ON clause correctly.
        """
        if not self.target_catalog.entities:
            raise RuntimeError(
                "No Target Entities Detected. Call upsert_catalog() before upsert_data()."
            )
        results: list[dict[str, Any]] = []
        src_entity_map = {e.name.upper(): e for e in self.source_catalog.entities}

        for target_entity in self.target_catalog.entities:
            src_entity = src_entity_map.get(target_entity.name.upper())
            if src_entity is None:
                msg = f"No source entity found for target '{target_entity.name}'"
                logger.error(msg)
                results.append({"entity": target_entity.name, "status": "error", "message": msg})
                continue

            # Source plugin gets its own catalog with its own entity + locators
            src_catalog = Catalog(
                name=self.source_catalog.name,
                entities=[src_entity],
                properties=self.source_catalog.properties,
            )
            source_resp = self.source.get_data(src_catalog)
            if not source_resp.ok:
                msg = f"Data extraction failed [{source_resp.code}]: {source_resp.message}"
                logger.error(msg)
                results.append({"entity": target_entity.name, "status": "error", "message": msg})
                continue

            # Build MERGE ON clause from source PKs — source identity drives matching,
            # but the target system owns its own DB-level primary key.
            merge_on: list[OperatorGroup] = []
            src_pk_names = {c.name.upper() for c in src_entity.primary_key_columns}
            if src_pk_names:
                on_cols = [c for c in target_entity.columns if c.name.upper() in src_pk_names]
                if on_cols:
                    merge_on = [OperatorGroup(
                        condition="AND",
                        operation_group=[
                            Operation(independent=col, operator="==", dependent=pa.field(col.name))
                            for col in on_cols
                        ],
                    )]

            # Target plugin gets its own catalog with its own entity + locators
            tgt_catalog = self.target_catalog.model_copy(update={
                "entities": [target_entity],
                "operator_groups": merge_on,
            })
            upsert_resp = self.target.upsert_data(tgt_catalog, source_resp.data)

            if not upsert_resp.ok:
                msg = (
                    f"upsert_data failed for {target_entity.name}: "
                    f"[{upsert_resp.code}] {upsert_resp.message}"
                )
                logger.error(msg)
                results.append({"entity": target_entity.name, "status": "error", "message": msg})
            else:
                logger.info("    %s → %s complete.", src_entity.name, target_entity.name)
                results.append({"entity": target_entity.name, "status": "ok"})

        return results


def run_migration(job: CatalogMigration) -> None:
    """Bootloader entry point."""
    job.get_catalog()
    job.upsert_catalog()
    job.upsert_data()
