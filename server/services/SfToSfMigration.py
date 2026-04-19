"""
Salesforce org-to-org full migration service.

Pattern mirrors FullMigration (SF -> Oracle) but targets a second Salesforce org.
Allowed imports: plugin facades + the 4 core plugin files only.
"""
from __future__ import annotations

from typing import Any

from server.plugins.PluginRegistry import get_plugin
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import ArrowReader, Catalog, Entity, Column
from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


class SfToSfMigration:
    """
    Full org-to-org migration between two Salesforce instances.

    Follows the same discover -> prepare -> migrate_data pattern as FullMigration.
    Source and target are treated as opaque Plugin implementations — no
    Salesforce-specific APIs are used in this service layer.

    Key SF-to-SF differences from SF-to-Oracle:
    - No DDL phase (Salesforce schema is immutable via the data API). The
      assumption is that the target org is pre-provisioned with the same schema
      as the source, or a subset thereof.
    - No column rename step — the SF plugin produces streams with bare field
      names (``Name``, not ``Account_Name``).
    - Upsert key detection is delegated to the plugin. To override per-entity,
      set ``entity.properties["external_id_field"] = "YourField__c"`` on the
      catalog returned by ``prepare()`` before calling ``migrate_data()``.
    """

    source: Plugin
    target: Plugin
    source_catalog: Catalog
    target_catalog: Catalog

    def __init__(
        self,
        source_kwargs: dict[str, Any] | None = None,
        target_kwargs: dict[str, Any] | None = None,
        entities: list[str] | None = None,
    ) -> None:
        self.source = get_plugin("salesforce", **(source_kwargs or {}))
        self.target = get_plugin("salesforce", **(target_kwargs or {}))
        self.source_catalog = Catalog()
        self.target_catalog = Catalog()
        self._entity_filter: set[str] | None = set(entities) if entities else None
        self._migration_catalog: Catalog | None = None

    # ------------------------------------------------------------------
    # Step 1 — Discovery
    # ------------------------------------------------------------------

    def discover(self) -> None:
        """
        Discover entity and column metadata on both orgs.

        Two calls per org mirrors FullMigration
        """
        logger.info("Step 1: Discovering source schema...")

        resp = self.source.get_catalog(Catalog())
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Source entity discovery failed [{resp.code}]: {resp.message}")
        self.source_catalog = resp.data
        if self._entity_filter:
            self.source_catalog.entities = [
                e for e in self.source_catalog.entities
                if (e.name or "") in self._entity_filter
            ]

        logger.info(f"  Found {len(self.source_catalog.entities)} source entities.")

        resp = self.source.get_catalog(self.source_catalog)
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Source column discovery failed [{resp.code}]: {resp.message}")
        self.source_catalog = resp.data

        logger.info("Step 1: Discovering target schema...")

        resp = self.target.get_catalog(Catalog())
        if not resp.ok or not resp.data:
            raise RuntimeError(f"Target entity discovery failed [{resp.code}]: {resp.message}")
        tgt_entity_list = resp.data

        # Only describe target columns for objects that also exist on the source.
        source_names = {e.name for e in self.source_catalog.entities}
        tgt_to_describe = [e for e in tgt_entity_list.entities if e.name in source_names]

        if tgt_to_describe:
            tgt_sub = tgt_entity_list.model_copy(update={"entities": tgt_to_describe})
            resp = self.target.get_catalog(tgt_sub)
            if not resp.ok or not resp.data:
                raise RuntimeError(f"Target column discovery failed [{resp.code}]: {resp.message}")
            self.target_catalog = resp.data
        else:
            self.target_catalog = tgt_entity_list

        logger.info(f"  Found {len(self.target_catalog.entities)} matching target entities.")

    # ------------------------------------------------------------------
    # Step 2 — Prepare
    # ------------------------------------------------------------------

    def prepare(self) -> Catalog:
        """
        Build the migration catalog.

        For each source entity that also exists on the target, retains only
        columns that are writable on the target (``not col.is_read_only``) and
        have a valid Arrow type mapping. Entities with no writable columns are
        skipped. Upsert key detection is a plugin responsibility.

        Returns the migration catalog (target entities + writable columns).
        No DDL is attempted — Salesforce schema is fixed.
        """
        logger.info("Step 2: Preparing migration catalog...")

        tgt_entity_map: dict[str, Entity] = {e.name: e for e in self.target_catalog.entities}
        migration_entities: list[Entity] = []

        for src_entity in self.source_catalog.entities:
            name = src_entity.name or ""
            tgt_entity = tgt_entity_map.get(name)
            if tgt_entity is None:
                logger.warning(f"  {name}: not found on target org — skipping.")
                continue

            tgt_col_map: dict[str, Column] = {c.name: c for c in tgt_entity.columns}
            migration_cols: list[Column] = []

            for col in src_entity.columns:
                # Skip columns with no Arrow mapping (compound types, etc.)
                if col.arrow_type_id is None:
                    continue
                # Skip columns absent from target or read-only there.
                tgt_col = tgt_col_map.get(col.name)
                if tgt_col is None or tgt_col.is_read_only:
                    continue
                migration_cols.append(tgt_col)

            if not migration_cols:
                logger.warning(f"  {name}: no writable columns after filtering — skipping.")
                continue

            migration_entities.append(Entity(
                name=name,
                parent_names=tgt_entity.parent_names,
                alias=tgt_entity.alias,
                columns=migration_cols,
            ))
            logger.info(f"  {name}: {len(migration_cols)} columns queued.")

        self._migration_catalog = Catalog(entities=migration_entities)
        logger.info(f"  {len(migration_entities)} entities queued for migration.")
        return self._migration_catalog

    # ------------------------------------------------------------------
    # Step 3 — Data Migration
    # ------------------------------------------------------------------

    def migrate_data(self) -> list[dict[str, Any]]:
        """
        Stream data from source and upsert into target, entity by entity.

        Uses only ``Plugin.get_data`` and ``Plugin.upsert_data``.
        Upsert key selection is fully delegated to the plugin — it resolves
        from ``entity.properties["external_id_field"]`` if set, then falls back
        to auto-detecting the first unique column in the catalog.
        """
        if self._migration_catalog is None:
            raise RuntimeError("Call prepare() before migrate_data().")

        logger.info("Step 3: Migrating data...")
        results: list[dict[str, Any]] = []

        src_entity_map = {e.name: e for e in self.source_catalog.entities}

        for tgt_entity in self._migration_catalog.entities:
            name = tgt_entity.name
            src_entity = src_entity_map.get(name)

            if src_entity is None:
                results.append({"entity": name, "status": "skipped"})
                continue

            logger.info(f"  {name} ...")

            try:
                src_sub = self.source_catalog.model_copy(update={"entities": [src_entity]})
                resp: PluginResponse[ArrowReader] = self.source.get_data(src_sub)
                if not resp.ok or resp.data is None:
                    msg = f"get_data failed for {name}: [{resp.code}] {resp.message}"
                    logger.error(f"    {msg}")
                    results.append({"entity": name, "status": "error", "message": msg})
                    continue

                tgt_sub = self._migration_catalog.model_copy(update={"entities": [tgt_entity]})
                resp = self.target.upsert_data(tgt_sub, resp.data)
                if not resp.ok:
                    msg = f"upsert_data failed for {name}: [{resp.code}] {resp.message}"
                    logger.error(f"    {msg}")
                    results.append({"entity": name, "status": "error", "message": msg})
                else:
                    logger.info(f"    {name}: complete.")
                    results.append({"entity": name, "status": "ok"})

            except Exception as exc:
                logger.exception(f"    Migration failed for {name}")
                results.append({"entity": name, "status": "exception", "message": str(exc)})

        return results

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def prepare_all(self) -> Catalog:
        """Run discover + prepare and return the migration catalog."""
        self.discover()
        return self.prepare()

    def run_all(self) -> list[dict[str, Any]]:
        """Run discover -> prepare -> migrate_data in sequence."""
        self.prepare_all()
        return self.migrate_data()


def run() -> int:
    """Entry point for the bootloader (full org migration, source org from .env)."""
    migration = SfToSfMigration(
        source_kwargs={},
        target_kwargs={},  # populate with target org credentials
    )
    try:
        results = migration.run_all()
        ok   = sum(1 for r in results if r.get("status") == "ok")
        fail = len(results) - ok
        for r in results:
            if r.get("status") == "ok":
                logger.info(f"  OK: {r['entity']}")
            else:
                logger.error(f"  FAIL: {r.get('entity', '?')} -- {r.get('message', '')}")
        logger.info(f"Migration complete: {ok} succeeded, {fail} failed.")
        return 0 if fail == 0 else 1
    except Exception as exc:
        logger.exception(f"Migration aborted: {exc}")
        return 1
