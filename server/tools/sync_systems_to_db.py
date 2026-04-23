"""
Full schema discovery across all registered plugins, cached into the Oracle Catalog Registry.

Run manually to refresh the metadata cache:
    python server/services/sync_systems.py

One SYSTEM-owned Catalog is written per discovered entity, allowing the UI
to query and display them as base building blocks for federated queries.
"""
from __future__ import annotations

import json
import logging
from typing import cast

import oracledb



from server.plugins.PluginModels import Catalog, Column, Entity, Locator
from server.plugins.PluginRegistry import PLUGIN, get_plugin, list_plugins

from server.core.OracleClient import OracleClient

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# RAW SQL: Oracle Upsert (MERGE INTO)
# Matches on OWNER + REGISTRY_KEY to align with current CATALOG_REGISTRY
# schema used by registry APIs.
# -------------------------------------------------------------------
UPSERT_CATALOG_SQL = """
MERGE INTO catalog_registry trg
USING (
    SELECT :owner AS owner,
           :registry_key AS registry_key,
           :catalog_json AS catalog_json
    FROM dual
) src
ON (trg.owner = src.owner AND trg.registry_key = src.registry_key)
WHEN MATCHED THEN
    UPDATE SET 
        catalog_json = src.catalog_json,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (owner, registry_key, catalog_json, created_at, updated_at)
    VALUES (src.owner, src.registry_key, src.catalog_json, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
"""

SYSTEM_REGISTRY_OWNER = "SYSTEM"


def _sanitize_entity_for_storage(entity: Entity) -> Entity:
    """Strip plugin-native payloads that can be recursive/non-JSON-serializable."""
    sanitized_columns: list[Column] = []
    for col in entity.columns:
        locator = None
        if col.locator:
            locator = Locator(
                plugin=col.locator.plugin,
                environment=col.locator.environment,
                namespace=col.locator.namespace,
                entity_name=col.locator.entity_name,
                additional_locators=None,
            )

        sanitized_columns.append(
            Column(
                name=col.name,
                alias=col.alias,
                locator=locator,
                raw_type=col.raw_type,
                arrow_type_id=col.arrow_type_id,
                primary_key=col.primary_key,
                is_unique=col.is_unique,
                is_nullable=col.is_nullable,
                is_read_only=col.is_read_only,
                is_compound_key=col.is_compound_key,
                is_foreign_key=col.is_foreign_key,
                foreign_key_entity=col.foreign_key_entity,
                foreign_key_column=col.foreign_key_column,
                max_length=col.max_length,
                precision=col.precision,
                scale=col.scale,
                serialized_null_value=col.serialized_null_value,
                default_value=None,
                enum_values=[],
                timezone=col.timezone,
                properties={},
            )
        )

    return Entity(
        name=entity.name,
        alias=entity.alias,
        namespace=entity.namespace,
        columns=sanitized_columns,
        properties={},
    )


def _catalog_json_for_storage(plugin_name: str, entity: Entity) -> str:
    """Serialize one-entity Catalog to JSON with a safe fallback path."""
    base_catalog = Catalog(
        name=f"{plugin_name}_{entity.name}",
        scope="SYSTEM",
        entities=[entity],
    )
    try:
        return base_catalog.model_dump_json(exclude_none=True)
    except (RecursionError, TypeError, ValueError):
        logger.warning(
            "Falling back to sanitized catalog serialization for '%s:%s'",
            plugin_name,
            entity.name,
        )
        fallback_catalog = Catalog(
            name=f"{plugin_name}_{entity.name}",
            scope="SYSTEM",
            entities=[_sanitize_entity_for_storage(entity)],
        )
        # Keep serialization deterministic across runs for stable cache diffs.
        return json.dumps(fallback_catalog.model_dump(exclude_none=True), separators=(",", ":"))



def sync_plugin(plugin_name: str, conn: OracleClient) -> int:
    """Discover full schema for one plugin and upsert to Oracle. Returns entity count."""
    p = get_plugin(cast(PLUGIN, plugin_name))

    resp = p.get_catalog(Catalog())
    if not resp.ok:
        # Excel discovery requires file_path/namespace context and may be intentionally
        # unconfigured in environments that do not use the Excel plugin.
        if plugin_name == "excel" and "No Excel file path specified" in (resp.message or ""):
            logger.warning(
                "Skipping plugin '%s': %s",
                plugin_name,
                resp.message,
            )
            return 0
        raise RuntimeError(f"Discovery failed for '{plugin_name}': {resp.message}")

    entities = resp.data.entities if resp.data else []
    
    binds = []
    for entity in entities:
        binds.append({
            "owner": SYSTEM_REGISTRY_OWNER,
            "registry_key": f"{plugin_name}:{entity.name}",
            "catalog_json": _catalog_json_for_storage(plugin_name, entity),
        })

    if binds:
        with conn.cursor() as cursor:
            cursor.setinputsizes(catalog_json=oracledb.DB_TYPE_CLOB)
            # executemany handles the array of dictionaries in a single C-level loop
            cursor.executemany(UPSERT_CATALOG_SQL, binds)
            conn.commit()

    logger.info("Synced %d entities for plugin '%s'", len(entities), plugin_name)
    return len(entities)


def sync_all() -> dict[str, int]:
    """Sync all registered plugins. Returns {plugin_name: entity_count}."""
    results: dict[str, int] = {}
    
    conn = OracleClient()
    # Open a single connection for the entire sync run
    for plugin_name in list_plugins():
        try:
            results[plugin_name] = sync_plugin(plugin_name, conn)
        except Exception as exc:
            logger.exception("Failed to sync plugin '%s': %s", plugin_name, exc)
            results[plugin_name] = 0
                
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    counts = sync_all()
    for name, count in counts.items():
        print(f"  {name}: {count} entities")