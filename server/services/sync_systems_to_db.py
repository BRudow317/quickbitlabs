"""
Full schema discovery across all registered plugins, cached into the Oracle Catalog Registry.

Run manually to refresh the metadata cache:
    python server/services/sync_systems.py

One SYSTEM scope Catalog is written per discovered entity, allowing the UI 
to query and display them as base building blocks for federated queries.
"""
from __future__ import annotations

import logging
import uuid
from typing import cast



from server.plugins.PluginModels import Catalog
from server.plugins.PluginRegistry import PLUGIN, get_plugin, list_plugins

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# RAW SQL: Oracle Upsert (MERGE INTO)
# Matches on 'name' and 'namespace' to ensure we update existing 
# system catalogs rather than duplicating them.
# -------------------------------------------------------------------
UPSERT_CATALOG_SQL = """
MERGE INTO catalog_registry trg
USING (
    SELECT :catalog_id AS catalog_id,
           :name AS name,
           :namespace AS namespace,
           'SYSTEM' AS scope,
           :catalog_json AS catalog_json
    FROM dual
) src
ON (trg.name = src.name AND trg.namespace = src.namespace AND trg.scope = src.scope)
WHEN MATCHED THEN
    UPDATE SET 
        catalog_json = src.catalog_json,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (catalog_id, name, namespace, scope, catalog_json)
    VALUES (src.catalog_id, src.name, src.namespace, src.scope, src.catalog_json)
"""

def get_db_connection() -> oracledb.Connection:
    """Factory for your Oracle DB connection. Replace with your actual pooling/config logic."""
    return oracledb.connect(
        user="your_db_user", 
        password="your_db_password", 
        dsn="your_db_dsn"
    )

def sync_plugin(plugin_name: str, conn: oracledb.Connection) -> int:
    """Discover full schema for one plugin and upsert to Oracle. Returns entity count."""
    p = get_plugin(cast(PLUGIN, plugin_name))

    resp = p.get_catalog(Catalog())
    if not resp.ok:
        raise RuntimeError(f"Discovery failed for '{plugin_name}': {resp.message}")

    entities = resp.data.entities if resp.data else []
    
    binds = []
    for entity in entities:
        # Wrap each discovered entity into a standalone base Catalog object
        base_catalog = Catalog(
            name=f"{plugin_name}_{entity.name}",
            plugin=cast(PLUGIN, plugin_name),
            entities=[entity]
        )

        binds.append({
            "catalog_id": str(uuid.uuid4()),  # Only used if NOT MATCHED (new insert)
            "name": entity.name,
            "namespace": plugin_name,         # e.g., 'salesforce', 'oracle'
            "catalog_json": base_catalog.model_dump_json(exclude_none=True)
        })

    if binds:
        with conn.cursor() as cursor:
            # executemany handles the array of dictionaries in a single C-level loop
            cursor.executemany(UPSERT_CATALOG_SQL, binds)
            conn.commit()

    logger.info("Synced %d entities for plugin '%s'", len(entities), plugin_name)
    return len(entities)


def sync_all() -> dict[str, int]:
    """Sync all registered plugins. Returns {plugin_name: entity_count}."""
    results: dict[str, int] = {}
    
    # Open a single connection for the entire sync run
    with get_db_connection() as conn:
        for plugin_name in list_plugins():
            try:
                results[plugin_name] = sync_plugin(plugin_name, conn)
            except Exception as exc:
                logger.error("Failed to sync plugin '%s': %s", plugin_name, exc)
                results[plugin_name] = 0
                
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    counts = sync_all()
    for name, count in counts.items():
        print(f"  {name}: {count} entities")