"""
Full schema discovery across all registered plugins, cached as Parquet files.

Run manually to refresh the metadata cache:
    python server/services/sync_systems.py

One parquet file is written per plugin to server/metadata/<plugin>.parquet.
Schema: entity_name (string), entity_json (large_string — full Entity model JSON).
Column-level Locators are embedded inside entity_json by the plugin during discovery,
so the cache can be fed directly into the federation layer via /api/data/.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from server.plugins.PluginModels import Catalog
from server.plugins.PluginRegistry import PLUGIN, get_plugin, list_plugins

logger = logging.getLogger(__name__)

METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata"

_SCHEMA = pa.schema([
    pa.field("entity_name", pa.string()),
    pa.field("entity_json", pa.large_string()),
])


def sync_plugin(plugin_name: str) -> int:
    """Discover full schema for one plugin and write to parquet. Returns entity count."""
    p = get_plugin(cast(PLUGIN, plugin_name))

    resp = p.get_catalog(Catalog())
    if not resp.ok:
        raise RuntimeError(f"Discovery failed for '{plugin_name}': {resp.message}")

    entities = resp.data.entities if resp.data else []

    table = pa.table(
        {
            "entity_name": pa.array([e.name for e in entities], type=pa.string()),
            "entity_json": pa.array([e.model_dump_json() for e in entities], type=pa.large_string()),
        },
        schema=_SCHEMA,
    )

    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, METADATA_DIR / f"{plugin_name}.parquet")
    logger.info("Synced %d entities for plugin '%s'", len(entities), plugin_name)
    return len(entities)


def sync_all() -> dict[str, int]:
    """Sync all registered plugins. Returns {plugin_name: entity_count}"""
    results: dict[str, int] = {}
    for plugin_name in list_plugins():
        try:
            results[plugin_name] = sync_plugin(plugin_name)
        except Exception as exc:
            logger.error("Failed to sync plugin '%s': %s", plugin_name, exc)
            results[plugin_name] = 0
    return results


if __name__ == "__main__":
    counts = sync_all()
    for name, count in counts.items():
        print(f"  {name}: {count} entities")
