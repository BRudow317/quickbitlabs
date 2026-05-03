"""
sync_systems.py - Offline schema discovery for all registered plugins.

Calls get_catalog(Catalog()) (blank = full discovery) for each registered plugin
and writes the resulting Catalog to CATALOG_REGISTRY under the 'SYSTEM' owner.
One row per plugin, keyed by plugin name. Re-running overwrites the existing snapshot.

This is the data source for GET /api/session/ (SessionService.load_session).

Usage:
    python -m server.services.sync_systems
"""
from __future__ import annotations

import logging
from typing import cast

from server.services.catalog_registry import CatalogRegistryService
from server.plugins.PluginModels import Catalog
from server.plugins.PluginRegistry import PLUGIN, PLUGIN_REGISTRY

logger = logging.getLogger(__name__)


def sync_all() -> dict[str, str]:
    """
    Discover and cache the full schema for every registered plugin.
    Returns {plugin_name: 'ok' | error_message} for all attempted plugins.
    Failures are logged and collected but do not abort the remaining plugins.
    """
    registry = CatalogRegistryService()
    results: dict[str, str] = {}

    for plugin_name in PLUGIN_REGISTRY:
        try:
            from server.plugins.PluginRegistry import get_plugin
            plugin = get_plugin(cast(PLUGIN, plugin_name))
        except Exception as exc:
            results[plugin_name] = f"load error: {exc}"
            logger.warning(f"sync_systems: {plugin_name} → could not load plugin: {exc}")
            continue

        try:
            response = plugin.get_catalog(Catalog())
            if not response.ok or response.data is None:
                msg = response.message or "get_catalog returned not ok"
                if response.code == 501:
                    results[plugin_name] = "not_implemented"
                    logger.info(f"sync_systems: {plugin_name} → not implemented, skipped")
                else:
                    results[plugin_name] = msg
                    logger.warning(f"sync_systems: {plugin_name} → {msg}")
                continue

            catalog: Catalog = response.data
            registry.save(owner="SYSTEM", registry_key=plugin_name, catalog=catalog)
            results[plugin_name] = "ok"
            logger.info(f"sync_systems: {plugin_name} → ok ({len(catalog.entities)} entities)")
        except Exception as exc:
            results[plugin_name] = str(exc)
            logger.exception(f"sync_systems: {plugin_name} → exception")

    return results


if __name__ == "__main__":
    import sys
    from pathlib import Path

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    results = sync_all()
    print("\n--- sync_systems results ---")
    for name, status in results.items():
        print(f"  {name:20s}  {status}")
