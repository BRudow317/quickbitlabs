"""
Loads the metadata cache written by sync_systems.py and returns a unified
Catalog containing every entity from every plugin.

This is the lightweight session bootstrap — it reads local Parquet files only.
It does NOT trigger live schema discovery; run sync_systems.sync_all() separately.

Column Locators are preserved exactly as written by each plugin, so the
returned Catalog can be sliced by the caller and forwarded to /api/data/
without any additional locator manipulation.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from server.plugins.PluginModels import Catalog, Entity

METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata"


def load_session() -> Catalog:
    """
    Read all cached Parquet files and return a single Catalog containing
    every entity from every registered plugin.
    Returns an empty Catalog if no cache files exist yet.
    """
    if not METADATA_DIR.exists():
        return Catalog()

    entities: list[Entity] = []

    for parquet_file in sorted(METADATA_DIR.glob("*.parquet")):
        table = pq.read_table(parquet_file, columns=["entity_json"])
        for row in table["entity_json"].to_pylist():
            if row:
                entities.append(Entity.model_validate_json(row))

    return Catalog(entities=entities)
