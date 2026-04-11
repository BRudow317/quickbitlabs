
"""
python ./scripts/boot.py -v -l ./.logs --env homelab --config ../.secrets/.env --exec ./main.py
"""
from __future__ import annotations
from typing import Any, TYPE_CHECKING

from server.plugins.PluginRegistry import get_plugin
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import ArrowStream, Catalog, Entity, Column

if TYPE_CHECKING:
    from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


class FullMigration:
    source: Plugin
    target: Plugin

    def __init__(
        self,
        source_plugin: str,
        target_plugin: str,
        **kwargs: Any,
    ):
        self.source = get_plugin(source_plugin, **kwargs.get('source_kwargs', {}))
        self.source_catalog = Catalog(name= kwargs.get('source_catalog_name', None))
        self.target = get_plugin(target_plugin, **kwargs.get('target_kwargs', {}))
        self.target_catalog = Catalog(name= kwargs.get('target_catalog_name', None))

    def discover(self) -> None:
        """Source describes its objects and populates source_* entities on the Catalog."""
        source_catalog_response = self.source.get_catalog(catalog=self.source_catalog)
        if not source_catalog_response.ok or not source_catalog_response.data:
            raise RuntimeError(f"Discovery failed [{source_catalog_response.code}]: {source_catalog_response.message}")
        self.source_catalog = source_catalog_response.data

        target_catalog_response = self.target.get_catalog(catalog=self.target_catalog)
        if not target_catalog_response.ok or not target_catalog_response.data:
            raise RuntimeError(f"Discovery failed [{target_catalog_response.code}]: {target_catalog_response.message}")
        self.target_catalog = target_catalog_response.data

    def prepare(self) -> None:
        """apply DDL for source entity."""
        migration_catalog = self.source_catalog.model_copy()
        migration_catalog.entities = []
        for source_entity in self.source_catalog.entities:
            for source_col in source_entity.columns:
                if source_col not in self.target_catalog.entity_map[source_entity.name].columns:
                    if source_entity not in migration_catalog.entities:
                        migration_catalog.entities.append(source_entity)
                    self.target_catalog.entity_map[source_entity.name].columns.append(source_col)
        self.migration_catalog = migration_catalog
    
    def migrate_data(self) -> None:
        self.migration_catalog.name = self.source_catalog.name
        response = self.source.get_data(self.migration_catalog)
        if response.ok and response.data:
            self.migration_catalog.name = self.target_catalog.name
            self.target.upsert_data(self.migration_catalog, response.data)
        else: 
            raise RuntimeError(f"FullMigration.migrate_data Failed: [{response.code}]: {response.message}")
    
def run() -> int:
    job_dict = {
        "source_catalog_name": "homelab",
        "target_catalog_name": "brudow",
    }
    migration_job = FullMigration(
        source_plugin="Salesforce",
        target_plugin="Oracle",
        kwargs=job_dict,
    )
    migration_job.discover()
    migration_job.prepare()
    migration_job.migrate_data()
    return 0
