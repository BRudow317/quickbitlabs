"""
CatalogPublisher service: AST -> Oracle Metadata Registry via Plugin Protocol.

python Q:/scripts/boot.py -v -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./publish.py
"""
from __future__ import annotations

import logging
import uuid
import json
from typing import Any, cast

from server.plugins.PluginModels import Catalog, Entity, Column, Locator
from server.plugins.PluginRegistry import get_plugin, PLUGIN

logger = logging.getLogger(__name__)

class CatalogPublisher:
    """Orchestrates registering a 1:1 mapped Catalog AST into the central registry."""
    
    def __init__(
        self,
        target_catalog: Catalog,
        registry_plugin_name: str = "oracle" # The plugin that houses the registry
    ):
        self.target_catalog = target_catalog
        
        # Ensure the catalog has an ID before we serialize it
        if not self.target_catalog.catalog_id:
            self.target_catalog.catalog_id = str(uuid.uuid4())
            
        self.registry_plugin = get_plugin(cast(PLUGIN, registry_plugin_name))
        
        # 1. Map the Registry Table exactly 1:1 with your Catalog Model
        self.registry_catalog = Catalog(
            name="system_registry_upsert",
            entities=[
                Entity(
                    name="catalog_registry",
                    columns=[
                        # Scalars
                        Column(name="catalog_id", arrow_type_id="string", primary_key=True),
                        Column(name="name", arrow_type_id="string"),
                        Column(name="alias", arrow_type_id="string", is_nullable=True),
                        Column(name="namespace", arrow_type_id="string", is_nullable=True),
                        Column(name="scope", arrow_type_id="string"),
                        Column(name="source_type", arrow_type_id="string", is_nullable=True),
                        Column(name="owner_user_id", arrow_type_id="string", is_nullable=True),
                        Column(name="team_id", arrow_type_id="string", is_nullable=True),
                        Column(name="limit", arrow_type_id="int64", is_nullable=True),
                        
                        # Complex Arrays/Dicts mapped to CLOBs (large_string)
                        Column(name="entities", arrow_type_id="large_string"),
                        Column(name="operator_groups", arrow_type_id="large_string"),
                        Column(name="joins", arrow_type_id="large_string"),
                        Column(name="sort_columns", arrow_type_id="large_string"),
                        Column(name="properties", arrow_type_id="large_string"),
                    ]
                )
            ]
        )
        
        locator = Locator(plugin=cast(PLUGIN, registry_plugin_name))
        for col in self.registry_catalog.entities[0].columns:
            col.locator = locator

    def publish(self) -> str:
        """Converts the target_catalog to Arrow and routes it through upsert_data."""
        # 1. Unroll the Pydantic model directly into the payload 
        # We serialize the lists into JSON strings for the database CLOBs
        payload = [{
            "catalog_id": self.target_catalog.catalog_id,
            "name": self.target_catalog.name,
            "alias": self.target_catalog.alias,
            "namespace": self.target_catalog.namespace, 
            "scope": self.target_catalog.scope,
            "source_type": self.target_catalog.source_type,
            "owner_user_id": self.target_catalog.owner_user_id,
            "team_id": self.target_catalog.team_id,
            "limit": self.target_catalog.limit,
            
            # Serialize nested arrays
            "entities": json.dumps([e.model_dump(mode='json', exclude_none=True) for e in self.target_catalog.entities]),
            "operator_groups": json.dumps([g.model_dump(mode='json', exclude_none=True) for g in self.target_catalog.operator_groups]),
            "joins": json.dumps([j.model_dump(mode='json', exclude_none=True) for j in self.target_catalog.joins]),
            "sort_columns": json.dumps([s.model_dump(mode='json', exclude_none=True) for s in self.target_catalog.sort_columns]),
            "properties": json.dumps(self.target_catalog.properties) if self.target_catalog.properties else "{}"
        }]
        
        # 2. Let the framework bridge the types to Arrow
        arrow_stream = self.registry_catalog.arrow_reader(data=payload)
        
        # 3. Pass to the Oracle Plugin 
        resp = self.registry_plugin.upsert_data(
            catalog=self.registry_catalog, 
            data=arrow_stream
        )
        
        if not resp.ok:
            raise RuntimeError(f"Failed to publish catalog [{resp.code}]: {resp.message}")
        if not resp.data or "catalog_id" not in resp.data:
            raise RuntimeError(f"Invalid response from registry plugin: {resp.data}")
        return resp.data["catalog_id"]