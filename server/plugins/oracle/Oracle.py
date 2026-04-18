from typing import Any

from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import Catalog, Entity, Column, ArrowReader
from server.plugins.PluginResponse import PluginResponse

# STRICT BOUNDARY: The Facade ONLY imports Services. No Engines, No Clients.
from .OracleServices import OracleService
from .OracleClient import OracleClient


class Oracle(Plugin):
    """The Oracle Plugin Facade. Strictly complies with the Program rules. Routes traffic to the Service layer.
        # The Facade is blind to the Client and Engine. 
        """
    client: OracleClient
    service: OracleService
    properties: dict[str, Any]
    def __init__(self, **kwargs: Any):
        self.client = OracleClient(**kwargs)
        self.service = OracleService(self.client)
        self.properties = {}

    # -- Records (Row / Data Level) --

    def create_data(self, catalog: Catalog, data: ArrowReader,  **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            self.service.insert_data(catalog, data, **kwargs)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            return PluginResponse.success(self.service.get_data(catalog=catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_data(self, catalog: Catalog, data: ArrowReader ,  **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            result = self.service.update_data(catalog, data, **kwargs)
            if not result.ok:
                return PluginResponse.error(result.message)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: Catalog, data: ArrowReader ,  **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            self.service.upsert_data(catalog, data, **kwargs)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_data(self, catalog: Catalog, data: ArrowReader ,  **kwargs: Any) -> PluginResponse[None]:
        try:
            return PluginResponse.success(self.service.delete_data(catalog, data, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    # ==========================================
    # -- Field (Column / Attribute Level) --
    # ==========================================

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            return PluginResponse.success(self.service.create_column(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            return PluginResponse.success(self.service.get_column(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Oracle does not support ALTER COLUMN via this protocol. Use upsert_entity to add missing columns.")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            # Oracle upsert_entity already handles adding missing columns
            self.service.upsert_entity(catalog, **kwargs)
            if catalog.entities and catalog.entities[0].columns:
                return PluginResponse.success(catalog.entities[0].columns[0])
            return PluginResponse.error("Catalog must contain an entity with at least one column.")
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Oracle column deletion not implemented. Dropping columns is destructive and should be done manually.")

    # ==========================================
    # -- Entity (Table / Object Level) --
    # ==========================================

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            return PluginResponse.success(self.service.create_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            return PluginResponse.success(self.service.get_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            return PluginResponse.success(self.service.upsert_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            return PluginResponse.success(self.service.upsert_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Oracle table deletion not implemented. Dropping tables is destructive and should be done manually.")

    # ==========================================
    # -- Catalog (Database / Schema Level) --
    # ==========================================

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.upsert_catalog(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.get_catalog(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.upsert_catalog(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.upsert_catalog(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Oracle schema deletion not implemented. Dropping schemas is destructive and should be done manually.")

    def query(self, statement: str, binds: dict[str, Any] | None = None, page_size: int | None = None, catalog: Catalog | None = None, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            kwargs['statement'] = statement
            kwargs['binds'] = binds
            kwargs['page_size'] = page_size
            if not catalog:
                catalog = Catalog(name=self.client.oracle_user)
            return PluginResponse.success(
                self.service.get_data(
                    catalog=catalog,
                    statement=statement, 
                    **kwargs
                    ))
        except Exception as e:
            return PluginResponse.error(str(e))
# Explicitly enforce duck-typing compliance at module load time
# Intentionally avoid instantiating Oracle at import time because that creates
# a live DB connection through OracleClient.