from typing import Any

from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import CatalogModel, EntityModel, FieldModel, ArrowStream, QueryModel
from server.plugins.PluginResponse import PluginResponse

# STRICT BOUNDARY: The Facade ONLY imports Services. No Engines, No Clients.
from .OracleServices import OracleServices


class Oracle(Plugin):
    """The Oracle Plugin Facade. Strictly complies with the Program rules. Routes traffic to the Service layer.
        # The Facade is blind to the Client and Engine. 
        """
    def __init__(self, **kwargs: Any):
        self.service = OracleServices(**kwargs)

    # -- Records (Row / Data Level) --

    def create_data(self, catalog: CatalogModel, entity: EntityModel, data: ArrowStream, query: QueryModel | None = None,  **kwargs: Any) -> PluginResponse[ArrowStream]:
        try:
            return PluginResponse.success(self.service.insert_data(catalog, data, query=query, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_data(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[ArrowStream]:
        try:
            return PluginResponse.success(self.service.get_data(catalog=catalog, entity=entity, query=query, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_data(self, catalog: CatalogModel, entity: EntityModel, data: ArrowStream, query: QueryModel | None = None,  **kwargs: Any) -> PluginResponse[ArrowStream]:
        try:
            return PluginResponse.success(self.service.update_data(catalog, data, query=query, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: CatalogModel, entity: EntityModel, data: ArrowStream, query: QueryModel | None = None,  **kwargs: Any) -> PluginResponse[ArrowStream]:
        try:
            return PluginResponse.success(self.service.upsert_data(catalog, data, query=query, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_data(self, catalog: CatalogModel, entity: EntityModel, data: ArrowStream, query: QueryModel | None = None,  **kwargs: Any) -> PluginResponse[ArrowStream]:
        try:
            return PluginResponse.success(self.service.delete_data(catalog, data, query=query, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    # ==========================================
    # -- Field (Column / Attribute Level) --
    # ==========================================

    def create_field(self, catalog: CatalogModel, entity: EntityModel, field: FieldModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.create_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_field(self, catalog: CatalogModel, entity: EntityModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.get_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_field(self, catalog: CatalogModel, entity: EntityModel, field: FieldModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.update_field(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_field(self, catalog: CatalogModel, entity: EntityModel, field: FieldModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.upsert_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_field(self, catalog: CatalogModel, entity: EntityModel, field: FieldModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.delete_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    # ==========================================
    # -- Entity (Table / Object Level) --
    # ==========================================

    def create_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[EntityModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.create_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[EntityModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.get_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[EntityModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.update_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[EntityModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.upsert_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_entity(self, catalog: CatalogModel,**kwargs: Any) -> PluginResponse[EntityModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.delete_entity(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    # ==========================================
    # -- Catalog (Database / Schema Level) --
    # ==========================================

    def create_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Oracle plugin cannot create catalogs.")

    def get_catalog(self, catalog: CatalogModel | None = None, **kwargs: Any) -> PluginResponse[CatalogModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.get_catalog(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Oracle plugin cannot update catalogs.")

    def upsert_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Oracle plugin cannot upsert catalogs.")

    def delete_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Oracle Service Not Available.")

# Explicitly enforce duck-typing compliance at module load time
assert isinstance(Oracle(), Plugin)