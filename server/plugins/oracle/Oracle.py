from typing import Any

from server.models.PluginProtocol import Plugin
from server.models.PluginModels import CatalogModel, EntityModel, FieldModel, Records
from server.models.PluginResponse import PluginResponse

# STRICT BOUNDARY: The Facade ONLY imports Services. No Engines, No Clients.
from .OracleServices import OracleServices


class Oracle:
    """The Oracle Plugin Facade. Strictly complies with the Program rules. Routes traffic to the Service layer.
        # The Facade is blind to the Client and Engine. 
        """

    def __init__(self, **kwargs: Any):
        self.service = OracleServices(**kwargs)

    # -- Records (Row / Data Level) --

    def create(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        try:
            return PluginResponse.success(self.service.insert_records(catalog, records, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[Records]:
        try:
            return PluginResponse.success(self.service.get_records(catalog=catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        try:
            return PluginResponse.success(self.service.update_records(catalog, records, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        try:
            return PluginResponse.success(self.service.upsert_records(catalog, records, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        try:
            return PluginResponse.success(self.service.delete_records(catalog, records, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    # ==========================================
    # -- Catalog (Database / Schema Level) --
    # ==========================================

    def create_catalog(self, catalog: CatalogModel | str, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Oracle plugin cannot create catalogs.")

    def get_catalog(self, catalog: CatalogModel | str | None = None, **kwargs: Any) -> PluginResponse[CatalogModel]:
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
    # -- Field (Column / Attribute Level) --
    # ==========================================

    def create_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.create_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def get_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.get_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_field(self, catalog: CatalogModel | str, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.update_field(catalog, **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_field(self, catalog: CatalogModel | str, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.upsert_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_field(self, catalog: CatalogModel, field: FieldModel, **kwargs: Any) -> PluginResponse[FieldModel]:
        try:
            return PluginResponse.not_implemented("Oracle Service Not Available.")
            # return PluginResponse.success(self.service.delete_field(catalog,  **kwargs))
        except Exception as e:
            return PluginResponse.error(str(e))

# Explicitly enforce duck-typing compliance at module load time
assert isinstance(Oracle(), Plugin)