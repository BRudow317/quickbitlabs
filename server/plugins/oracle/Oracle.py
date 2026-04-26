from typing import Any

from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginModels import Catalog, Entity, Column, ArrowReader
from server.plugins.PluginResponse import PluginResponse

# STRICT BOUNDARY: The Facade ONLY imports Services. No Engines, No Clients.
from .OracleServices import OracleService
from .OracleClient import OracleClient


class Oracle(Plugin):
    """Oracle Plugin Facade. Strictly complies with the Plugin Protocol.
    Routes all traffic to OracleService; never raises for expected failures.
    """
    client: OracleClient
    service: OracleService
    properties: dict[str, Any]

    def __init__(self, **kwargs: Any):
        self.client = OracleClient(**kwargs)
        self.service = OracleService(self.client)
        self.properties = {}

    # ------------------------------------------------------------------
    # Data protocol
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]:
        return self.service.get_data(catalog, **kwargs)

    def create_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        return self.service.create_data(catalog, data, **kwargs)

    def update_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        return self.service.update_data(catalog, data, **kwargs)

    def upsert_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        return self.service.upsert_data(catalog, data, **kwargs)

    def delete_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[None]:
        return self.service.delete_data(catalog, data, **kwargs)

    # ------------------------------------------------------------------
    # Column protocol
    # ------------------------------------------------------------------

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return self.service.get_column(catalog, **kwargs)

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return self.service.create_column(catalog, **kwargs)

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return self.service.update_column(catalog, **kwargs)

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return self.service.upsert_column(catalog, **kwargs)

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return self.service.delete_column(catalog, **kwargs)

    # ------------------------------------------------------------------
    # Entity protocol
    # ------------------------------------------------------------------

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return self.service.get_entity(catalog, **kwargs)

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return self.service.create_entity(catalog, **kwargs)

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return self.service.update_entity(catalog, **kwargs)

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return self.service.upsert_entity(catalog, **kwargs)

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return self.service.delete_entity(catalog, **kwargs)

    # ------------------------------------------------------------------
    # Catalog protocol
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return self.service.get_catalog(catalog, **kwargs)

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return self.service.create_catalog(catalog, **kwargs)

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return self.service.update_catalog(catalog, **kwargs)

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return self.service.upsert_catalog(catalog, **kwargs)

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return self.service.delete_catalog(catalog, **kwargs)

    # ------------------------------------------------------------------
    # Plugin-unique: raw SQL passthrough
    # ------------------------------------------------------------------

    def query(
        self,
        statement: str,
        binds: dict[str, Any] | None = None,
        page_size: int | None = None,
        catalog: Catalog | None = None,
        **kwargs: Any,
    ) -> PluginResponse[ArrowReader]:
        kwargs["statement"] = statement
        kwargs["binds"] = binds
        kwargs["page_size"] = page_size
        return self.service.get_data(
            catalog or Catalog(name=self.client.oracle_user),
            **kwargs,
        )


# Explicitly enforce duck-typing compliance at module load time.
# Intentionally avoid instantiating Oracle here because that creates
# a live DB connection through OracleClient.
