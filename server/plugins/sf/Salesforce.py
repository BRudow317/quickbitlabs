from __future__ import annotations

from typing import Any, Literal


from server.plugins.PluginProtocol import Plugin
from server.plugins.sf.engines.SfClient import SfClient
from server.plugins.sf.engines.SfRestEngine import SfRest
from server.plugins.sf.engines.SfBulk2Engine import Bulk2
from server.plugins.sf.services.SfServices import SfService
from server.plugins.PluginModels import ArrowReader, Entity, Column, Catalog, Records
from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)

class Salesforce(Plugin):
    """Facade Interface for Salesforce operations"""
    client: SfClient
    service: SfService

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        base_url: str | None = None,
        access_token: str | None = None,
    ):
        self.client = SfClient(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            base_url=base_url,
            access_token=access_token,
        )
        self.service = SfService(self.client)

    @property
    def rest(self) -> SfRest:
        return self.service.rest

    @property
    def bulk2(self) -> Bulk2:
        return self.service.bulk2

    # plugin specifics
    def query(self, soql: str, object_name: str | None = None, query_type: Literal['Rest', 'Bulk2'] = 'Rest', return_type: Literal['Records', 'ArrowReader'] = 'Records') -> Records | ArrowReader:
        """Execute a SOQL query and return results as either Records or ArrowReader.
        This is only to be used when other existing functionality is insufficient."""
        return self.service.query(soql, object_name, query_type, return_type)

    def bulk_query(self, soql: str, object_name: str | None = None) -> Records:
        """Execute a SOQL query via the Bulk 2.0 API and return results as Records. This is only to be used when other existing functionality is insufficient."""
        return self.service.bulk_query(soql, object_name)

    def get_limits(self) -> dict[str, Any]:
        """Get current API limits for the org. This is only to be used when other existing functionality is insufficient."""
        return self.service.get_limits()

    # Catalog
    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return self.service.get_catalog(catalog, **kwargs)

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog creation via API")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog modification via API")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog upsertion via API")

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support catalog deletion via API")


    # Entity
    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return self.service.get_entity(catalog, **kwargs)

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity modification via data API")

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support entity deletion via data API")


    # Column
    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return self.service.get_column(catalog, **kwargs)

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field modification via data API")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support field deletion via data API")

    # ArrowReader operations
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
