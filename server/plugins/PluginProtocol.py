from __future__ import annotations

from typing import Any, Protocol, runtime_checkable, Literal
from .PluginModels import Catalog, Entity, Column, ArrowStream
from .PluginResponse import PluginResponse

@runtime_checkable
class Plugin(Protocol):
    """
    Universal interface for any data system.
    Five verbs:  create, get, update, upsert, delete
    Three nouns: catalog, entity, column
    stream:  
        Records - dictionary wrapped Iterable[dict[str, Any]] (deprecated but still some support)
        ArrowStream - PyArrow RecordBatchReader
    
    All operations return PluginResponse[T] - never raises for expected failures.
    PluginResponse.not_implemented() so the caller stays in control.
    Each Plugin handles its own batch limits internally.
    
    The catalog is the envelope that carries the metadata and context 
    needed to perform the requested operation.

    The Catalog is the core contract of the plugin system. It may contain no entities, or one, or many. It may contain operator groups for filtering, sort fields for ordering, and a limit for constraining the result set. The Catalog is the universal way to pass along all the contextual information about what data you want to interact with, and how you want to interact with it.

    The Expectation is that the plugin will use the information in the catalog to perform the requested operation, and return a PluginResponse with the result. If the plugin does not support the requested operation, it should return PluginResponse.not_implemented() with an appropriate message. This way, higher level services can decide how to handle unsupported operations, whether that's by falling back to a different plugin, returning an error to the user, or something else.

    The plugin should implement it's own internal logic for how to handle the catalog and perform the requested operation, but by adhering to this core contract, it can be used interchangeably with other plugins by higher level services that rely on this contract. This allows for a flexible and extensible system where new plugins can be added without breaking existing functionality, as long as they adhere to the core contract defined by this Plugin protocol.

    Some examples of how the catalog might be used in different plugins:
    - A SQL database plugin might use the catalog to construct a SQL query, using the entities as tables, and columns as fields, and applying filters, sorting, and limits as specified in the catalog.
    - A file-based plugin might use the catalog to determine which files to read or write, and how to structure the data within those files based on the entities and columns defined in the catalog.
    - An API-based plugin might use the catalog to determine which endpoints to call, and how to structure the request and response data based on the entities and columns defined in the catalog.

    One currently implemented example of the catalog and overcoming limitations by a plugin is the Salesforce plugin. Salesforce has a rigid data model and typically only accepts data retrieval for a single object at a time. The plugin implementation then needed to bridge this gap by implementing its own internal strategies to handle catalogs with multiple entities, such as executing multiple queries and stitching the results together with data frames, while still adhering to the core contract of the Plugin protocol. This way, higher level services can still use the Salesforce plugin with multi-entity catalogs without needing to know about the internal complexities of how the plugin handles those cases.

    This is not an exhaustive list of what a plugin facade can provide, just the minimum core contract higher level services can rely on. And to ensure the developer is thinking about the core CRUD operations at every level of the system: Catalog, Entity, Field, and Records, as this program is more than a multi system orchestrator, its a universal data interaction and operator, and these are the core verbs and nouns of that language.

    By ensuring every plugin implements this core contract, services can be designed independent of the plugin. 

    kwargs can be used to pass along additional information your plugin may need. Perhaps you want to pass along a sql string for execution, or a dictionary of binds, or something specialized and specific to your plugin. That's fine, and that's what kwargs are for. This way you don't break other higher level services that rely on the core contract, but you can still provide additional functionality and flexibility for your plugin's unique features and capabilities.
    """
    # catalog aka: schema, namespace, database, folder, directory, etc. - the container of entities/objects/tables/files sharing the same namespace
    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def get_catalog(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Catalog]: ...
    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]: ...
    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    # entity aka: object, table, file/ etc. - the container of columns/fields
    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def get_entity(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Entity]: ...
    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]: ...
    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    # column aka: field, attribute, properties etc. - the individual data points within an entity/table/file that act as locators for the actual data values, and have metadata describing the data type, format, constraints, etc.
    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def get_column(self, catalog: Catalog, **kwargs: Any)    -> PluginResponse[Column]: ...
    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]: ...
    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]: ...

    # data protocols - and how data might be requested and passed
    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowStream]: ...
    def create_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any
                    ) -> PluginResponse[ArrowStream]: ...
    def update_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any
                    ) -> PluginResponse[ArrowStream]: ...
    def upsert_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any
                    ) -> PluginResponse[ArrowStream]: ...
    def delete_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any
                    ) -> PluginResponse[None]: ...
    
    # Plugin Specifics
    # def raw_query(self, **kwargs: Any) -> PluginResponse[ArrowStream]: ...
    # def raw_command(self, **kwargs: Any) -> PluginResponse[Any]: ...

