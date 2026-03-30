from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .PluginModels import CatalogModel, EntityModel, FieldModel, Records, QueryModel

from .PluginResponse import PluginResponse

@runtime_checkable
class Plugin(Protocol):
    """
    Universal interface for any data system.
    Five verbs:  create (DDL), get/read, update/alter, upsert, delete/drop
    Three nouns: catalog, entity, field
    One stream:  Records (Iterable[dict[str, Any]])
    
    All operations return PluginResponse[T] — never raises for expected failures.
    PluginResponse.not_implemented() so the caller stays in control.
    Each Plugin handles its own batch limits internally.
    
    The catalog is the envelope that carries the metadata and context 
    needed to perform the requested operation.

    This is not an exhaustive list of what a plugin facade can provide, just the minimum core contract higher level services can rely on. And to ensure the developer is thinking about the core CRUD operations at every level of the system: Catalog, Entity, Field, and Records, as this program is more than a multi system orchestrator, its a universal data interaction and operator, and these are the core verbs and nouns of that language.

    By ensuring every plugin implements this core contract, services can be designed independent of the plugin. 

    kwargs can be used to pass along additional information your plugin may need. Perhaps you want to pass along a sql string for execution, or a dictionary of binds, or something specialized and specific to your plugin. That's fine, and that's what kwargs are for. This way you don't break other higher level services that rely on the core contract, but you can still provide additional functionality and flexibility for your plugin's unique features and capabilities.
    """

    def create_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def get_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def update_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def upsert_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def delete_catalog(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    # entity aka: object, table, etc. — the container of fields/columns

    def create_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def get_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def update_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def upsert_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def delete_entity(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    # field aka: column, attribute, properties etc. — the individual data points within an entity/table

    def create_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def get_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def update_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def upsert_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    def delete_field(self, catalog: CatalogModel, **kwargs: Any) -> PluginResponse[CatalogModel]: ...

    # Records aka: rows, tuples, documents, etc. — the individual data entries within an entity/table

    # Records = Iterable[dict[str, Any]]
    def insert_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]: ...
    
    def get_records(self, 
                    catalog: CatalogModel | None = None, 
                    query: QueryModel | None = None, 
                    **kwargs: Any) -> PluginResponse[Records]: 
        """Reads data. Must accept either a Catalog Envelope or a QueryModel AST."""
        ...
        
    def update_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]: ...
    def upsert_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]: ...
    def delete_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> PluginResponse[Records]: ...