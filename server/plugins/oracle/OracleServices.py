from typing import Any
import logging

from server.plugins.PluginModels import CatalogModel, EntityModel, Records, QueryModel
from .OracleEngine import OracleEngine
from .OracleDialect import build_dynamic_sql, build_insert_sql, build_update_sql, build_merge_sql, build_delete_sql
from .OracleTypeMap import map_field_to_oracledb_input_size

logger = logging.getLogger(__name__)

class OracleServices:
    """The Orchestrator layer. Bridges the pure Pydantic contracts with the raw execution engine."""
    def __init__(self, engine: OracleEngine):
        self.engine = engine

    def _build_input_sizes(self, entity: EntityModel) -> dict[str, Any]:
        """Maps the Pydantic fields to oracledb native types."""
        sizes = {}
        for field in entity.fields:
            if not field.read_only:
                bind_name = field.target_name or field.source_name
                sizes[bind_name] = map_field_to_oracledb_input_size(field)
        return sizes

    # ---------------------------------------------------------
    # READ OPERATIONS
    # ---------------------------------------------------------

    def get_records(
        self, 
        catalog: CatalogModel | None = None,
        sql_statement: str | None = None,
        model_query: QueryModel | None = None,
        **kwargs: Any
    ) -> Records:
        binds: dict[str, Any] = kwargs.get('binds', {})
        
        # Ensure only exactly ONE method is provided
        provided_args = sum(x is not None for x in [catalog, sql_statement, model_query])
        if provided_args > 1:
            raise ValueError("Ambiguous request: Provide exactly one of catalog, sql_statement, or model_query.")

        if model_query:
            sql, binds = build_dynamic_sql(model_query)
            return self.engine.query(sql, binds=binds)
            
        if sql_statement:
            return self.engine.query(sql_statement, binds=binds)
            
        if catalog:
            logger.warning("Querying full catalog objects. This may result in a Cartesian product.")
            field_names = []
            table_names = []
            
            for entity in catalog.entities:
                table_names.append(entity.source_name)
                for field in entity.fields:
                    field_names.append(f"{entity.source_name}.{field.source_name}")
                    
            select_fields = ", ".join(field_names) if field_names else "*"
            from_tables = ", ".join(table_names)
            
            sql = f"SELECT {select_fields} FROM {from_tables}"
            return self.engine.query(sql, binds=binds)
            
        raise ValueError("Must provide catalog, sql_statement, or model_query.")

    # ---------------------------------------------------------
    # WRITE OPERATIONS (Strict Catalog Contracts)
    # ---------------------------------------------------------

    def insert_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> None:
        """INSERT: Streams records into Oracle via the Catalog envelope."""
        for entity in catalog.entities:
            # Notice: The dialect or engine might need the catalog's namespace in the future
            sql = build_insert_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            
            self.engine.execute_many(sql, records, input_sizes)

    def update_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> None:
        """UPDATE: Updates existing records based on Primary Key."""
        for entity in catalog.entities:
            if not entity.primary_key_fields:
                raise ValueError(f"Cannot update {entity.source_name}: No Primary Keys defined.")

            sql = build_update_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            
            self.engine.execute_many(sql, records, input_sizes)

    def upsert_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> None:
        """UPSERT: Uses Oracle MERGE statement."""
        for entity in catalog.entities:
            if not entity.primary_key_fields:
                raise ValueError(f"Cannot upsert {entity.source_name}: No Primary Keys defined.")

            sql = build_merge_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            self.engine.execute_many(sql, records, input_sizes)

    def delete_records(self, catalog: CatalogModel, records: Records, **kwargs: Any) -> None:
        """DELETE: Removes records based on Primary Key."""
        for entity in catalog.entities:
            if not entity.primary_key_fields:
                raise ValueError(f"Cannot delete from {entity.source_name}: No Primary Keys defined.")

            sql = build_delete_sql(entity)
            
            pk_names = {pk.target_name or pk.source_name for pk in entity.primary_key_fields}
            input_sizes = {k: v for k, v in self._build_input_sizes(entity).items() if k in pk_names}
            
            self.engine.execute_many(sql, records, input_sizes)