from typing import Any
import logging

from server.plugins.PluginModels import Catalog, Entity, ArrowStream
from .OracleEngine import OracleEngine
from .OracleDialect import build_dynamic_sql, build_insert_sql, build_update_sql, build_merge_sql, build_delete_sql
from .OracleTypeMap import map_field_to_oracledb_input_size

logger = logging.getLogger(__name__)

class OracleServices:
    """The Orchestrator layer. Bridges the pure Pydantic contracts with the raw execution engine."""
    def __init__(self, engine: OracleEngine):
        self.engine = engine

    def _build_input_sizes(self, entity: Entity) -> dict[str, Any]:
        """Maps the Pydantic fields to oracledb native types."""
        sizes = {}
        for field in entity.columns:
            if not field.is_read_only:
                bind_name = field.name
                sizes[bind_name] = map_field_to_oracledb_input_size(field)
        return sizes

    # ---------------------------------------------------------
    # READ OPERATIONS
    # ---------------------------------------------------------

    def get_data(
        self, 
        catalog: Catalog | None = None,
        sql_statement: str | None = None,
        **kwargs: Any
    ) -> ArrowStream:
        binds: dict[str, Any] = kwargs.get('binds', {})
        
        # Ensure only exactly ONE method is provided
        provided_args = sum(x is not None for x in [catalog, sql_statement, kwargs.get('model_query')])
        if provided_args > 1:
            raise ValueError("Ambiguous request: Provide exactly one of catalog, sql_statement, or model_query.")

        model_query = kwargs.get('model_query')
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
                table_names.append(entity.name)
                for field in entity.columns:
                    field_names.append(f"{entity.name}.{field.name}")
                    
            select_fields = ", ".join(field_names) if field_names else "*"
            from_tables = ", ".join(table_names)
            
            sql = f"SELECT {select_fields} FROM {from_tables}"
            return self.engine.query(sql, binds=binds)
            
        raise ValueError("Must provide catalog, sql_statement, or model_query.")

    # ---------------------------------------------------------
    # WRITE OPERATIONS (Strict Catalog Contracts)
    # ---------------------------------------------------------

    def insert_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> None:
        """INSERT: Streams data into Oracle via the Catalog envelope."""
        for entity in catalog.entities:
            # Notice: The dialect or engine might need the catalog's namespace in the future
            sql = build_insert_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            
            self.engine.execute_many(sql, data, input_sizes)

    def update_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> None:
        """UPDATE: Updates existing data based on Primary Key."""
        for entity in catalog.entities:
            if not entity.primary_key_columns:
                raise ValueError(f"Cannot update {entity.name}: No Primary Keys defined.")

            sql = build_update_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            
            self.engine.execute_many(sql, data, input_sizes)

    def upsert_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> None:
        """UPSERT: Uses Oracle MERGE statement."""
        for entity in catalog.entities:
            if not entity.primary_key_columns:
                raise ValueError(f"Cannot upsert {entity.name}: No Primary Keys defined.")

            sql = build_merge_sql(entity)
            input_sizes = self._build_input_sizes(entity)
            self.engine.execute_many(sql, data, input_sizes)

    def delete_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> None:
        """DELETE: Removes data based on Primary Key."""
        for entity in catalog.entities:
            if not entity.primary_key_columns:
                raise ValueError(f"Cannot delete from {entity.name}: No Primary Keys defined.")

            sql = build_delete_sql(entity)
            
            pk_names = {pk.name for pk in entity.primary_key_columns}
            input_sizes = {k: v for k, v in self._build_input_sizes(entity).items() if k in pk_names}
            
            self.engine.execute_many(sql, data, input_sizes)