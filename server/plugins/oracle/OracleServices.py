from typing import Any
import logging

from server.plugins.PluginModels import Catalog, Entity, ArrowStream
from server.plugins.oracle.OracleTypeMap import map_python_to_oracledb_input_size, map_python_to_oracle_ddl
from server.plugins.PluginResponse import PluginResponse
from .OracleEngine import OracleEngine
from .OracleDialect import build_insert_dml, build_update_dml, build_merge_dml, build_delete_dml, build_select
from .OracleArrowFrame import OracleArrowFrame
from .OracleClient import OracleClient

logger = logging.getLogger(__name__)

class OracleService:
    client: OracleClient
    engine: OracleEngine
    arrow_frame: OracleArrowFrame

    def __init__(self, client: OracleClient):
        self.client = client
        self.engine = OracleEngine(schema=self.client.oracle_user.upper(),client=client)
        self.arrow_frame = OracleArrowFrame(client)

    def _build_input_sizes(self, catalog: Catalog) -> dict[str, Any]:
        """Maps the Pydantic fields to oracledb native types."""
        sizes = {}
        for entity in catalog.entities:
            for column in entity.columns:
                column.properties['python_type'] = map_python_to_oracle_ddl(column)  # Ensure the column has a valid mapping
                column.properties['bind_name'] = column.name
                sizes[column.name] = map_python_to_oracledb_input_size(column)
        return sizes

    # READ OPERATIONS
    def get_data(
        self, 
        catalog: Catalog,
        client: OracleClient,
        **kwargs: Any
    ) -> ArrowStream:
        binds: dict[str, Any] = kwargs.get('binds', {})
        
        query = kwargs.get('query', None)
        binds = kwargs.get('binds', {})
        if query: return self.engine.query(query, binds=binds)
        if catalog:
            sql, binds = build_select(catalog)
        
            return self.engine.query(sql, binds)

    # ---------------------------------------------------------
    # WRITE OPERATIONS (Strict Catalog Contracts)
    # ---------------------------------------------------------

    def insert_data(self, catalog: Catalog, data: ArrowStream, client: OracleClient, **kwargs: Any) -> None:
        """INSERT: Streams data into Oracle via the Catalog envelope."""
        for entity in catalog.entities:
            # Notice: The dialect or engine might need the catalog's namespace in the future
            sql = build_insert_dml(catalog, entity)
            input_sizes = self._build_input_sizes(catalog)
            
            self.engine.execute_many(sql, data, input_sizes)

    # OracleServices.py

    def update_data(self, catalog: Catalog, stream: ArrowStream, client: OracleClient, **kwargs) -> PluginResponse[ArrowStream]:
        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for update.")
            
        try:
            # Generate a list of SQL strings for every entity in the catalog
            sql_statements = [build_update_dml(catalog, entity) for entity in catalog.entities]
            
            # Pass the list of statements and the single stream to the execution frame
            self.arrow_frame.fast_update_stream(sql_statements, stream)
            # input_sizes = self._build_input_sizes(catalog)
            
            return PluginResponse.success(None, "Update successful")
        except Exception as e:
            return PluginResponse.error(str(e))
            

    def upsert_data(self, catalog: Catalog, data: ArrowStream, client: OracleClient, **kwargs: Any) -> None:
        """UPSERT: Uses Oracle MERGE statement."""
        for entity in catalog.entities:
            if not entity.primary_key_columns:
                raise ValueError(f"Cannot upsert {entity.name}: No Primary Keys defined.")

            sql = build_merge_dml(catalog, entity)
            input_sizes = self._build_input_sizes(catalog)
            self.engine.execute_many(sql, data, input_sizes)

    def delete_data(self, catalog: Catalog, data: ArrowStream, client: OracleClient, **kwargs: Any) -> None:
        """DELETE: Removes data based on Primary Key."""
        for entity in catalog.entities:
            if not entity.primary_key_columns:
                raise ValueError(f"Cannot delete from {entity.name}: No Primary Keys defined.")
            sql = build_delete_dml(catalog, entity)
            pk_names = {pk.name for pk in entity.primary_key_columns}
            input_sizes = {k: v for k, v in self._build_input_sizes(catalog).items() if k in pk_names}
            
            self.engine.execute_many(sql=sql, data=data, input_sizes=input_sizes)