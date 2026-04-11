from typing import Any
import logging

from server.plugins.PluginModels import Catalog, Entity, Column, ArrowStream
from server.plugins.oracle.OracleTypeMap import (
    map_oracle_to_arrow,
    map_oracle_to_python,
    map_python_to_oracledb_input_size,
    map_python_to_oracle_ddl,
    map_arrow_to_oracle_ddl,
    map_column_to_oracledb_input_size,
)
from server.plugins.PluginResponse import PluginResponse
from .OracleEngine import OracleEngine, OracleSchema, OracleTable
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
        """Maps columns to oracledb native types for cursor.setinputsizes().
        Prefers arrow_type_id (universal) over raw_type (Oracle-specific)."""
        sizes = {}
        for entity in catalog.entities:
            for column in entity.columns:
                if column.arrow_type_id:
                    sizes[column.name] = map_column_to_oracledb_input_size(column)
                elif column.raw_type:
                    if not column.properties.get("python_type"):
                        column.properties["python_type"] = map_oracle_to_python(column.raw_type, column.scale)
                    column.properties["oracle_ddl"] = map_python_to_oracle_ddl(column)
                    column.properties['bind_name'] = column.name
                    sizes[column.name] = map_python_to_oracledb_input_size(column)
                else:
                    sizes[column.name] = None
        return sizes

    # READ OPERATIONS
    def _resolve_schema_name(self, catalog: Catalog) -> str:
        return (catalog.name or catalog.qualified_name or self.client.oracle_user).upper()

    def _list_schema_tables(self, schema_name: str) -> list[str]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        return schema.list_table_names()

    def _fetch_table_primary_keys(self, schema_name: str, table_name: str) -> set[str]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        return table.fetch_primary_keys()

    def _fetch_table_columns(
        self,
        schema_name: str,
        table_name: str,
        requested_columns: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        schema = OracleSchema(client=self.client, schema_name=schema_name)
        table = OracleTable(table_name=table_name, schema=schema)
        rows = table._fetch_tab_columns or []
        if not requested_columns:
            return rows
        requested = {c.upper() for c in requested_columns}
        return [row for row in rows if str(row.get("COLUMN_NAME", "")).upper() in requested]

    def _column_from_row(self, table_name: str, row: dict[str, Any], primary_keys: set[str]) -> Column:
        name = str(row["COLUMN_NAME"])
        raw_type = str(row["DATA_TYPE"])
        scale = row["DATA_SCALE"]
        precision = row["DATA_PRECISION"]
        max_length = row["CHAR_LENGTH"]
        return Column(
            name=name,
            qualified_name=f"{table_name}.{name}",
            raw_type=raw_type,
            arrow_type_id=map_oracle_to_arrow(raw_type, scale),
            primary_key=name in primary_keys,
            is_nullable=str(row["NULLABLE"]).upper() == "Y",
            max_length=int(max_length) if max_length is not None else None,
            precision=int(precision) if precision is not None else None,
            scale=int(scale) if scale is not None else None,
            properties={"python_type": map_oracle_to_python(raw_type, scale)},
        )
    
    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> Catalog:
        schema_name = self._resolve_schema_name(catalog)

        if not catalog.entities:
            entities: list[Entity] = []
            for table_name in self._list_schema_tables(schema_name):
                pk_set = self._fetch_table_primary_keys(schema_name, table_name)
                rows = self._fetch_table_columns(schema_name, table_name)
                columns = [self._column_from_row(table_name, row, pk_set) for row in rows]
                entities.append(
                    Entity(
                        name=table_name,
                        qualified_name=f"{schema_name}.{table_name}",
                        columns=columns,
                    )
                )
            catalog.entities = entities
            catalog.name = schema_name
            catalog.qualified_name = schema_name
            return catalog

        for idx, entity in enumerate(catalog.entities):
            table_name = (entity.name or entity.qualified_name or "").upper()
            if not table_name:
                continue

            requested_names: set[str] | None = None
            if entity.columns:
                requested_names = {c.name.upper() for c in entity.columns if c.name}

            pk_set = self._fetch_table_primary_keys(schema_name, table_name)
            rows = self._fetch_table_columns(schema_name, table_name, requested_names)
            fetched_by_name = {
                str(row["COLUMN_NAME"]).upper(): self._column_from_row(table_name, row, pk_set)
                for row in rows
            }

            if entity.columns:
                populated_cols: list[Column] = []
                for col in entity.columns:
                    match = fetched_by_name.get(col.name.upper()) if col.name else None
                    populated_cols.append(match if match else col)
                entity.columns = populated_cols
            else:
                entity.columns = list(fetched_by_name.values())

            entity.name = table_name
            entity.qualified_name = f"{schema_name}.{table_name}"
            catalog.entities[idx] = entity

        catalog.name = schema_name
        catalog.qualified_name = schema_name
        return catalog

    def get_data(
        self, 
        catalog: Catalog,
        **kwargs: Any
    ) -> ArrowStream:
        binds: dict[str, Any] = kwargs.get('binds', {})
        
        query = kwargs.get('query', None)
        binds = kwargs.get('binds', {})
        if query: return self.arrow_frame.arrow_stream(query, parameters=binds)
        if catalog:
            sql, binds = build_select(catalog)
        
            return self.arrow_frame.arrow_stream(sql, parameters=binds)

    # ---------------------------------------------------------
    # WRITE OPERATIONS (Strict Catalog Contracts)
    # ---------------------------------------------------------

    def insert_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> None:
        """INSERT: Streams data into Oracle via the Catalog envelope."""
        for entity in catalog.entities:
            sql = build_insert_dml(catalog, entity)
            input_sizes = self._build_input_sizes(catalog)
            self.arrow_frame.execute_many(sql, data, input_sizes)


    def update_data(self, catalog: Catalog, stream: ArrowStream, **kwargs) -> PluginResponse[None]:
        if not catalog.entities:
            return PluginResponse.error("Catalog must contain at least one entity for update.") 
        try:
            # Generate a list of SQL strings for every entity in the catalog
            sql_statements = (build_update_dml(catalog, entity) for entity in catalog.entities)
            self.arrow_frame.execute_many(sql=sql_statements, data=stream)
            return PluginResponse.success(None, "Update successful")
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: Catalog, data: ArrowStream,  **kwargs: Any) -> None:
        """UPSERT: Uses Oracle MERGE statement."""
        for entity in catalog.entities:
            sql = build_merge_dml(catalog, entity)
            input_sizes = self._build_input_sizes(catalog)
            self.arrow_frame.execute_many(sql, data, input_sizes)

    def delete_data(self, catalog: Catalog, data: ArrowStream,  **kwargs: Any) -> None:
        """DELETE: Removes data based on Primary Key."""
        for entity in catalog.entities:
            sql, binds = build_delete_dml(catalog, entity)
            input_sizes = self._build_input_sizes(catalog)
            self.arrow_frame.execute_many(sql=sql, data=data, input_sizes=input_sizes)

    # ---------------------------------------------------------
    # DDL / SCHEMA OPERATIONS
    # ---------------------------------------------------------

    def _build_column_ddl(self, column: Column) -> str:
        """Build a single column DDL clause from a Column model."""
        if column.arrow_type_id:
            ddl_type = map_arrow_to_oracle_ddl(column)
        elif column.raw_type:
            if not column.properties.get("python_type"):
                column.properties["python_type"] = map_oracle_to_python(column.raw_type, column.scale)
            ddl_type = map_python_to_oracle_ddl(column)
        else:
            ddl_type = "VARCHAR2(255 CHAR)"
        nullable = "NULL" if column.is_nullable else "NOT NULL"
        return f"{column.name} {ddl_type} {nullable}"

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """Hydrate a single entity with column metadata from Oracle."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema_name(catalog)
        entity = catalog.entities[0]
        table_name = (entity.name or entity.qualified_name or "").upper()
        pk_set = self._fetch_table_primary_keys(schema_name, table_name)
        rows = self._fetch_table_columns(schema_name, table_name)
        columns = [self._column_from_row(table_name, row, pk_set) for row in rows]
        return Entity(name=table_name, qualified_name=f"{schema_name}.{table_name}", columns=columns)

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """CREATE TABLE from the first entity in catalog."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema_name(catalog)
        entity = catalog.entities[0]
        table_name = entity.name.upper()

        col_defs = [self._build_column_ddl(col) for col in entity.columns]

        pk_cols = entity.primary_key_columns
        constraint = ""
        if pk_cols:
            pk_names = ", ".join(c.name for c in pk_cols)
            constraint = f", CONSTRAINT {table_name}_PK PRIMARY KEY ({pk_names})"

        sql = f"CREATE TABLE {schema_name}.{table_name} ({', '.join(col_defs)}{constraint})"
        logger.info(f"DDL: {sql[:200]}")
        self.engine.execute_ddl(sql)
        return entity

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> Entity:
        """CREATE TABLE if it doesn't exist, or ALTER TABLE ADD missing columns."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity.")
        schema_name = self._resolve_schema_name(catalog)
        entity = catalog.entities[0]
        table_name = entity.name.upper()

        existing_tables = {t.upper() for t in self._list_schema_tables(schema_name)}
        if table_name not in existing_tables:
            logger.info(f"Table {schema_name}.{table_name} does not exist — creating.")
            return self.create_entity(catalog, **kwargs)

        # Table exists — check for missing columns
        existing_rows = self._fetch_table_columns(schema_name, table_name) or []
        existing_names = {str(r["COLUMN_NAME"]).upper() for r in existing_rows}

        new_cols = [c for c in entity.columns if c.name.upper() not in existing_names]
        if new_cols:
            col_defs = [self._build_column_ddl(col) for col in new_cols]
            sql = f"ALTER TABLE {schema_name}.{table_name} ADD ({', '.join(col_defs)})"
            logger.info(f"DDL: {sql[:200]}")
            self.engine.execute_ddl(sql)
            logger.info(f"Added {len(new_cols)} column(s) to {schema_name}.{table_name}.")
        else:
            logger.info(f"Table {schema_name}.{table_name} already aligned — no DDL needed.")

        return entity

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> Catalog:
        """Upsert all entities in catalog (CREATE/ALTER tables as needed)."""
        for entity in catalog.entities:
            sub = catalog.model_copy(update={"entities": [entity]})
            self.upsert_entity(sub, **kwargs)
        return catalog

    def get_column(self, catalog: Catalog, **kwargs: Any) -> Column:
        """Fetch a single column's metadata from Oracle."""
        if not catalog.entities or not catalog.entities[0].columns:
            raise ValueError("Catalog must specify an entity with at least one column.")
        schema_name = self._resolve_schema_name(catalog)
        entity = catalog.entities[0]
        col = entity.columns[0]
        table_name = (entity.name or "").upper()
        pk_set = self._fetch_table_primary_keys(schema_name, table_name)
        rows = self._fetch_table_columns(schema_name, table_name, {col.name.upper()})
        if not rows:
            raise ValueError(f"Column '{col.name}' not found on {schema_name}.{table_name}.")
        return self._column_from_row(table_name, rows[0], pk_set)

    def create_column(self, catalog: Catalog, **kwargs: Any) -> Column:
        """ALTER TABLE ADD a single column."""
        if not catalog.entities or not catalog.entities[0].columns:
            raise ValueError("Catalog must specify an entity with at least one column.")
        schema_name = self._resolve_schema_name(catalog)
        entity = catalog.entities[0]
        col = entity.columns[0]
        table_name = (entity.name or "").upper()
        col_ddl = self._build_column_ddl(col)
        sql = f"ALTER TABLE {schema_name}.{table_name} ADD ({col_ddl})"
        logger.info(f"DDL: {sql}")
        self.engine.execute_ddl(sql)
        return col