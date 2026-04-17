
from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal, Any, TypeAlias
from collections.abc import Iterator, Iterable
import pyarrow as pa
from polars._typing import (
        ArrowArrayExportable,
        ArrowStreamExportable,
        Orientation,
        PolarsDataType,
        SchemaDefinition,
        SchemaDict,
        
    )

# Universal data formats
# https://arrow.apache.org/docs/python/api/datatypes.html
# https://arrow.apache.org/cookbook/py/

Records: TypeAlias = Iterable[dict[str, Any]] # aka json/dict
ArrowStream: TypeAlias = pa.RecordBatchReader
# Polars zero-copy data frame interchange protocol types
# polars_data_frame: TypeAlias = SupportsInterchange
polars_arrow_stream: TypeAlias = ArrowStreamExportable
polars_arrow_array: TypeAlias = ArrowArrayExportable
polars_orientation: TypeAlias = Orientation
polars_type: TypeAlias = PolarsDataType
polars_schema: TypeAlias = SchemaDefinition
polars_schema_dict: TypeAlias = SchemaDict

arrow_field: TypeAlias = pa.Field
arrow_table: TypeAlias = pa.Table
arrow_schema: TypeAlias = pa.Schema
arrow_batch: TypeAlias = pa.RecordBatch
arrow_array: TypeAlias = pa.Array
arrow_chunked_array: TypeAlias = pa.ChunkedArray
arrow_table_groupby: TypeAlias = pa.TableGroupBy
# arrow_interchange_protocol: TypeAlias = pa.interchange.from_dataframe

arrow_type: TypeAlias = pa.DataType
arrow_dict: TypeAlias = pa.DictionaryType
arrow_json: TypeAlias = pa.JsonType
arrow_struct: TypeAlias = pa.StructType
arrow_map: TypeAlias = pa.MapType
arrow_list: TypeAlias = pa.ListType
arrow_large_list: TypeAlias = pa.LargeListType
arrow_list_view: TypeAlias = pa.ListViewType
arrow_large_list_view: TypeAlias = pa.LargeListViewType



arrow_types = {
    # Primitives
    "null": pa.null(),
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    
    # Binary & String
    "string": pa.string(),
    "string_view": pa.string_view(),
    "utf8": pa.utf8(),
    "large_string": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    
    # Temporal (Default units)
    "date32": pa.date32(),
    "date64": pa.date64(),
    "timestamp_s": pa.timestamp('s'),
    "timestamp_ms": pa.timestamp('ms'),
    "timestamp_us": pa.timestamp('us'),
    "timestamp_ns": pa.timestamp('ns'),
    "time32_s": pa.time32('s'),
    "time32_ms": pa.time32('ms'),
    "time64_us": pa.time64('us'),
    "time64_ns": pa.time64('ns'),
    "duration_s": pa.duration('s'),
    
    # Common Parameterized Defaults
    "decimal128": pa.decimal128(38, 9),
    "decimal256": pa.decimal256(76, 18),

    # Complex Types
    "json": pa.json_(),
    "uuid": pa.uuid(),
    "list": pa.list_(),
    "large_list": pa.large_list(),
    "list_view": pa.list_view(),
    "large_list_view": pa.large_list_view(),
    "dictionary": pa.dictionary(),
    "struct": pa.struct(),
    "map": pa.map_(),
}

arrow_type_literal = Literal[
    "null", "bool", "int8", "int16", "int32", "int64", "uint8", 
    "uint16", "uint32", "uint64", "float16", "float32", "float64",
    "string", "utf8", "large_string", "binary", "large_binary",
    "date32", "date64", "timestamp_s", "timestamp_ms", "timestamp_us", 
    "timestamp_ns", "time32_s", "time32_ms", "time64_us", "time64_ns", 
    "duration_s", "decimal128", "decimal256", "json", "uuid", "string_view", "list", "large_list", "struct", "map", "list_view", "large_list_view", "dictionary"
]

# python types
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date",     # datetime.date
    "time",     # datetime.time
    "byte",
    "bytearray",
    "json",     # dict or list
]

class Column(BaseModel):
    name: str
    alias: str | None = None
    parent_names: list[str] | None = None
    raw_type: str | None = None
    arrow_type_id: arrow_type_literal | None = None
    primary_key: bool = False
    is_unique: bool = False
    is_nullable: bool = True
    is_read_only: bool = False
    is_compound_key: bool = False
    is_foreign_key: bool = False
    foreign_key_entity: str | None = None
    foreign_key_column: str | None = None
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    serialized_null_value: str | None = None
    default_value: Any = None
    enum_values: list[Any] = Field(default_factory=list)
    timezone: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    @property
    def arrow_type(self) -> pa.DataType | None:
        """Dynamically build the C++ object, accounting for parameterized types."""
        if not self.arrow_type_id: return None
        arrow_type = self.arrow_type_id
        if arrow_type.startswith("decimal"):
            p = self.precision if self.precision is not None else 38
            s = self.scale if self.scale is not None else 9
            return pa.decimal128(p, s)  
        if arrow_type.startswith("timestamp"):
            unit = arrow_type.split("_")[1]
            return pa.timestamp(unit, tz=self.timezone)
        return arrow_types.get(arrow_type)
    @property
    def qualified_name(self) -> str:
        if self.parent_names:
            return ".".join(self.parent_names + [self.name])
        return self.name

class Entity(BaseModel):
    name: str
    alias: str | None = None
    parent_names: list[str] | None = None
    columns: list[Column] = Field(default_factory=list)
    properties: dict[str, Any] = Field(default_factory=dict)
    @property
    def primary_key_columns(self) -> list[Column]:
        return [f for f in self.columns if f.primary_key]
    @property
    def column_map(self) -> dict[str, Column]:
        return {f.name: f for f in self.columns}
    @property
    def qualified_name(self) -> str:
        if self.parent_names:
            return ".".join(self.parent_names + [self.name])
        return self.name

class Sort(BaseModel):
    entity: Entity
    column: Column
    direction: Literal["ASC", "DESC"] = "ASC"

class Join(BaseModel):
    left_catalog: Catalog | None = None
    left_entity: Entity
    left_column: Column
    right_catalog: Catalog | None = None
    right_entity: Entity
    right_column: Column
    join_type: Literal["INNER", "LEFT", "OUTER"] = "INNER" # removed "RIGHT", 

class Operator(BaseModel):
    """The single equal sign '=' is an assignment operator, it is not an equality operator."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    independent: Column
    operator: Literal["=", "==", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None

class OperatorGroup(BaseModel):
    condition: Literal["AND", "OR", "NOT"]
    operators: list[Operator | OperatorGroup] = Field(default_factory=list)

class Catalog(BaseModel):
    name: str | None = None
    qualified_name: str | None = None
    entities: list[Entity] = Field(default_factory=list)
    operator_groups: list[OperatorGroup] = Field(default_factory=list)
    joins: list[Join] = Field(default_factory=list)
    sort_fields: list[Sort] = Field(default_factory=list)
    limit: int | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

    @property
    def entity_map(self) -> dict[str, Entity]:
        return {e.name: e for e in self.entities}
    
# Catalog.model_rebuild()
    def arrow_schema(self) -> pa.Schema:
        nullable_entities: set[str] = set()
        for j in self.joins:
            if j.join_type in ("LEFT", "OUTER"):
                nullable_entities.add(j.right_entity.name)
            if j.join_type in ("RIGHT", "OUTER"):
                nullable_entities.add(j.left_entity.name)

        arrow_fields = []
        for entity in self.entities:
            for column in entity.columns:
                arrow_type = column.arrow_type
                if not arrow_type: continue
                
                # Safety-widen decimals to max precision (38) to avoid "precision mismatch" errors
                # TODO implement proper sniffing and metadata describe logic to set the correct precision and scale in Column.arrow_type to avoid this inefficiency
                if pa.types.is_decimal(arrow_type):
                    arrow_type = pa.decimal128(38, arrow_type.scale)
                    
                final_nullable = column.is_nullable or (entity.name in nullable_entities)
                arrow_fields.append(pa.field(f"{entity.name}_{column.name}", arrow_type, nullable=final_nullable))

        schema = pa.schema(arrow_fields)
        return schema

    def arrow_stream(self, data: Records|ArrowStream, chunk_size: int = 50_000) -> ArrowStream:
        schema = self.arrow_schema()
        if len(schema) == 0:
            return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))

        def batch_generator() -> Iterator[pa.RecordBatch]:
            row_count = 0
            field_map: dict[str, list[Any]] = {name: [] for name in schema.names}
            for row in data if data else []:
                for name in schema.names:
                    field_map[name].append(row.get(name))
                row_count += 1
                if row_count == chunk_size:
                    arrays = [pa.array(field_map[f], type=schema.field(f).type) for f in schema.names]
                    yield pa.record_batch(arrays, schema=schema)
                    field_map = {name: [] for name in schema.names}
                    row_count = 0
            if row_count > 0:
                arrays = [pa.array(field_map[f], type=schema.field(f).type) for f in schema.names]
                yield pa.record_batch(arrays, schema=schema)

        return pa.RecordBatchReader.from_batches(schema, batch_generator())


catalog_doc_string =     """
    Catalog is the top-level wrapper. It may be called schema, namespace, database, etc. in different systems, 
    but the concept is the same: a container for entities/objects/tables sharing the same namespace.

    Catalog acts as the envelope that carries the metadata and context needed to perform the requested operation. 
    It may be populated with all entities and columns, or just the relevant ones needed for a specific operation. 

    Higher level services can utilize catalogs in different ways, some examples:
    - Provide an empty catalog with just a name to retrieve the entire schema metadata.
    - Provide a catalog with one entity and one column to retrieve the metadata for that specific column
    - Provide a catalog with multiple entities and columns to perform operations on those specific objects.
    - Provide a catalog with relevant entities and columns as the envelope to carry the metadata needed to 
        perform the requested operation.

    As a pydantic BaseModel you can serialize the entire working contract to JSON with 
        catalog.model_dump()
        or dict with catalog.model_dump_json(),
        and rehydrate with Catalog.model_validate()
        or Catalog.model_validate_json() respectively
    """

file_doc_string = """
    When implementing a plugin it is up to you to decide how to route the catalog, but the catalog or an entity, and the columns needed for operations should be passed along. 

    The intention is not to populate an entire catalog and every entity and every column every call, but to provide the relevant metadata needed to perform the requested operation. 

    The standard objects are not an exhaustive list of what a plugin facade can provide, just the minimum columns higher level services can rely building upon.

    An example might be implementing a csv plugin where the catalog is the source directory, the entity is the file, and the columns are the columns, and records, the rows.

    Another example might be a REST API plugin where the catalog is the base URL, the entity is the endpoint, and the columns are the query parameters or body parameters needed to perform the request.

    Another example might be a SQL plugin where the catalog is the database, the entity is the table, and the columns are the columns needed to perform the query or DML operation.

    Perhaps you only know the catalog to search for, the target plugin may accept an a catalog with an empty entity list and populate the entire catalog with its metadata, so the caller service can inspect and make relevant decisions about which entities and columns to operate on in subsequent calls.

    Or perhaps you want to know the type of a specific column, so you provide a catalog with one entity and one column, and the plugin returns the catalog with the column's metadata populated or just an entity with its columns. 

    This is how the protocols should be designed, and implemented. The catalog or the entity is the envelope that carries the metadata and context needed to perform the requested operation.
    
    As a pydantic BaseModel you can serialize the entire working contract to JSON with 
        catalog.model_dump()
        or dict with catalog.model_dump_json(),
        and rehydrate with CatalogModel.model_validate()
        or CatalogModel.model_validate_json() respectively

    It can also serve as a base for implementing openapi schemas, ORM models, or any other structured representation of metadata you need.

"""