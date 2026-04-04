from __future__ import annotations
from pydantic import BaseModel, PrivateAttr, Field
from typing import Literal, Any
from collections.abc import Iterable
from polars import DataFrame
import pyarrow as pa

# universal data formats
Records = Iterable[dict[str, Any]]
PolarsDataFrame = DataFrame

# python types
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date",     # datetime.date
    "time",     # datetime.time
    "binary",   # bytes or bytearray
    "json",     # dict or list
]

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
}

class CatalogModel(BaseModel):
    """One of the smallest, but most important parts of the entire project.
    All metadata exists as a subset of this object.
    Catalog -> Entities -> Fields -> Records(data, records, bytes, json, etc.)

    Catalog is the top-level wrapper. It may be called schema, namespace, database, etc. in different systems, but the concept is the same: a container for entities/objects/tables. 

    When implementing a plugin it is up to you to decide how to route the catalog, but the catalog, its entity, and the fields needed for operations should be passed along. 

    The intention is not to populate an entire catalog and every entity and every field every call, but to provide the relevant metadata needed to perform the requested operation. 

    An example might be implementing a csv plugin where the catalog is the source directory, the entity is the file, and the fields are the columns, and records, the rows.

    Another example might be a REST API plugin where the catalog is the base URL, the entity is the endpoint, and the fields are the query parameters or body parameters needed to perform the request.

    Another example might be a SQL plugin where the catalog is the database, the entity is the table, and the fields are the columns needed to perform the query or DML operation.

    Perhaps you only know the catalog to search for, the target plugin may accept an a catalog with an empty entity list and populate the entire catalog with its metadata, so the caller service can inspect and make relevant decisions about which entities and fields to operate on in subsequent calls.

    Or perhaps you want to know the type of a specific field, so you provide a catalog with one entity and one field, and the plugin returns the catalog with the field's metadata populated.

    This is how the protocols should be designed, and implemented. The catalog is the envelope that carries the metadata and context needed to perform the requested operation.
    
    As a pydantic BaseModel you can serialize the entire working contract to JSON with catalog.json() or dict with catalog.dict(), and rehydrate with CatalogModel.parse_raw() or CatalogModel.parse_obj() respectively.

    It can also serve as a base for implementing openapi schemas, ORM models, or any other structured representation of metadata you need.
    """
    source_name: str = ''
    target_name: str | None = None
    entities: list[EntityModel] = Field(default_factory=list)
    source_description: str | None = None

    @property
    def entity_map(self) -> dict[str, EntityModel]:
        return {e.source_name: e for e in self.entities}

class EntityModel(BaseModel):
    source_name: str
    target_name: str | None = None
    fields: list[FieldModel] = Field(default_factory=list)
    source_description: str | None = None

    @property
    def primary_key_fields(self) -> list[FieldModel]:
        return [f for f in self.fields if f.primary_key]

    @property
    def field_map(self) -> dict[str, FieldModel]:
        return {f.source_name: f for f in self.fields}

class FieldModel(BaseModel):
    source_name: str
    python_type: PythonTypes | None = None
    arrow_type: pa.DataType | None = None
    raw_type: str | None = None
    primary_key: bool = False
    unique: bool = False
    is_foreign_key: bool = False
    foreign_key_table: str | None = None
    foreign_key_column: str | None = None
    is_compound_key: bool = False
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    source_description: str | None = None
    nullable: bool = True
    source_null_value: Any | None = None
    read_only: bool = False
    default_value: Any | None = None
    enum_values: list[Any] = Field(default_factory=list)
    timezone: str | None = None
    target_name: str | None = None



class FilterModel(BaseModel):
    field: str           # e.g., "purchase_date"
    operator: Literal["==", "!=", ">", "<", ">=", "<=", "IN", "LIKE"]
    value: Any           # e.g., "2021-01-01"

class JoinModel(BaseModel):
    left_entity: str
    left_field: str
    right_entity: str
    right_field: str
    join_type: Literal["INNER", "LEFT", "RIGHT", "OUTER"] = "INNER"

class FilterCondition(BaseModel):
    field: str
    operator: Literal["==", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"]
    value: Any

class FilterGroup(BaseModel):
    condition: Literal["AND", "OR", "NOT"]
    # A group can contain basic conditions, or nested groups!
    filters: list[FilterCondition | FilterGroup]

class NativeQuery(BaseModel):
    """The escape hatch for system-specific raw queries."""
    statement: str
    binds: dict[str, Any] = Field(default_factory=dict)

class QueryModel(BaseModel):
    """The universal JSON representation of a data request."""
    entities: list[str] = Field(default_factory=list)         
    fields: list[str] = Field(default_factory=list)            
    
    # Replaces the flat list to support AND/OR/NOT nesting
    filter_group: FilterGroup | None = None 
    joins: list[JoinModel] = Field(default_factory=list)
    limit: int | None = None
    
    native: NativeQuery | None = None
