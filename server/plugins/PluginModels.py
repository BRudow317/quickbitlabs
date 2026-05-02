from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import TYPE_CHECKING, Literal, Any, TypeAlias
from collections.abc import Iterator, Iterable
import pyarrow as pa
from pyarrow import ipc
from fastapi import UploadFile
from server.plugins.PluginRegistry import PLUGIN
from pyarrow.ipc import (
    RecordBatchFileWriter, 
    RecordBatchStreamWriter,
    RecordBatchFileReader,
    RecordBatchStreamReader,
)
    

# https://arrow.apache.org/docs/python/api.html
# https://arrow.apache.org/docs/python/api/datatypes.html
# https://arrow.apache.org/cookbook/py/

# Universal data formats
Records: TypeAlias = Iterable[dict[str, Any]] | dict[str, Any] | list[dict[str, Any]] # aka json/dict
ArrowReader: TypeAlias = pa.RecordBatchReader | RecordBatchStreamReader | RecordBatchFileReader

arrow_type_literal = Literal[
    "null", "bool", "int8", "int16", "int32", "int64", "uint8",
    "uint16", "uint32", "uint64", "float16", "float32", "float64",
    "string", "utf8", "large_string", "binary", "large_binary",
    "date32", "date64", "timestamp_s", "timestamp_ms", "timestamp_us",
    "timestamp_ns", "time32_s", "time32_ms", "time64_us", "time64_ns",
    "duration_s", "duration_ms", "duration_us", "duration_ns",
    "decimal128", "decimal256", "json", "uuid", "string_view",
    "list", "large_list", "struct", "map", "list_view", "large_list_view", "dictionary"
]

ARROW_TYPE: dict[arrow_type_literal, pa.DataType] = {
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
    "duration_ms": pa.duration('ms'),
    "duration_us": pa.duration('us'),
    "duration_ns": pa.duration('ns'),

    # Common Parameterized Defaults
    "decimal128": pa.decimal128(38, 9),
    "decimal256": pa.decimal256(76, 18),

    # Complex types - default parameterizations for schema inference.
    # Use Column.arrow_type_meta to store and reconstruct the specific inner types.
    "list": pa.list_(pa.utf8()),
    "large_list": pa.large_list(pa.utf8()),
    "struct": pa.struct([]),
    "map": pa.map_(pa.utf8(), pa.utf8()),
    "dictionary": pa.dictionary(pa.int32(), pa.utf8()),
}

def pa_type_to_literal(t: pa.DataType) -> arrow_type_literal:
    """Map a PyArrow DataType to its arrow_type_literal string (predicate-based, parameter-agnostic)."""
    if pa.types.is_null(t):         return "null"
    if pa.types.is_boolean(t):      return "bool"
    if pa.types.is_int8(t):         return "int8"
    if pa.types.is_int16(t):        return "int16"
    if pa.types.is_int32(t):        return "int32"
    if pa.types.is_int64(t):        return "int64"
    if pa.types.is_uint8(t):        return "uint8"
    if pa.types.is_uint16(t):       return "uint16"
    if pa.types.is_uint32(t):       return "uint32"
    if pa.types.is_uint64(t):       return "uint64"
    if pa.types.is_float16(t):      return "float16"
    if pa.types.is_float32(t):      return "float32"
    if pa.types.is_float64(t):      return "float64"
    if pa.types.is_decimal(t):      return "decimal128"
    if pa.types.is_large_string(t): return "large_string"
    if pa.types.is_string(t):       return "utf8"
    if pa.types.is_large_binary(t): return "large_binary"
    if pa.types.is_binary(t):       return "binary"
    if pa.types.is_date32(t):       return "date32"
    if pa.types.is_date64(t):       return "date64"
    if pa.types.is_timestamp(t):    return f"timestamp_{t.unit}"  # type: ignore[attr-defined]
    if pa.types.is_time32(t):       return f"time32_{t.unit}"     # type: ignore[attr-defined]
    if pa.types.is_time64(t):       return f"time64_{t.unit}"     # type: ignore[attr-defined]
    if pa.types.is_duration(t):     return f"duration_{t.unit}"   # type: ignore[attr-defined]
    if pa.types.is_list(t):         return "list"
    if pa.types.is_large_list(t):   return "large_list"
    if pa.types.is_struct(t):       return "struct"
    if pa.types.is_map(t):          return "map"
    if pa.types.is_dictionary(t):   return "dictionary"
    return "string"


def pa_type_to_meta(t: pa.DataType) -> dict[str, Any] | None:
    """Extract inner-type parameters from a complex PyArrow type for storage in Column.arrow_type_meta.
    Returns None for scalar types that carry no inner-type metadata.
    """
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return {"value_type": pa_type_to_literal(t.value_type)}  # type: ignore[attr-defined]
    if pa.types.is_struct(t):
        return {
            "fields": [
                {"name": t.field(i).name, "type": pa_type_to_literal(t.field(i).type)}
                for i in range(t.num_fields)  # type: ignore[attr-defined]
            ]
        }
    if pa.types.is_map(t):
        return {
            "key_type": pa_type_to_literal(t.key_type),    # type: ignore[attr-defined]
            "value_type": pa_type_to_literal(t.item_type), # type: ignore[attr-defined]
        }
    if pa.types.is_dictionary(t):
        return {
            "index_type": pa_type_to_literal(t.index_type),  # type: ignore[attr-defined]
            "value_type": pa_type_to_literal(t.value_type),  # type: ignore[attr-defined]
        }
    return None


def _meta_to_pa_type(type_str: str | None) -> pa.DataType | None:
    """Resolve a stored arrow_type_literal string back to a pa.DataType for use inside meta reconstruction."""
    if not type_str:
        return None
    return ARROW_TYPE.get(type_str)  # type: ignore[arg-type]


class Locator(BaseModel):
    """The strict contract defining the absolute origin of a scalar"""
    plugin: PLUGIN | None = None # 'oracle', 'salesforce', 'excel', 'parquet', 'feather', 'frontend', etc..
    url: str | None = None
    is_file: bool = False
    environment: str | None = None # 'dev01', 'sf-devint'
    namespace: str | None = None   # schema or namespace, etc. 'oradwh01', 'sobjects'
    entity_name: str | None = None # 'account', 'financials', tables, etc
    additional_locators: dict[str, Any] | None = None

class Column(BaseModel):
    name: str
    alias: str | None = None
    locator: Locator | None = None
    raw_type: str | None = None
    arrow_type_id: arrow_type_literal | None = None
    arrow_type_meta: dict[str, Any] | None = None
    primary_key: bool = False
    is_unique: bool = False
    is_nullable: bool = True
    is_read_only: bool = False
    is_compound_key: bool = False
    is_foreign_key: bool = False
    foreign_key_entity: str | None = None
    foreign_key_column: str | None = None
    is_foreign_key_enforced: bool = False
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    serialized_null_value: str | None = None
    default_value: Any = None
    enum_values: list[Any] = Field(default_factory=list)
    timezone: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    ordinal_position: int | None = None
    is_computed: bool = False
    is_deprecated: bool = False
    is_hidden: bool = False
    description: str | None = None
    @property
    def arrow_type(self) -> pa.DataType | None:
        """Dynamically build the pa.DataType, accounting for parameterized and complex types."""
        if not self.arrow_type_id: return None
        arrow_type = self.arrow_type_id
        meta = self.arrow_type_meta or {}

        if arrow_type == "decimal256":
            p = self.precision if self.precision is not None else 76
            s = self.scale if self.scale is not None else 18
            return pa.decimal256(p, s)
        if arrow_type.startswith("decimal"):
            p = self.precision if self.precision is not None else 38
            s = self.scale if self.scale is not None else 9
            return pa.decimal128(p, s)
        if arrow_type.startswith("timestamp"):
            unit = arrow_type.split("_")[1]
            return pa.timestamp(unit, tz=self.timezone)

        if arrow_type == "list":
            return pa.list_(_meta_to_pa_type(meta.get("value_type")) or pa.utf8())
        if arrow_type == "large_list":
            return pa.large_list(_meta_to_pa_type(meta.get("value_type")) or pa.utf8())
        if arrow_type == "struct":
            fields = [
                pa.field(f["name"], _meta_to_pa_type(f.get("type")) or pa.utf8())
                for f in meta.get("fields", [])
            ]
            return pa.struct(fields)
        if arrow_type == "map":
            kt = _meta_to_pa_type(meta.get("key_type")) or pa.utf8()
            vt = _meta_to_pa_type(meta.get("value_type")) or pa.utf8()
            return pa.map_(kt, vt)
        if arrow_type == "dictionary":
            it = _meta_to_pa_type(meta.get("index_type")) or pa.int32()
            vt = _meta_to_pa_type(meta.get("value_type")) or pa.utf8()
            return pa.dictionary(it, vt)

        return ARROW_TYPE.get(arrow_type)
    @property
    def qualified_name(self) -> str:
        if self.locator and self.locator.entity_name:
            return ".".join([self.locator.entity_name, self.name])
        return self.name

class Entity(BaseModel):
    name: str
    alias: str | None = None
    namespace: str | None = None
    description: str | None = None
    entity_type: Literal["table", "view", "materialized_view", "external", "api_endpoint", "procedure", "file", "unknown"]  = "unknown"
    plugin: PLUGIN | None = None
    row_count_estimate: int | None = None
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
        if self.namespace:
            return ".".join([self.namespace, self.name])
        return self.name
    @property
    def locator_list(self) -> list[Locator] | None:
        result = [c.locator for c in self.columns if c.locator]
        return result if result else None
    @property
    def locator(self) -> Locator | None:
        """Convenience: returns the first column locator. Use locator_list for all."""
        locs = self.locator_list
        return locs[0] if locs else None

class Sort(BaseModel):
    column: Column
    direction: Literal["ASC", "DESC"] = "ASC"
    nulls_first: bool | None = None

class Join(BaseModel):
    left_entity: Entity
    left_column: Column
    right_entity: Entity
    right_column: Column
    join_type: Literal["INNER", "LEFT", "OUTER"] = "INNER" # removed "RIGHT", 

class Operation(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    independent: Column
    operator: Literal["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "BETWEEN", "NOT BETWEEN", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None

class OperatorGroup(BaseModel):
    condition: Literal["AND", "OR", "NOT"]
    operation_group: list[Operation | OperatorGroup] = Field(default_factory=list)

class Assignment(BaseModel):
    """A scalar mutation: column = value. The operator is implicit — being in catalog.assignments means assignment."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    column: Column
    value: str | list[Any] | pa.Field | Column | None

def _collect_plugins_from_group(group: OperatorGroup) -> set[str]:
    """Recursively collect all plugin names referenced in an OperatorGroup tree."""
    plugins: set[str] = set()
    for op in group.operation_group:
        if isinstance(op, OperatorGroup):
            plugins |= _collect_plugins_from_group(op)
        elif op.independent.locator and op.independent.locator.plugin:
            plugins.add(op.independent.locator.plugin)
    return plugins

class Catalog(BaseModel):
    catalog_id: str | None = None
    name: str | None = None
    alias: str | None = None
    namespace: str | None = None
    version: int = 1
    description: str | None = None
    scope: Literal["SYSTEM", "TEAM", "USER"] = "USER"
    source_type: PLUGIN | Literal["federation"] | None = None
    entities: list[Entity] = Field(default_factory=list)
    filters: list[OperatorGroup] = Field(default_factory=list)
    assignments: list[Assignment] = Field(default_factory=list)
    joins: list[Join] = Field(default_factory=list)
    sort_columns: list[Sort] = Field(default_factory=list)
    limit: int | None = None
    offset: int | None = None
    owner_username: str | None = None
    team_id: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    
    @property
    def arrow_schema(self) -> pa.Schema:
        # Detect columns whose simple name appears in more than one entity (join name conflicts)
        name_counts: dict[str, int] = {}
        for entity in self.entities:
            for column in entity.columns:
                if column.arrow_type:
                    name_counts[column.name] = name_counts.get(column.name, 0) + 1

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
                # Safety-widen decimals to max precision to avoid "precision mismatch" errors
                # TODO: replace with proper column-level sniffing once metadata describe is complete
                if pa.types.is_decimal(arrow_type):
                    arrow_type = pa.decimal128(38, arrow_type.scale)

                # Qualify only conflicting names to keep single-entity schemas unchanged
                field_name = (
                    f"{entity.name}.{column.name}"
                    if name_counts.get(column.name, 0) > 1
                    else column.name
                )
                final_nullable = column.is_nullable or (entity.name in nullable_entities)
                arrow_fields.append(pa.field(field_name, arrow_type, nullable=final_nullable))

        schema = pa.schema(arrow_fields, metadata={
            b"catalog": self.model_dump_json().encode()
        })
        return schema
   
    def arrow_reader(
            self,
            data: Iterable[dict[str,Any]] | pa.Table | pa.RecordBatch | pa.RecordBatchReader | RecordBatchStreamReader, 
            chunk_size: int = 50_000
        ) -> ArrowReader | pa.RecordBatchReader:
        
        schema = self.arrow_schema
        
        if len(schema) == 0 or data is None:
            return pa.RecordBatchReader.from_batches(schema, iter([]))

        if isinstance(data, pa.Table):
            table: pa.Table = data
            if table.schema.equals(schema, check_metadata=False):
                return table.to_reader(max_chunksize=chunk_size)
            return table.cast(schema).to_reader(max_chunksize=chunk_size)

        if isinstance(data, pa.RecordBatch):
            batch: pa.RecordBatch = data
            return pa.RecordBatchReader.from_batches(schema, iter([batch.cast(schema)]))

        if isinstance(data, pa.RecordBatchReader) or isinstance(data, RecordBatchStreamReader):
            reader: pa.RecordBatchReader = data
            def _cast_stream() -> Iterator[pa.RecordBatch]:
                for batch in reader:
                    yield batch.cast(schema)
            return pa.RecordBatchReader.from_batches(schema, _cast_stream())

        # SLOW PATH: Fallback for Python Dicts / JSON
        # Build a mapping: schema field name → source dict key (simple column name).
        # Mirrors the name_counts logic in arrow_schema so qualified names
        # (e.g. "ACCOUNT.ID") resolve back to the plain dict key ("ID").
        _name_counts: dict[str, int] = {}
        for _entity in self.entities:
            for _col in _entity.columns:
                if _col.arrow_type:
                    _name_counts[_col.name] = _name_counts.get(_col.name, 0) + 1
        _field_to_source: dict[str, str] = {}
        for _entity in self.entities:
            for _col in _entity.columns:
                if _col.arrow_type:
                    _fn = (
                        f"{_entity.name}.{_col.name}"
                        if _name_counts.get(_col.name, 0) > 1
                        else _col.name
                    )
                    _field_to_source[_fn] = _col.name

        def batch_generator() -> Iterator[pa.RecordBatch]:
            row_count = 0
            field_map: dict[str, list[Any]] = {name: [] for name in schema.names}

            for row in data:
                for name in schema.names:
                    source_key = _field_to_source.get(name, name) or name
                    field_map[name].append(row.get(source_key))
                row_count += 1
                
                if row_count == chunk_size:
                    arrays = [pa.array(field_map[f], type=schema.field(f).type) for f in schema.names]
                    yield pa.record_batch(arrays, schema=schema)
                    for lst in field_map.values():
                        lst.clear()
                    row_count = 0
                    
            if row_count > 0:
                arrays = [pa.array(field_map[f], type=schema.field(f).type) for f in schema.names]
                yield pa.record_batch(arrays, schema=schema)

        return pa.RecordBatchReader.from_batches(schema, batch_generator())
    
    @staticmethod
    def deserialize_arrow_stream(file: UploadFile) -> pa.RecordBatchReader | ArrowReader:
        """Converts an uploaded binary file back into a PyArrow RecordBatchReader."""
        try:
            record_batch_stream_reader = pa.ipc.open_stream(file.file)
            metadata = record_batch_stream_reader.schema.metadata
            if not metadata or b"catalog" not in metadata:
                raise ValueError("Stream is missing required catalog metadata")
            catalog = Catalog.model_validate_json(metadata[b"catalog"])
            return catalog.arrow_reader(record_batch_stream_reader)
        except pa.ArrowInvalid:
            return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))

    @staticmethod
    def serialize_arrow_stream(stream: ArrowReader | pa.RecordBatchReader) -> bytes:
        """Consumes an ArrowStream and writes it to IPC binary bytes for the HTTP response."""
        if stream is None:
            return b""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, stream.schema) as writer:
            for batch in stream:
                writer.write_batch(batch)
        return sink.getvalue().to_pybytes()


    @staticmethod
    def stream_arrow_ipc(reader: ArrowReader | pa.RecordBatchReader) -> Iterator[bytes]:
        """
        !!! This is a broken function and should not be used. !!!
        Stream Arrow IPC incrementally: yields schema header, then one chunk per batch. 
        """
        sink = pa.BufferOutputStream()
        pos = 0
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            # Schema message is written on context entry
            chunk = sink.getvalue().to_pybytes()
            yield chunk[pos:]
            pos = len(chunk)

            for batch in reader:
                writer.write_batch(batch)
                chunk = sink.getvalue().to_pybytes()
                yield chunk[pos:]
                pos = len(chunk)

        # EOS marker written by context manager exit
        chunk = sink.getvalue().to_pybytes()
        if len(chunk) > pos:
            yield chunk[pos:]

    @property
    def federate(self) -> list[Catalog]:
        """
        Divides a federated master Catalog into plugin-specific child Catalogs.
        Each child carries only entities connected by internal joins within one system.
        Cross-system filters and assignments remain on the master for DuckDB to resolve.
        """
        if not self.entities:
            return []

        # 1. Build internal adjacency list (only joins within the same plugin)
        internal_adj: dict[str, set[str]] = {e.name: set() for e in self.entities}
        for j in self.joins:
            if (j.left_entity.locator and j.right_entity.locator and 
                j.left_entity.locator.plugin == j.right_entity.locator.plugin):
                internal_adj[j.left_entity.name].add(j.right_entity.name)
                internal_adj[j.right_entity.name].add(j.left_entity.name)

        # 2. Find connected clusters within each plugin
        visited = set()
        children: list[Catalog] = []
        
        for root_entity in self.entities:
            if root_entity.name in visited:
                continue
            
            cluster_entities: list[Entity] = []
            stack = [root_entity.name]
            visited.add(root_entity.name)
            
            plugin_name = root_entity.locator.plugin if root_entity.locator else None
            
            while stack:
                curr_name = stack.pop()
                curr_entity = next(e for e in self.entities if e.name == curr_name)
                cluster_entities.append(curr_entity)
                for neighbor_name in internal_adj[curr_name]:
                    if neighbor_name not in visited:
                        visited.add(neighbor_name)
                        stack.append(neighbor_name)
            
            # 3. Build child catalog for this cluster
            child = self.model_copy(update={
                "source_type": plugin_name,
                "entities": cluster_entities,
                "joins": [
                    j for j in self.joins
                    if j.left_entity.name in [e.name for e in cluster_entities]
                    and j.right_entity.name in [e.name for e in cluster_entities]
                ],
                "sort_columns": [
                    s for s in self.sort_columns
                    if s.column.locator and s.column.locator.entity_name in [e.name for e in cluster_entities]
                ],
                "filters": [
                    g for g in self.filters
                    if _collect_plugins_from_group(g) == {plugin_name}
                ],
                "assignments": [
                    a for a in self.assignments
                    if a.column.locator and a.column.locator.plugin == plugin_name
                ],
                "limit": None,      # applied post-federation only
            })
            children.append(child)

        return children

    @staticmethod
    def ipc_new_stream(sink, schema, *, options=None) -> RecordBatchStreamWriter:
        return ipc.new_stream(
            sink=sink, 
            schema=schema, 
            options=options
        )

    @staticmethod
    def ipc_open_stream(source, *, options=None, memory_pool=None) -> RecordBatchStreamReader: 
        return ipc.open_stream(
            source=source, 
            options=options, 
            memory_pool=memory_pool
        )

    @staticmethod
    def ipc_open_file(source, footer_offset=None, *, options=None, memory_pool=None) -> RecordBatchFileReader:
        return ipc.open_file(
            source=source,
            footer_offset=footer_offset,
            options=options,
            memory_pool=memory_pool
        )
    
    @staticmethod
    def ipc_new_file(sink, schema=None, *, options=None, metadata=None) -> RecordBatchFileWriter:
        return ipc.new_file(
            sink=sink,
            schema=schema,
            options=options,
            metadata=metadata
        )

    @staticmethod
    def write_arrow_dataset(data, base_dir, *, basename_template=None, format=None,
                  partitioning=None, partitioning_flavor=None,
                  schema=None, filesystem=None, file_options=None, use_threads=True,
                  preserve_order=False, max_partitions=None, max_open_files=None,
                  max_rows_per_file=None, min_rows_per_group=None,
                  max_rows_per_group=None, file_visitor=None,
                  existing_data_behavior='error', create_dir=True) -> None:
        from pyarrow.dataset import write_dataset
        write_dataset(
            data=data, 
            base_dir=base_dir,
            basename_template=basename_template, format=format,
            partitioning=partitioning, partitioning_flavor=partitioning_flavor,
            schema=schema, 
            filesystem=filesystem,
            file_options=file_options,
            use_threads=use_threads,
            preserve_order=preserve_order,
            max_partitions=max_partitions,
            max_open_files=max_open_files,
            max_rows_per_file=max_rows_per_file, 
            min_rows_per_group=min_rows_per_group,
            max_rows_per_group=max_rows_per_group, 
            file_visitor=file_visitor,
            existing_data_behavior=existing_data_behavior, 
            create_dir=create_dir
            )


Catalog.model_rebuild()
OperatorGroup.model_rebuild()    

catalog_doc_string =     """

    """

file_doc_string = """

    As a pydantic BaseModel you can serialize the entire working contract to JSON with 
        catalog.model_dump()
        or dict with catalog.model_dump_json(),
        and rehydrate with CatalogModel.model_validate()
        or CatalogModel.model_validate_json() respectively
"""


"""
from polars._typing import (
        ArrowArrayExportable,
        ArrowStreamExportable,
        Orientation,
        PolarsDataType,
        SchemaDefinition,
        SchemaDict,
        
    )

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
"""

"""
# python types

"""

"""
import pyarrow.types as types
    from pyarrow.dataset import Dataset, dataset, FileSystemDataset
    from pyarrow import (
        # Buffer
        Buffer,
        # IO
        NativeFile, PythonFile,
        BufferedInputStream, BufferedOutputStream, CacheOptions,
        CompressedInputStream, CompressedOutputStream,
        TransformInputStream, transcoding_input_stream,
        FixedSizeBufferWriter,
        BufferReader, BufferOutputStream,
        OSFile, MemoryMappedFile, memory_map,
        create_memory_map, MockOutputStream,
        input_stream, output_stream,
        have_libhdfs,
        # Data Groupings
        ChunkedArray, 
        RecordBatch, 
        Table, 
        table,
        concat_arrays, 
        concat_tables, 
        TableGroupBy,
        RecordBatchReader, 
        concat_batches,
        # addtl data types
        DataType
        # Exceptions
    )
"""