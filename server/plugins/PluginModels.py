from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import TYPE_CHECKING, Literal, Any, TypeAlias
from collections.abc import Iterator, Iterable
import pyarrow as pa
from pyarrow import ipc
from fastapi import UploadFile
from server.plugins.PluginRegistry import PLUGIN

if TYPE_CHECKING:
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
Records: TypeAlias = Iterable[dict[str, Any]] # aka json/dict
ArrowReader: TypeAlias = pa.RecordBatchReader

arrow_type_literal = Literal[
    "null", "bool", "int8", "int16", "int32", "int64", "uint8", 
    "uint16", "uint32", "uint64", "float16", "float32", "float64",
    "string", "utf8", "large_string", "binary", "large_binary",
    "date32", "date64", "timestamp_s", "timestamp_ms", "timestamp_us", 
    "timestamp_ns", "time32_s", "time32_ms", "time64_us", "time64_ns", 
    "duration_s", "decimal128", "decimal256", "json", "uuid", "string_view", "list", "large_list", "struct", "map", "list_view", "large_list_view", "dictionary"
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
    
    # Common Parameterized Defaults
    "decimal128": pa.decimal128(38, 9),
    "decimal256": pa.decimal256(76, 18),

    # Complex Types
    # "json": pa.json_(),
    # "uuid": pa.uuid(),
    # "list": pa.list_(),
    # "large_list": pa.large_list(),
    # "list_view": pa.list_view(),
    # "large_list_view": pa.large_list_view(),
    # "dictionary": pa.dictionary(),
    # "struct": pa.struct(),
    # "map": pa.map_(),
}

class Locator(BaseModel):
    """The strict contract defining the absolute origin of a scalar"""
    plugin: PLUGIN | None = None # 'oracle', 'salesforce', 'excel', 'parquet', 'feather', 'frontend', etc..
    environment: str | None = None # 'dev01', 'sf-devint'
    namespace: str | None = None   # schema or namespace, etc. 'oradwh01', 'sobjects'
    entity_name: str | None = None # 'account', 'financials', tables, etc
    additional_locators: dict | None = None
    @property
    def fully_path(self) -> str:
        parts = [self.plugin, self.environment, self.namespace, self.entity_name]
        return ".".join([p for p in parts if p])

class Column(BaseModel):
    name: str
    alias: str | None = None
    locator: Locator | None = None
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
    def locator(self) -> Locator | None:
        if len(self.columns) > 0:
            return self.columns[0].locator

class Sort(BaseModel):
    column: Column
    direction: Literal["ASC", "DESC"] = "ASC"

class Join(BaseModel):
    left_entity: Entity
    left_column: Column
    right_entity: Entity
    right_column: Column
    join_type: Literal["INNER", "LEFT", "OUTER"] = "INNER" # removed "RIGHT", 

class Operation(BaseModel):
    """The single equal sign '=' is an assignment operator, it is not an equality operator."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    independent: Column
    operator: Literal["=", "==", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"]
    dependent: str | list[Any] | pa.Field | Column | None

class OperatorGroup(BaseModel):
    condition: Literal["AND", "OR", "NOT"]
    operation_group: list[Operation | OperatorGroup] = Field(default_factory=list)

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
    scope: Literal["SYSTEM", "TEAM", "USER"] = "USER"
    source_type: PLUGIN | Literal["federation"] | None = None
    entities: list[Entity] = Field(default_factory=list)
    operator_groups: list[OperatorGroup] = Field(default_factory=list)
    joins: list[Join] = Field(default_factory=list)
    sort_columns: list[Sort] = Field(default_factory=list)
    limit: int | None = None
    owner_user_id: str | None = None
    team_id: str | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    
    @property
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
                arrow_fields.append(pa.field(f"{column.name}", arrow_type, nullable=final_nullable))

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
        def batch_generator() -> Iterator[pa.RecordBatch]:
            row_count = 0
            field_map: dict[str, list[Any]] = {name: [] for name in schema.names}
            
            for row in data:
                for name in schema.names:
                    field_map[name].append(row.get(name))
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


    def stream_arrow_ipc(self, reader: ArrowReader | pa.RecordBatchReader) -> Iterator[bytes]:
        """Efficient Arrow IPC streaming using native buffers."""
        
        # 1. Open a buffer stream
        sink = pa.BufferOutputStream()
        
        # 2. Initialize the writer with the schema
        with pa.ipc.new_stream(sink, reader.schema) as writer:
            # Yield the initial schema header
            yield sink.getvalue().to_pybytes()
            sink.reset()
            
            # 3. Write batches
            for batch in reader:
                writer.write_batch(batch)
                # Yield the serialized batch
                yield sink.getvalue().to_pybytes()
                sink.reset()
                
        # 4. Finalize the stream (The context manager writes the EOS)
        yield sink.getvalue().to_pybytes()

    @property
    def federate(self) -> list[Catalog]:
        """
        Divides a federated master Catalog into cluster-specific child Catalogs.
        Each child carries only entities connected by internal joins within one system.
        Cross-system joins and operator groups remain on the master for DuckDB to resolve.
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
                "operator_groups": [
                    g for g in self.operator_groups
                    if _collect_plugins_from_group(g) == {plugin_name}
                    # TODO: refine to only include groups referencing columns in this cluster
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
        """
        Create reader for Arrow streaming format.

        Parameters
        ----------
        source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
            Either an in-memory buffer, or a readable file object.
        options : pyarrow.ipc.IpcReadOptions
            Options for IPC serialization.
            If None, default values will be used.
        memory_pool : MemoryPool, default None
            If None, default memory pool is used.

        Returns
        -------
        reader : RecordBatchStreamReader
            A reader for the given source
        """
        return ipc.open_stream(
            source=source, 
            options=options, 
            memory_pool=memory_pool
        )

    @staticmethod
    def ipc_open_file(source, footer_offset=None, *, options=None, memory_pool=None) -> RecordBatchFileReader:
        """
    Create reader for Arrow file format.

    Parameters
    ----------
    source : bytes/buffer-like, pyarrow.NativeFile, or file-like Python object
        Either an in-memory buffer, or a readable file object.
    footer_offset : int, default None
        If the file is embedded in some larger file, this is the byte offset to
        the very end of the file data.
    options : pyarrow.ipc.IpcReadOptions
        Options for IPC serialization.
        If None, default values will be used.
    memory_pool : MemoryPool, default None
        If None, default memory pool is used.

    Returns
    -------
    reader : RecordBatchFileReader
        A reader for the given source
    """
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
        """
        Write a dataset to a given format and partitioning.

        Parameters
        ----------
        data : Dataset, Table/RecordBatch, RecordBatchReader, list of \
    Table/RecordBatch, or iterable of RecordBatch
            The data to write. This can be a Dataset instance or
            in-memory Arrow data. If an iterable is given, the schema must
            also be given.
        base_dir : str
            The root directory where to write the dataset.
        basename_template : str, optional
            A template string used to generate basenames of written data files.
            The token '{i}' will be replaced with an automatically incremented
            integer. If not specified, it defaults to
            "part-{i}." + format.default_extname
        format : FileFormat or str
            The format in which to write the dataset. Currently supported:
            "parquet", "ipc"/"arrow"/"feather", and "csv". If a FileSystemDataset
            is being written and `format` is not specified, it defaults to the
            same format as the specified FileSystemDataset. When writing a
            Table or RecordBatch, this keyword is required.
        partitioning : Partitioning or list[str], optional
            The partitioning scheme specified with the ``partitioning()``
            function or a list of field names. When providing a list of
            field names, you can use ``partitioning_flavor`` to drive which
            partitioning type should be used.
        partitioning_flavor : str, optional
            One of the partitioning flavors supported by
            ``pyarrow.dataset.partitioning``. If omitted will use the
            default of ``partitioning()`` which is directory partitioning.
        schema : Schema, optional
        filesystem : FileSystem, optional
        file_options : pyarrow.dataset.FileWriteOptions, optional
            FileFormat specific write options, created using the
            ``FileFormat.make_write_options()`` function.
        use_threads : bool, default True
            Write files in parallel. If enabled, then maximum parallelism will be
            used determined by the number of available CPU cores. Using multiple
            threads may change the order of rows in the written dataset if
            preserve_order is set to False.
        preserve_order : bool, default False
            Preserve the order of rows. If enabled, order of rows in the dataset are
            guaranteed to be preserved even if use_threads is set to True. This may
            cause notable performance degradation.
        max_partitions : int, default 1024
            Maximum number of partitions any batch may be written into.
        max_open_files : int, default 1024
            If greater than 0 then this will limit the maximum number of
            files that can be left open. If an attempt is made to open
            too many files then the least recently used file will be closed.
            If this setting is set too low you may end up fragmenting your
            data into many small files.
        max_rows_per_file : int, default 0
            Maximum number of rows per file. If greater than 0 then this will
            limit how many rows are placed in any single file. Otherwise there
            will be no limit and one file will be created in each output
            directory unless files need to be closed to respect max_open_files
        min_rows_per_group : int, default 0
            Minimum number of rows per group. When the value is greater than 0,
            the dataset writer will batch incoming data and only write the row
            groups to the disk when sufficient rows have accumulated.
        max_rows_per_group : int, default 1024 * 1024
            Maximum number of rows per group. If the value is greater than 0,
            then the dataset writer may split up large incoming batches into
            multiple row groups.  If this value is set, then min_rows_per_group
            should also be set. Otherwise it could end up with very small row
            groups.
        file_visitor : function
            If set, this function will be called with a WrittenFile instance
            for each file created during the call.  This object will have both
            a path attribute and a metadata attribute.

            The path attribute will be a string containing the path to
            the created file.

            The metadata attribute will be the parquet metadata of the file.
            This metadata will have the file path attribute set and can be used
            to build a _metadata file.  The metadata attribute will be None if
            the format is not parquet.

            Example visitor which simple collects the filenames created::

                visited_paths = []

                def file_visitor(written_file):
                    visited_paths.append(written_file.path)
        existing_data_behavior : 'error' | 'overwrite_or_ignore' | \
    'delete_matching'
            Controls how the dataset will handle data that already exists in
            the destination.  The default behavior ('error') is to raise an error
            if any data exists in the destination.

            'overwrite_or_ignore' will ignore any existing data and will
            overwrite files with the same name as an output file.  Other
            existing files will be ignored.  This behavior, in combination
            with a unique basename_template for each write, will allow for
            an append workflow.

            'delete_matching' is useful when you are writing a partitioned
            dataset.  The first time each partition directory is encountered
            the entire directory will be deleted.  This allows you to overwrite
            old partitions completely.
        create_dir : bool, default True
            If False, directories will not be created.  This can be useful for
            filesystems that do not require directories.
        """
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