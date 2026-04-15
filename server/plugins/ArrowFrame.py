"""
This is an expirimental interchange wrapper.

ArrowFrame - The Universal "God Class" interchange wrapper for PyArrow data.

Architecture notes:
- Internal storage is always pa.Table. RecordBatchReader / RecordBatch inputs are
  materialized on construction (ArrowFrame is not a streaming object).
- Fully implements the Python dataframe interchange protocol (DataFrame.__dataframe__).
- Wraps pyarrow.acero, csv, json, parquet, feather, ipc, dataset, and fs as a
  unified facade so plugins never need to import multiple pyarrow sub-packages.
- to_catalog() bridges the live data back to the Pydantic metadata contract without
  importing system-specific code-keeping the plugin boundary clean.
"""
from __future__ import annotations

import io
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal, Sequence, TypeAlias

import pandas
import polars
import pyarrow as pa
import pyarrow.acero as acero
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.dataset as ds
import pyarrow.feather as feather
import pyarrow.ipc as ipc
import pyarrow.json as pa_json
import pyarrow.parquet as pq
from pyarrow.interchange.buffer import DlpackDeviceType
from pyarrow.interchange.column import (
    _PYARROW_KINDS,
    CategoricalDescription,
    Dtype,
    DtypeKind,
    Endianness,
    NoBufferPresent,
    _PyArrowColumn,
)
from pyarrow.interchange.from_dataframe import from_dataframe

from server.plugins.DataFrame import (
    Buffer as Buffer_,
    Column as Column_,
    ColumnBuffers,
    ColumnNullType,
    DataFrame,
)
from server.plugins.PluginModels import (
    Catalog,
    Column as CatalogColumn,
    Entity,
    arrow_type_literal,
    arrow_types,
)

# ---------------------------------------------------------------------------
# Polars - optional dependency; type aliases are defined in the try block only.
# In the except branch we assign plain Any so there is only one TypeAlias
# declaration per name (avoids Pyright "declared as TypeAlias and can be
# assigned only once" errors).
# ---------------------------------------------------------------------------
try:
    from polars._typing import (
        ArrowArrayExportable,
        ArrowStreamExportable,
        Orientation,
        PolarsDataType,
        SchemaDefinition,
        SchemaDict,
    )
    polars_arrow_stream: TypeAlias = ArrowStreamExportable
    polars_arrow_array: TypeAlias = ArrowArrayExportable
    polars_orientation: TypeAlias = Orientation
    polars_type: TypeAlias = PolarsDataType
    polars_schema: TypeAlias = SchemaDefinition
    polars_schema_dict: TypeAlias = SchemaDict
except ImportError:
    polars_arrow_stream = Any  # type: ignore[assignment]
    polars_arrow_array = Any  # type: ignore[assignment]
    polars_orientation = Any  # type: ignore[assignment]
    polars_type = Any  # type: ignore[assignment]
    polars_schema = Any  # type: ignore[assignment]
    polars_schema_dict = Any  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Arrow / universal type aliases (re-exported for plugin convenience)
# ---------------------------------------------------------------------------
Records: TypeAlias = Iterable[dict[str, Any]]
ArrowStream: TypeAlias = pa.RecordBatchReader
RecordBatchReader: TypeAlias = pa.RecordBatchReader
RecordBatch: TypeAlias = pa.RecordBatch
arrow_batch: TypeAlias = pa.RecordBatch
arrow_field: TypeAlias = pa.Field
arrow_table: TypeAlias = pa.Table
arrow_schema: TypeAlias = pa.Schema
arrow_array: TypeAlias = pa.Array
arrow_chunked_array: TypeAlias = pa.ChunkedArray
arrow_table_groupby: TypeAlias = pa.TableGroupBy
arrow_type: TypeAlias = pa.DataType
arrow_dict: TypeAlias = pa.DictionaryType
arrow_struct: TypeAlias = pa.StructType
arrow_map: TypeAlias = pa.MapType
arrow_list: TypeAlias = pa.ListType
arrow_large_list: TypeAlias = pa.LargeListType

# Expose pyarrow.fs.FSSpecHandler so plugins can build filesystems without
# importing pyarrow.fs directly.
try:
    from pyarrow.fs import FSSpecHandler as FSSpecHandler  # noqa: F401
except ImportError:
    FSSpecHandler = None  # type: ignore[assignment,misc]


# ===========================================================================
# ArrowBuffer - interchange protocol buffer wrapper
# ===========================================================================
class ArrowBuffer(Buffer_):
    """
    Contiguous memory buffer wrapping a pa.Buffer for the interchange protocol.
    https://data-apis.org/dataframe-protocol/latest/API.html#buffer-object
    """

    def __init__(self, x: pa.Buffer, allow_copy: bool = True) -> None:
        self._x = x

    @property
    def bufsize(self) -> int:
        return self._x.size

    @property
    def ptr(self) -> int:
        return self._x.address

    def __dlpack__(self) -> object:
        raise NotImplementedError("__dlpack__")

    def __dlpack_device__(self) -> tuple[DlpackDeviceType, int | None]:
        if self._x.is_cpu:
            return (DlpackDeviceType.CPU, None)
        raise NotImplementedError("__dlpack_device__ for non-CPU buffers")

    def __repr__(self) -> str:
        return (
            f"ArrowBuffer(bufsize={self.bufsize}, ptr={self.ptr}, "
            f"device={self.__dlpack_device__()[0].name})"
        )


# ===========================================================================
# ArrowColumn - interchange protocol column wrapper
# ===========================================================================
class ArrowColumn(Column_):
    """
    Single-column view implementing the interchange protocol Column spec.
    Constructed by ArrowFrame.get_column() - not meant for direct instantiation.

    Also carries optional PluginModels-style metadata (arrow_type_id, precision,
    etc.) so it can participate in catalog operations when the caller populates them.
    https://arrow.apache.org/docs/format/Columnar.html
    """

    # Interchange state
    _col: pa.Array
    _allow_copy: bool
    _dtype: tuple[DtypeKind, int, str, str]

    # Optional catalog metadata (class-level defaults; callers may override per instance)
    name: str | None = None
    qualified_name: str | None = None
    raw_type: str | None = None
    arrow_type_id: arrow_type_literal | None = None
    primary_key: bool = False
    is_unique: bool = False
    is_nullable: bool = True
    is_read_only: bool = False
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    timezone: str | None = None
    enum_values: list[Any] = []
    properties: dict[str, Any] = {}

    @property
    def arrow_type(self) -> pa.DataType | None:
        """Resolve arrow_type_id to a live pa.DataType, honouring parameterised types."""
        if not self.arrow_type_id:
            return None
        tid = self.arrow_type_id
        if tid.startswith("decimal"):
            p = self.precision if self.precision is not None else 38
            s = self.scale if self.scale is not None else 9
            return pa.decimal128(p, s)
        if tid.startswith("timestamp"):
            unit = tid.split("_")[1]
            return pa.timestamp(unit, tz=self.timezone)
        return arrow_types.get(tid)

    def __init__(
        self, column: pa.Array | pa.ChunkedArray, allow_copy: bool = True
    ) -> None:
        if isinstance(column, pa.ChunkedArray):
            if column.num_chunks == 1:
                column = column.chunk(0)
            else:
                if not allow_copy:
                    raise RuntimeError(
                        "Chunks must be combined but allow_copy=False"
                    )
                column = column.combine_chunks()

        self._allow_copy = allow_copy

        if pa.types.is_boolean(column.type):
            if not allow_copy:
                raise RuntimeError(
                    "Boolean column will be cast to uint8 but allow_copy=False"
                )
            self._dtype = self._dtype_from_arrowdtype(column.type, 8)
            self._col = pc.cast(column, pa.uint8())
        else:
            self._col = column
            dtype = self._col.type
            try:
                bit_width = dtype.bit_width
            except ValueError:
                bit_width = 8
            self._dtype = self._dtype_from_arrowdtype(dtype, bit_width)

    # --- Interchange protocol -----------------------------------------------

    def size(self) -> int:
        return len(self._col)

    @property
    def offset(self) -> int:
        return self._col.offset

    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str] | Dtype:
        return self._dtype

    def _dtype_from_arrowdtype(
        self, dtype: pa.DataType, bit_width: int
    ) -> tuple[DtypeKind, int, str, str]:
        if pa.types.is_timestamp(dtype):
            ts = dtype.unit[0]
            tz = dtype.tz if dtype.tz else ""
            return DtypeKind.DATETIME, bit_width, f"ts{ts}:{tz}", Endianness.NATIVE
        if pa.types.is_dictionary(dtype):
            arr = self._col
            idx_info = _PYARROW_KINDS.get(arr.indices.type)
            if idx_info is None:
                raise ValueError(f"Dictionary index type {arr.indices.type} not supported")
            _, f_string = idx_info
            return DtypeKind.CATEGORICAL, bit_width, f_string or "", Endianness.NATIVE
        kind_info = _PYARROW_KINDS.get(dtype)
        if kind_info is None:
            raise ValueError(f"Data type {dtype} not supported by interchange protocol")
        kind, f_string = kind_info
        return kind, bit_width, f_string or "", Endianness.NATIVE

    @property
    def describe_categorical(self) -> CategoricalDescription:
        arr = self._col
        if not pa.types.is_dictionary(arr.type):
            raise TypeError("describe_categorical only valid for categorical dtype")
        # _PyArrowColumn implements the same interchange Column protocol as Column_
        # but is not a registered subclass - suppress the type error here.
        return {
            "is_ordered": self._col.type.ordered,
            "is_dictionary": True,
            "categories": _PyArrowColumn(arr.dictionary),  # type: ignore[return-value]
        }

    @property
    def describe_null(self) -> tuple[ColumnNullType, Any]:
        if self.null_count == 0:
            return ColumnNullType.NON_NULLABLE, None
        return ColumnNullType.USE_BITMASK, 0

    @property
    def null_count(self) -> int | None:
        n = self._col.null_count
        return n if n != -1 else None

    @property
    def metadata(self) -> dict[str, Any]:
        return {}

    def num_chunks(self) -> int:
        return 1

    def get_chunks(self, n_chunks: int | None = None) -> Iterable[ArrowColumn]:
        if n_chunks and n_chunks > 1:
            chunk_size = self.size() // n_chunks
            if self.size() % n_chunks != 0:
                chunk_size += 1
            array = self._col
            for start in range(0, chunk_size * n_chunks, chunk_size):
                yield _PyArrowColumn(array.slice(start, chunk_size), self._allow_copy)  # type: ignore[misc]
        else:
            yield self

    def get_buffers(self) -> ColumnBuffers:
        buffers: ColumnBuffers = {"data": self._get_data_buffer(), "validity": None, "offsets": None}
        try:
            buffers["validity"] = self._get_validity_buffer()
        except NoBufferPresent:
            pass
        try:
            buffers["offsets"] = self._get_offsets_buffer()
        except NoBufferPresent:
            pass
        return buffers

    def _get_data_buffer(self) -> tuple[ArrowBuffer, Any]:
        array = self._col
        dtype = self.dtype
        if pa.types.is_dictionary(array.type):
            array = array.indices
            dtype = _PyArrowColumn(array).dtype
        n = len(array.buffers())
        if n == 2:
            return ArrowBuffer(array.buffers()[1]), dtype
        elif n == 3:
            return ArrowBuffer(array.buffers()[2]), dtype
        raise NoBufferPresent("Unexpected buffer count for column dtype")

    def _get_validity_buffer(self) -> tuple[ArrowBuffer, Any]:
        dtype = (DtypeKind.BOOL, 1, "b", Endianness.NATIVE)
        buff = self._col.buffers()[0]
        if buff:
            return ArrowBuffer(buff), dtype
        raise NoBufferPresent("No missing values - no validity buffer present")

    def _get_offsets_buffer(self) -> tuple[ArrowBuffer, Any]:
        array = self._col
        n = len(array.buffers())
        if n == 2:
            raise NoBufferPresent("Fixed-length dtype has no offsets buffer")
        elif n == 3:
            dtype_t = self._col.type
            if pa.types.is_large_string(dtype_t) or pa.types.is_large_binary(dtype_t):
                offset_dtype = (DtypeKind.INT, 64, "l", Endianness.NATIVE)
            else:
                offset_dtype = (DtypeKind.INT, 32, "i", Endianness.NATIVE)
            return ArrowBuffer(array.buffers()[1]), offset_dtype
        raise NoBufferPresent("Unexpected buffer count for column dtype")


# ===========================================================================
# ArrowFrame - The God Class
# ===========================================================================
class ArrowFrame(DataFrame):
    """
    Universal in-memory dataframe wrapper over pa.Table.

    Implements:
    - Python dataframe interchange protocol (DataFrame.__dataframe__)
    - Acero-backed relational API: filter, select, join, aggregate, sort_by
    - IO facade: read/write csv, json, parquet, feather, ipc, dataset
    - Metadata bridge: to_catalog() -> Catalog (Pydantic)
    - Zero-copy interop: to_polars() / from_polars(), to_pandas() / from_pandas()

    https://data-apis.org/dataframe-protocol/latest/API.html
    https://arrow.apache.org/docs/python/interchange_protocol.html
    """

    _df: pa.Table
    _allow_copy: bool
    _nan_as_null: bool

    # Optional entity-level metadata
    name: str | None = None
    parent_name: str | None = None
    qualified_name: str | None = None
    properties: dict[str, Any] = {}

    # -----------------------------------------------------------------------
    # Built by any Iterable
    # -----------------------------------------------------------------------

    def __init__(
        self,
        df: pa.Table | pa.RecordBatch | pa.RecordBatchReader |
        pandas.DataFrame | polars.DataFrame | DataFrame |
        dict | list | set | tuple,
        nan_as_null: bool = False,
        allow_copy: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        Accepts pa.Table, pa.RecordBatch, or pa.RecordBatchReader.
        RecordBatchReader is eagerly materialized - ArrowFrame is not streaming.
        """

        if isinstance(df, ArrowFrame):
            self._df = df._df

        elif isinstance(df, pa.RecordBatch):
            self._df = pa.Table.from_batches([df])
        elif isinstance(df, pandas.DataFrame):
            self._df = pa.Table.from_pandas(df, preserve_index=False)
        elif isinstance(df, polars.DataFrame):
            self._df = from_dataframe(df, allow_copy=allow_copy)
        elif isinstance(df, dict):
            self._df = pa.table(df)
        elif isinstance(df, (list, set, tuple)):
            self._df = pa.Table.from_pylist(df)
        elif isinstance(df, pa.Table):
            self._df = df
        elif isinstance(df, pa.RecordBatchReader):
            self._df = df.read_all()
        elif isinstance(df, DataFrame) and hasattr(df, "__dataframe__"):
            self._df = from_dataframe(df, allow_copy=allow_copy)
        else:
            raise TypeError(f"Unsupported type for ArrowFrame construction: {type(df)}")

        self._allow_copy = allow_copy

        for k, v in kwargs.items():
            setattr(self, k, v)
        if nan_as_null: 
            print("nan_as_null=True has no effect; use default nan_as_null=False")
    # --- Factory class methods ---

    @classmethod
    def from_reader(cls, reader: pa.RecordBatchReader, **kwargs: Any) -> ArrowFrame:
        """Materialize a streaming RecordBatchReader into an ArrowFrame."""
        return cls(reader.read_all(), **kwargs)

    @classmethod
    def from_batch(cls, batch: pa.RecordBatch, **kwargs: Any) -> ArrowFrame:
        return cls(pa.Table.from_batches([batch]), **kwargs)

    @classmethod
    def from_batches(
        cls,
        batches: list[pa.RecordBatch],
        schema: pa.Schema | None = None,
        **kwargs: Any,
    ) -> ArrowFrame:
        return cls(pa.Table.from_batches(batches, schema=schema), **kwargs)

    @classmethod
    def from_pydict(
        cls, data: dict[str, Any], schema: pa.Schema | None = None, **kwargs: Any
    ) -> ArrowFrame:
        """Build from a dict of column-name -> list/array."""
        return cls(pa.table(data, schema=schema), **kwargs)

    @classmethod
    def from_pylist(
        cls, data: list[dict[str, Any]], schema: pa.Schema | None = None, **kwargs: Any
    ) -> ArrowFrame:
        """Build from a list of row dicts."""
        return cls(pa.Table.from_pylist(data, schema=schema), **kwargs)

    @classmethod
    def from_interchange(cls, df_obj: Any, allow_copy: bool = True) -> ArrowFrame:
        """
        Import from any object implementing __dataframe__ (e.g. pandas, polars, modin).
        Uses pyarrow.interchange.from_dataframe for zero-copy where possible.
        """
        table = from_dataframe(df_obj, allow_copy=allow_copy)
        return cls(table, allow_copy=allow_copy)

    # -----------------------------------------------------------------------
    # Interchange protocol - DataFrame ABC
    # -----------------------------------------------------------------------

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> ArrowFrame:
        return ArrowFrame(self._df, nan_as_null, allow_copy)
    
    def __eq__(self, other):
        if not isinstance(other, ArrowFrame):
            return NotImplemented
        return self._df.equals(other._df)
    
    def __len__(self):
        return self._df.num_rows
    
    def __getitem__(self, index):
        return self._df[index]
    
    def __setitem__(self, index, value):
        self._df[index] = value
    
    def __delitem__(self, index):
        del self._df[index]
    
    def __contains__(self, item):
        return item in self._df
    
    def __iter__(self):
        return iter(self._df.columns)
    
    def __reversed__(self):
        return reversed(self._df.columns)
    
    def __add__(self, other):
        return self._df + other._df
    
    @property
    def metadata(self) -> dict[str, Any]:
        if self._df.schema.metadata:
            return {
                "pyarrow." + k.decode("utf8"): v.decode("utf8")
                for k, v in self._df.schema.metadata.items()
            }
        return {}

    def num_columns(self) -> int:
        return self._df.num_columns  # property, not method

    def num_rows(self) -> int:
        return self._df.num_rows  # property, not method

    def num_chunks(self) -> int:
        return len(self._df.to_batches())

    def column_names(self) -> Iterable[str]:
        return self._df.schema.names

    def get_column(self, i: int) -> ArrowColumn:
        return ArrowColumn(self._df.column(i), allow_copy=self._allow_copy)

    def get_column_by_name(self, name: str) -> ArrowColumn:
        return ArrowColumn(self._df.column(name), allow_copy=self._allow_copy)

    def get_columns(self) -> Iterable[ArrowColumn]:
        return [ArrowColumn(col, allow_copy=self._allow_copy) for col in self._df.columns]

    def select_columns(self, indices: Sequence[int]) -> ArrowFrame:
        return ArrowFrame(self._df.select(list(indices)), self._nan_as_null, self._allow_copy)

    def select_columns_by_name(self, names: Sequence[str]) -> ArrowFrame:
        return ArrowFrame(self._df.select(list(names)), self._nan_as_null, self._allow_copy)

    def get_chunks(self, n_chunks: int | None = None) -> Iterable[ArrowFrame]:
        if n_chunks and n_chunks > 1:
            chunk_size = self._df.num_rows // n_chunks
            if self._df.num_rows % n_chunks != 0:
                chunk_size += 1
            batches = self._df.to_batches(max_chunksize=chunk_size)
            if len(batches) == n_chunks - 1:
                batches.append(pa.record_batch([], schema=self._df.schema))
        else:
            batches = self._df.to_batches()
        return [ArrowFrame(pa.Table.from_batches([b]), self._nan_as_null, self._allow_copy) for b in batches]

    # -----------------------------------------------------------------------
    # Arrow-native data access
    # -----------------------------------------------------------------------

    @property
    def schema(self) -> pa.Schema:
        return self._df.schema

    def to_table(self) -> pa.Table:
        return self._df

    def to_batches(self, max_chunksize: int | None = None) -> list[pa.RecordBatch]:
        return self._df.to_batches(max_chunksize=max_chunksize)

    def to_reader(self) -> pa.RecordBatchReader:
        return pa.RecordBatchReader.from_batches(self._df.schema, self._df.to_batches())

    def to_pydict(self) -> dict[str, list[Any]]:
        return self._df.to_pydict()

    def to_pylist(self) -> list[dict[str, Any]]:
        return self._df.to_pylist()

    def combine_chunks(self) -> ArrowFrame:
        return ArrowFrame(self._df.combine_chunks(), self._nan_as_null, self._allow_copy)

    # Backwards-compatible aliases for existing callers
    def get_table(self) -> pa.Table:
        return self.to_table()

    def get_schema(self) -> pa.Schema:
        return self.schema

    def get_column_names(self) -> list[str]:
        return self._df.schema.names

    def get_column_types(self) -> list[pa.DataType]:
        return [field.type for field in self._df.schema]

    def get_batch(self) -> pa.RecordBatch:
        return pa.RecordBatch.from_arrays(self._df.columns, schema=self._df.schema)

    def get_batch_reader(self) -> pa.RecordBatchReader:
        return self.to_reader()

    # -----------------------------------------------------------------------
    # Relational API - powered by pyarrow.acero
    # -----------------------------------------------------------------------

    def filter(self, expression: pc.Expression) -> ArrowFrame:
        """
        Filter rows matching a PyArrow compute expression.
        Internally executes via the acero Filter node.

        Example:
            frame.filter(pc.field("status") == "active")
            frame.filter((pc.field("amount") > 100) & pc.field("active").cast(pa.bool_()))
        """
        return ArrowFrame(self._df.filter(expression), self._nan_as_null, self._allow_copy)

    def select(self, columns: list[str] | list[int]) -> ArrowFrame:
        """Project to a column subset by name or positional index."""
        return ArrowFrame(self._df.select(columns), self._nan_as_null, self._allow_copy)

    def join(
        self,
        right: ArrowFrame,
        keys: str | list[str],
        right_keys: str | list[str] | None = None,
        join_type: Literal[
            "left semi", "right semi", "left anti", "right anti",
            "inner", "left outer", "right outer", "full outer"
        ] = "left outer",
        left_suffix: str | None = None,
        right_suffix: str | None = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
    ) -> ArrowFrame:
        """
        Hash-join with another ArrowFrame using acero's HashJoinNode.

        Example:
            orders.join(customers, keys="customer_id", join_type="inner")
        """
        result = self._df.join(
            right._df,
            keys=keys,
            right_keys=right_keys,
            join_type=join_type,
            left_suffix=left_suffix,
            right_suffix=right_suffix,
            coalesce_keys=coalesce_keys,
            use_threads=use_threads,
        )
        return ArrowFrame(result, self._nan_as_null, self._allow_copy)

    def aggregate(
        self,
        aggregations: list[tuple[str, str] | tuple[str, str, str]],
        group_by: list[str] | str | None = None,
    ) -> ArrowFrame:
        """
        Group and aggregate using acero's AggregateNode.

        aggregations: list of (column, function) or (column, function, alias) tuples.
          Functions: "sum", "mean", "min", "max", "count", "count_all", etc.

        Example:
            frame.aggregate([("amount", "sum", "total"), ("id", "count", "n")], group_by="region")
        """
        by: list[str] = (
            [group_by] if isinstance(group_by, str) else (group_by or [])
        )
        result = self._df.group_by(by).aggregate(aggregations)
        return ArrowFrame(result, self._nan_as_null, self._allow_copy)

    def sort_by(
        self,
        sort_keys: str | list[str] | list[tuple[str, Literal["ascending", "descending"]]],
    ) -> ArrowFrame:
        """
        Sort rows via acero's OrderByNode.

        sort_keys can be:
          - "col"                             (single column, ascending)
          - ["col1", "col2"]                  (multiple columns, all ascending)
          - [("col1", "ascending"), ("col2", "descending")]
        """
        return ArrowFrame(self._df.sort_by(sort_keys), self._nan_as_null, self._allow_copy)

    def limit(self, n: int, offset: int = 0) -> ArrowFrame:
        """Return at most n rows starting from offset."""
        return ArrowFrame(self._df.slice(offset, n), self._nan_as_null, self._allow_copy)

    def rename(self, mapping: dict[str, str]) -> ArrowFrame:
        """Rename columns according to {old_name: new_name} mapping."""
        new_names = [mapping.get(col, col) for col in self._df.schema.names]
        return ArrowFrame(self._df.rename_columns(new_names), self._nan_as_null, self._allow_copy)

    def cast(self, target_schema: pa.Schema, safe: bool = True) -> ArrowFrame:
        """Cast columns to the types described by target_schema."""
        return ArrowFrame(self._df.cast(target_schema, safe=safe), self._nan_as_null, self._allow_copy)

    def append_column(self, name: str, array: pa.Array | pa.ChunkedArray) -> ArrowFrame:
        """Append a new column, returning a new ArrowFrame."""
        return ArrowFrame(self._df.append_column(name, array), self._nan_as_null, self._allow_copy)

    def drop_column(self, name: str) -> ArrowFrame:
        """Remove a column by name, returning a new ArrowFrame."""
        idx = self._df.schema.get_field_index(name)
        return ArrowFrame(self._df.remove_column(idx), self._nan_as_null, self._allow_copy)

    def combine_frames(self, other: ArrowFrame) -> ArrowFrame:
        """Vertical concatenation (union-all) of two ArrowFrames with identical schemas."""
        return ArrowFrame(
            pa.concat_tables([self._df, other._df]),
            self._nan_as_null,
            self._allow_copy,
        )

    def build_plan(
        self, *node_sequence: tuple[str, acero.ExecNodeOptions]
    ) -> ArrowFrame:
        """
        Build and execute an explicit acero Declaration plan starting from this
        frame's data. For advanced use cases that need acero nodes not exposed
        by the higher-level wrappers above.

        Each argument is a (node_name, NodeOptions) pair, applied in order.

        Example:
            frame.build_plan(
                ("filter", acero.FilterNodeOptions(pc.field("x") > 0)),
                ("project", acero.ProjectNodeOptions([pc.field("x"), pc.field("y")])),
            )
        """
        source = acero.Declaration("table_source", acero.TableSourceNodeOptions(self._df))
        nodes = [source] + [acero.Declaration(name, opts) for name, opts in node_sequence]
        plan = acero.Declaration.from_sequence(nodes)
        return ArrowFrame(plan.to_table(), self._nan_as_null, self._allow_copy)

    # -----------------------------------------------------------------------
    # IO facade - read (class methods) and write (instance methods)
    # -----------------------------------------------------------------------

    # --- CSV ---

    @classmethod
    def read_csv(
        cls,
        source: str | Path | io.IOBase,
        read_options: pa_csv.ReadOptions | None = None,
        parse_options: pa_csv.ParseOptions | None = None,
        convert_options: pa_csv.ConvertOptions | None = None,
    ) -> ArrowFrame:
        table = pa_csv.read_csv(
            source,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
        return cls(table)

    def to_csv(
        self,
        destination: str | Path | io.IOBase,
        write_options: pa_csv.WriteOptions | None = None,
    ) -> None:
        pa_csv.write_csv(self._df, destination, write_options=write_options)

    # --- JSON ---

    @classmethod
    def read_json(
        cls,
        source: str | Path | io.IOBase,
        read_options: pa_json.ReadOptions | None = None,
        parse_options: pa_json.ParseOptions | None = None,
    ) -> ArrowFrame:
        table = pa_json.read_json(
            source,
            read_options=read_options,
            parse_options=parse_options,
        )
        return cls(table)

    def to_json(self, destination: str | Path | io.IOBase, **pandas_kwargs: Any) -> None:
        """
        Write as JSON via pandas. PyArrow has no native JSON writer; this is the
        recommended path for JSON output from Arrow data.
        """
        self._df.to_pandas().to_json(destination, **pandas_kwargs)

    # --- Parquet ---

    @classmethod
    def read_parquet(
        cls,
        source: str | Path,
        columns: list[str] | None = None,
        filters: Any | None = None,
        **kwargs: Any,
    ) -> ArrowFrame:
        return cls(pq.read_table(source, columns=columns, filters=filters, **kwargs))

    def to_parquet(self, destination: str | Path, **kwargs: Any) -> None:
        pq.write_table(self._df, destination, **kwargs)

    # --- Feather / Arrow IPC (v2 on-disk format) ---

    @classmethod
    def read_feather (
        cls,
        source: str | Path,
        columns: list[str] | None = None,
        **kwargs: Any,
    ) -> ArrowFrame:
        return cls(feather.read_table(source, columns=columns, **kwargs))

    def to_feather(self, destination: str | Path, **kwargs: Any) -> None:
        feather.write_feather(self._df, destination, **kwargs)

    # --- IPC file / stream ---

    @classmethod
    def read_ipc_file(cls, source: str | Path | io.IOBase) -> ArrowFrame:
        reader = ipc.open_file(source)
        return cls(reader.read_all())

    @classmethod
    def read_ipc_stream(cls, source: str | Path | io.IOBase) -> ArrowFrame:
        reader = ipc.open_stream(source)
        return cls(reader.read_all())

    def to_ipc_file(self, destination: str | Path | io.IOBase) -> None:
        """Write as an IPC random-access file (seekable)."""
        with ipc.new_file(destination, self._df.schema) as writer:
            writer.write_table(self._df)

    def to_ipc_stream(self, destination: str | Path | io.IOBase) -> None:
        """Write as an IPC stream (can be piped / non-seekable)."""
        with ipc.new_stream(destination, self._df.schema) as writer:
            writer.write_table(self._df)

    def to_ipc_bytes(self, stream: bool = False) -> bytes:
        """Serialise to an in-memory IPC byte string."""
        buf = io.BytesIO()
        if stream:
            self.to_ipc_stream(buf)
        else:
            self.to_ipc_file(buf)
        return buf.getvalue()

    @classmethod
    def from_ipc_bytes(cls, data: bytes, stream: bool = False) -> ArrowFrame:
        buf = io.BytesIO(data)
        return cls.read_ipc_stream(buf) if stream else cls.read_ipc_file(buf)

    # --- Dataset (partitioned on-disk) ---

    @classmethod
    def read_dataset(
        cls,
        source: str | Path,
        schema: pa.Schema | None = None,
        format: str | None = None,
        columns: list[str] | None = None,
        filter: pc.Expression | None = None,
        **kwargs: Any,
    ) -> ArrowFrame:
        dataset = ds.dataset(source, schema=schema, format=format)
        return cls(dataset.to_table(columns=columns, filter=filter))

    def write_dataset(
        self,
        base_dir: str | Path,
        format: str = "parquet",
        partitioning: Any | None = None,
        **kwargs: Any,
    ) -> None:
        ds.write_dataset(
            self._df, base_dir, format=format, partitioning=partitioning, **kwargs
        )

    # -----------------------------------------------------------------------
    # Metadata bridge - to_catalog()
    # -----------------------------------------------------------------------

    @staticmethod
    def _arrow_type_to_id(t: pa.DataType) -> str | None:
        """Map a live pa.DataType to the arrow_type_literal string used by PluginModels."""
        if pa.types.is_null(t):          return "null"
        if pa.types.is_boolean(t):       return "bool"
        if pa.types.is_int8(t):          return "int8"
        if pa.types.is_int16(t):         return "int16"
        if pa.types.is_int32(t):         return "int32"
        if pa.types.is_int64(t):         return "int64"
        if pa.types.is_uint8(t):         return "uint8"
        if pa.types.is_uint16(t):        return "uint16"
        if pa.types.is_uint32(t):        return "uint32"
        if pa.types.is_uint64(t):        return "uint64"
        if pa.types.is_float16(t):       return "float16"
        if pa.types.is_float32(t):       return "float32"
        if pa.types.is_float64(t):       return "float64"
        if pa.types.is_string(t):        return "string"    # covers utf8 alias
        if pa.types.is_large_string(t):  return "large_string"  # covers large_utf8
        if pa.types.is_binary(t):        return "binary"
        if pa.types.is_large_binary(t):  return "large_binary"
        if pa.types.is_date32(t):        return "date32"
        if pa.types.is_date64(t):        return "date64"
        if pa.types.is_timestamp(t):     return f"timestamp_{t.unit}"
        if pa.types.is_time32(t):        return f"time32_{t.unit}"
        if pa.types.is_time64(t):        return f"time64_{t.unit}"
        if pa.types.is_duration(t):      return "duration_s"
        if pa.types.is_decimal128(t):    return "decimal128"
        if pa.types.is_decimal256(t):    return "decimal256"
        if pa.types.is_dictionary(t):    return "dictionary"
        if pa.types.is_struct(t):        return "struct"
        if pa.types.is_map(t):           return "map"
        if pa.types.is_list(t):          return "list"
        if pa.types.is_large_list(t):    return "large_list"
        return "string"  # safe fallback for unsupported/complex types

    def to_catalog(
        self,
        name: str | None = None,
        entity_name: str | None = None,
    ) -> Catalog:
        """
        Generate a system-agnostic Catalog from the current Arrow schema.

        Column names, types, nullability, precision, scale, and timezone are
        derived directly from the Arrow schema - no system-specific knowledge
        is required, keeping the plugin boundary intact.

        Args:
            name:        Catalog name (e.g. database / namespace). Defaults to "ArrowFrame".
            entity_name: Entity name (e.g. table). Defaults to `name` or "frame".
        """
        schema = self._df.schema
        columns: list[CatalogColumn] = []
        for field in schema:
            t = field.type
            columns.append(CatalogColumn(
                name=field.name,
                qualified_name=field.name,
                arrow_type_id=self._arrow_type_to_id(t),  # type: ignore[arg-type]
                is_nullable=field.nullable,
                precision=t.precision if hasattr(t, "precision") else None,
                scale=t.scale if hasattr(t, "scale") else None,
                timezone=getattr(t, "tz", None),
            ))
        ename = entity_name or name or "frame"
        entity = Entity(name=ename, qualified_name=ename, columns=columns)
        return Catalog(name=name or "ArrowFrame", entities=[entity])

    # -----------------------------------------------------------------------
    # Interchange: Polars (zero-copy via Arrow PyCapsule Interface)
    # -----------------------------------------------------------------------

    def to_polars(self) -> Any:
        """
        Zero-copy conversion to a Polars DataFrame.
        PyArrow Table exposes __arrow_c_stream__ (PyCapsule Interface) which
        Polars consumes without copying data when the type mapping allows it.
        Requires polars to be installed.
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars is not installed; pip install polars") from None
        return pl.from_arrow(self._df)

    @classmethod
    def from_polars(cls, df: Any, allow_copy: bool = True) -> ArrowFrame:
        """
        Zero-copy import from a Polars DataFrame (or any ArrowStreamExportable).
        Polars implements __arrow_c_stream__ (PyCapsule Interface).
        pa.RecordBatchReader.from_stream() consumes it without copying.
        """
        table = pa.RecordBatchReader.from_stream(df).read_all()
        return cls(table, allow_copy=allow_copy)

    # -----------------------------------------------------------------------
    # Interchange: Pandas (zero-copy path where Arrow types allow)
    # -----------------------------------------------------------------------

    def to_pandas(self) -> pandas.DataFrame:
        """
        Convert to pandas.DataFrame. PyArrow uses a zero-copy path for
        primitive types and copies only for types that require conversion
        (e.g. nested structs, fixed-size decimals).
        """
        return self._df.to_pandas()

    @classmethod
    def from_pandas(
        cls,
        df: pandas.DataFrame,
        preserve_index: bool | None = None,
        schema: pa.Schema | None = None,
    ) -> ArrowFrame:
        """
        Import from a pandas.DataFrame using pa.Table.from_pandas().
        Set preserve_index=False to drop the pandas index column.
        """
        return cls(pa.Table.from_pandas(df, schema=schema, preserve_index=preserve_index))

    # -----------------------------------------------------------------------
    # Interchange: any __dataframe__ protocol consumer
    # -----------------------------------------------------------------------

    @classmethod
    def from_any(cls, obj: Any, allow_copy: bool = True) -> ArrowFrame:
        """
        Import from any library that implements __dataframe__ (polars, modin,
        cuDF, vaex, etc.). Delegates to pyarrow.interchange.from_dataframe.
        """
        return cls.from_interchange(obj, allow_copy=allow_copy)

    # -----------------------------------------------------------------------
    # Convenience repr
    # -----------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"ArrowFrame(rows={self._df.num_rows}, cols={self._df.num_columns}, "
            f"schema={self._df.schema.to_string(show_field_metadata=False)})"
        )
