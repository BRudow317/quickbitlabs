from __future__ import annotations
from dataclasses import Field, dataclass
from typing import Literal, Any, TypeAlias, Iterable, Sequence
from collections.abc import Iterator, Iterable
import pyarrow as pa
from pyarrow.acero import (
    filter,
    # select,
    # join,
    # aggregate,
    # group_by,
)
from pyarrow import csv, json, parquet
import pyarrow.compute as pc
from pyarrow.interchange.from_dataframe import from_dataframe
from pyarrow.interchange.buffer import DlpackDeviceType, _PyArrowBuffer
from pyarrow.types import is_dictionary, is_struct, is_map, is_list, is_large_list, is_fixed_size_list, is_union, is_null
from pyarrow.interchange.column import _PYARROW_KINDS, Dtype, DtypeKind, ColumnNullType, ColumnBuffers, CategoricalDescription, Endianness, NoBufferPresent, _PyArrowColumn
from pyarrow.interchange.dataframe import _PyArrowDataFrame
import pandas
from polars._typing import (
        ArrowArrayExportable,
        ArrowStreamExportable,
        Orientation,
        PolarsDataType,
        SchemaDefinition,
        SchemaDict,
    )
from DataFrame import Column as Column_
from DataFrame import Buffer as Buffer_
from DataFrame import( 
    DataFrame, 
    ColumnBuffers, 
    # Dtype, 
    CategoricalDescription, 
    ColumnNullType,
    # DlpackDeviceType,
    # DtypeKind,
)




# Universal data formats
# https://arrow.apache.org/docs/python/api/datatypes.html
Records: TypeAlias = Iterable[dict[str, Any]] # aka json/dict


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

# Polars zero-copy data frame interchange protocol types
# polars_data_frame: TypeAlias = SupportsInterchange
polars_arrow_stream: TypeAlias = ArrowStreamExportable
polars_arrow_array: TypeAlias = ArrowArrayExportable
polars_orientation: TypeAlias = Orientation
polars_type: TypeAlias = PolarsDataType
polars_schema: TypeAlias = SchemaDefinition
polars_schema_dict: TypeAlias = SchemaDict



class ArrowBuffer(Buffer_):
    """
    Data in the buffer is guaranteed to be contiguous in memory.

    Note that there is no dtype attribute present, a buffer can be thought of
    as simply a block of memory. However, if the column that the buffer is
    attached to has a dtype that's supported by DLPack and ``__dlpack__`` is
    implemented, then that dtype information will be contained in the return
    value from ``__dlpack__``.

    This distinction is useful to support both data exchange via DLPack on a
    buffer and (b) dtypes like variable-length strings which do not have a
    fixed number of bytes per element.
    """
    
    def __init__(self, x: pa.Buffer, allow_copy: bool = True) -> None:
        """
        Handle PyArrow Buffers.
        """
        self._x = x

    @property
    def bufsize(self) -> int:
        """
        Buffer size in bytes.
        """
        return self._x.size

    @property
    def ptr(self) -> int:
        """
        Pointer to start of the buffer as an integer.
        """
        return self._x.address

    def __dlpack__(self):
        """
        Produce DLPack capsule (see array API standard).

        Raises:
            - TypeError : if the buffer contains unsupported dtypes.
            - NotImplementedError : if DLPack support is not implemented

        Useful to have to connect to array libraries. Support optional because
        it's not completely trivial to implement for a Python-only library.
        """
        raise NotImplementedError("__dlpack__")

    def __dlpack_device__(self) -> tuple[DlpackDeviceType, int | None]:
        """
        Device type and device ID for where the data in the buffer resides.
        Uses device type codes matching DLPack.
        Note: must be implemented even if ``__dlpack__`` is not.
        """
        if self._x.is_cpu:
            return (DlpackDeviceType.CPU, None)
        else:
            raise NotImplementedError("__dlpack_device__")

    def __repr__(self) -> str:
        return (
            "PyArrowBuffer(" +
            str(
                {
                    "bufsize": self.bufsize,
                    "ptr": self.ptr,
                    "device": self.__dlpack_device__()[0].name,
                }
            ) +
            ")"
        )


class ArrowColumn(Column_):
    """
    https://arrow.apache.org/docs/format/Columnar.html

    A column object, with only the methods and properties required by the
    interchange protocol defined.

    A column can contain one or more chunks. Each chunk can contain up to three
    buffers - a data buffer, a mask buffer (depending on null representation),
    and an offsets buffer (if variable-size binary; e.g., variable-length
    strings).

    TBD: there's also the "chunk" concept here, which is implicit in Arrow as
         multiple buffers per array (= column here). Semantically it may make
         sense to have both: chunks were meant for example for lazy evaluation
         of data which doesn't fit in memory, while multiple buffers per column
         could also come from doing a selection operation on a single
         contiguous buffer.

         Given these concepts, one would expect chunks to be all of the same
         size (say a 10,000 row dataframe could have 10 chunks of 1,000 rows),
         while multiple buffers could have data-dependent lengths. Not an issue
         in pandas if one column is backed by a single NumPy array, but in
         Arrow it seems possible.
         Are multiple chunks *and* multiple buffers per column necessary for
         the purposes of this interchange protocol, or must producers either
         reuse the chunk concept for this or copy the data?

    Note: this Column object can only be produced by ``__dataframe__``, so
          doesn't need its own version or ``__column__`` protocol.
    """
    _field: arrow_field
    _array: arrow_array
    name: str
    qualified_name: str | None = None
    raw_type: str | None = None
    arrow_type_id: arrow_type_literal | None = None # needs to become pyarrow types
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
    enum_values: list[Any] = []
    timezone: str | None = None
    _metadata: dict[str, Any] = {}
    properties: dict[str, Any] = {}

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

    def __init__(
        self, column: pa.Array | pa.ChunkedArray, allow_copy: bool = True
    ) -> None:
        """
        Handles PyArrow Arrays and ChunkedArrays.
        """
        # Store the column as a private attribute
        if isinstance(column, pa.ChunkedArray):
            if column.num_chunks == 1:
                column = column.chunk(0)
            else:
                if not allow_copy:
                    raise RuntimeError(
                        "Chunks will be combined and a copy is required which "
                        "is forbidden by allow_copy=False"
                    )
                column = column.combine_chunks()

        self._allow_copy = allow_copy

        if pa.types.is_boolean(column.type):
            if not allow_copy:
                raise RuntimeError(
                    "Boolean column will be casted to uint8 and a copy "
                    "is required which is forbidden by allow_copy=False"
                )
            self._dtype = self._dtype_from_arrowdtype(column.type, 8)
            self._col = pc.cast(column, pa.uint8())
        else:
            self._col = column
            dtype = self._col.type
            try:
                bit_width = dtype.bit_width
            except ValueError:
                # in case of a variable-length strings, considered as array
                # of bytes (8 bits)
                bit_width = 8
            self._dtype = self._dtype_from_arrowdtype(dtype, bit_width)
    
    def size(self) -> int:
        """
        Size of the column, in elements.

        Corresponds to DataFrame.num_rows() if column is a single chunk;
        equal to size of this current chunk otherwise.

        Is a method rather than a property because it may cause a (potentially
        expensive) computation for some dataframe implementations.
        """
        return len(self._col)

    @property
    def offset(self) -> int:
        """
        Offset of first element.

        May be > 0 if using chunks; for example for a column with N chunks of
        equal size M (only the last chunk may be shorter),
        ``offset = n * M``, ``n = 0 .. N-1``.
        """
        return self._col.offset

    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str] | Dtype:
        """
        Dtype description as a tuple ``(kind, bit-width, format string, endianness)``.

        Bit-width : the number of bits as an integer
        Format string : data type description format string in Apache Arrow C
                        Data Interface format.
        Endianness : current only native endianness (``=``) is supported

        Notes:
            - Kind specifiers are aligned with DLPack where possible (hence the
              jump to 20, leave enough room for future extension)
            - Masks must be specified as boolean with either bit width 1 (for bit
              masks) or 8 (for byte masks).
            - Dtype width in bits was preferred over bytes
            - Endianness isn't too useful, but included now in case in the future
              we need to support non-native endianness
            - Went with Apache Arrow format strings over NumPy format strings
              because they're more complete from a dataframe perspective
            - Format strings are mostly useful for datetime specification, and
              for categoricals.
            - For categoricals, the format string describes the type of the
              categorical in the data buffer. In case of a separate encoding of
              the categorical (e.g. an integer to string mapping), this can
              be derived from ``self.describe_categorical``.
            - Data types not included: complex, Arrow-style null, binary, decimal,
              and nested (list, struct, map, union) dtypes.
        """
        return self._dtype
    
    def _dtype_from_arrowdtype(
        self, dtype: pa.DataType, bit_width: int
    ) -> tuple[DtypeKind, int, str, str]:
        """
        See `self.dtype` for details.
        """
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void)
        #       not handled datetime and timedelta both map to datetime
        #       (is timedelta handled?)

        if pa.types.is_timestamp(dtype):
            kind = DtypeKind.DATETIME
            ts = dtype.unit[0]
            tz = dtype.tz if dtype.tz else ""
            f_string = f"ts{ts}:{tz}"
            return kind, bit_width, f_string, Endianness.NATIVE
        elif pa.types.is_dictionary(dtype):
            kind = DtypeKind.CATEGORICAL
            arr = self._col
            indices_dtype = arr.indices.type
            _, f_string = _PYARROW_KINDS.get(indices_dtype)
            return kind, bit_width, f_string, Endianness.NATIVE
        else:
            kind, f_string = _PYARROW_KINDS.get(dtype, (None, None))
            if kind is None:
                raise ValueError(
                    f"Data type {dtype} not supported by interchange protocol")

            return kind, bit_width, f_string, Endianness.NATIVE

    @property
    def describe_categorical(self) -> CategoricalDescription:
        """
        If the dtype is categorical, there are two options:
        - There are only values in the data buffer.
        - There is a separate non-categorical Column encoding categorical values.

        Raises TypeError if the dtype is not categorical

        Returns the dictionary with description on how to interpret the data buffer:
            - "is_ordered" : bool, whether the ordering of dictionary indices is
                             semantically meaningful.
            - "is_dictionary" : bool, whether a mapping of
                                categorical values to other objects exists
            - "categories" : Column representing the (implicit) mapping of indices to
                             category values (e.g. an array of cat1, cat2, ...).
                             None if not a dictionary-style categorical.

        TBD: are there any other in-memory representations that are needed?
        """
        arr = self._col
        if not pa.types.is_dictionary(arr.type):
            raise TypeError(
                "describe_categorical only works on a column with "
                "categorical dtype!"
            )

        return {
            "is_ordered": self._col.type.ordered,
            "is_dictionary": True,
            "categories": _PyArrowColumn(arr.dictionary),
        }

    @property
    def describe_null(self) -> tuple[ColumnNullType, Any]:
        """
        Return the missing value (or "null") representation the column dtype
        uses, as a tuple ``(kind, value)``.

        Value : if kind is "sentinel value", the actual value. If kind is a bit
        mask or a byte mask, the value (0 or 1) indicating a missing value. None
        otherwise.
        """
        # In case of no missing values, we need to set ColumnNullType to
        # non nullable as in the current __dataframe__ protocol bit/byte masks
        # cannot be None
        if self.null_count == 0:
            return ColumnNullType.NON_NULLABLE, None
        else:
            return ColumnNullType.USE_BITMASK, 0

    @property
    def null_count(self) -> int | None:
        """
        Number of null elements, if known.

        Note: Arrow uses -1 to indicate "unknown", but None seems cleaner.
        """
        arrow_null_count = self._col.null_count
        n = arrow_null_count if arrow_null_count != -1 else None
        return n

    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata for the column. See `DataFrame.metadata` for more details.
        """
        return self._metadata

    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        return 1

    def get_chunks(self, n_chunks: int|None = None) -> Iterable[Column]:
        """
        Return an iterator yielding the chunks.

        See `DataFrame.get_chunks` for details on ``n_chunks``.
        """
        if n_chunks and n_chunks > 1:
            chunk_size = self.size() // n_chunks
            if self.size() % n_chunks != 0:
                chunk_size += 1

            array = self._col
            i = 0
            for start in range(0, chunk_size * n_chunks, chunk_size):
                yield _PyArrowColumn(
                    array.slice(start, chunk_size), self._allow_copy
                )
                i += 1
        else:
            yield self

    def get_buffers(self) -> ColumnBuffers:
        """
        Return a dictionary containing the underlying buffers.

        The returned dictionary has the following contents:

            - "data": a two-element tuple whose first element is a buffer
                      containing the data and whose second element is the data
                      buffer's associated dtype.
            - "validity": a two-element tuple whose first element is a buffer
                          containing mask values indicating missing data and
                          whose second element is the mask value buffer's
                          associated dtype. None if the null representation is
                          not a bit or byte mask.
            - "offsets": a two-element tuple whose first element is a buffer
                         containing the offset values for variable-size binary
                         data (e.g., variable-length strings) and whose second
                         element is the offsets buffer's associated dtype. None
                         if the data buffer does not have an associated offsets
                         buffer.
        """
        buffers: ColumnBuffers = {
            "data": self._get_data_buffer(),
            "validity": None,
            "offsets": None,
        }

        try:
            buffers["validity"] = self._get_validity_buffer()
        except NoBufferPresent:
            pass

        try:
            buffers["offsets"] = self._get_offsets_buffer()
        except NoBufferPresent:
            pass

        return buffers
    def _get_data_buffer(
        self,
    ) -> tuple[ArrowBuffer, Any]:  # Any is for self.dtype tuple
        """
        Return the buffer containing the data and the buffer's
        associated dtype.
        """
        array = self._col
        dtype = self.dtype

        # In case of dictionary arrays, use indices
        # to define a buffer, codes are transferred through
        # describe_categorical()
        if pa.types.is_dictionary(array.type):
            array = array.indices
            dtype = _PyArrowColumn(array).dtype

        n = len(array.buffers())
        if n == 2:
            return ArrowBuffer(array.buffers()[1]), dtype
        elif n == 3:
            return ArrowBuffer(array.buffers()[2]), dtype
        else:
            raise NoBufferPresent(
                "Unexpected number of buffers for this column's dtype"
            )

    def _get_validity_buffer(self) -> tuple[ArrowBuffer, Any]:
        """
        Return the buffer containing the mask values indicating missing data
        and the buffer's associated dtype.
        Raises NoBufferPresent if null representation is not a bit or byte
        mask.
        """
        # Define the dtype of the returned buffer
        dtype = (DtypeKind.BOOL, 1, "b", Endianness.NATIVE)
        array = self._col
        buff = array.buffers()[0]
        if buff:
            return ArrowBuffer(buff), dtype
        else:
            raise NoBufferPresent(
                "There are no missing values so "
                "does not have a separate mask")

    def _get_offsets_buffer(self) -> tuple[ArrowBuffer, Any]:
        """
        Return the buffer containing the offset values for variable-size binary
        data (e.g., variable-length strings) and the buffer's associated dtype.
        Raises NoBufferPresent if the data buffer does not have an associated
        offsets buffer.
        """
        array = self._col
        n = len(array.buffers())
        if n == 2:
            raise NoBufferPresent(
                "This column has a fixed-length dtype so "
                "it does not have an offsets buffer"
            )
        elif n == 3:
            # Define the dtype of the returned buffer
            dtype = self._col.type
            if pa.types.is_large_string(dtype):
                dtype = (DtypeKind.INT, 64, "l", Endianness.NATIVE)
            else:
                dtype = (DtypeKind.INT, 32, "i", Endianness.NATIVE)
            return ArrowBuffer(array.buffers()[1]), dtype
        else: 
            raise NoBufferPresent(
                "Unexpected number of buffers for this column's dtype"
            )
        

class ArrowFrame(DataFrame):
    """ The God Class!!!

    https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrow-pycapsule-interface
    The Arrow PyCapsule Interface
    Rationale
    The C data interface, C stream interface and C device interface allow moving Arrow data between different implementations of Arrow. However, these interfaces don’t specify how Python libraries should expose these structs to other libraries. Prior to this, many libraries simply provided export to PyArrow data structures, using the _import_from_c and _export_to_c methods. However, this always required PyArrow to be installed. In addition, those APIs could cause memory leaks if handled improperly.

    ---

    # Dataframe Interchange Protocol
    The interchange protocol is implemented for pa.Table and pa.RecordBatch and is used to interchange data between PyArrow and other dataframe libraries that also have the protocol implemented. The data structures that are supported in the protocol are primitive data types plus the dictionary data type. The protocol also has missing data support and it supports chunking, meaning accessing the data in “batches” of rows.

    Initialize the ArrowFrame with either a pa.Table, a pa.RecordBatch, or a
    pa.RecordBatchReader (ArrowStream).

    https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema
    """
    _df: pa.Table | pa.RecordBatch
    _allow_copy: bool
    _nan_as_null: bool
    name: str
    parent_name: str | None = None
    qualified_name: str | None = None
    columns: list[ArrowColumn] = []
    properties: dict[str, Any] = {}
    @property
    def primary_key_columns(self) -> list[ArrowColumn]:
        return [f for f in self.columns if f.primary_key]
    @property
    def column_map(self) -> dict[str, ArrowColumn]:
        return {f.name: f for f in self.columns}

    def __init__(
        self, df: pa.Table | pa.RecordBatch,
        nan_as_null: bool = False,
        allow_copy: bool = True
    ) -> None:
        """
        Constructor - an instance of this (private) class is returned from
        `pa.Table.__dataframe__` or `pa.RecordBatch.__dataframe__`.
        """
        self._df = df
        # ``nan_as_null`` is a keyword intended for the consumer to tell the
        # producer to overwrite null values in the data with ``NaN`` (or
        # ``NaT``).
        if nan_as_null is True:
            raise RuntimeError(
                "nan_as_null=True currently has no effect, "
                "use the default nan_as_null=False"
            )
        self._nan_as_null = nan_as_null
        self._allow_copy = allow_copy

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> ArrowFrame:
        """
        Construct a new exchange object, potentially changing the parameters.
        ``nan_as_null`` is a keyword intended for the consumer to tell the
        producer to overwrite null values in the data with ``NaN``.
        It is intended for cases where the consumer does not support the bit
        mask or byte mask that is the producer's native representation.
        ``allow_copy`` is a keyword that defines whether or not the library is
        allowed to make a copy of the data. For example, copying data would be
        necessary if a library supports strided buffers, given that this
        protocol specifies contiguous buffers.
        """
        return ArrowFrame(self._df, nan_as_null, allow_copy)
    
    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata for the data frame, as a dictionary with string keys. The
        contents of `metadata` may be anything, they are meant for a library
        to store information that it needs to, e.g., roundtrip losslessly or
        for two implementations to share data that is not (yet) part of the
        interchange protocol specification. For avoiding collisions with other
        entries, please add name the keys with the name of the library
        followed by a period and the desired name, e.g, ``pandas.indexcol``.
        """
        if self._df.schema.metadata:
            schema_metadata = {"pyarrow." + k.decode('utf8'): v.decode('utf8')
                               for k, v in self._df.schema.metadata.items()}
            return schema_metadata
        else:
            return {}



    def num_columns(self) -> int:
        """Return the number of columns in the DataFrame"""
        return self._df.num_columns()


    def num_rows(self) -> int:
        """
        Return the number of rows in the DataFrame, if available.
        """
        return self._df.num_rows()


    def num_chunks(self) -> int:
        """Return the number of chunks the DataFrame consists of"""
        if isinstance(self._df, pa.RecordBatch):
            return 1
        else:
            # pyarrow.Table can have columns with different number
            # of chunks so we take the number of chunks that
            # .to_batches() returns as it takes the min chunk size
            # of all the columns (to_batches is a zero copy method)
            batches = self._df.to_batches()
            return len(batches)


    def column_names(self) -> Iterable[str]:
        """Return an iterator yielding the column names"""
        return self._df.schema.names


    def get_column(self, i: int) -> ArrowColumn:
        """
        Return the column at the indicated position.
        """
        return ArrowColumn(self._df.column(i),
                              allow_copy=self._allow_copy
                              )


    def get_column_by_name(self, name: str) -> ArrowColumn:
        """
        Return the column whose name is the indicated name.
        """
        return ArrowColumn(self._df.column(name),
                              allow_copy=self._allow_copy)


    def get_columns(self) -> Iterable[ArrowColumn]:
        """
        Return an iterator yielding the columns.
        """
        return [
            ArrowColumn(col, allow_copy=self._allow_copy)
            for col in self._df.columns
        ]


    def select_columns(self, indices: Sequence[int]) -> ArrowFrame:
        """
        Create a new DataFrame by selecting a subset of columns by index.
        """
        return ArrowFrame(
            self._df.select(list(indices)), self._nan_as_null, self._allow_copy
        )

    def select_columns_by_name(self, names: Sequence[str]) -> ArrowFrame:
        """
        Create a new DataFrame by selecting a subset of columns by name.
        """
        return ArrowFrame(
            self._df.select(list(names)), self._nan_as_null, self._allow_copy
        )


    def get_chunks(self, n_chunks: int | None = None) -> Iterable[ArrowFrame]:
        """
        Return an iterator yielding the chunks.

        By default (None), yields the chunks that the data is stored as by the
        producer. If given, ``n_chunks`` must be a multiple of
        ``self.num_chunks()``, meaning the producer must subdivide each chunk
        before yielding it.

        Note that the producer must ensure that all columns are chunked the
        same way.
        """
                # Subdivide chunks
        if n_chunks and n_chunks > 1:
            chunk_size = self.num_rows() // n_chunks
            if self.num_rows() % n_chunks != 0:
                chunk_size += 1
            if isinstance(self._df, pa.Table):
                batches = self._df.to_batches(max_chunksize=chunk_size)
            else:
                batches = []
                for start in range(0, chunk_size * n_chunks, chunk_size):
                    batches.append(self._df.slice(start, chunk_size))
            # In case when the size of the chunk is such that the resulting
            # list is one less chunk then n_chunks -> append an empty chunk
            if len(batches) == n_chunks - 1:
                batches.append(pa.record_batch([[]], schema=self._df.schema))
        # yields the chunks that the data is stored as
        else:
            if isinstance(self._df, pa.Table):
                batches = self._df.to_batches()
            else:
                batches = [self._df]

        # Create an iterator of RecordBatches
        iterator = [ArrowFrame(batch,
                                      self._nan_as_null,
                                      self._allow_copy)
                    for batch in batches]
        return iterator
    
    def get_batch(self) -> pa.RecordBatch:
        return (
            pa.RecordBatch.from_arrays(self._df.columns, self._df.schema)
        )
    
    def get_table(self) -> pa.Table:
        return self._df if isinstance(self._df, pa.Table) else pa.Table.from_batches([self._df])
    
    def get_schema(self) -> pa.Schema:
        return self._df.schema if isinstance(self._df, pa.Table) else self._df.schema
    
    def get_column_names(self) -> list[str]:
        return self._df.schema.names if isinstance(self._df, pa.Table) else self._df.schema.names
    
    def get_column_types(self) -> list[pa.DataType]:
        return [field.type for field in self._df.schema] if isinstance(self._df, pa.Table) else [field.type for field in self._df.schema]
    
    def set_column_types(self, column: ArrowColumn | pa.Array, type: pa.DataType) -> ArrowColumn | pa.Array:
        arr: pa.Array = pa.array(column)
        return arr.cast(type)

    def combine_chunks(self) -> ArrowFrame:
        """
        Return a new ArrowFrame with all chunks combined into a single chunk.

        Note: this may be an expensive operation for some dataframe
              implementations, so it is not done automatically.
        """
        if isinstance(self._df, pa.Table):
            return ArrowFrame(self._df.combine_chunks(), self._nan_as_null, self._allow_copy)
        else:
            return self
    
    def combine_record_batches(self, left_batch: pa.RecordBatch | ArrowFrame, right_batch: pa.RecordBatch) -> ArrowFrame:
        if isinstance(left_batch, ArrowFrame):
            left_batch = left_batch.get_batch()
        return ArrowFrame(pa.Table.from_batches([left_batch, right_batch]))
    
    def create_table(self, columns: list[ArrowColumn] | list[list[Any]] | list[pa.Array], schema: pa.Schema | list[str] | None = None) -> ArrowFrame:
        arrays = [col._col for col in columns]
        return ArrowFrame(pa.Table.from_arrays(arrays, schema=schema))
    
    def get_batch_reader(self):
        """
        Return a RecordBatchReader (ArrowStream) for the DataFrame.
        """
        if isinstance(self._df, pa.RecordBatch):
            return pa.RecordBatchReader.from_batches(self._df.schema, [self._df])
        else:
            return pa.RecordBatchReader.from_batches(self._df.schema, self._df.to_batches())

    def to_polars(self) -> polars_arrow_stream:
        """
        Convert to a Polars DataFrame using the Arrow interchange protocol.
        """
        pass

    def from_polars(self, df: polars_arrow_stream) -> ArrowFrame:
        """
        Create an ArrowFrame from a Polars DataFrame using the Arrow interchange protocol.
        """
        pass

    def to_pandas(self) -> pandas.DataFrame:
        """
        Convert to a Pandas DataFrame using the Arrow interchange protocol.
        """
        pass

    def from_pandas(self, df: pandas.DataFrame) -> ArrowFrame:
        """
        Create an ArrowFrame from a Pandas DataFrame using the Arrow interchange protocol.
        """
        pass

    def to_arrow(self) -> pa.Table:
        """
        Convert to a PyArrow Table.
        """
        return self._df if isinstance(self._df, pa.Table) else pa.Table.from_batches([self._df])

    def from_arrow(self, table: pa.Table | pa.RecordBatch) -> ArrowFrame:
        """
        Create an ArrowFrame from a PyArrow Table.
        """
        return ArrowFrame(table, self._nan_as_null, self._allow_copy)