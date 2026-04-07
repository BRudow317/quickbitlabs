from __future__ import annotations
import pyarrow as pa
import polars as pl
import oracledb
from typing import Any, Iterator, Iterable, TYPE_CHECKING
from .OracleClient import OracleClient

from .arrow_array import ArrowArray
from oracledb.arrow_impl import DataFrameImpl
from oracledb.base import BaseMetaClass
from oracledb import errors
from polars._typing import (
        ArrowArrayExportable,
        ArrowStreamExportable,
        Orientation,
        PolarsDataType,
        SchemaDefinition,
        SchemaDict,
    )
from polars.interchange.protocol import SupportsInterchange

class OracleDataFrame:
    """Wraps the native oracledb.OracleDataFrame to provide direct zero-copy ecosystem conversions."""
    def __init__(
            self,
            odf: oracledb.DataFrame
            ):
        self._odf: oracledb.DataFrame = odf
    
    def __call__(self) -> oracledb.DataFrame:
        """Returns the underlying oracledb.OracleDataFrame for direct access to its methods."""
        return self._odf
    
    def _initialize(self, impl: DataFrameImpl):
        """
        Initializes the object given the implementation.
        """
        self._impl = impl
        self._arrays = [ArrowArray._from_impl(a) for a in impl.get_arrays()]
        self._arrays_by_name = {}
        for array in self._arrays:
            self._arrays_by_name[array.name] = array

    def __arrow_c_stream__(self, requested_schema=None):
        """Returns the ArrowArrayStream PyCapsule which allows direct conversion
        to foreign data frames that support this interface.
        """
        if requested_schema is not None:
            raise NotImplementedError("requested_schema")
        return self._impl.get_stream_capsule()

    def get_column(self, i: int) -> ArrowArray:
        """
        Returns an :ref:`ArrowArray <oraclearrowarrayobj>` object for the
        column at the given index ``i``. If the index is out of range, an
        IndexError exception is raised.
        """
        if i < 0 or i >= self.num_columns():
            raise IndexError(
                f"Column index {i} is out of bounds for "
                f"DataFrame with {self.num_columns()} columns"
            )
        return self._arrays[i]

    def get_column_by_name(self, name: str) -> ArrowArray:
        """
        Returns an :ref:`ArrowArray <oraclearrowarrayobj>` object for the
        column with the given name ``name``. If the column name is not found,
        a KeyError exception is raised.
        """
        try:
            return self._arrays_by_name[name]
        except KeyError:
            raise KeyError(f"Column {name} not found in DataFrame")

    def column_arrays(self) -> list[ArrowArray]:
        """Return the underlying Arrow PyCapsule arrays.
        original: return self._arrays
        """
        return self.column_arrays()

    def column_names(self) -> list[str]:
        """Return the column names of the data frame.
        original: return [a.name for a in self._arrays]"""
        return self.column_names()

    def num_rows(self) -> int:
        """Return the number of rows in the data frame."""
        return self.num_rows()

    def num_columns(self) -> int:
        """Return the number of columns in the data frame."""
        return self.num_columns()

    def to_pyarrow(self) -> pa.Table:
        """Zero-copy conversion to a PyArrow Table via the PyCapsule interface."""
        return pa.Table.from_arrays(self.column_arrays(), names=self.column_names())

    def to_polars(self) -> pl.DataFrame | pl.LazyFrame:
        """Zero-copy conversion to a Polars DataFrame."""
        ret = pl.from_arrow(self.to_pyarrow())
        if isinstance(ret, pl.LazyFrame):
            return ret
        if isinstance(ret, pl.DataFrame):
            return ret
        raise TypeError(f"Unexpected return type from pl.from_arrow: {type(ret)}")
    
    def from_dataframe(self, 
                       df: SupportsInterchange | ArrowArrayExportable | ArrowStreamExportable,
                        *,
                        allow_copy: bool | None = None,
                        rechunk: bool = True,
                    ) -> pl.DataFrame: 
        """Build a Polars DataFrame from any dataframe supporting the PyCapsule Interface."""
        polar_data_frame: pl.DataFrame = pl.from_dataframe(df=df, 
                          allow_copy=allow_copy, 
                          rechunk=rechunk)
        return polar_data_frame

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        """Yield PyArrow RecordBatches directly from the underlying data."""
        return self.to_pyarrow().to_batches()


class OracleArrowFrame:
    """Facade for executing queries and streaming native Arrow data from Oracle."""
    
    def __init__(self, client: OracleClient):
        self.client: OracleClient = client

    def fetch_df_all(
        self, 
        statement: str, 
        parameters: list | tuple | dict | None = None, 
        arraysize: int = 1000, 
        fetch_decimals: bool = True
    ) -> OracleDataFrame:
        """Fetch all rows into a single zero-copy OracleDataFrame facade."""
        odf: oracledb.DataFrame = self.client.get_con().fetch_df_all(
            statement=statement,
            parameters=parameters,
            arraysize=arraysize,
            fetch_decimals=fetch_decimals
        )
        return OracleDataFrame(odf)

    def fetch_odf(
        self, 
        statement: str, 
        parameters: list | tuple | dict | None = None, 
        size: int = 50_000, 
        fetch_decimals: bool = True
    ) -> Iterator[OracleDataFrame]:
        """Yield batches of zero-copy OracleDataFrame facades for memory-safe streaming.
        Connection.fetch_df_batches(statement, parameters=None, size=None)"""
        iterator = self.client.get_con().fetch_df_batches(
            statement=statement,
            parameters=parameters,
            size=size,
            fetch_decimals=fetch_decimals
        )
        for odf in iterator:
            yield OracleDataFrame(odf)

    def get_arrow_batches(
        self, 
        statement: str, 
        parameters: list | tuple | dict | None = None, 
        batch_size: int = 50_000
    ) -> Iterator[pa.RecordBatch]:
        for odf in self.fetch_odf(statement, parameters, size=batch_size):
            yield from odf.to_batches()

    def insert_df(self):
        """Bulk Insert	Connection.insert_df()	Direct Path Load"""

        
    def fast_insert_df(self, statement: str, parameters: Any,
        *,
        batcherrors: bool = False,
        arraydmlrowcounts: bool = False,
        suspend_on_success: bool = False,
        batch_size: int = 2**32 - 1,):
        """Fast Insert	Cursor.executemany()	Bind Arrow-supported objects
        To insert data currently in OracleDataFrame format into Oracle Database requires 
        it to be converted. For example, you could convert it into a Pandas DataFrame for 
        insert with the Pandas method to_sql(). Or convert into a Python list via the 
        PyArrow Table.to_pylist() method and then use standard python-oracledb 
        functionality to execute a SQL INSERT statement.
        """
        cursor: oracledb.Cursor = self.client.get_con().cursor()
        with cursor:
            cursor.executemany(statement=statement, 
                            parameters=parameters,
                            batcherrors=batcherrors,
                            arraydmlrowcounts=arraydmlrowcounts,
                            suspend_on_success=suspend_on_success,
                            batch_size=batch_size)