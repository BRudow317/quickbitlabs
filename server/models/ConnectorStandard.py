from __future__ import annotations
from pydantic import BaseModel, Field, PrivateAttr
from typing import Literal, Any
from collections.abc import Iterable

# universal data format
DataStream = Iterable[dict[str, Any]]

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


class Column(BaseModel):
    source_name: str
    target_name: str | None = None          # set during mapping if names diverge
    datatype: PythonTypes
    raw_type: str | None = None          # raw source type ("currency", "VARCHAR2")
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    length: int | None = None               # strings
    precision: int | None = None            # numbers
    scale: int | None = None                # numbers
    source_description: str | None = None   # SF label, postgres comment, etc.
    # ── contract fields ──
    read_only: bool = False                 # formula fields, auto-numbers, system timestamps — skip on write
    default_value: Any | None = None             # value to use when source yields null for a non-nullable column
    enum_values: list[Any] | None = None   # valid values for picklists / ENUMs — validated before send
    timezone: str | None = None            # IANA tz name of source ("America/New_York", "UTC"); None = already UTC-aware
    array: bool = False                    # disambiguates json datatype: True = list, False = dict

    _table: Table | None = PrivateAttr(default=None)

    @property
    def table(self) -> Table | None:
        return self._table


class Table(BaseModel):
    source_name: str
    target_name: str | None = None
    columns: list[Column] = Field(default_factory=list)
    source_description: str | None = None

    _schema: Schema | None = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:  # noqa: ARG002
        for col in self.columns:
            col._table = self

    @property
    def parent_schema(self) -> Schema | None:
        return self._schema

    @property
    def primary_key_columns(self) -> list[Column]:
        return [c for c in self.columns if c.primary_key]

    @property
    def column_map(self) -> dict[str, Column]:
        return {c.source_name: c for c in self.columns}


class Schema(BaseModel):
    """One of the smallest, but most important parts of the entire project.
    All metadata exists as a subset of this object.
    Schema -> Tables(objects) -> Columns(fields, attributes) -> DataStream(data, records, bytes, json, etc.)"""
    source_name: str = ''
    target_name: str | None = None
    tables: list[Table] = Field(default_factory=list)
    source_description: str | None = None

    def model_post_init(self, __context: Any) -> None:  # noqa: ARG002
        for table in self.tables:
            table._schema = self
            for col in table.columns:
                col._table = table

    @property
    def table_map(self) -> dict[str, Table]:
        return {t.source_name: t for t in self.tables}
