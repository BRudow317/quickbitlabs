from __future__ import annotations
from pydantic import BaseModel
from typing import Literal
from typing import Any
from collections.abc import Iterable

# universal data format
data: Iterable[dict[str, Any]]

# python types
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date", # datetime.date
    "time", # datetime.time
    "binary", # bytes or bytearray
    "json", # dict or list 
]

class Column(BaseModel):
    source_name: str
    target_name: str | None = None # set during mapping if names diverge
    datatype: PythonTypes
    source_type: str | None = None # raw source type ("currency", "VARCHAR2")
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    length: int | None = None # strings
    precision: int | None = None # numbers
    scale: int | None = None # numbers
    description: str | None = None # SF label, postgres comment, etc.

class Table(BaseModel):
    source_name: str
    target_name: str | None = None
    columns: list[Column]
    description: str | None = None

class Schema(BaseModel):
    source_name: str
    target_name: str | None = None
    tables: list[Table]