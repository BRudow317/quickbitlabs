from __future__ import annotations

from pydantic import BaseModel
from typing import Any

class UniversalColumn(BaseModel):
    """An agnostic representation of a database column or API field."""
    name: str
    # We restrict the types to a known universal set
    datatype: str  # e.g., 'string', 'integer', 'float', 'boolean', 'datetime'
    primary_key: bool = False
    nullable: bool = True
    length: int | None = None

class UniversalTable(BaseModel):
    """An agnostic representation of a database table or Object."""
    name: str
    columns: list[UniversalColumn]

class UniversalRecord(BaseModel):
    """An agnostic wrapper for a single row of data."""
    stream_name: str
    data: dict[str, Any]