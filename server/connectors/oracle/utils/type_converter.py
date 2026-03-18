from __future__ import annotations
from typing import Any
from sqlalchemy import (
    String, Integer, Float, Boolean, DateTime, Date, Time, LargeBinary, JSON, Numeric
)
from sqlalchemy.types import TypeEngine
from server.models.ConnectorStandard import PythonTypes

# SQLAlchemy -> PythonTypes
ORACLE_TO_PYTHON: dict[type[TypeEngine], PythonTypes] = {
    String: "string",
    Integer: "integer",
    Float: "float",
    Numeric: "float", # Oracle NUMBER maps here
    Boolean: "boolean",
    DateTime: "datetime",
    Date: "date",
    Time: "time",
    LargeBinary: "binary",
    JSON: "json",
}

# PythonTypes -> SQLAlchemy
PYTHON_TO_ORACLE: dict[PythonTypes, type[TypeEngine]] = {
    "string": String,
    "integer": Integer,
    "float": Float,
    "boolean": Boolean,
    "datetime": DateTime,
    "date": Date,
    "time": Time,
    "binary": LargeBinary,
    "json": JSON,
}

def get_python_type(sa_type: Any) -> PythonTypes:
    """Map a SQLAlchemy type instance to a ConnectorStandard PythonType."""
    for sa_base, py_type in ORACLE_TO_PYTHON.items():
        if isinstance(sa_type, sa_base):
            # Special case for NUMBER(p, 0) -> integer
            if isinstance(sa_type, Numeric) and sa_type.scale == 0:
                return "integer"
            return py_type
    return "string"
