from __future__ import annotations

from sqlalchemy import (
    String, Integer, Float, Boolean, DateTime,
    Date, Time, LargeBinary, Text, SmallInteger,
    BigInteger, Numeric, REAL,
)
from sqlalchemy.dialects.postgresql import JSONB, JSON, UUID, ARRAY
from sqlalchemy.types import TypeEngine

from server.models.StandardTemplate import PythonTypes

import logging
logger = logging.getLogger(__name__)

# PythonTypes -> SQLAlchemy (used when writing schema TO postgres)
PYTHON_TO_PG: dict[PythonTypes, type[TypeEngine]] = {
    'string':   String,
    'integer':  Integer,
    'float':    Float,
    'boolean':  Boolean,
    'datetime': DateTime,
    'date':     Date,
    'time':     Time,
    'binary':   LargeBinary,
    'json':     JSONB,
}

# SQLAlchemy type class -> PythonTypes (used when reading schema FROM postgres)
PG_TO_PYTHON: list[tuple[type[TypeEngine], PythonTypes]] = [
    (Boolean,      'boolean'),
    (SmallInteger, 'integer'),
    (BigInteger,   'integer'),
    (Integer,      'integer'),
    (Numeric,      'float'),
    (REAL,         'float'),
    (Float,        'float'),
    (DateTime,     'datetime'),
    (Date,         'date'),
    (Time,         'time'),
    (LargeBinary,  'binary'),
    (JSONB,        'json'),
    (JSON,         'json'),
    (ARRAY,        'json'),
    (UUID,         'string'),
    (Text,         'string'),
    (String,       'string'),  # keep last — catch-all for char types
]


def pg_to_python_type(sa_type: TypeEngine) -> PythonTypes:
    """Map a reflected SQLAlchemy type instance to a PythonType."""
    for sa_cls, python_type in PG_TO_PYTHON:
        if isinstance(sa_type, sa_cls):
            return python_type
    logger.warning(f"Unknown PG type '{type(sa_type).__name__}', defaulting to 'string'")
    return 'string'


def pg_source_type(sa_type: TypeEngine) -> str:
    """Capture the raw Postgres type string for the Column.source_type field."""
    try:
        return str(sa_type)
    except Exception:
        return type(sa_type).__name__