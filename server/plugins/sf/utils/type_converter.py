from __future__ import annotations

import datetime
import json
from typing import Any, TypeVar

from server.models.ConnectorStandard import PythonTypes

# FieldDefinition.DataType (bulk describe) → PythonTypes
# DataType comes with optional params e.g. "Text(80)", "Number(18, 0)" — strip before lookup.
SF_FIELDDEF_TYPE_MAP: dict[str, PythonTypes] = {
    'id':                    'string',
    'text':                  'string',
    'textarea':              'string',
    'longtextarea':          'string',
    'html':                  'string',
    'richtextarea':          'string',
    'phone':                 'string',
    'url':                   'string',
    'email':                 'string',
    'picklist':              'string',
    'multi-select picklist': 'string',
    'combobox':              'string',
    'reference':             'string',
    'autonumber':            'string',
    'encryptedtext':         'string',
    'hierarchy':             'string',
    'anytype':               'string',
    'number':                'float',
    'currency':              'float',
    'percent':               'float',
    'double':                'float',
    'integer':               'integer',
    'long':                  'integer',
    'checkbox':              'boolean',
    'boolean':               'boolean',
    'date':                  'date',
    'date/time':             'datetime',
    'datetime':              'datetime',
    'time':                  'time',
    'base64':                'binary',
    'file':                  'binary',
    'json':                  'json',
    # Compound — filtered upstream before this map is consulted
    'address':               'json',
    'location':              'json',
}

# Individual SObject describe response `type` field → PythonTypes
SF_TYPE_MAP: dict[str, PythonTypes] = {
    'id':              'string',
    'string':          'string',
    'textarea':        'string',
    'email':           'string',
    'phone':           'string',
    'url':             'string',
    'encryptedstring': 'string',
    'picklist':        'string',
    'multipicklist':   'string',
    'combobox':        'string',
    'reference':       'string',
    'anyType':         'string',
    'int':             'integer',
    'integer':         'integer',
    'double':          'float',
    'currency':        'float',
    'percent':         'float',
    'boolean':         'boolean',
    'date':            'date',
    'datetime':        'datetime',
    'time':            'time',
    'base64':          'binary',
    'address':         'json',
    'location':        'json',
}


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() == 'true'


def _to_datetime(v: str) -> datetime.datetime:
    if v.endswith('Z'):
        v = v[:-1] + '+00:00'
    elif len(v) > 5 and v[-5] in '+-' and ':' not in v[-5:]:
        v = v[:-2] + ':' + v[-2:]
    return datetime.datetime.fromisoformat(v)


_SF_CONVERTERS: dict[str, Any] = {
    'int':      int,
    'integer':  int,
    'double':   float,
    'currency': float,
    'percent':  float,
    'boolean':  _to_bool,
    'date':     datetime.date.fromisoformat,
    'datetime': _to_datetime,
    'time':     datetime.time.fromisoformat,
}


def sf_to_python(sf_type: str, value: Any) -> Any:
    """Convert a Salesforce field value to its native Python type."""
    if value is None or value == '':
        return None
    converter = _SF_CONVERTERS.get(sf_type)
    if converter:
        try:
            return converter(value)
        except (ValueError, TypeError):
            return value
    return value


def python_to_sf(value: Any) -> str:
    """Convert a Python value to its Salesforce API string representation."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def cast_record(record: dict[str, Any], field_types: dict[str, str]) -> dict[str, Any]:
    """Apply sf_to_python to each field in a record using a {field_name: sf_type} map."""
    return {
        k: sf_to_python(field_types[k], v) if k in field_types else v
        for k, v in record.items()
    }


def prepare_record(record: dict[str, Any]) -> dict[str, Any]:
    """Convert Python values to SF API strings, dropping Nones."""
    return {k: python_to_sf(v) for k, v in record.items() if v is not None}
