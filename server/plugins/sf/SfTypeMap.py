from __future__ import annotations
import datetime, json, re
from typing import Any, TypeVar
from server.plugins.PluginModels import PythonTypes
import pyarrow as pa

SF_TYPE_TO_ARROW = {
    "string":          pa.utf8(),
    "textarea":        pa.large_utf8(),
    "phone":           pa.utf8(),
    "email":           pa.utf8(),
    "url":             pa.utf8(),
    "picklist":        pa.utf8(),       # or pa.dictionary if cardinality is low
    "multipicklist":   pa.large_utf8(), # semicolon-separated, SF's crime against data
    "combobox":        pa.utf8(),
    "id":              pa.utf8(),       # SF IDs are 18-char strings
    "reference":       pa.utf8(),       # FK — SF ID stored as string, metadata captured separately
    "boolean":         pa.bool_(),
    "int":             pa.int32(),
    "long":            pa.int64(),
    "double":          pa.float64(),
    "currency":        pa.decimal128(18, 2),  # don't use float for money
    "percent":         pa.float64(),
    "date":            pa.date32(),
    "datetime":        pa.timestamp("ms", tz="UTC"),  # SF datetimes are always UTC
    "time":            pa.time64("us"),
    "base64":          pa.large_binary(),
    "encryptedstring": pa.utf8(),       # SF-side encrypted, comes to you as string
    "anyType":         pa.utf8(),       # fallback, SF uses this for polymorphic value fields
    "address":         None,            # compound — exclude at describe time
    "location":        None,            # compound geolocation — exclude
}


# FieldDefinition.DataType (bulk describe) → PythonTypes
# DataType comes with optional params e.g. "Text(80)", "Number(18, 0)" — strip before lookup.
def _normalize_fielddef_type(raw: str) -> str:
    """Strip parameters from FieldDefinition DataType strings.
    e.g. 'Text(80)' → 'text', 'Number(18, 0)' → 'number'
    """
    return re.sub(r'\(.*\)', '', raw).strip().lower()
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

_CLEAR = object()  # sentinel for explicit null writes to SF

def prepare_record(record: dict[str, Any]) -> dict[str, Any]:
    """Convert Python values to SF API representation.
    Use _CLEAR as a value to explicitly null a field in SF.
    Omit a key entirely to leave the field unchanged.
    """
    out = {}
    for k, v in record.items():
        if v is _CLEAR:
            out[k] = None  # explicit null — SF will clear the field
        elif v is not None:
            out[k] = python_to_sf(v)
    return out
