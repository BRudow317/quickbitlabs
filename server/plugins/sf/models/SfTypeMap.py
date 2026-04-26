from __future__ import annotations
import datetime, json, re
from decimal import Decimal
from typing import Any, Literal, TypeVar
from server.plugins.PluginModels import Catalog, Entity, Column, ArrowReader, Records, arrow_type_literal, pa_type_to_literal
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
    "reference":       pa.utf8(),       # FK - SF ID stored as string, metadata captured separately
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
    "address":         None,            # compound - exclude at describe time
    "location":        None,            # compound geolocation - exclude
}

# ---------------------------------------------------------------
# SF type → arrow_type_literal  (Column.arrow_type_id)
# ---------------------------------------------------------------
SF_TO_ARROW_LITERAL: dict[str, arrow_type_literal] = {
    "string":          "utf8",
    "textarea":        "large_string",
    "phone":           "utf8",
    "email":           "utf8",
    "url":             "utf8",
    "picklist":        "utf8",
    "multipicklist":   "large_string",
    "combobox":        "utf8",
    "id":              "utf8",
    "reference":       "utf8",
    "boolean":         "bool",
    "int":             "int32",
    "long":            "int64",
    "double":          "float64",
    "currency":        "decimal128",
    "percent":         "float64",
    "date":            "date32",
    "datetime":        "timestamp_ms",
    "time":            "time64_us",
    "base64":          "large_binary",
    "encryptedstring": "utf8",
    "anyType":         "utf8",
}

# FieldDefinition.DataType (bulk describe) → PythonTypes
# DataType comes with optional params e.g. "Text(80)", "Number(18, 0)" - strip before lookup.
def _normalize_fielddef_type(raw: str) -> str:
    """Strip parameters from FieldDefinition DataType strings.
    e.g. 'Text(80)' -> 'text', 'Number(18, 0)' -> 'number'
    """
    return re.sub(r'\(.*\)', '', raw).strip().lower()

# Legacy mapping.
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date",     # datetime.date
    "time",     # datetime.time
    "byte",
    "bytearray",
    "json",     # dict or list
]

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
    'base64':                'byte',
    'file':                  'byte',
    'json':                  'json',
    # Compound - filtered upstream before this map is consulted
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
    'base64':          'byte',
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


def _to_time(v: str) -> datetime.time:
    # Salesforce time fields use ISO-8601 with a trailing 'Z' (e.g. '00:00:00.000Z')
    # which datetime.time.fromisoformat() rejects. Strip it before parsing.
    if isinstance(v, str) and v.endswith('Z'):
        v = v[:-1]
    return datetime.time.fromisoformat(v)


ARROW_TO_SF_TYPE: dict[arrow_type_literal, str] = {
    "null":         "anyType",
    "bool":         "boolean",
    "int8":         "int",
    "int16":        "int",
    "int32":        "int",
    "int64":        "long",
    "uint8":        "int",
    "uint16":       "int",
    "uint32":       "long",
    "uint64":       "long",
    "float16":      "double",
    "float32":      "double",
    "float64":      "double",
    "decimal128":   "currency",
    "decimal256":   "currency",
    "string":       "string",
    "utf8":         "string",
    "large_string": "textarea",
    "string_view":  "string",
    "binary":       "base64",
    "large_binary": "base64",
    "date32":       "date",
    "date64":       "date",
    "timestamp_s":  "datetime",
    "timestamp_ms": "datetime",
    "timestamp_us": "datetime",
    "timestamp_ns": "datetime",
    "time32_s":     "time",
    "time32_ms":    "time",
    "time64_us":    "time",
    "time64_ns":    "time",
    "duration_s":   "string",
    "duration_ms":  "string",
    "duration_us":  "string",
    "duration_ns":  "string",
    "list":         "multipicklist",
    "large_list":   "multipicklist",
    "list_view":    "multipicklist",
    "large_list_view": "multipicklist",
    "struct":       "anyType",
    "map":          "anyType",
    "dictionary":   "picklist",
    "json":         "anyType",
    "uuid":         "string",
}


def arrow_to_sf_type(t: pa.DataType) -> str:
    """Map any pa.DataType to its closest Salesforce field type string."""
    return ARROW_TO_SF_TYPE.get(pa_type_to_literal(t), "anyType")


def _to_decimal(v: Any) -> Decimal:
    # str() avoids floating-point representation noise (e.g. 1234.56 not 1234.5599999...)
    return Decimal(str(v))


_SF_CONVERTERS: dict[str, Any] = {
    'int':      int,
    'integer':  int,
    'double':   float,
    'currency': _to_decimal,  # decimal128 requires Decimal, not float
    'percent':  float,
    'boolean':  _to_bool,
    'date':     datetime.date.fromisoformat,
    'datetime': _to_datetime,
    'time':     _to_time,
}


def sf_to_python(sf_type: str, value: Any) -> Any:
    """Convert a Salesforce field value to its native Python type."""
    if value is None or value == '': return None
    converter = _SF_CONVERTERS.get(sf_type)
    if converter:
        try: return converter(value)
        except (ValueError, TypeError): return value
    return value


def python_to_sf(value: Any) -> str:
    """Convert a Python value to its Salesforce API string representation."""
    if value is None: return ''
    if isinstance(value, bool): return 'true' if value else 'false'
    if isinstance(value, datetime.datetime): return value.isoformat()
    if isinstance(value, datetime.date): return value.isoformat()
    if isinstance(value, datetime.time): return value.isoformat()
    if isinstance(value, (dict, list)): return json.dumps(value)
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
        if v is _CLEAR: out[k] = None  # explicit null - SF will clear the field
        elif v is not None: out[k] = python_to_sf(v)
    return out


def date_to_iso8601(date: datetime.date) -> str:
    """Returns an ISO8601 string from a date"""
    datetimestr = date.strftime('%Y-%m-%dT%H:%M:%S')
    timezonestr = date.strftime('%z')
    return (
        f'{datetimestr}{timezonestr[0:3]}:{timezonestr[3:5]}'
        .replace(':', '%3A')
        .replace('+', '%2B')
    )