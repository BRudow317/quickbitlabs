from __future__ import annotations
import re
import urllib.parse
from datetime import date, datetime, timezone
from string import Formatter
from typing import Any, AnyStr, Iterable, cast
from server.plugins.PluginModels import Catalog, Entity, Operator, OperatorGroup

soql_escapes = str.maketrans({
    '\\': '\\\\',
    '\'': '\\\'',
    '"': '\\"',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
    '\b': '\\b',
    '\f': '\\f',
})

soql_like_escapes = str.maketrans({
    '%': '\\%',
    '_': '\\_',
})


class SoqlFormatter(Formatter):
    """ Custom formatter to apply quoting or the :literal format spec """

    def format_field(self, value: Any, format_spec: str) -> Any:
        if not format_spec:
            return quote_soql_value(value)
        if format_spec == 'literal':
            # literal: allows circumventing everything while still using the same format string
            return value
        if format_spec == 'like':
            # like: allows escaping substring used in LIKE expression
            # does not quote
            return (str(value).translate(soql_escapes)
                    .translate(soql_like_escapes))
        return super().format_field(value, format_spec)


def format_soql(query: str, *args: Any, **kwargs: Any) -> str:
    """ Insert values quoted for SOQL into a format string """
    return SoqlFormatter().vformat(query, args, kwargs)

def quote_soql_value(value: Any) -> str:
    """ Quote/escape either an individual value or a list of values
    for a SOQL value expression """
    if isinstance(value, str):
        return "'" + value.translate(soql_escapes) + "'"
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    if value is None:
        return 'null'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, set, tuple)):
        quoted_items = [quote_soql_value(member) for member in cast(Iterable[Any], value)]
        return '(' + ','.join(quoted_items) + ')'
    if isinstance(value, datetime):
        # Salesforce spec requires a datetime literal that is not naive and without MS
        value = value.replace(microsecond=0)
        value = value.astimezone(tz=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise ValueError('unquotable value type')


def format_external_id(field: str, value: str | bytes) -> str:
    """ Create an external ID string for use with get() or upsert() """
    return field + '/' + urllib.parse.quote(value, safe='')

def _escape_soql_value(value: Any) -> str:
    """Escape a scalar value for safe SOQL interpolation."""
    if value is None: return "null"
    if isinstance(value, bool): return "true" if value else "false"
    if isinstance(value, (int, float)): return str(value)
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, date): return value.isoformat()
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"

def _operator_to_soql(op: Operator) -> str:
    field = op.independent.name
    operator = op.operator
    if operator == "IS NULL": return f"{field} = null"
    if operator == "IS NOT NULL": return f"{field} != null"
    if operator in ("=", "=="): return f"{field} = {_escape_soql_value(op.dependent)}"
    if operator == "IN":
        values = ", ".join(_escape_soql_value(v) for v in (op.dependent or []))
        return f"{field} IN ({values})"
    return f"{field} {operator} {_escape_soql_value(op.dependent)}"

def _group_to_soql(group: OperatorGroup) -> str:
    parts: list[str] = []
    for item in group.operators:
        if isinstance(item, OperatorGroup): parts.append(f"({_group_to_soql(item)})")
        else: parts.append(_operator_to_soql(item))
    if not parts: return ""
    if group.condition == "NOT": return f"NOT ({' AND '.join(parts)})"
    return f" {group.condition} ".join(parts)

def build_soql(catalog: Catalog, entity: Entity) -> str:
    """
    Build a SOQL SELECT from Catalog + Entity using PluginModels contracts.
    Columns without an Arrow type mapping are excluded (compound types, etc).
    Operator groups, sort fields, and limit are applied when present.
    """
    queryable = [c for c in entity.columns if c.arrow_type is not None]
    if not queryable: raise ValueError(f"No queryable columns found for entity '{entity.name}'.")
    fields = ", ".join(c.name for c in queryable)
    soql = f"SELECT {fields} FROM {entity.name}"
    where_parts = [
        p for g in catalog.operator_groups
        if g.operators and (p := _group_to_soql(g))
    ]
    if where_parts: soql += f" WHERE {' AND '.join(where_parts)}"
    if catalog.sort_fields:
        order_clauses = [f"{s.column.name} {s.direction}" for s in catalog.sort_fields]
        soql += f" ORDER BY {', '.join(order_clauses)}"
    if catalog.limit is not None: soql += f" LIMIT {catalog.limit}"

    return soql

def get_object_from_soql(soql: str) -> str | None:
    """Attempt to parse the main object name from a SOQL query string for routing purposes."""
    soql_no_parens = re.sub(r'\(.*?\)', '', soql)
    match = re.search(r'\bFROM\s+([a-zA-Z0-9_]+)', soql_no_parens, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def build_count_soql(object_name: str) -> str:
    """Build a SOQL aggregate count query for an SObject.
    Uses COUNT() (no field arg) so the total is returned in response['totalSize']
    without needing to parse an aggregate record field.
    """
    return f"SELECT COUNT() FROM {object_name}"

def build_null_check_soql(object_name: str, column_name: str) -> str:
    """Build a SOQL query to detect whether any records have a null value in a
    column that Salesforce describe reports as non-nullable (nillable=false).
    Returns a COUNT() so only 'totalSize' needs to be inspected.
    """
    return f"SELECT COUNT() FROM {object_name} WHERE {column_name} = null"

def filter_null_bytes(b: AnyStr) -> AnyStr:
        """https://github.com/airbytehq/airbyte/issues/8300"""
        if isinstance(b, str):
            return b.replace("\x00", "")
        if isinstance(b, bytes):
            return b.replace(b"\x00", b"")
        raise TypeError("Expected str or bytes")