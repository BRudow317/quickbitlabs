from __future__ import annotations

import datetime
import re
from typing import Any

from server.plugins.PluginModels import Catalog, Entity, Operator, OperatorGroup

def _escape_soql_value(value: Any) -> str:
    """Escape a scalar value for safe SOQL interpolation."""
    if value is None: return "null"
    if isinstance(value, bool): return "true" if value else "false"
    if isinstance(value, (int, float)): return str(value)
    if isinstance(value, datetime.datetime): return value.isoformat()
    if isinstance(value, datetime.date): return value.isoformat()
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
