from __future__ import annotations
from typing import Any
import pyarrow as pa
from server.plugins.PluginModels import Catalog, Column, Entity, Operation, OperatorGroup

def _get_root_entity(catalog: Catalog) -> Entity:
    if len(catalog.entities) == 1:
        return catalog.entities[0]
    right_names = {j.right_entity.name for j in catalog.joins}
    roots = [e for e in catalog.entities if e.name not in right_names]
    return roots[0] if roots else catalog.entities[0]

def _bind_name(col: str, binds: dict[str, Any]) -> str:
    base = col.replace(".", "_")
    if base not in binds:
        return base
    n = 2
    while f"{base}_{n}" in binds:
        n += 1
    return f"{base}_{n}"

def _col_ref(col: Column) -> str:
    """Produce a proper DuckDB double-quoted reference: "Entity"."Col" or just "Col"."""
    entity = col.locator.entity_name if col.locator else None
    return f'"{entity}"."{col.name}"' if entity else f'"{col.name}"'

def _parse_operation(op: Operation, binds: dict[str, Any]) -> str:
    col_sql = _col_ref(op.independent)
    bind_key = op.independent.name  # stable key for the binds dict
    operator = op.operator
    dependent = op.dependent

    if operator == "IS NULL":
        return f"{col_sql} IS NULL"
    if operator == "IS NOT NULL":
        return f"{col_sql} IS NOT NULL"

    sql_op = "=" if operator == "==" else operator

    if isinstance(dependent, pa.Field):
        return f"{col_sql} {sql_op} ?"

    if isinstance(dependent, Column):
        return f"{col_sql} {sql_op} {_col_ref(dependent)}"

    if sql_op in ("IN", "NOT IN"):
        if not isinstance(dependent, list) or not dependent:
            return "1=0" if sql_op == "IN" else "1=1"
        placeholders: list[str] = []
        for val in dependent:
            name = _bind_name(bind_key, binds)
            placeholders.append(f"${name}")
            binds[name] = val
        return f"{col_sql} {sql_op} ({', '.join(placeholders)})"

    if sql_op in ("BETWEEN", "NOT BETWEEN"):
        if not isinstance(dependent, list) or len(dependent) != 2:
            raise ValueError(f"{sql_op} requires a 2-element list [low, high], got: {dependent!r}")
        lo_name = _bind_name(f"{bind_key}_lo", binds)
        hi_name = _bind_name(f"{bind_key}_hi", binds)
        binds[lo_name] = dependent[0]
        binds[hi_name] = dependent[1]
        return f"{col_sql} {sql_op} ${lo_name} AND ${hi_name}"

    name = _bind_name(bind_key, binds)
    binds[name] = dependent
    return f"{col_sql} {sql_op} ${name}"

def _parse_operator_group(group: OperatorGroup, binds: dict[str, Any]) -> str:
    if not group.operation_group:
        return ""
    clauses: list[str] = []
    for item in group.operation_group:
        if isinstance(item, OperatorGroup):
            clause = _parse_operator_group(item, binds)
            if clause:
                clauses.append(f"({clause})")
        else:
            clauses.append(_parse_operation(item, binds))
    if group.condition == "NOT":
        return f"NOT ({clauses[0]})" if clauses else ""
    return f" {group.condition} ".join(clauses)

def _build_where(operator_groups: list[OperatorGroup]) -> tuple[str, dict[str, Any]]:
    if not operator_groups:
        return "", {}
    binds: dict[str, Any] = {}
    clauses: list[str] = []
    for group in operator_groups:
        clause = _parse_operator_group(group, binds)
        if clause:
            clauses.append(f"({clause})")
    if not clauses:
        return "", {}
    return " WHERE " + " AND ".join(clauses), binds

def build_duckdb_select(catalog: Catalog) -> tuple[str, dict[str, Any]]:
    if not catalog.entities:
        raise ValueError("Catalog must have at least one entity.")

    root = _get_root_entity(catalog)

    # Column list - DuckDB views will have un-qualified names, 
    # but the master query joins them, so we should use qualified names if there are joins.
    # Actually, if we use Views, the view name acts as the table name.
    col_names = []
    for e in catalog.entities:
        for c in e.columns:
            col_names.append(f'"{e.name}"."{c.name}"')
    
    cols_str = ", ".join(col_names) if col_names else "*"

    # FROM + JOINs
    join_parts: list[str] = []
    for j in catalog.joins:
        join_parts.append(
            f'{j.join_type} JOIN "{j.right_entity.name}" '
            f'ON "{j.left_entity.name}"."{j.left_column.name}" = "{j.right_entity.name}"."{j.right_column.name}"'
        )
    join_clause = (" " + " ".join(join_parts)) if join_parts else ""

    # WHERE
    where_clause, binds = _build_where(catalog.operator_groups)

    # ORDER BY
    sort_parts: list[str] = []
    for s in catalog.sort_columns:
        entity_ref = s.column.locator.entity_name if s.column.locator else ""
        col_ref = f'"{entity_ref}"."{s.column.name}"' if entity_ref else f'"{s.column.name}"'
        nulls = ""
        if s.nulls_first is True:
            nulls = " NULLS FIRST"
        elif s.nulls_first is False:
            nulls = " NULLS LAST"
        sort_parts.append(f"{col_ref} {s.direction}{nulls}")
    sort_clause = (" ORDER BY " + ", ".join(sort_parts)) if sort_parts else ""

    # LIMIT / OFFSET
    limit_clause = ""
    if catalog.limit and catalog.offset:
        limit_clause = f" LIMIT {catalog.limit} OFFSET {catalog.offset}"
    elif catalog.limit:
        limit_clause = f" LIMIT {catalog.limit}"
    elif catalog.offset:
        limit_clause = f" OFFSET {catalog.offset}"

    sql = (
        f'SELECT {cols_str} '
        f'FROM "{root.name}"'
        f"{join_clause}"
        f"{where_clause}"
        f"{sort_clause}"
        f"{limit_clause}"
    )
    return sql, binds
