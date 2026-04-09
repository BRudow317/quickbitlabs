from typing import Any
import pyarrow as pa
from server.plugins.PluginModels import Catalog, Column, OperatorGroup, Operator, Entity

def _get_root_entity(catalog: Catalog) -> Entity:
    if len(catalog.entities) == 1: return catalog.entities[0]
    right_entities = {j.right_entity.name for j in catalog.joins}
    roots = [e for e in catalog.entities if e.name not in right_entities] or []
    if not roots: raise ValueError("Circular or missing joins detected.")
    return roots[0]

def _parse_operator(operator: Operator, binds: dict[str, Any], bind_counter: int) -> tuple[str, int]:
    independent = operator.independent
    column_left = getattr(independent, "name", None)
    if not column_left:
        raise ValueError(f"Invalid operator: Left-hand side must be a Column. Got {type(independent).__name__}")
    
    op = operator.operator
    dependent: str | pa.Field | Column | None = operator.dependent

    if op == "IS NULL": return f"{column_left} IS NULL", bind_counter
    if op == "IS NOT NULL": return f"{column_left} IS NOT NULL", bind_counter

    sql_op = "=" if op == "==" else op

    # --- SCENARIO A: Stream Bind (pa.Field in dependent = bind from Arrow payload) ---
    # e.g. Operator(independent=status_col, operator="=", dependent=pa.field("status"))
    # → WHERE STATUS = :status
    if isinstance(dependent, pa.Field):
        return f"{column_left} {sql_op} :{getattr(dependent, 'name')}", bind_counter

    # --- SCENARIO C: Static String Literal ---
    if sql_op == "IN":
        if not isinstance(dependent, list) or not dependent: return "1=0", bind_counter
        in_binds = []
        for list_val in dependent:
            bind_name = f"bind_{bind_counter}"
            in_binds.append(f":{bind_name}")
            binds[bind_name] = list_val
            bind_counter += 1
        return f"{column_left} IN ({', '.join(in_binds)})", bind_counter

    bind_name = f"bind_{bind_counter}"
    binds[bind_name] = dependent
    bind_counter += 1
    return f"{column_left} {sql_op} :{bind_name}", bind_counter

def _parse_operator_group(group: OperatorGroup, binds: dict[str, Any], bind_counter: int) -> tuple[str, int]:
    if not group.operators: return "", bind_counter
    clauses = []
    for item in group.operators:
        if isinstance(item, OperatorGroup):
            sub_clause, bind_counter = _parse_operator_group(item, binds, bind_counter)
            if sub_clause: clauses.append(f"({sub_clause})")
        else:
            cond_clause, bind_counter = _parse_operator(item, binds, bind_counter)
            clauses.append(cond_clause)

    if group.condition == "NOT": combined = f" NOT ({clauses[0]})" if clauses else ""
    else: combined = f" {group.condition} ".join(clauses)
    return combined, bind_counter

def build_filters(operator_groups: list[OperatorGroup]) -> tuple[str, dict[str, Any]]:
    if not operator_groups: return "", {}
    binds: dict[str, Any] = {}
    bind_counter = 0
    clauses = []
    for group in operator_groups:
        clause, bind_counter = _parse_operator_group(group, binds, bind_counter)
        if clause: clauses.append(f"({clause})")
    if not clauses: return "", {}
    return " WHERE " + " AND ".join(clauses), binds

def parse_catalog(catalog: Catalog) -> tuple[str, str, str, str, dict[str, Any]]:
    if not catalog.entities: raise ValueError("Catalog must have at least one entity.")
    join_clause = ""
    if catalog.joins:
        joins = [f"{j.join_type} JOIN {j.right_entity.name} ON {j.left_column.name} = {j.right_column.name}" for j in catalog.joins]
        join_clause = " " + " ".join(joins)
        
    where_clause, binds = build_filters(catalog.operator_groups)
    sort_clause = ""
    if catalog.sort_fields:
        sorts = [f"{s.column.name} {s.direction}" for s in catalog.sort_fields]
        sort_clause += " ORDER BY " + ", ".join(sorts)
    
    limit_clause = f" FETCH FIRST {catalog.limit} ROWS ONLY" if catalog.limit else ""
    return join_clause, where_clause, sort_clause, limit_clause, binds

def build_select(catalog: Catalog) -> tuple[str, dict[str, Any]]:
    join_clause, where_clause, sort_clause, limit_clause, binds = parse_catalog(catalog)
    select_cols = [c.name for e in catalog.entities for c in e.columns]
    cols_str = ", ".join(select_cols)
    return f"SELECT {cols_str} FROM {_get_root_entity(catalog).name}{join_clause}{where_clause}{sort_clause}{limit_clause}", binds

def build_insert_dml(catalog: Catalog, entity: Entity) -> str:
    cols = [c.name for c in entity.columns]
    binds = ", ".join([f":{c.name}" for c in entity.columns])
    return f"INSERT INTO {entity.name} ({', '.join(cols)}) VALUES ({binds})"

def build_delete_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    _, where_clause, _, _, binds = parse_catalog(catalog)
    return f"DELETE FROM {entity.name}{where_clause}", binds

def build_update_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    _, where_clause, _, _, binds = parse_catalog(catalog)
    if not where_clause:
        raise ValueError(f"Cannot update {entity.name}: No Operator conditions defined in catalog.")
    set_str = ", ".join([f"{c.name} = :{c.name}" for c in entity.columns])
    return f"UPDATE {entity.name} SET {set_str}{where_clause}", binds
        

def build_merge_dml(catalog: Catalog, entity: Entity) -> str:
    pk_cols = entity.primary_key_columns
    if not pk_cols:
        raise ValueError(f"Cannot merge: Entity {entity.name} has no primary keys.")
    update_cols = [c for c in entity.columns if c not in pk_cols]
    on_clause = " AND ".join([f"tgt.{c.name} = src.{c.name}" for c in pk_cols])
    set_clause = ", ".join([f"tgt.{c.name} = src.{c.name}" for c in update_cols])
    insert_cols = ", ".join([c.name for c in entity.columns])
    values_cols = ", ".join([f"src.{c.name}" for c in entity.columns])
    bind_selects = ", ".join([f":{c.name} AS {c.name}" for c in entity.columns])
    return f"MERGE INTO {entity.name} tgt USING (SELECT {bind_selects} FROM DUAL) src ON ({on_clause}) WHEN MATCHED THEN UPDATE SET {set_clause} WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({values_cols})"