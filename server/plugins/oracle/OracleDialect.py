from __future__ import annotations

from typing import Any
import pyarrow as pa

from server.plugins.PluginModels import (
    Assignment, Catalog, Column, Entity, Operation, OperatorGroup,
)


def _q(identifier: str) -> str:
    """Double-quote each part of a possibly-dotted Oracle identifier.

    Escapes Oracle reserved words (USER, DATE, LEVEL, etc.) that would
    otherwise be interpreted as keywords rather than column/table names.
    Handles schema.table and table.column patterns correctly.
    """
    return ".".join(f'"{part}"' for part in identifier.split("."))


# ##########################################
# Entity resolution
# ##########################################

def _get_root_entity(catalog: Catalog) -> Entity:
    """Return the join-topology root: the entity never on a join's right side.

    For single-entity catalogs this is always the one entity.
    Used for SELECT (FROM table) and as a fallback when no assignment ops exist.
    """
    if len(catalog.entities) == 1:
        return catalog.entities[0]
    right_names = {j.right_entity.name for j in catalog.joins}
    roots = [e for e in catalog.entities if e.name not in right_names]
    if not roots:
        raise ValueError("Circular or missing joins detected in Catalog.")
    return roots[0]


def _get_target_entity(catalog: Catalog) -> Entity:
    """Return the DML write-target entity.

    For multi-entity (join) catalogs, the write target is the entity whose
    column appears in catalog.assignments.  A join catalog may carry read-only
    entities purely for filtering — e.g. EMPLOYEES joined to filter by
    department while the actual write target is PAYCHECKS.

    Falls back to _get_root_entity (join-topology left side) only when no
    assignment operations are present in the catalog.
    """
    if len(catalog.entities) == 1:
        return catalog.entities[0]

    if catalog.assignments:
        target_names = {
            a.column.locator.entity_name.upper()
            for a in catalog.assignments
            if a.column.locator and a.column.locator.entity_name
        }
        for entity in catalog.entities:
            if entity.name.upper() in target_names:
                return entity

    return _get_root_entity(catalog)


# ##########################################
# WHERE clause builder (recursive OperatorGroup → SQL fragment)
# ##########################################

def _bind_name(col: str, binds: dict[str, Any]) -> str:
    """Return a unique bind variable name based on the column name.

    Uses the column name directly; appends _2, _3 … only if the name is
    already taken (same column appearing more than once in one predicate).
    """
    if col not in binds:
        return col
    n = 2
    while f"{col}_{n}" in binds:
        n += 1
    return f"{col}_{n}"


def _parse_operation(op: Operation, binds: dict[str, Any]) -> str:
    """Translate a single Operation into a SQL predicate fragment.

    Operator mapping:
      "IS NULL" / "IS NOT NULL"  → SQL directly, no bind
      "IN" with list             → IN (:col, :col_2, …)
      pa.Field dependent         → stream bind (:field_name), no static bind added
      Column dependent           → column reference, no bind
      anything else              → static bind (:col)
    """
    col = op.independent.name
    operator = op.operator
    dependent = op.dependent
    sql_col = _q(col)

    if operator == "IS NULL":
        return f"{sql_col} IS NULL"
    if operator == "IS NOT NULL":
        return f"{sql_col} IS NOT NULL"

    sql_op = operator

    if isinstance(dependent, pa.Field):
        return f"{sql_col} {sql_op} :{getattr(dependent, 'name')}"

    if isinstance(dependent, Column):
        return f"{sql_col} {sql_op} {_q(dependent.qualified_name)}"

    if sql_op in ("IN", "NOT IN"):
        if not isinstance(dependent, list) or not dependent:
            return "1=0" if sql_op == "IN" else "1=1"
        placeholders: list[str] = []
        for val in dependent:
            name = _bind_name(col, binds)
            placeholders.append(f":{name}")
            binds[name] = val
        return f"{sql_col} {sql_op} ({', '.join(placeholders)})"

    if sql_op in ("BETWEEN", "NOT BETWEEN"):
        if not isinstance(dependent, list) or len(dependent) != 2:
            raise ValueError(f"{sql_op} requires a 2-element list [low, high], got: {dependent!r}")
        lo_name = _bind_name(f"{col}_lo", binds)
        hi_name = _bind_name(f"{col}_hi", binds)
        binds[lo_name] = dependent[0]
        binds[hi_name] = dependent[1]
        return f"{sql_col} {sql_op} :{lo_name} AND :{hi_name}"

    name = _bind_name(col, binds)
    binds[name] = dependent
    return f"{sql_col} {sql_op} :{name}"


def _parse_operator_group(group: OperatorGroup, binds: dict[str, Any]) -> str:
    """Recursively translate an OperatorGroup into a SQL fragment."""
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


def _build_where(filters: list[OperatorGroup]) -> tuple[str, dict[str, Any]]:
    """Convert a list of OperatorGroups into a WHERE clause string + bind dict.

    Returns ("", {}) when the list is empty.
    Multiple top-level groups are joined with AND.
    """
    if not filters:
        return "", {}

    binds: dict[str, Any] = {}
    clauses: list[str] = []

    for group in filters:
        clause = _parse_operator_group(group, binds)
        if clause:
            clauses.append(f"({clause})")

    if not clauses:
        return "", {}

    return " WHERE " + " AND ".join(clauses), binds


# ##########################################
# SELECT
# ##########################################

def build_select(catalog: Catalog) -> tuple[str, dict[str, Any]]:
    """Build a full SELECT statement from a Catalog.

    Returns (sql, binds).

    Column list: uses Column.qualified_name (entity_name.col_name) so that
    multi-entity JOINs produce unambiguous references.  Falls back to "*"
    when no columns are declared.

    Table name: uses Entity.qualified_name (namespace.name) for schema
    qualification when namespace is set.

    Oracle pagination: FETCH FIRST N ROWS ONLY (not ROWNUM / LIMIT).
    """
    if not catalog.entities:
        raise ValueError("Catalog must have at least one entity to build SELECT.")

    root = _get_root_entity(catalog)

    # FROM + JOINs
    join_parts: list[str] = []
    for j in catalog.joins:
        join_parts.append(
            f"{j.join_type} JOIN {_q(j.right_entity.qualified_name)} "
            f"ON {_q(j.left_column.qualified_name)} = {_q(j.right_column.qualified_name)}"
        )
    join_clause = (" " + " ".join(join_parts)) if join_parts else ""

    # Column list
    col_names = [_q(c.qualified_name) for e in catalog.entities for c in e.columns]
    cols_str = ", ".join(col_names) if col_names else "*"

    # WHERE
    where_clause, binds = _build_where(catalog.filters)

    # ORDER BY
    sort_parts: list[str] = []
    for s in catalog.sort_columns:
        nulls = ""
        if s.nulls_first is True:
            nulls = " NULLS FIRST"
        elif s.nulls_first is False:
            nulls = " NULLS LAST"
        sort_parts.append(f"{_q(s.column.qualified_name)} {s.direction}{nulls}")
    sort_clause = (" ORDER BY " + ", ".join(sort_parts)) if sort_parts else ""

    # LIMIT / OFFSET (Oracle syntax)
    limit_clause = ""
    if catalog.limit and catalog.offset:
        limit_clause = f" OFFSET {catalog.offset} ROWS FETCH NEXT {catalog.limit} ROWS ONLY"
    elif catalog.limit:
        limit_clause = f" FETCH FIRST {catalog.limit} ROWS ONLY"
    elif catalog.offset:
        limit_clause = f" OFFSET {catalog.offset} ROWS"

    sql = (
        f"SELECT {cols_str} "
        f"FROM {_q(root.qualified_name)}"
        f"{join_clause}"
        f"{where_clause}"
        f"{sort_clause}"
        f"{limit_clause}"
    )
    return sql, binds


# ##########################################
# INSERT
# ##########################################

def build_insert_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    """INSERT INTO schema.table (col, …) VALUES (:col, …)

    Column/value list is driven by catalog.assignments — the same mechanism as
    build_update_dml's SET clause.  Falls back to all entity.columns as stream
    binds when no assignments exist.

    TODO: Callers must strip identity/managed columns from entity.columns before calling
    this function. The identity PK ({TABLE}_ID) and audit columns must not appear in the
    INSERT column list — Oracle generates them automatically. There is currently no guard
    here; the strip responsibility falls entirely on OracleServices.create_data.
    """
    if catalog.assignments:
        col_parts: list[str] = []
        val_parts: list[str] = []
        binds: dict[str, Any] = {}
        for a in catalog.assignments:
            col = a.column.name
            dep = a.value
            col_parts.append(_q(col))
            if isinstance(dep, pa.Field):
                val_parts.append(f":{getattr(dep, 'name')}")
            elif isinstance(dep, Column):
                val_parts.append(_q(dep.qualified_name))
            else:
                name = _bind_name(col, binds)
                binds[name] = dep
                val_parts.append(f":{name}")
        sql = (
            f"INSERT INTO {_q(entity.qualified_name)} "
            f"({', '.join(col_parts)}) VALUES ({', '.join(val_parts)})"
        )
        return sql, binds

    cols = [c.name for c in entity.columns]
    sql = (
        f"INSERT INTO {_q(entity.qualified_name)} "
        f"({', '.join(_q(c) for c in cols)}) VALUES ({', '.join(f':{c}' for c in cols)})"
    )
    return sql, {}


# ##########################################
# UPDATE
# ##########################################

def build_update_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    """UPDATE schema.table SET … WHERE …

    SET clause: built from catalog.assignments. The column is what gets written;
    the value is pa.Field for a stream bind, Column for a column reference,
    None for NULL, or a static scalar for a literal.

    If no assignments are present, falls back to streaming all entity.columns
    (:col_name bind per column from the Arrow stream).

    WHERE clause: built from catalog.filters.
    Requires at least one filter to prevent unbounded full-table updates.
    """
    where_clause, where_binds = _build_where(catalog.filters)
    if not where_clause:
        raise ValueError(
            f"UPDATE on '{entity.name}' requires at least one filter "
            f"in catalog.filters to prevent full-table overwrites."
        )

    if catalog.assignments:
        set_parts: list[str] = []
        set_binds: dict[str, Any] = {}
        for a in catalog.assignments:
            col = a.column.name
            dep = a.value
            if isinstance(dep, pa.Field):
                set_parts.append(f"{_q(col)} = :{getattr(dep, 'name')}")
            elif isinstance(dep, Column):
                set_parts.append(f"{_q(col)} = {_q(dep.qualified_name)}")
            else:
                # Covers None (→ NULL via bind) and any static scalar
                name = _bind_name(col, set_binds)
                set_binds[name] = dep
                set_parts.append(f"{_q(col)} = :{name}")
        all_binds = {**where_binds, **set_binds}
        set_str = ", ".join(set_parts)
    else:
        set_str = ", ".join(f"{_q(c.name)} = :{c.name}" for c in entity.columns)
        all_binds = where_binds

    return f"UPDATE {_q(entity.qualified_name)} SET {set_str}{where_clause}", all_binds


# ##########################################
# MERGE ON clause helpers (catalog.filters → tgt.x = src.y)
# ##########################################

def _build_merge_operation(
    op: Operation,
    binds: dict[str, Any],
) -> tuple[str, str]:
    """Translate one Operation into a MERGE ON predicate.

    Returns (clause_str, independent_col_name).

    Dependent mapping:
      pa.Field → src.field_name  (value from the Arrow stream row)
      Column   → src.col_name   (another catalog column, also from stream)
      static   → :col           (named static bind)
    """
    col = op.independent.name
    operator = op.operator
    dependent = op.dependent
    sql_op = operator
    qcol = _q(col)

    if operator == "IS NULL":
        return f"tgt.{qcol} IS NULL", col
    if operator == "IS NOT NULL":
        return f"tgt.{qcol} IS NOT NULL", col

    if isinstance(dependent, pa.Field):
        dep_name = getattr(dependent, 'name')
        return f"tgt.{qcol} {sql_op} src.{_q(dep_name)}", col

    if isinstance(dependent, Column):
        return f"tgt.{qcol} {sql_op} src.{_q(dependent.name)}", col

    name = _bind_name(col, binds)
    binds[name] = dependent
    return f"tgt.{qcol} {sql_op} :{name}", col


def _build_merge_group(
    group: OperatorGroup,
    binds: dict[str, Any],
) -> tuple[str, set[str]]:
    """Recursively translate an OperatorGroup for MERGE ON context.

    Returns (clause_str, on_col_names).
    """
    clauses: list[str] = []
    on_cols: set[str] = set()

    for item in group.operation_group:
        if isinstance(item, OperatorGroup):
            clause, cols = _build_merge_group(item, binds)
            on_cols |= cols
            if clause:
                clauses.append(f"({clause})")
        else:
            clause, col = _build_merge_operation(item, binds)
            on_cols.add(col)
            clauses.append(clause)

    if group.condition == "NOT":
        combined = f"NOT ({clauses[0]})" if clauses else ""
    else:
        combined = f" {group.condition} ".join(clauses)

    return combined, on_cols


def _build_merge_on(
    filters: list[OperatorGroup],
) -> tuple[str, dict[str, Any], set[str]]:
    """Convert catalog.filters into a MERGE ON clause.

    Returns (on_sql, binds, on_col_names).
    on_col_names: independent column names in the ON clause, excluded from
    WHEN MATCHED UPDATE SET so we don't clobber the match key.
    """
    binds: dict[str, Any] = {}
    clauses: list[str] = []
    on_cols: set[str] = set()

    for group in filters:
        clause, cols = _build_merge_group(group, binds)
        on_cols |= cols
        if clause:
            clauses.append(f"({clause})")

    return " AND ".join(clauses), binds, on_cols


# ##########################################
# MERGE (upsert)
# ##########################################

def build_merge_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    """MERGE INTO schema.table tgt USING (SELECT :col AS col … FROM DUAL) src …

    The ON clause is derived from catalog.filters — no primary-key metadata is
    required.  Use pa.Field as the dependent in an Operation to reference a
    stream column on the src side, e.g.:

        Operation(independent=id_col, operator="=", dependent=pa.field("ID"))
        → ON (tgt.ID = src.ID)

    Columns not referenced in the ON clause are SET in WHEN MATCHED.
    Static binds (non-pa.Field dependents in filters) are merged with each
    stream row by execute_many.

    TODO: The USING SELECT clause is built from entity.columns. If entity.columns contains
    the identity PK ({TABLE}_ID) or audit columns, they will appear in the USING SELECT
    and the INSERT column list. This causes ORA-32795 for identity columns and overrides
    Oracle DEFAULT expressions for audit columns. Callers must strip these before calling
    this function (OracleServices.upsert_data currently handles audit cols but not identity PK).

    TODO: The WHEN NOT MATCHED INSERT branch does not exclude identity columns. Once the
    managed entity boilerplate is implemented, this function needs an `identity_cols` parameter
    (or reads a marker from Column.is_computed / Column.properties) to omit those columns
    from the INSERT list so Oracle's sequence generates the value.

    TODO: The WHEN MATCHED UPDATE SET clause updates ALL non-ON columns unconditionally.
    For managed entities, UPDATED_DATE and UPDATED_BY should be set via SYSTIMESTAMP/USER
    expressions injected by _inject_merge_audit in OracleServices, not from source values.
    This coupling (build_merge_dml builds raw SQL, OracleServices patches it via string
    manipulation) is fragile. Consider a `managed_cols` parameter here so the expressions
    are generated cleanly rather than injected via string surgery post-hoc.
    """
    if not catalog.filters:
        raise ValueError(
            f"MERGE on '{entity.name}' requires catalog.filters "
            f"to define the ON clause match condition."
        )

    on_sql, binds, on_col_names = _build_merge_on(catalog.filters)

    non_match = [c for c in entity.columns if c.name not in on_col_names]
    bind_selects = ", ".join(f":{c.name} AS {_q(c.name)}" for c in entity.columns)
    insert_cols = ", ".join(_q(c.name) for c in entity.columns)
    src_vals = ", ".join(f"src.{_q(c.name)}" for c in entity.columns)

    matched_part = (
        "WHEN MATCHED THEN UPDATE SET "
        + ", ".join(f"tgt.{_q(c.name)} = src.{_q(c.name)}" for c in non_match)
        if non_match else ""
    )

    sql = (
        f"MERGE INTO {_q(entity.qualified_name)} tgt "
        f"USING (SELECT {bind_selects} FROM DUAL) src "
        f"ON ({on_sql}) "
        + (f"{matched_part} " if matched_part else "")
        + f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({src_vals})"
    )
    return sql, binds


# ##########################################
# DELETE
# ##########################################

def build_delete_dml(catalog: Catalog, entity: Entity) -> tuple[str, dict[str, Any]]:
    """DELETE FROM schema.table [WHERE …]

    With catalog.filters: static predicates → execute once; binds returned.
    Without filters: stream-based → WHERE uses entity column binds
    (:col_name per column, values come from Arrow stream rows).  The caller
    controls which columns are in the entity — include only the key/filter
    columns needed (e.g. just ID for a key-based stream delete).
    """
    where_clause, binds = _build_where(catalog.filters)
    if where_clause:
        return f"DELETE FROM {_q(entity.qualified_name)}{where_clause}", binds

    if entity.columns:
        predicates = " AND ".join(f"{_q(c.name)} = :{c.name}" for c in entity.columns)
        return f"DELETE FROM {_q(entity.qualified_name)} WHERE {predicates}", {}

    return f"DELETE FROM {_q(entity.qualified_name)}", {}


# ##########################################
# Copy-Swap rebuild SELECT (for ALTER TABLE via Copy-Swap DDL strategy)
# ##########################################

def build_rebuild_select(entity: Entity, existing_names: set[str]) -> str:
    """Build the SELECT expression list for a Copy-Swap table rebuild.

    Used in:   INSERT INTO schema.TABLE_TMP (cols) SELECT <this> FROM schema.TABLE

    Rules:
    - Existing columns: CAST to target DDL type (with special handling for LOBs,
      timestamps, and booleans which cannot be CAST in the normal sense).
    - New columns: fill with a type-safe default or NULL.

    TODO: Exclude the identity PK column from the SELECT entirely. The identity column
    in TABLE_TMP will auto-generate a new value — it must not appear in the INSERT
    column list or the SELECT. Currently entity.columns is used verbatim, so the caller
    in OracleServices._rebuild_copy_swap must strip identity columns before calling this.

    TODO: Exclude audit columns from the CAST path and instead inject Oracle expressions
    (SYSTIMESTAMP for dates, USER for by-cols) for UPDATED_DATE/UPDATED_BY, and preserve
    the existing CREATED_DATE/CREATED_BY values with a plain column reference (no CAST).
    Currently audit columns are CAST like regular columns which may coerce stored timestamps.

    TODO: Type promotion mapping for the CAST expressions. When a column's arrow_type_id
    maps to a wider Oracle type than what's currently in the table (e.g. the existing column
    is VARCHAR2(100) but the target DDL type is VARCHAR2(500)), the CAST is correct.
    But when the types are incompatible (e.g. NUMBER → VARCHAR2), the CAST will fail at
    runtime. This should be detected in upsert_entity before calling _rebuild_copy_swap
    and a TO_CHAR / TO_NUMBER conversion expression used instead of a bare CAST.
    """
    from server.plugins.oracle.OracleTypeMap import map_arrow_to_oracle_ddl

    parts: list[str] = []
    for col in entity.columns:
        ddl_type = map_arrow_to_oracle_ddl(col).upper()
        is_lob = "CLOB" in ddl_type or "BLOB" in ddl_type
        is_temporal = "DATE" in ddl_type or "TIMESTAMP" in ddl_type
        col_exists = col.name.upper() in existing_names
        atype = col.arrow_type_id or ""

        col_name = col.name
        if col_exists:
            if is_lob or is_temporal:
                # Oracle handles LOB and temporal internal moves without CAST
                parts.append(f"{col_name} AS {col_name}")
            elif atype == "bool":
                # Salesforce string booleans → Oracle NUMBER(1,0)
                expr = (
                    f"CASE WHEN {col_name} IN ('true', '1', 'Y') THEN 1 "
                    f"WHEN {col_name} IN ('false', '0', 'N') THEN 0 "
                    f"ELSE NULL END"
                )
                parts.append(f"CAST({expr} AS {ddl_type}) AS {col_name}")
            else:
                parts.append(f"CAST({col_name} AS {ddl_type}) AS {col_name}")
        else:
            # New column — provide a type-correct default
            if is_lob:
                if col.default_value is not None:
                    parts.append(f"TO_CLOB('{col.default_value}') AS {col_name}")
                elif not col.is_nullable:
                    parts.append(f"EMPTY_CLOB() AS {col_name}")
                else:
                    parts.append(f"NULL AS {col_name}")

            elif is_temporal:
                if col.default_value is not None:
                    parts.append(
                        f"TO_TIMESTAMP('{col.default_value}', "
                        f"'YYYY-MM-DD HH24:MI:SS') AS {col_name}"
                    )
                elif not col.is_nullable:
                    parts.append(
                        f"TO_TIMESTAMP('1970-01-01 00:00:00', "
                        f"'YYYY-MM-DD HH24:MI:SS') AS {col_name}"
                    )
                else:
                    parts.append(f"CAST(NULL AS {ddl_type}) AS {col_name}")

            else:
                if col.default_value is not None:
                    val = (
                        f"'{col.default_value}'"
                        if isinstance(col.default_value, str)
                        else str(col.default_value)
                    )
                    parts.append(f"CAST({val} AS {ddl_type}) AS {col_name}")
                elif not col.is_nullable:
                    if atype in ("string", "utf8", "large_string"):
                        dummy = "' '"
                    elif any(
                        atype.startswith(t)
                        for t in ("int", "uint", "float", "decimal")
                    ) or atype == "bool":
                        dummy = "0"
                    else:
                        dummy = "NULL"
                    parts.append(f"CAST({dummy} AS {ddl_type}) AS {col_name}")
                else:
                    parts.append(f"CAST(NULL AS {ddl_type}) AS {col_name}")

    return ", ".join(parts)
