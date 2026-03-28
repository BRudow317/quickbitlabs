from typing import Any
from server.models.PluginModels import QueryModel, FilterGroup, FilterCondition, EntityModel

# =========================================================
# 1. READ OPERATIONS (The AST Translators)
# =========================================================

def build_dynamic_sql(query: QueryModel) -> tuple[str, dict[str, Any]]:
    """
    Translates the universal QueryModel AST into Oracle SQL.
    Returns: (sql_string, bind_dictionary)
    """
    if query.native:
        return query.native.statement, query.native.binds

    binds: dict[str, Any] = {}
    bind_counter = 0

    select_clause = ", ".join(query.fields) if query.fields else "*"
    from_clause = query.entities[0]
    
    for j in query.joins:
        from_clause += f" {j.join_type} JOIN {j.right_entity} ON {j.left_entity}.{j.left_field} = {j.right_entity}.{j.right_field}"

    where_clause = ""
    if query.filter_group:
        where_string, bind_counter = _parse_filter_group(query.filter_group, binds, bind_counter)
        if where_string:
            where_clause = f" WHERE {where_string}"

    limit_clause = ""
    if query.limit is not None:
        limit_clause = f" FETCH FIRST {query.limit} ROWS ONLY"

    sql = f"SELECT {select_clause} FROM {from_clause}{where_clause}{limit_clause}"
    
    return sql, binds

def _parse_filter_group(group: FilterGroup, binds: dict[str, Any], bind_counter: int) -> tuple[str, int]:
    if not group.filters:
        return "", bind_counter

    clauses = []
    
    for item in group.filters:
        if isinstance(item, FilterGroup):
            sub_clause, bind_counter = _parse_filter_group(item, binds, bind_counter)
            if sub_clause:
                clauses.append(f"({sub_clause})")
        else:
            cond_clause, bind_counter = _parse_condition(item, binds, bind_counter)
            clauses.append(cond_clause)

    if group.condition == "NOT":
        combined = f" NOT ({clauses[0]})" if clauses else ""
    else:
        joiner = f" {group.condition} "
        combined = joiner.join(clauses)

    return combined, bind_counter

def _parse_condition(cond: FilterCondition, binds: dict[str, Any], bind_counter: int) -> tuple[str, int]:
    field = cond.field
    op = cond.operator
    val = cond.value

    if op == "IS NULL":
        return f"{field} IS NULL", bind_counter
    if op == "IS NOT NULL":
        return f"{field} IS NOT NULL", bind_counter

    sql_op = "=" if op == "==" else op

    if sql_op == "IN":
        if not isinstance(val, list) or not val:
            return "1=0", bind_counter 
            
        in_binds = []
        for list_val in val:
            bind_name = f"bind_{bind_counter}"
            in_binds.append(f":{bind_name}")
            binds[bind_name] = list_val
            bind_counter += 1
            
        in_string = ", ".join(in_binds)
        return f"{field} IN ({in_string})", bind_counter

    bind_name = f"bind_{bind_counter}"
    binds[bind_name] = val
    bind_counter += 1

    return f"{field} {sql_op} :{bind_name}", bind_counter


# =========================================================
# 2. WRITE OPERATIONS (The CRUD Generators)
# =========================================================

def build_insert_sql(entity: EntityModel) -> str:
    """Generates a parameterized INSERT statement."""
    table_name = entity.target_name or entity.source_name
    cols = []
    binds = []
    
    for field in entity.fields:
        if not field.read_only:
            col_name = field.target_name or field.source_name
            cols.append(col_name)
            binds.append(f":{col_name}")
            
    return f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(binds)})"


def build_update_sql(entity: EntityModel) -> str:
    """Generates a parameterized UPDATE statement based on Primary Keys."""
    table_name = entity.target_name or entity.source_name
    set_clauses = []
    where_clauses = []
    
    for field in entity.fields:
        if not field.read_only:
            col_name = field.target_name or field.source_name
            if field.primary_key:
                where_clauses.append(f"{col_name} = :{col_name}")
            else:
                set_clauses.append(f"{col_name} = :{col_name}")
                
    return f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"


def build_delete_sql(entity: EntityModel) -> str:
    """Generates a parameterized DELETE statement based on Primary Keys."""
    table_name = entity.target_name or entity.source_name
    where_clauses = []
    
    for field in entity.primary_key_fields:
        col_name = field.target_name or field.source_name
        where_clauses.append(f"{col_name} = :{col_name}")
        
    return f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)}"


def build_merge_sql(entity: EntityModel) -> str:
    """
    Generates an Oracle MERGE (Upsert) statement. 
    Uses DUAL so it can be safely executed via cursor.executemany().
    """
    table_name = entity.target_name or entity.source_name
    
    pk_cols = []
    val_cols = []
    all_cols = []
    
    for field in entity.fields:
        if not field.read_only:
            col_name = field.target_name or field.source_name
            all_cols.append(col_name)
            if field.primary_key:
                pk_cols.append(col_name)
            else:
                val_cols.append(col_name)
                
    # 1. Build the USING clause (mapping binds to DUAL)
    using_select = ", ".join([f":{c} as {c}" for c in all_cols])
    
    # 2. Build the ON clause (matching PKs)
    on_clause = " AND ".join([f"dest.{pk} = src.{pk}" for pk in pk_cols])
    
    # 3. Build the UPDATE SET clause
    update_set = ", ".join([f"dest.{c} = src.{c}" for c in val_cols])
    
    # 4. Build the INSERT clause
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"src.{c}" for c in all_cols])
    
    # Assemble the Oracle MERGE string
    sql = f"""
        MERGE INTO {table_name} dest
        USING (SELECT {using_select} FROM DUAL) src
        ON ({on_clause})
    """
    
    if update_set:
        sql += f" WHEN MATCHED THEN UPDATE SET {update_set}"
        
    sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    
    return sql