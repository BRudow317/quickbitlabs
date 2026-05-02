from __future__ import annotations
from decimal import Decimal, InvalidOperation
import re
from typing import Iterable, Any
from datetime import date, datetime


_NULL_BYTE_RE = re.compile(r'\x00')
_COMMA_RE = re.compile(r',')
_DATE_FMT = '%Y-%m-%d'
_TIMESTAMP_FMTS = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
_TZ_OFFSET_RE = re.compile(r'[+-]\d{2}:\d{2}$')


# Words that Oracle truly rejects as unquoted DDL identifiers (ORA-00903 / ORA-00904).
# This list intentionally omits keywords that are reserved in SQL syntax but still work
# as unquoted table/column names in DDL (e.g. CASE, DATE, GROUP, FILE, SIZE, etc. are
# reserved SQL keywords but Oracle accepts them in CREATE TABLE / CREATE INDEX contexts).
# Source: Oracle 19c V$RESERVED_WORDS where RESERVED='Y' and commonly tested by hand.
# Use load_reserved_words_from_db() to get the authoritative list for your exact version.
_ORACLE_RESERVED: frozenset[str] = frozenset({
    # DML / query keywords that cause parse errors as unquoted identifiers
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "AS",
    "HAVING", "ORDER", "GROUP", "BY", "DISTINCT", "ALL", "ANY",
    "UNION", "INTERSECT", "MINUS", "EXCEPT",
    # DDL keywords
    "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "COMMENT",
    "TABLE", "VIEW", "INDEX", "SYNONYM", "SEQUENCE", "TRIGGER",
    "PROCEDURE", "FUNCTION", "PACKAGE", "TYPE", "CLUSTER",
    # DML keywords
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "MERGE",
    # Join / subquery keywords
    "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", "NATURAL", "USING",
    "ON", "EXISTS", "WITH",
    # Control / transaction
    "COMMIT", "ROLLBACK", "SAVEPOINT", "GRANT", "REVOKE",
    "START", "CONNECT", "RESOURCE", "PUBLIC", "IDENTIFIED",
    # Literals / nulls
    "NULL", "TRUE", "FALSE",
    # Oracle rowset pseudo-columns that shadow real column names
    "ROWID", "ROWNUM", "LEVEL",
})

# Fallback suffix used when a column name collides with a reserved word.
_COL_SUFFIX = "_COL"
# Fallback suffix used when a TABLE name collides with a reserved word.
_TBL_SUFFIX = "_TBL"


def _snake_base(value: str) -> str:
    """Convert an arbitrary identifier string to UPPER_SNAKE form (no reserved-word check)."""
    s = str(value).strip()
    if not s:
        return ""
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s = s.strip('_').upper()
    if s and s[0].isdigit():
        s = 'C_' + s
    return s


def _apply_max_len(s: str, max_len: int, suffix: str) -> str:
    if len(s) <= max_len:
        return s
    if s.endswith(suffix):
        base = s[: max_len - len(suffix)].rstrip('_')
        return f'{base}{suffix}'
    return s[:max_len].rstrip('_')


def to_oracle_table_name(
    value: str,
    max_len: int = 128,
    reserved: frozenset[str] | None = None,
) -> str:
    """Convert a source table/object name to a safe Oracle table identifier.

    Reserved-word conflicts get the ``_TBL`` suffix so the result is clearly
    a table name, not a column name.  Falls back to ``'TBL'`` for empty input.
    """
    if reserved is None:
        reserved = _ORACLE_RESERVED
    s = _snake_base(value)
    if not s:
        return 'TBL'
    if s in reserved:
        s = f'{s}{_TBL_SUFFIX}'
    return _apply_max_len(s, max_len, _TBL_SUFFIX) or 'TBL'


def to_oracle_column_name(
    value: str,
    max_len: int = 128,
    reserved: frozenset[str] | None = None,
) -> str:
    """Convert a source field/column name to a safe Oracle column identifier.

    Reserved-word conflicts get the ``_COL`` suffix.
    Falls back to ``'COL'`` for empty input.
    """
    if reserved is None:
        reserved = _ORACLE_RESERVED
    s = _snake_base(value)
    if not s:
        return 'COL'
    if s in reserved:
        s = f'{s}{_COL_SUFFIX}'
    return _apply_max_len(s, max_len, _COL_SUFFIX) or 'COL'


def to_oracle_snake(
    value: str,
    max_len: int = 128,
    reserved: Iterable[str] = _ORACLE_RESERVED,
    reserved_prefix: str | None = None,
    *,
    is_table: bool = False,
) -> str:
    """Convert a source identifier to a safe Oracle name.

    Prefer the explicit ``to_oracle_table_name`` / ``to_oracle_column_name``
    helpers.  This wrapper exists for backwards-compatibility.

    Args:
        is_table: When True, uses ``_TBL`` for reserved-word conflicts instead
                  of ``_COL``.  Set this when mapping entity/table names.
    """
    reserved_set = frozenset(s.upper() for s in reserved)
    if is_table:
        return to_oracle_table_name(value, max_len=max_len, reserved=reserved_set)
    return to_oracle_column_name(value, max_len=max_len, reserved=reserved_set)


def load_reserved_words_from_db(cursor: Any) -> frozenset[str]:
    """Query V$RESERVED_WORDS for this Oracle instance's authoritative reserved-word list.

    Returns only words where RESERVED='Y' — i.e. identifiers that require double-quoting.
    Keywords where RESERVED='N' (e.g. CASE, DATE, SIZE) are excluded because Oracle
    accepts them as unquoted table/column names in DDL contexts.

    Usage::

        with client.connect().cursor() as cur:
            live_reserved = load_reserved_words_from_db(cur)
        safe_name = to_oracle_table_name(src_name, reserved=live_reserved)
    """
    cursor.execute("SELECT KEYWORD FROM V$RESERVED_WORDS WHERE RESERVED = 'Y'")
    return frozenset(row[0].upper() for row in cursor.fetchall())

import pyarrow as pa
from server.plugins.PluginModels import ArrowReader, Column, Entity, Locator
from server.plugins.oracle.OracleTypeMap import (
    map_arrow_to_oracle_ddl,
    map_oracle_to_arrow,
    map_oracle_to_python,
    map_column_to_oracledb_input_size,
    map_python_to_oracle_ddl,
    map_python_to_oracledb_input_size,
)

import logging
_tools_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Managed entity boilerplate
# ---------------------------------------------------------------------------

AUDIT_COLS: frozenset[str] = frozenset({"CREATED_DATE", "CREATED_BY", "UPDATED_DATE", "UPDATED_BY"})

AUDIT_COL_DDLS: tuple[str, ...] = (
    "CREATED_DATE TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
    "CREATED_BY   VARCHAR2(100 CHAR) DEFAULT 'SYSTEM' NOT NULL",
    "UPDATED_DATE TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL",
    "UPDATED_BY   VARCHAR2(100 CHAR) DEFAULT 'SYSTEM' NOT NULL",
)

AUDIT_COL_DDL_MAP: dict[str, str] = {
    "CREATED_DATE": AUDIT_COL_DDLS[0],
    "CREATED_BY":   AUDIT_COL_DDLS[1],
    "UPDATED_DATE": AUDIT_COL_DDLS[2],
    "UPDATED_BY":   AUDIT_COL_DDLS[3],
}


def managed_pk_name(table_name: str) -> str:
    """Return the standard surrogate PK column name: {TABLE_NAME}_ID."""
    return f"{table_name.upper()}_ID"


def managed_pk_ddl(table_name: str) -> str:
    """Return the full DDL fragment for the managed identity PK column."""
    col = managed_pk_name(table_name)
    return (
        f"{col} NUMBER GENERATED BY DEFAULT AS IDENTITY "
        f"CONSTRAINT PK_{table_name.upper()} PRIMARY KEY"
    )


# ---------------------------------------------------------------------------
# Oracle DDL type family analysis
# ---------------------------------------------------------------------------

ORACLE_DDL_FAMILIES: dict[str, str] = {
    "NUMBER": "number", "FLOAT": "number", "BINARY_FLOAT": "number", "BINARY_DOUBLE": "number",
    "VARCHAR2": "varchar2", "NVARCHAR2": "varchar2", "CHAR": "varchar2", "NCHAR": "varchar2",
    "CLOB": "clob", "NCLOB": "clob",
    "BLOB": "blob", "RAW": "blob", "LONG RAW": "blob",
    "DATE": "temporal",
    "TIMESTAMP": "temporal",
    "TIMESTAMP WITH TIME ZONE": "temporal",
    "TIMESTAMP WITH LOCAL TIME ZONE": "temporal",
    "JSON": "json",
}

# Families where the existing Oracle type already stores the desired type without any DDL change.
FAMILY_SUPERSEDES: dict[str, frozenset[str]] = {
    "clob": frozenset({"varchar2"}),
}


def oracle_ddl_family(ddl_type: str) -> str:
    """Return the broad Oracle storage family for a DDL type string (e.g. 'NUMBER(10,2)' → 'number')."""
    upper = ddl_type.upper().split("(")[0].strip()
    return ORACLE_DDL_FAMILIES.get(upper, "other")


def type_change_action(
    existing_row: dict[str, Any],
    incoming_col: Column,
) -> tuple[str, str | None]:
    """Determine what DDL action is needed when an incoming column type may differ from Oracle's.

    Compares Oracle DDL type families (not arrow types) to avoid false positives from
    arrow-type promotions that map to the same Oracle storage (e.g. int64 vs decimal128 → NUMBER).

    Returns:
        ("none", None)         — no change; existing Oracle storage already covers incoming
        ("modify", new_ddl)    — safe in-place VARCHAR2 widening via ALTER TABLE MODIFY
        ("rebuild", new_ddl)   — incompatible type change; caller must trigger Copy-Swap
    """
    existing_raw = str(existing_row.get("DATA_TYPE", ""))
    existing_char_length = existing_row.get("CHAR_LENGTH")
    incoming_arrow = incoming_col.arrow_type_id
    if incoming_arrow is None:
        return "none", None

    desired_ddl = map_arrow_to_oracle_ddl(incoming_col)
    existing_family = oracle_ddl_family(existing_raw)
    desired_family = oracle_ddl_family(desired_ddl)

    if desired_family in FAMILY_SUPERSEDES.get(existing_family, frozenset()):
        return "none", None

    if existing_family == desired_family:
        if existing_family == "varchar2" and desired_ddl.startswith("VARCHAR2"):
            new_len = incoming_col.max_length or 4000
            old_len = int(existing_char_length) if existing_char_length else 0
            if new_len > old_len:
                return "modify", desired_ddl
        return "none", None

    return "rebuild", desired_ddl


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

def column_ddl(col: Column) -> str:
    """Return the DDL fragment for a single column (e.g. 'NAME VARCHAR2(100 CHAR) NULL')."""
    if col.arrow_type_id:
        ddl_type = map_arrow_to_oracle_ddl(col)
    elif col.raw_type:
        if not col.properties.get("python_type"):
            col.properties["python_type"] = map_oracle_to_python(col.raw_type, col.scale)
        ddl_type = map_python_to_oracle_ddl(col)
    else:
        ddl_type = "VARCHAR2(4000 CHAR)"
    nullable = "NULL" if col.is_nullable else "NOT NULL"
    return f"{col.name} {ddl_type} {nullable}"


# ---------------------------------------------------------------------------
# Model hydration
# ---------------------------------------------------------------------------

def column_from_row(
    schema_name: str,
    table_name: str,
    row: dict[str, Any],
    pk_set: set[str],
    unique_set: set[str],
    fk_map: dict[str, dict[str, Any]],
) -> Column:
    """Build a Column model from a raw ALL_TAB_COLUMNS row + constraint metadata."""
    name = str(row["COLUMN_NAME"])
    raw_type = str(row["DATA_TYPE"])
    scale = row.get("DATA_SCALE")
    precision = row.get("DATA_PRECISION")
    max_length = row.get("CHAR_LENGTH")
    column_id = row.get("COLUMN_ID")
    data_default = row.get("DATA_DEFAULT")
    is_virtual = str(row.get("VIRTUAL_COLUMN") or "NO").upper() == "YES"
    is_hidden = str(row.get("HIDDEN_COLUMN") or "NO").upper() == "YES"
    is_pk = name in pk_set
    fk_info = fk_map.get(name)
    return Column(
        name=name,
        locator=Locator(plugin="oracle", namespace=schema_name, entity_name=table_name),
        raw_type=raw_type,
        arrow_type_id=map_oracle_to_arrow(raw_type, scale),
        primary_key=is_pk,
        is_unique=is_pk or name in unique_set,
        is_compound_key=is_pk and len(pk_set) > 1,
        is_nullable=str(row.get("NULLABLE", "Y")).upper() == "Y",
        is_read_only=is_virtual,
        is_computed=is_virtual,
        is_hidden=is_hidden,
        is_foreign_key=fk_info is not None,
        foreign_key_entity=fk_info["REF_TABLE"] if fk_info else None,
        foreign_key_column=fk_info["REF_COLUMN"] if fk_info else None,
        is_foreign_key_enforced=(
            str(fk_info.get("STATUS", "")).upper() == "ENABLED"
        ) if fk_info else False,
        max_length=int(max_length) if max_length is not None else None,
        precision=int(precision) if precision is not None else None,
        scale=int(scale) if scale is not None else None,
        ordinal_position=int(column_id) if column_id is not None else None,
        serialized_null_value="NULL",
        default_value=str(data_default).strip() if data_default is not None else None,
        properties={"python_type": map_oracle_to_python(raw_type, scale)},
    )


# ---------------------------------------------------------------------------
# DML / data helpers
# ---------------------------------------------------------------------------

def input_sizes_for_entity(entity: Entity) -> dict[str, Any]:
    """Return oracledb input size hints for all columns in an entity."""
    sizes: dict[str, Any] = {}
    for col in entity.columns:
        if col.arrow_type_id:
            sizes[col.name] = map_column_to_oracledb_input_size(col)
        elif col.raw_type:
            if not col.properties.get("python_type"):
                col.properties["python_type"] = map_oracle_to_python(col.raw_type, col.scale)
            sizes[col.name] = map_python_to_oracledb_input_size(col)
        else:
            sizes[col.name] = None
    return sizes


def empty_reader(entity: Entity) -> ArrowReader:
    """Return an empty Arrow RecordBatchReader shaped to an entity's columns."""
    fields = [
        pa.field(c.name, c.arrow_type or pa.null(), nullable=True)
        for c in entity.columns
        if c.arrow_type is not None
    ]
    schema = pa.schema(fields) if fields else pa.schema([])
    return pa.RecordBatchReader.from_batches(schema, iter([]))


def inject_merge_audit(sql: str) -> str:
    """Inject SYSTIMESTAMP/USER audit expressions into a MERGE statement.

    Audit columns are excluded from the USING SELECT before build_merge_dml is called.
    This function re-inserts them as Oracle-computed expressions in both WHEN branches.
    All four audit columns are always injected — every managed Oracle table has them.

    TODO: UPDATED_BY should use the application-level session user, not the DB USER function.
    """
    no_match_marker = "WHEN NOT MATCHED THEN INSERT"

    if "WHEN MATCHED THEN UPDATE SET" in sql:
        idx = sql.index(no_match_marker)
        sql = (
            sql[:idx].rstrip()
            + ", tgt.UPDATED_DATE = SYSTIMESTAMP, tgt.UPDATED_BY = USER "
            + sql[idx:]
        )

    try:
        nm_idx = sql.index(no_match_marker)
        ins_open = sql.index("(", nm_idx)
        ins_close = sql.index(")", ins_open)
        val_close = sql.rindex(")")
        sql = (
            sql[:ins_close]
            + ", CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY"
            + sql[ins_close:val_close]
            + ", SYSTIMESTAMP, USER, SYSTIMESTAMP, USER"
            + sql[val_close:]
        )
    except ValueError:
        _tools_logger.warning("inject_merge_audit: MERGE markers not found — audit injection skipped")

    return sql


# ---------------------------------------------------------------------------
# Legacy cell normalization (pre-framework CSV ingestion utilities)
# ---------------------------------------------------------------------------

def normalize_cell(raw: str, data_type: str) -> Any:
    value = _NULL_BYTE_RE.sub('', raw)
    if not value.strip(): return None
    if data_type == 'NUMBER': return _to_decimal(value)
    elif data_type == 'DATE': return _to_date(value)
    elif data_type == 'TIMESTAMP': return _to_datetime(value)
    else: return value.strip()

def _to_decimal(value: str) -> Decimal | None:
    cleaned = _COMMA_RE.sub('', value.strip())
    try: return Decimal(cleaned)
    except InvalidOperation: return None

def _to_date(value: str) -> date | str:
    stripped = value.strip()
    try: return datetime.strptime(stripped[:10], _DATE_FMT).date()
    except ValueError: return stripped

def _to_datetime(value: str) -> datetime | str:
    stripped = value.strip(); cleaned = _TZ_OFFSET_RE.sub('', stripped)
    for fmt in _TIMESTAMP_FMTS:
        try: return datetime.strptime(cleaned, fmt)
        except ValueError: continue
    return stripped