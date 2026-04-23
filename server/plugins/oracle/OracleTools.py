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