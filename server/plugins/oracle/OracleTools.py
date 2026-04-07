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


_ORACLE_RESERVED = frozenset({
"ACCESS","ADD","ALL","ALTER","AND","ANY","AS","ASC","AUDIT","BETWEEN","BY","CHAR","CHECK","CLUSTER","COLUMN",
"COMMENT","COMPRESS","CONNECT","CREATE","CURRENT","DATE","DECIMAL","DEFAULT","DELETE","DESC","DISTINCT","DROP","ELSE",
"EXCLUSIVE","EXISTS","FILE","FLOAT","FOR","FROM","GRANT","GROUP","HAVING","IDENTIFIED","IMMEDIATE","IN","INCREMENT",
"INDEX","INITIAL","INSERT","INTEGER","INTERSECT","INTO","IS","LEVEL","LIKE","LOCK","LONG","MAXEXTENTS","MINUS",
"MLSLABEL","MODE","MODIFY","NOAUDIT","NOCOMPRESS","NOT","NOWAIT","NULL","NUMBER","OF","OFFLINE","ON","ONLINE",
"OPTION","OR","ORDER","PCTFREE","PRIOR","PRIVILEGES","PUBLIC","RAW","RENAME","RESOURCE","REVOKE","ROW","ROWID",
"ROWNUM","ROWS","SELECT","SESSION","SET","SHARE","SIZE","SMALLINT","START","SUCCESSFUL","SYNONYM","SYSDATE",
"TABLE","THEN","TO","TRIGGER","UID","UNION","UNIQUE","UPDATE","USER","VALIDATE","VALUES","VARCHAR","VARCHAR2",
"VIEW","WHENEVER","WHERE","WITH",
# Additional Oracle reserved words not in the legacy list
"CASE","CROSS","CUBE","FETCH","FULL","INNER","JOIN","LEFT","MERGE","NATURAL","OFFSET",
"OUTER","RIGHT","ROLLUP","USING","WHEN"})


def to_oracle_snake(value: str, max_len: int = 128, reserved: Iterable[str] = _ORACLE_RESERVED, reserved_prefix: str | None = None) -> str:
    s = str(value).strip()
    if not s: return 'COL'
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    # s = re.sub(r'_{2,}', '_', s)
    s= s.strip('_').upper()
    if not s: return 'COL'
    if s[0].isdigit():
        s = 'C_' + s
    if s in reserved:
        s = f'{reserved_prefix}{s}' if reserved_prefix else f'{s}_COL'
    if len(s) > max_len:
        prefix = reserved_prefix or ''
        if reserved_prefix and s.startswith(prefix):
            s = f'{prefix}{s[len(prefix):max_len].rstrip("_")}'
        elif s.endswith('_COL'):
            base = s[:max_len - 4].rstrip('_')
            s = f'{base}_COL'
        else:
            s = s[:max_len].rstrip('_')
    return s if s else 'COL'

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