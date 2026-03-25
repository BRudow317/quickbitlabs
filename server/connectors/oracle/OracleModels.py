from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING, Iterable
import re
import logging
import oracledb
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
if TYPE_CHECKING:
    from .OracleClient import OracleClient
logger = logging.getLogger(__name__)

_NULL_BYTE_RE = re.compile(r'\x00')
_COMMA_RE = re.compile(r',')
_DATE_FMT = '%Y-%m-%d'
_TIMESTAMP_FMTS = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']
_TZ_OFFSET_RE = re.compile(r'[+-]\d{2}:\d{2}$')

ORACLE_MAX_VARCHAR2_CHAR = 4000
varchar2_growth_buffer = 50

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


@dataclass
class OracleTable:
    oracle_client: OracleClient
    table_name: str = ''
    schema_name: str = ''
    column_map: dict[str, OracleColumn] = field(default_factory=dict)
    _fetched_db_col: list[dict[str, Any]] | None = field(default=None, init=False)
    _insert_sql_stmt: str | None = field(default=None, init=False)
    _input_sizes: dict[str, object] | None = field(default=None, init=False)
    _active_plan: list[tuple] | None = field(default=None, init=False)

    @property
    def qualified_name(self) -> str:
        if self.table_name is None: raise ValueError('Error: table_name cannot be None')
        if self.schema_name in (None, ''): return self.table_name
        return f'{self.schema_name}.{self.table_name}'
    @property
    def insert_sql_stmt(self) -> str:
        if not self.column_map:
            raise ValueError(f'Cannot generate insert_sql for {self.qualified_name}; columns empty')
        if self._insert_sql_stmt not in (None,''): return self._insert_sql_stmt
        
        columns=[]; binds=[]
        for _, col in self.column_map.items(): 
            bind_name = col.bind_name
            columns.append(bind_name)
            binds.append(bind_name if bind_name.startswith(':') else f':{bind_name}')
        self._insert_sql_stmt = f"INSERT INTO {self.qualified_name} ({','.join(columns)}) VALUES ({','.join(binds)})"
        logger.debug(f'Generated insert SQL for {self.qualified_name}:\n{self._insert_sql_stmt}')
        return self._insert_sql_stmt
    @property
    def _fetch_tab_columns(self) -> list[dict[str, Any]]| None:
        if self._fetched_db_col is not None: return self._fetched_db_col
        sql = f"""SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, CHAR_USED, COLUMN_ID, NULLABLE
                    FROM ALL_TAB_COLUMNS WHERE OWNER = :owner 
                    AND TABLE_NAME = :table_name 
                    ORDER BY COLUMN_ID
                """
        with self.oracle_client.get_con().cursor() as cursor:
            cursor.execute(sql, {'owner': self.schema_name.upper(), 'table_name': self.table_name.upper()})
            cursor_desc = cursor.description or []
            col_names = [col[0] for col in cursor_desc] or []
            cursor.rowfactory = lambda *args: dict(zip(col_names, args))
            res: list[dict[str, Any]] = cursor.fetchall()
        if len(res) > 0:
            self._fetched_db_col = res
            return res
        else :
            self._fetched_db_col = None
            return None
        
    def _wipe_fetch_cache(self) -> None:
        self._fetched_db_col = None
        self._insert_sql_stmt = None
        self._input_sizes = None
        self._active_plan = None
    
    def _align_columns(self) -> None:
        sf = self._fetch_tab_columns
        if not sf:
            self._build_new_table()
            self._wipe_fetch_cache()
            self._align_columns(); return
        db_col_map = {row['COLUMN_NAME']: row for row in sf}
        new_cols = []
        for col_obj in self.column_map.values():
            row = db_col_map.get(col_obj.bind_name)
            if row is None:
                col_obj.oracle_name = col_obj.target_name
                col_obj.is_new = True
                new_cols.append(col_obj)
            else:
                col_obj.oracle_name = row['COLUMN_NAME']
                col_obj.data_type = row['DATA_TYPE']
                col_obj.char_used = row.get('CHAR_USED')
                col_obj.nullable = row.get('NULLABLE') == 'Y'
                col_obj.is_new = False
                current_db_length = int(str(row.get('CHAR_LENGTH') or 0))
                if col_obj.data_type == 'VARCHAR2' and col_obj.effective_max_varchar2 > current_db_length:
                    col_obj.char_length = current_db_length
                    self._alter_modify_existing_column(col_obj)
                    self._wipe_fetch_cache()
                    self._align_columns(); return
        if new_cols:
            self._alter_add_columns(new_cols)
            self._wipe_fetch_cache()
            self._align_columns(); return
    
    def _build_new_table(self) -> None:
        col_defs = []
        for oracle_column in self.column_map.values():
            oracle_column.oracle_name = oracle_column.target_name
            oracle_column.is_new = True
            col_defs.append(oracle_column.column_definition())
        cols_stmt = ','.join(col_defs)
        sql = f'CREATE TABLE {self.qualified_name} ({cols_stmt})'
        with self.oracle_client.get_con().cursor() as cursor:
            cursor.execute(sql)
        
    
    def _alter_modify_existing_column(self, col: OracleColumn) -> None:
        sql = f'ALTER TABLE {self.qualified_name} MODIFY ({col.bind_name} VARCHAR2({col.effective_max_varchar2} CHAR))'
        with self.oracle_client.get_con().cursor() as cursor:
            cursor.execute(sql)

    def _alter_add_columns(self, new_columns: list[OracleColumn]) -> None:
        col_defs = ','.join(col.column_definition() for col in new_columns)
        sql = f'ALTER TABLE {self.qualified_name} ADD ({col_defs})'
        with self.oracle_client.get_con().cursor() as cursor:
            cursor.execute(sql)
        
    
    def build_input_sizes(self) -> dict[str, object]:
        if self._input_sizes is not None:
            return self._input_sizes
        try:
            sizes={}
            for col in self.column_map.values():
                bind_name = col.bind_name
                col.data_type
                if col.data_type in ('VARCHAR2','NVARCHAR2','CHAR'):
                    max_len = col.char_length or col.detected_max_length or 4000
                    sizes[bind_name] = int(max_len)
                elif col.data_type in ('NUMBER','FLOAT'): sizes[bind_name] = oracledb.DB_TYPE_NUMBER
                elif col.data_type == 'DATE': sizes[bind_name] = oracledb.DB_TYPE_DATE
                elif col.data_type.startswith('TIMESTAMP'): sizes[bind_name] = oracledb.DB_TYPE_TIMESTAMP
                elif col.data_type == 'CLOB': sizes[bind_name] = oracledb.DB_TYPE_LONG
                elif col.data_type == 'BLOB': sizes[bind_name] = oracledb.DB_TYPE_BLOB
                elif col.data_type == 'RAW': sizes[bind_name] = oracledb.DB_TYPE_RAW
                elif col.data_type == 'JSON': sizes[bind_name] = oracledb.DB_TYPE_JSON
                else: sizes[bind_name] = None
            self._input_sizes = sizes
            return sizes
        except Exception as e:
            logger.error(f'Error: OracleTable.build_input_sizes Error: {e}')
            raise
    
    @property
    def active_plan(self) -> list[tuple]:
        self._active_plan = [(
            col.csv_index, 
            col.bind_name, 
            col.data_type
            ) for col in self.column_map.values() if col.csv_index is not None]
        return self._active_plan

    @staticmethod
    def construct_column_map(col_dict: dict[str,dict[str,str]]) -> dict[str, OracleColumn]:
        column_map={}
        for target_name, col_info_dict in col_dict.items():
            max_col_size = int(col_info_dict.get('max_col_size') or 0)
            data_type = 'CLOB' if max_col_size > ORACLE_MAX_VARCHAR2_CHAR else 'VARCHAR2'
            column_map[target_name] = OracleColumn(
                target_name=target_name, 
                csv_col_name=col_info_dict.get('csv_col_name'), 
                csv_index=int(str(col_info_dict.get('index'))), 
                max_col_size=max_col_size, 
                data_type=data_type,
                col_info_dict=col_info_dict
                )
        return column_map
    
    @staticmethod
    def construct_table(
        col_dict: dict[str, dict[str, str]], 
        table_name: str, 
        schema_name: str, 
        oracle_client: OracleClient
        ) -> OracleTable:
        oracle_table = OracleTable(
            table_name=table_name, 
            schema_name=schema_name,
            oracle_client=oracle_client)
        oracle_table.column_map = OracleTable.construct_column_map(col_dict)
        oracle_table._align_columns()
        return oracle_table


class OracleColumn:
    csv_col_name: str
    target_name: str
    csv_index: int
    detected_max_length: int
    data_type: str
    oracle_name: str | None
    nullable: bool = True
    char_length: int | None
    char_used: str | None
    is_new: bool = False
    col_info_dict: dict[str, str]
    def __init__(self,
                csv_index: int,
                max_col_size: int,
                col_info_dict: dict[str, str] = {},
                target_name: str | None = None,
                csv_col_name: str|None = None,
                data_type: str = 'VARCHAR2',
                oracle_name: str | None = None,
                nullable: bool = True,
                char_length: int | None = 0,
                char_used: str | None = None,
                is_new: bool = False,
                **kwargs
                ) -> None:
        self.csv_col_name = csv_col_name or col_info_dict.get('csv_col_name', '')
        self.target_name = target_name or col_info_dict.get('target_name', '')
        self.csv_index = csv_index or int(str(col_info_dict.get('index') or 0))
        self.detected_max_length = max_col_size or int(str(col_info_dict.get('max_col_size') or 0))
        self.data_type = data_type
        self.oracle_name = oracle_name
        self.nullable = nullable
        self.char_length = char_length
        self.char_used = char_used
        self.is_new = is_new
        self.col_info_dict = col_info_dict
        self.args = kwargs
    @property
    def bind_name(self) -> str:
        return self.oracle_name or self.target_name or (self.csv_col_name or '')
    
    @property
    def effective_max_varchar2(self) -> int:
        observed_char_len = max((self.char_length or 0), self.detected_max_length)
        if observed_char_len > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(f"observed_char_len {observed_char_len} exceeds Oracle limit {ORACLE_MAX_VARCHAR2_CHAR}")
        buffered = observed_char_len + varchar2_growth_buffer
        return min(buffered, ORACLE_MAX_VARCHAR2_CHAR)

    def column_definition(self) -> str:
        if self.data_type == 'VARCHAR2':
            type_clause = f'VARCHAR2({self.effective_max_varchar2} CHAR)'
        elif self.data_type == 'NUMBER': type_clause = 'NUMBER'
        elif self.data_type == 'DATE': type_clause = 'DATE'
        elif self.data_type == 'TIMESTAMP': type_clause = 'TIMESTAMP'
        elif self.data_type == 'CLOB': type_clause = 'CLOB'
        else: raise ValueError(f"Unrecognised data_type '{self.data_type}' on column '{self.bind_name}'.")
        nullable_clause = 'NULL' if self.nullable else 'NOT NULL'
        return f'{self.bind_name} {type_clause} {nullable_clause}'



def to_oracle_snake(value: str, max_len: int = 128, reserved: Iterable[str] = _ORACLE_RESERVED, reserved_prefix: str | None = None) -> str:
    s = str(value).strip()
    if not s:
        return 'COL'
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s = re.sub(r'_{2,}', '_', s).strip('_').upper()
    if not s:
        return 'COL'
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

