from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import logging, oracledb
from .OracleClient import OracleUser
from .oracle_config import ORACLE_MAX_VARCHAR2_CHAR, effective_max_varchar2
logger = logging.getLogger(__name__)

@dataclass
class OracleTable:
    session: OracleUser
    table_name: str = ''
    schema_name: str = ''
    column_map: dict[str, OracleColumn] = field(default_factory=dict)
    test_run: bool = False
    _fetched_db_col: list[dict[str, Any]] | None = field(default=None, init=False)
    _insert_sql_stmt: str | None = field(default=None, init=False)
    @property
    def qualified_name(self) -> str:
        if self.table_name is None: raise ValueError('Error: table_name cannot be None')
        if self.schema_name is None: return self.table_name
        return f'{self.schema_name}.{self.table_name}'
    @property
    def insert_sql_stmt(self) -> str:
        if not self.column_map:
            raise ValueError(f'Cannot generate insert_sql for {self.qualified_name}; columns empty')
        columns=[]; binds=[]
        for key, col in self.column_map.items():
            oracle_col = col.oracle_name or key
            columns.append(oracle_col)
            if not col.csv_header_name: raise ValueError(f"Cannot build bind for column '{oracle_col}': csv_header_name missing")
            binds.append(col.bind_name if col.bind_name.startswith(':') else f':{col.bind_name}')
        return f"INSERT INTO {self.qualified_name} ({','.join(columns)})VALUES ({','.join(binds)})"
    @property
    def _fetch_tab_columns(self):
        logger.debug('Enter: OracleTable._fetch_tab_columns')
        try:
            if self._fetched_db_col is not None:
                return self._fetched_db_col
            sql = """SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, CHAR_USED, COLUMN_ID, NULLABLE
                     FROM ALL_TAB_COLUMNS WHERE OWNER = :owner 
                     AND TABLE_NAME = :table_name 
                     ORDER BY COLUMN_ID
                    """
            res = self.session.fetchall(sql, {'owner': self.schema_name, 'table_name': self.table_name})
            self._fetched_db_col = list(res)
            return res
        except Exception as e:
            logger.error(f'Error: OracleTable._fetch_tab_columns Error: {e}')
            raise
    def _wipe_fetch_cache(self) -> None:
        object.__setattr__(self, '_insert_sql_stmt', None)
    def add_column(self, col: OracleColumn) -> None:
        if col.oracle_name not in self.column_map and col.oracle_name is not None:
            self.column_map[col.oracle_name] = col
    def _align_columns(self) -> None:
        try:
            if len(self._fetch_tab_columns) > 0:
                for _, col_obj in self.column_map.items():
                    all_tab_row = [row for row in self._fetch_tab_columns if row.get('COLUMN_NAME') == col_obj.target_name]
                    if all_tab_row:
                        oracle_column = self.column_map.get(str(all_tab_row[0].get('COLUMN_NAME')))
                        if oracle_column:
                            oracle_column.column_id = int(str(all_tab_row[0].get('COLUMN_ID')))
                            oracle_column.oracle_name = str(all_tab_row[0].get('COLUMN_NAME'))
                            oracle_column.data_type = str(all_tab_row[0].get('DATA_TYPE'))
                            oracle_column.char_length = int(str(all_tab_row[0].get('CHAR_LENGTH') or 0))
                            oracle_column.char_used = str(all_tab_row[0].get('CHAR_USED'))
                            oracle_column.nullable = str(all_tab_row[0].get('NULLABLE')) == 'Y'
                            oracle_column.is_new = False
                self._check_existing_column_size()
            else:
                self._build_new_table()
        except Exception as e:
            logger.error(f'Error: OracleTable._align_columns Error: {e}')
            raise
    def _check_existing_column_size(self) -> None:
        all_tab_columns = self._fetch_tab_columns
        all_tab_names = [row.get('COLUMN_NAME') for row in all_tab_columns]
        new_cols=[]
        for oc_obj in self.column_map.values():
            if oc_obj.target_name not in all_tab_names:
                oc_obj.oracle_name = oc_obj.target_name
                oc_obj.is_new = True
                new_cols.append(oc_obj)
            else:
                oc_obj.is_new = False
                all_tab_row = [row for row in all_tab_columns if row['COLUMN_NAME'] == oc_obj.target_name][0]
                if all_tab_row.get('DATA_TYPE') == 'VARCHAR2':
                    current_db_limit = int(str(all_tab_row.get('CHAR_LENGTH') or 0))
                    current_data_max = oc_obj.char_length or 0
                    if current_data_max > current_db_limit:
                        self._alter_modify_existing_column(oc_obj)
        if new_cols:
            self._alter_add_columns(new_cols)
    def _build_new_table(self) -> None:
        for oracle_column in self.column_map.values():
            oracle_column.oracle_name = oracle_column.target_name
            oracle_column.is_new = True
        cols_sql = ','.join(col.column_definition() for col in self.column_map.values())
        create_table_ddl = f'CREATE TABLE {self.qualified_name} ({cols_sql})'
        self.session.execute_sql(create_table_ddl)
        self._wipe_fetch_cache(); self._align_columns()
    def _alter_modify_existing_column(self, col: OracleColumn) -> None:
        if col.data_type != 'VARCHAR2':
            raise ValueError(f'build_alter_modify only supports VARCHAR2 columns: {col.oracle_name}')
        if (col.char_length or 0) > ORACLE_MAX_VARCHAR2_CHAR:
            raise ValueError(f'{col.oracle_name} observed length exceeds VARCHAR2 limit')
        new_size = effective_max_varchar2(col.char_length or 0)
        stmt = f'ALTER TABLE {self.qualified_name} MODIFY ({col.oracle_name} VARCHAR2({new_size} CHAR))'
        self.session.execute_sql(stmt)
        self._wipe_fetch_cache(); self._align_columns()
    def _alter_add_columns(self, new_columns: list[OracleColumn]) -> None:
        if not new_columns: raise ValueError('build_alter_add called with empty new_columns')
        col_defs = ','.join(col.column_definition() for col in new_columns)
        stmt = f'ALTER TABLE {self.qualified_name} ADD ({col_defs})'
        self.session.execute_sql(stmt); self._align_columns()
    def build_input_sizes(self) -> dict[str, object]:
        try:
            sizes={}
            for bind_name, col in self.column_map.items():
                dt=(col.data_type or '').upper()
                if dt in ('VARCHAR2','NVARCHAR2','CHAR'):
                    max_len = col.char_length or col.detected_max_length or 4000
                    sizes[bind_name] = int(max_len)
                elif dt in ('NUMBER','FLOAT'): sizes[bind_name] = oracledb.DB_TYPE_NUMBER
                elif dt == 'DATE': sizes[bind_name] = oracledb.DB_TYPE_DATE
                elif dt.startswith('TIMESTAMP'): sizes[bind_name] = oracledb.DB_TYPE_TIMESTAMP
                elif dt == 'CLOB': sizes[bind_name] = oracledb.DB_TYPE_CLOB
                elif dt == 'BLOB': sizes[bind_name] = oracledb.DB_TYPE_BLOB
                elif dt == 'RAW': sizes[bind_name] = oracledb.DB_TYPE_RAW
                elif dt == 'JSON': sizes[bind_name] = oracledb.DB_TYPE_JSON
                else: sizes[bind_name] = None
            return sizes
        except Exception as e:
            logger.error(f'Error: OracleTable.build_input_sizes Error: {e}')
            raise
    @staticmethod
    def to_oracle_snake(value: str) -> str:
        from .oracle_utils.to_oracle_snake import to_oracle_snake as tos
        return tos(value)
    @staticmethod
    def construct_column_map(header_list: list[str]) -> dict[str, OracleColumn]:
        column_map={}
        if not header_list: return column_map
        for idx, header in enumerate(header_list):
            base_name = OracleTable.to_oracle_snake(header)
            target_name = base_name; counter=2
            while target_name in column_map:
                target_name = f'{base_name}_{counter}'; counter += 1
            column_map[target_name] = OracleColumn(csv_header_name=header, target_name=target_name, csv_index=idx)
        return column_map
    @staticmethod
    def construct_table(headers: list[str], oracle_table: OracleTable) -> OracleTable:
        oracle_table.column_map = OracleTable.construct_column_map(headers)
        oracle_table._align_columns()
        return oracle_table
from dataclasses import dataclass
from .oracle_config import ORACLE_MAX_VARCHAR2_CHAR, varchar2_growth_buffer, effective_max_varchar2

@dataclass(slots=True)
class OracleColumn:
    csv_header_name: str | None = None
    csv_index: int | None = None
    target_name: str = ''
    py_data_type: str = ''
    detected_max_length: int | None = None
    oracle_name: str | None = None
    data_type: str = 'VARCHAR2'
    column_id: int | None = None
    is_key: bool = False
    is_foreign_key: bool = False
    nullable: bool = True
    char_length: int | None = None
    char_used: str | None = None
    data_scale: int | None = None
    data_precision: int | None = None
    is_new: bool = False
    needs_modified: bool = False
    is_ready: bool = False
    def __post_init__(self) -> None:
        from .oracle_utils.to_oracle_snake import to_oracle_snake
        if not self.target_name and self.csv_header_name:
            object.__setattr__(self, 'target_name', to_oracle_snake(self.csv_header_name))
    @property
    def bind_name(self) -> str:
        return self.oracle_name or self.target_name or (self.csv_header_name or '')
    def type_new_column(self) -> str:
        if self.data_type == 'UNKNOWN':
            raise ValueError(f"Cannot generate DDL for column '{self.oracle_name}': data_type is UNKNOWN. Run type inference before building DDL.")
        nullable_clause = 'NULL' if self.nullable else 'NOT NULL'
        type_clause = self._type_clause()
        return f'{self.oracle_name} {type_clause} {nullable_clause}'
    def _type_clause(self) -> str:
        if self.data_type == 'VARCHAR2':
            if (self.char_length or 0) > ORACLE_MAX_VARCHAR2_CHAR:
                raise ValueError(f"Column '{self.oracle_name}' observed length {self.char_length} exceeds VARCHAR2 limit.")
            sized = effective_max_varchar2(self.char_length) if self.char_length else varchar2_growth_buffer
            return f'VARCHAR2({sized} CHAR)'
        if self.data_type == 'NUMBER': return 'NUMBER'
        if self.data_type == 'DATE': return 'DATE'
        if self.data_type == 'TIMESTAMP': return 'TIMESTAMP'
        raise ValueError(f"Unrecognised data_type '{self.data_type}' on column '{self.oracle_name}'.")
    def column_definition(self) -> str:
        if self.data_type == 'UNKNOWN':
            raise ValueError(f"Cannot generate DDL for column '{self.oracle_name}': data_type is UNKNOWN.")
        nullable_clause = 'NULL' if self.nullable else 'NOT NULL'
        type_clause = self._type_clause()
        return f'{self.oracle_name} {type_clause} {nullable_clause}'
