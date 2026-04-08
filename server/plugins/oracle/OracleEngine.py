from __future__ import annotations

from typing import Any
import logging
import oracledb
from collections.abc import Iterator, Iterable
from .OracleClient import OracleClient
    
logger = logging.getLogger(__name__)

ORACLE_MAX_VARCHAR2_CHAR = 4000
varchar2_growth_buffer = 50


class OracleEngine:
    client: OracleClient
    default_schema: OracleSchema | str
    schemas: list[OracleSchema]
    """
    The Master Oracle Orchestrator.
    Handles the oracledb actions, executes raw SQL/DDL,
    and acts a stateful bridge between the OracleServices layer and the OracleClient.

    Nothing in this layer should import anything above it. If you find yourself needing to 
    import something from above, it likely means that logic belongs in the OracleServices layer instead.
    """
    def __init__(self, 
                schema: str | list[str] | None = None,
                client: OracleClient = OracleClient()
                ):
        self.client = client
        self.default_schema = self.client.oracle_user.upper() if schema is None else (schema[0] if isinstance(schema, list) else schema.upper())
        self.schemas = []


    # =========================================================
    # 2. THE MUSCLE: Data Stream Execution (CRUD)
    # =========================================================

    def query(self, sql: str, binds: dict[str, Any] | None = None, fetch_size: int = 10000) -> Iterator[dict[str, Any]]:
        """Executes a SELECT query and yields dictionaries efficiently."""
        binds = binds or {}
        
        with self.client.get_con().cursor() as cursor:
            try:
                cursor.arraysize = fetch_size
                cursor.execute(sql, binds)
                if cursor.description:
                    # Map Oracle columns to dictionary keys
                    columns = [col[0] for col in cursor.description]
                    cursor.rowfactory = lambda *args: dict(zip(columns, args))
                    while True:
                        rows = cursor.fetchmany(fetch_size)
                        if not rows:
                            break
                        yield from rows
                    # Map Oracle columns to dictionary keys
                    columns = [col[0] for col in cursor.description]
                    cursor.rowfactory = lambda *args: dict(zip(columns, args))
                
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows
                    
            except oracledb.Error as e:
                logger.error(f"Oracle read execution failed: {sql} | Error: {e}")
                raise
    # for ddl
    def execute_many(
        self, 
        sql: str, 
        records: Iterable[dict[str, Any]], 
        input_sizes: dict[str, Any], 
        batch_size: int = 10000
    ) -> Iterator[dict[str, Any]]:
        """Streams records into Oracle. Yields them back, tagged with errors if they fail."""
        with self.client.get_con().cursor() as cursor:
            if input_sizes:
                cursor.setinputsizes(**input_sizes)
                
            batch = []
            for record in records:
                batch.append(record)
                
                if len(batch) >= batch_size:
                    yield from self._flush_batch(cursor, sql, batch)
                    batch = []
                    
            if batch:
                yield from self._flush_batch(cursor, sql, batch)

    def _flush_batch(self, cursor: Any, sql: str, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pushes a chunk to Oracle. Attaches row-level errors without crashing the batch."""
        try:
            cursor.executemany(sql, batch, batcherrors=True)
            for error in cursor.getbatcherrors():
                failed_index = error.offset
                batch[failed_index]["__error"] = error.message  
            return batch    
        except oracledb.Error as e:
            logger.error(f"Oracle batch execution crashed: {e}")
            for record in batch:
                record["__error"] = str(e)
            return batch

    # =========================================================
    # 3. THE BUILDER: DDL Execution
    # =========================================================

    def execute_ddl(self, sql: str) -> None:
        """Executes structural mutations (CREATE, ALTER, DROP)."""
        with self.client.get_con().cursor() as cursor:
            try:
                cursor.execute(sql)
            except oracledb.Error as e:
                logger.error(f"Oracle DDL failed: {sql} | Error: {e}")
                raise

class OracleSchema:
    client: OracleClient
    schema_name: str
    tables: list[OracleTable]
    description: str | None
    schema_dict: dict[str, Any]
    
    def __init__(
            self,
            client: OracleClient = OracleClient(),
            schema_name: str = '',
            tables: list[OracleTable] = [],
            description: str | None = None
    ):
        self.client = client
        self.schema_name = schema_name
        if not self.schema_name or self.schema_name.strip() == '':
            self.schema_name = str(client.oracle_user).upper()         
        self.description = description
        self.tables = []
        if isinstance(tables, list):
            self.tables = tables
    

class OracleTable:
    table_name: str
    schema: OracleSchema
    columns: list[OracleColumn]
    _fetched_db_col: list[dict[str, Any]] | None 
    _insert_sql_stmt: str | None
    _input_sizes: dict[str, object] | None
    _active_plan: list[tuple] | None
    def __init__(self, table_name: str, schema: OracleSchema):
        self.table_name = table_name
        self.schema = schema
        self.columns = []
        self._fetched_db_col = None
        self._insert_sql_stmt = None
        self._input_sizes = None
        self._active_plan = None

    @property
    def qualified_name(self) -> str:
        if self.table_name is None: raise ValueError('Error: table_name cannot be None')
        if self.schema is None or self.schema.schema_name in (None, ''): return self.table_name
        return f'{self.schema.schema_name}.{self.table_name}'
    @property
    def insert_sql_stmt(self) -> str:
        if not self.columns:
            raise ValueError(f'Cannot generate insert_sql for {self.qualified_name}; columns empty')
        if self._insert_sql_stmt not in (None,''): return self._insert_sql_stmt
        
        columns=[]; binds=[]
        for col in self.columns:
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
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(sql, {'owner': self.schema.schema_name.upper(), 'table_name': self.table_name.upper()})
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
        for col_obj in self.columns:
            row = db_col_map.get(col_obj.bind_name)
            if row is None:
                col_obj.is_new = True
                new_cols.append(col_obj)
            else:
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
        for oracle_column in self.columns:
            oracle_column.is_new = True
            col_defs.append(oracle_column.column_definition())
        cols_stmt = ','.join(col_defs)
        sql = f'CREATE TABLE {self.qualified_name} ({cols_stmt})'
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(sql)
    
    def _alter_modify_existing_column(self, col: OracleColumn) -> None:
        sql = f'ALTER TABLE {self.qualified_name} MODIFY ({col.bind_name} VARCHAR2({col.effective_max_varchar2} CHAR))'
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(sql)

    def _alter_add_columns(self, new_columns: list[OracleColumn]) -> None:
        col_defs = ','.join(col.column_definition() for col in new_columns)
        sql = f'ALTER TABLE {self.qualified_name} ADD ({col_defs})'
        with self.schema.client.get_con().cursor() as cursor:
            cursor.execute(sql)
    
    def build_input_sizes(self) -> dict[str, object]:
        if self._input_sizes is not None:
            return self._input_sizes
        try:
            sizes={}
            for col in self.columns:
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
            col.bind_name,
            col.data_type
            ) for col in self.columns]
        return self._active_plan

    @staticmethod
    def construct_columns(col_dict: dict[str,dict[str,str]]) -> list[OracleColumn]:
        columns=[]
        for target_name, col_info_dict in col_dict.items():
            max_col_size = int(col_info_dict.get('max_col_size') or 0)
            data_type = 'CLOB' if max_col_size > ORACLE_MAX_VARCHAR2_CHAR else 'VARCHAR2'
            col = OracleColumn(
                target_name=target_name, 
                csv_col_name=col_info_dict.get('csv_col_name'), 
                csv_index=int(str(col_info_dict.get('index'))), 
                max_col_size=max_col_size, 
                data_type=data_type,
                col_info_dict=col_info_dict
                )
            columns.append(col)
        return columns


class OracleColumn:
    column_name: str
    table: OracleTable
    column_id: int | None = None
    detected_max_length: int
    data_type: str
    nullable: bool = True
    char_length: int | None
    char_used: str | None
    is_new: bool = False
    col_info_dict: dict[str, str]
    def __init__(self,
                max_col_size: int,
                col_info_dict: dict[str, str] = {},
                column_name: str | None = None,
                csv_col_name: str|None = None,
                data_type: str = 'VARCHAR2',
                nullable: bool = True,
                char_length: int | None = 0,
                char_used: str | None = None,
                is_new: bool = False,
                **kwargs
                ) -> None:
        self.csv_col_name = csv_col_name or col_info_dict.get('csv_col_name', '')
        self.column_name = column_name or col_info_dict.get('column_name', '')
        self.detected_max_length = max_col_size
        self.data_type = data_type
        self.nullable = nullable
        self.char_length = char_length
        self.char_used = char_used
        self.is_new = is_new
        self.col_info_dict = col_info_dict
        self.args = kwargs
    @property
    def bind_name(self) -> str:
        return self.column_name or (self.csv_col_name or '')
    
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
