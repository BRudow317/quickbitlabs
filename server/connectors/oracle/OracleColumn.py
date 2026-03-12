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
        from .to_oracle_snake import to_oracle_snake
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
