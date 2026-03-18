from __future__ import annotations
import os
import logging
from typing import Any, Iterator
from sqlalchemy import create_engine, text, Engine, MetaData, inspect, Table as SATable, Column as SAColumn, String
from sqlalchemy.engine import MappingResult

from server.connectors.oracle.utils.type_converter import PYTHON_TO_ORACLE, get_python_type
from server.connectors.oracle import effective_max_varchar2

logger = logging.getLogger(__name__)

class OracleClient:
    """
    SQLAlchemy-based client for Oracle Database.
    Handles the 'how' of connecting, reflecting, and executing.
    """
    engine: Engine
    metadata: MetaData

    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | str | None = None,
        sid: str | None = None,
        connection_string: str | None = None,
        **kwargs: Any
    ):
        if not connection_string:
            user = user or os.environ.get('ORACLE_USER')
            password = password or os.environ.get('ORACLE_PASS')
            host = host or os.environ.get('ORACLE_HOST')
            port = port or os.environ.get('ORACLE_PORT', '1521')
            sid = sid or os.environ.get('ORACLE_SID')
            connection_string = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={sid}"

        self.engine = create_engine(connection_string, **kwargs)
        self.metadata = MetaData()

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM DUAL"))
            return True
        except Exception as e:
            logger.error(f"Oracle connection failed: {e}")
            return False

    # ── Reflection (Discovery) ──

    def get_table_names(self, schema: str | None = None) -> list[str]:
        return inspect(self.engine).get_table_names(schema=schema)

    def reflect_table_columns(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        inspector = inspect(self.engine)
        pk_cols = inspector.get_pk_constraint(table_name, schema=schema).get('constrained_columns', [])
        
        columns = []
        for col in inspector.get_columns(table_name, schema=schema):
            col['is_pk'] = col['name'] in pk_cols
            columns.append(col)
        return columns

    # ── DDL (Structure) ──

    def create_table(self, table_name: str, columns: list[dict[str, Any]], schema: str | None = None) -> None:
        """
        Expects columns in a format ready for SAColumn:
        [{'name': str, 'type_key': PythonTypes, 'length': int, 'pk': bool, 'nullable': bool}]
        """
        sa_columns = []
        for col in columns:
            sa_type_cls = PYTHON_TO_ORACLE.get(col['type_key'], String)
            
            if sa_type_cls is String:
                length = col.get('length') or 50
                sa_type = sa_type_cls(effective_max_varchar2(length))
            else:
                sa_type = sa_type_cls()

            sa_columns.append(SAColumn(
                col['name'],
                sa_type,
                primary_key=col.get('pk', False),
                nullable=col.get('nullable', True),
            ))

        meta = MetaData(schema=schema)
        sa_table = SATable(table_name, meta, *sa_columns)
        sa_table.create(self.engine, checkfirst=True)

    def drop_table(self, table_name: str, schema: str | None = None) -> None:
        meta = MetaData(schema=schema)
        sa_table = SATable(table_name, meta)
        sa_table.drop(self.engine, checkfirst=True)

    # ── DML (Data) ──

    def stream_rows(self, table_name: str, schema: str | None = None) -> Iterator[dict[str, Any]]:
        qualified_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {qualified_name}"))
            for row in result:
                yield dict(row._mapping)

    def insert_rows(self, table_name: str, rows: list[dict[str, Any]], schema: str | None = None) -> None:
        sa_table = SATable(table_name, MetaData(schema=schema), autoload_with=self.engine)
        with self.engine.begin() as conn:
            conn.execute(sa_table.insert(), rows)
