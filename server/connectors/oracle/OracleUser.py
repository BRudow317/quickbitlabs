from __future__ import annotations
from dataclasses import dataclass, field
import os
from typing import Any
import logging
import oracledb
logger = logging.getLogger(__name__)

@dataclass(slots=True)
class OracleUser:
    oracle_user: str | None = field(default_factory=lambda: os.environ.get('ORACLE_USER'))
    oracle_pass: str | None = field(default_factory=lambda: os.environ.get('ORACLE_PASS'))
    oracle_host: str | None = field(default_factory=lambda: os.environ.get('ORACLE_HOST'))
    oracle_port: int | None = field(default_factory=lambda: int(os.environ.get('ORACLE_PORT', '1521')))
    oracle_sid: str | None = field(default_factory=lambda: os.environ.get('ORACLE_SID'))
    _current_connection: oracledb.Connection | None = None
    _open_cursors: list[oracledb.Cursor | None] = field(default_factory=list)
    def get_con(self) -> oracledb.Connection:
        try:
            logger.debug('Enter: OracleUser.get_con')
            if self._current_connection is not None:
                if self._current_connection.is_healthy():
                    self._current_connection.ping()
                    logger.debug('Connection Healthy: Returning Existing Connection')
                    return self._current_connection
            logger.debug('Connection Not Established: Creating New Connection')
            self._current_connection = oracledb.connect(user=self.oracle_user, password=self.oracle_pass, host=self.oracle_host, port=self.oracle_port, service_name=self.oracle_sid)
            return self._current_connection
        except oracledb.Error as e:
            logger.error(f'Unable to connect to Oracle Database: {e}')
            raise
    def close_con(self, connection: oracledb.Connection | None = None):
        try:
            if connection is not None:
                connection.close()
            elif self._current_connection is not None and self._current_connection.is_healthy():
                self._current_connection.close()
        except oracledb.Error as e:
            logger.warning(f'Error while closing connection: {e}')
        except Exception as e:
            logger.error(f'Critical Error while closing connection: {e}')
            raise
    def get_cursor(self, **input_sizes) -> oracledb.Cursor:
        try:
            con = self.get_con(); cursor = con.cursor()
            if input_sizes: cursor.setinputsizes(**input_sizes)
            return cursor
        except Exception as e:
            logger.error(f'OracleUser.get_cursor Error: {e}')
            raise
    def fetchall(self, sql: str, binds: dict[str, Any] | None = None):
        try:
            response = []
            cursor = self.get_cursor(); cursor.execute(sql, binds or {})
            description = cursor.description
            if description:
                columns = [col[0] for col in description]
                cursor.rowfactory = lambda *args: dict(zip(columns, args))
                response = cursor.fetchall()
            return response
        except Exception as e:
            logger.error(f'Error in OracleUser.fetchall: {e}')
            raise
    def execute_sql(self, sql: str, **input_sizes):
        logger.debug(f'Enter: OracleUser.execute_sql: {sql}')
        try:
            cursor = self.get_cursor(**input_sizes) if input_sizes else self.get_cursor()
            with cursor:
                cursor.prepare(sql)
                cursor.execute(sql)

                if cursor.

                if 'select' not in sql.lower():
                    self.get_con().commit()

        except Exception as e:
            self.get_con().rollback()
            logger.error(f'Error executing SQL: {sql} Error: {e}')
            raise
