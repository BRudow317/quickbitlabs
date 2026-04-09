from __future__ import annotations
import os
import logging
import oracledb

logger = logging.getLogger(__name__)

class OracleClient:
    oracle_user: str
    oracle_pass: str
    oracle_host: str
    oracle_port: int
    oracle_service: str
    _current_connection: oracledb.Connection

    def __init__(self,
                oracle_user: str = os.getenv('ORACLE_USER') or '',
                oracle_pass: str = os.getenv('ORACLE_PASS') or '',
                oracle_host: str = os.getenv('ORACLE_HOST') or '',
                oracle_port: int = int(p) if (p := os.getenv('ORACLE_PORT')) else 0,
                oracle_service: str = os.getenv('ORACLE_SERVICE') or ''
                 ):
        self.oracle_user = oracle_user
        self.oracle_pass = oracle_pass
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service = oracle_service
        self._connect()
    
    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"user={self.oracle_user!r}, "
            f"host={self.oracle_host!r}, "
            f"port={self.oracle_port!r}, "
            f"service={self.oracle_service!r})"
        )
    def _connect(self) -> None:
        try:
            self._current_connection = oracledb.connect(
                user=self.oracle_user,
                password=self.oracle_pass,
                host=self.oracle_host,
                port=self.oracle_port,
                service_name=self.oracle_service,
            )
            self._current_connection.autocommit = True
        except oracledb.Error as e:
            logger.error(f'Error connecting to Oracle: {self.__repr__}\nError: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during Oracle connection: {self.__repr__}\nError: {e}')
            raise
    
    def get_con(self) -> oracledb.Connection:
        if self._current_connection.is_healthy(): return self._current_connection
        self._connect()
        return self._current_connection
    
    def __call__(self):
        return self.get_con()
    
    def close(self) -> None:
        try:
            self._current_connection.close()
        except oracledb.Error as e:
            logger.error(f'Error closing Oracle connection: {self.__repr__}\nError: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during Oracle connection close: {self.__repr__}\nError: {e}')
            raise
    
    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logger.warning(f'Error during OracleClient cleanup: {self.__repr__}\nError: {e}')

