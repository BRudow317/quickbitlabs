from __future__ import annotations
from dataclasses import dataclass, field
import os
import logging
import oracledb
logger = logging.getLogger(__name__)

@dataclass(slots=True)
class OracleClient:
    oracle_user: str | None = field(default_factory=lambda: os.getenv('ORACLE_USER'))
    oracle_pass: str | None = field(default_factory=lambda: os.getenv('ORACLE_PASS'))
    oracle_host: str | None = field(default_factory=lambda: os.getenv('ORACLE_HOST'))
    oracle_port: int | None = field(default_factory=lambda: int(str(os.getenv('ORACLE_PORT', '0'))))
    oracle_service: str | None = field(default_factory=lambda: os.getenv('ORACLE_SID'))
    _current_connection: oracledb.Connection | None = None
    def __post_init__(self):
        logger.debug(f"oracle_user: {self.oracle_user}, oracle_pass: {'*' * len(self.oracle_pass) if self.oracle_pass else None}, oracle_host: {self.oracle_host}, oracle_port: {self.oracle_port}, oracle_service: {self.oracle_service}")
    def get_con(self) -> oracledb.Connection:
        try:
            if self._current_connection is not None and self._current_connection.is_healthy():
                return self._current_connection
            logger.debug('Connection Not Established: Creating New Connection')
            self._current_connection = oracledb.connect(user=self.oracle_user, password=self.oracle_pass, host=self.oracle_host, port=self.oracle_port, service_name=self.oracle_service)
            return self._current_connection
        except oracledb.Error as e:
            logger.error(f'Unable to connect to Oracle Database: \n{e}')
            raise