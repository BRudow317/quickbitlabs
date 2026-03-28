from __future__ import annotations
from dataclasses import dataclass, field
import os
import logging
import oracledb

logger = logging.getLogger(__name__)


@dataclass(slots=True)
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
                 ) -> None:
        self.oracle_user = oracle_user
        self.oracle_pass = oracle_pass
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service = oracle_service
        self._current_connection = oracledb.connect(
                user=self.oracle_user,
                password=self.oracle_pass,
                host=self.oracle_host,
                port=self.oracle_port,
                service_name=self.oracle_service,
            )
        logger.debug(
            f"OracleClient Initialized: "
            f"oracle_user={self.oracle_user!r}, oracle_host={self.oracle_host!r}, "
            f"oracle_port={self.oracle_port!r}, oracle_service={self.oracle_service!r}"
        )

    def get_con(self) -> oracledb.Connection:
        try:
            if self._current_connection is not None and self._current_connection.is_healthy():
                return self._current_connection
            logger.debug('Establishing new Oracle connection...')
            self._current_connection = oracledb.connect(
                user=self.oracle_user,
                password=self.oracle_pass,
                host=self.oracle_host,
                port=self.oracle_port,
                service_name=self.oracle_service,
            )
            return self._current_connection
        except oracledb.Error as e:
            logger.error(f'Unable to connect to Oracle: {e}')
            raise
