from __future__ import annotations
import os
import logging
import oracledb
from typing import Any, Iterator, Callable

from oracledb import LOB, Connection, Cursor, DataFrame, DbObjectType, DbType, Queue, DbObject, MessageProperties # ConnectionPool, Subscription
from oracledb.connection import Xid

logger = logging.getLogger(__name__)

class OracleClient:
    _oracle_user: str
    _oracle_pass: str
    _oracle_host: str
    _oracle_port: int
    _oracle_service: str
    _current_connection: Connection | None

    def __slot__(self):
        return (
            "_oracle_user", "_oracle_pass",
            "_oracle_host", "_oracle_port",
            "_oracle_service", "_current_connection"
        )
    def __frozen__(self):
        return True
    
    def __init__(self,
        oracle_user: str = os.getenv('ORACLE_USER') or '',
        oracle_pass: str = os.getenv('ORACLE_PASS') or '',
        oracle_host: str = os.getenv('ORACLE_HOST') or '',
        oracle_port: int = int(p) if (p := os.getenv('ORACLE_PORT')) else 0,
        oracle_service: str = os.getenv('ORACLE_SERVICE') or ''
    ) -> None:
        self._oracle_user = oracle_user
        self._oracle_pass = oracle_pass
        self._oracle_host = oracle_host
        self._oracle_port = oracle_port
        self._oracle_service = oracle_service
        self._current_connection = None
    
    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"user={self._oracle_user!r}, "
            f"host={self._oracle_host!r}, "
            f"port={self._oracle_port!r}, "
            f"service={self._oracle_service!r})"
        )
    def _connect(self) -> None:
        try:
            self._current_connection = oracledb.connect(
                user=self._oracle_user,
                password=self._oracle_pass,
                host=self._oracle_host,
                port=self._oracle_port,
                service_name=self._oracle_service,
            )
            self._current_connection.autocommit = False
        except oracledb.Error as e:
            logger.error(f'Error connecting to Oracle: {self.__repr__()}\\nError: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during Oracle connection: {self.__repr__()}\\nError: {e}')
            raise
    
    def connect(self) -> oracledb.Connection:
        if self._current_connection is not None and self._current_connection.is_healthy(): return self._current_connection
        self._connect()
        return self._current_connection
    
    def __call__(self):
        return self.connect()
    
    def close(self) -> None:
        if self._current_connection is None:
            return
        try:
            self._current_connection.close()
        except oracledb.Error as e:
            logger.error(f'Error closing Oracle connection: {self.__repr__()}\\nError: {e}')
            raise
        except Exception as e:
            logger.error(f'Unexpected error during Oracle connection close: {self.__repr__()}\\nError: {e}')
            raise
    
    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logger.warning(f'Error during OracleClient cleanup: {self.__repr__()}\\nError: {e}')
    
    def cursor(self, scrollable: bool = False) -> Cursor:
        return Cursor(self.connect(), scrollable)

    def direct_path_load(
        self,
        schema_name: str,
        table_name: str,
        column_names: list[str],
        data: Any,
        *,
        batch_size: int = 2**32 - 1,
    ) -> None:
        self.direct_path_load(
            schema_name, table_name, column_names, data, batch_size = batch_size
        )
    
    def fetch_df_all(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        arraysize: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> DataFrame:
        return self.fetch_df_all(
            statement,
            parameters,
            arraysize=arraysize,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    def fetch_df_batches(
        self,
        statement: str,
        parameters: list | tuple | dict | None = None,
        size: int | None = None,
        *,
        fetch_decimals: bool | None = None,
        requested_schema: Any | None = None,
    ) -> Iterator[DataFrame]:
        return self.fetch_df_batches(
            statement,
            parameters,
            size=size,
            fetch_decimals=fetch_decimals,
            requested_schema=requested_schema,
        )

    @property
    def auto_commit(self) -> bool:
        return self.connect().autocommit

    @auto_commit.setter
    def auto_commit(self, auto_commit: bool) -> None:
        self.connect().autocommit = auto_commit
    
    @property
    def is_healthy(self) -> bool:
        return self._current_connection is not None and self._current_connection.is_healthy()

    @property
    def current_schema(self) -> str:
        return self.current_schema
    
    @current_schema.setter
    def current_schema(self, schema_name: str) -> None:
        self.current_schema = schema_name
    
    @property
    def max_open_cursors(self) -> int:
        return self.max_open_cursors
    
    @property
    def session_id(self) -> int:
        return self.session_id
    
    @property
    def is_thin(self) -> bool:
        return self.connect().thin
    
    @property
    def username(self) -> str:
        return self.username
    
    @property
    def version(self) -> str:
        return self.version
    
    @property
    def ltxid(self) -> bytes:
        return self.ltxid
    
    @property
    def dsn(self) -> str:
        return self.dsn
    
    @property
    def internal_name(self) -> str:
        return self.internal_name

    @internal_name.setter
    def internal_name(self, value: str) -> None:
        self.internal_name = value

    @property
    def inputtypehandler(self) -> Callable:
        return self.inputtypehandler
    
    @inputtypehandler.setter
    def inputtypehandler(self, value: Callable) -> None:
        self.inputtypehandler = value

    @property
    def external_name(self) -> str:
        return self.external_name

    @external_name.setter
    def external_name(self, value: str) -> None:
        self.external_name = value
    
    @property
    def edition(self) -> str:
        return self.edition
    
    @property
    def econtext_id(self) -> str:
        return self.econtext_id
    
    @econtext_id.setter
    def econtext_id(self, value: str) -> None:
        self.econtext_id = value

    @property
    def db_name(self) -> str:
        return self.db_name

    @property
    def db_domain(self) -> str:
        return self.db_domain
    
    @property
    def client_identifier(self) -> str:
        return self.client_identifier
    
    @client_identifier.setter
    def client_identifier(self, value: str) -> None:
        self.client_identifier = value

    @property
    def call_timeout(self) -> int:
        return self.call_timeout
    
    @call_timeout.setter
    def call_timeout(self, value: int) -> None:
        self.call_timeout = value

    def cancel(self) -> None:
        self.cancel()

    def dbop(self, value: str) -> None:
        self.dbop(value)
    
    def action(self, value: str) -> None:
        self.action(value)

    def gettype(self, name: str) -> DbObjectType:
        return self.gettype(name)
    
    def ping(self) -> None:
        """Throws an exception if the connection is not healthy. Otherwise, returns None."""
        self.ping()

    def commit(self) -> None:
        """Commits the current transaction."""
        self.commit()

    def rollback(self) -> None:
        """Rolls back the current transaction."""
        self.rollback()
    
    def shutdown(self, mode: int = 0) -> None:
        """Shuts down the database."""
        self.shutdown(mode)

    def startup(
        self,
        force: bool = False,
        restrict: bool = False,
        pfile: str | None = None,
    ) -> None:
        """Starts up the database."""
        self.startup(force, restrict, pfile)
    
    def createlob(
        self, lob_type: DbType, data: str | bytes | None = None
    ) -> LOB:
        return self.createlob(lob_type, data)

    def encode_oson(self, value: Any) -> bytes:
        return self.encode_oson(value)
    
    def decode_oson(self, oson_value: bytes) -> Any:
        return self.decode_oson(oson_value)

    def msgproperties(
        self,
        payload: bytes | str | DbObject | None = None,
        correlation: str | None = None,
        delay: int | None = None,
        exceptionq: str | None = None,
        expiration: int | None = None,
        priority: int | None = None,
        recipients: list | None = None,
    ) -> MessageProperties:
        return self.msgproperties(
            payload,
            correlation,
            delay,
            exceptionq,
            expiration,
            priority,
            recipients,
        )

    def queue(
        self,
        name: str,
        payload_type: DbObjectType | str | None = None,
    ) -> Queue:
        return self.queue(name, payload_type)

    def subscribe(
        self,
        namespace: int = oracledb.SUBSCR_NAMESPACE_DBCHANGE,
        protocol: int = oracledb.SUBSCR_PROTO_CALLBACK,
        callback: Callable | None = None,
        timeout: int = 0,
        operations: int = oracledb.OPCODE_ALLOPS,
        port: int = 0,
        qos: int = oracledb.SUBSCR_QOS_DEFAULT,
        ip_address: str | None = None,
        grouping_class: int = oracledb.SUBSCR_GROUPING_CLASS_NONE,
        grouping_value: int = 0,
        grouping_type: int = oracledb.SUBSCR_GROUPING_TYPE_SUMMARY,
        name: str | None = None,
        client_initiated: bool = False,
        
    ) -> oracledb.Subscription:
        return self.subscribe(
            namespace,
            protocol,
            callback,
            timeout,
            operations,
            port,
            qos,
            ip_address,
            grouping_class,
            grouping_value,
            grouping_type,
            name,
            client_initiated
        )
    
    
    # Two-Phase Commit (TPC) Transactions & Global Transactions
    
    def begin(
        self,
        format_id: int = -1,
        transaction_id: str = "",
        branch_id: str = "",
    ) -> None:
        self.begin(format_id, transaction_id, branch_id)
    
    def xid(
        self,
        format_id: int,
        global_transaction_id: bytes | str,
        branch_qualifier: bytes | str,
    ) -> Xid:
        return self.xid(format_id, global_transaction_id, branch_qualifier)
    
    def prepare(self) -> bool:
        return self.prepare()
    
    def tpc_begin(
        self, xid: Xid, flags: int = oracledb.TPC_BEGIN_NEW, timeout: int = 0
    ) -> None:
        """Begins a Two-Phase Commit (TPC) on a global transaction using Xid"""
        self.tpc_begin(xid, flags, timeout)

    def tpc_commit(
        self, xid: Xid | None = None, one_phase: bool = False
    ) -> None:
        self.tpc_commit(xid, one_phase)

    def tpc_end(
        self, xid: Xid | None = None, flags: int = oracledb.TPC_END_NORMAL
    ) -> None:
        self.tpc_end(xid, flags)
    
    def tpc_forget(self, xid: Xid) -> None:
        self.tpc_forget(xid)

    def tpc_prepare(self, xid: Xid | None = None) -> bool:
        return self.tpc_prepare(xid)
    
    def tpc_recover(self) -> list:
        return self.tpc_recover()
    
    
    
    # Sessionless Transactions (Oracle 23c+)

    def begin_sessionless_transaction(
        self,
        transaction_id: str | bytes | None = None,
        timeout: int = 60,
        defer_round_trip: bool = False,
    ) -> bytes:
        return self.begin_sessionless_transaction(
            transaction_id, timeout, defer_round_trip
        )
        
    def suspend_sessionless_transaction(self) -> None:
        self.suspend_sessionless_transaction()

    def resume_sessionless_transaction(self, transaction_id: str | bytes) -> None:
        self.resume_sessionless_transaction(transaction_id)



    # Legacy methods

    def get_con(self) -> oracledb.Connection:
        """Legacy get connection method, kept for backward compatibility. Use connect()"""
        return self.connect()
