"""
SessionService - manages USER_SESSION, USER_SIGN_IN, and USER_REFRESH_TOKEN.

All session tables reference QBL_USERS.qbl_user_id (numeric FK) rather than the
raw username string.  Public methods still accept/return username strings; the
service resolves IDs internally via _get_user_id().

Audit column names are created_at / updated_at throughout (matching b_qbl_tables.sql).
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from server.db.ServerDatabase import ServerDatabase
from server.plugins.PluginModels import Catalog, Entity

logger = logging.getLogger(__name__)


def _hash_token(token: str) -> str:
    """SHA-256 hex digest of a raw JWT — what we store in the DB."""
    return hashlib.sha256(token.encode()).hexdigest()


class SessionService:

    _server_db: ServerDatabase

    def __init__(self, server_db: ServerDatabase):
        self._server_db = server_db

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_user_id(self, username: str) -> int | None:
        """Return qbl_user_id for *username*, or None if no such user exists."""
        sql = "SELECT qbl_user_id FROM QBL_USERS WHERE USERNAME = :username"
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, username=username)
            row = cur.fetchone()
        return int(row[0]) if row else None

    # ------------------------------------------------------------------
    # Catalog session bootstrap
    # ------------------------------------------------------------------

    def load_session(self, username: str | None = None) -> Catalog:
        """
        Read SYSTEM-owned catalog entries plus, when *username* is provided,
        that user's own entries. Assembles entities from CATALOG_ENTITIES rows —
        one row per Entity, no full-catalog JSON parsing.
        """
        if username:
            sql = """
                SELECT ce.entity_json
                  FROM CATALOG_REGISTRY cr
                  JOIN CATALOG_ENTITIES ce
                    ON ce.catalog_registry_id = cr.catalog_registry_id
                 WHERE cr.scope = 'SYSTEM'
                    OR (cr.scope = 'USER'
                        AND cr.owner_user_id = (
                            SELECT qbl_user_id FROM QBL_USERS WHERE USERNAME = :u))
            """
            params: dict = {"u": username}
        else:
            sql = """
                SELECT ce.entity_json
                  FROM CATALOG_REGISTRY cr
                  JOIN CATALOG_ENTITIES ce
                    ON ce.catalog_registry_id = cr.catalog_registry_id
                 WHERE cr.scope = 'SYSTEM'
            """
            params = {}
        entities: list[Entity] = []
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, **params)
            for (json_val,) in cur:
                if json_val is None:
                    continue
                json_str: str = json_val.read() if hasattr(json_val, "read") else json_val
                try:
                    entities.append(Entity.model_validate_json(json_str))
                except Exception:
                    logger.warning("Skipping malformed entity row in CATALOG_ENTITIES")
        return Catalog(entities=entities)

    def list_systems(self) -> list[str]:
        """Return sorted distinct source_type values for SYSTEM-owned catalogs.
        Reads the indexed scalar column — no JSON parsing required.
        """
        sql = """
            SELECT DISTINCT source_type
              FROM CATALOG_REGISTRY
             WHERE scope        = 'SYSTEM'
               AND source_type IS NOT NULL
               AND source_type != 'federation'
        """
        plugins: set[str] = set()
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql)
            for (source_type,) in cur:
                plugins.add(source_type)
        return sorted(plugins)

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    def create_session(
        self,
        username: str,
        token: str,
        expires_at: datetime,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> int:
        """Persist a new session row and return the generated USER_SESSION_ID."""
        import oracledb
        user_id = self._get_user_id(username)
        if user_id is None:
            raise ValueError(f"Cannot create session: user '{username}' not found in QBL_USERS")
        token_hash = _hash_token(token)
        sql = """
            INSERT INTO USER_SESSION
                (qbl_user_id, TOKEN_HASH, ISSUED_AT, EXPIRES_AT, IS_ACTIVE,
                 IP_ADDRESS, USER_AGENT, SESSION_DATA,
                 created_at, created_by, updated_at, updated_by)
            VALUES
                (:user_id, :token_hash, CURRENT_TIMESTAMP, :expires_at, 1,
                 :ip_address, :user_agent, NULL,
                 SYSTIMESTAMP, :username, SYSTIMESTAMP, :username)
            RETURNING USER_SESSION_ID INTO :session_id
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            session_id_var = cur.var(oracledb.NUMBER)
            cur.execute(
                sql,
                user_id=user_id,
                token_hash=token_hash,
                expires_at=expires_at,
                ip_address=ip_address,
                user_agent=user_agent,
                username=username,
                session_id=session_id_var,
            )
            con.commit()
        raw = session_id_var.getvalue()
        return int(raw[0]) if raw else -1

    def invalidate_session(self, token: str) -> bool:
        """Mark the session for *token* as inactive."""
        token_hash = _hash_token(token)
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE TOKEN_HASH = :th
               AND IS_ACTIVE  = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, th=token_hash)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def invalidate_all_sessions(self, username: str) -> int:
        """Invalidate every active session for *username*. Returns count of rows updated."""
        user_id = self._get_user_id(username)
        if user_id is None:
            return 0
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE qbl_user_id = :user_id
               AND IS_ACTIVE   = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, user_id=user_id)
            count = cur.rowcount
            con.commit()
        return count

    def list_active_sessions(self, username: str) -> list[dict]:
        """Return all non-expired, active sessions for *username*, newest first."""
        user_id = self._get_user_id(username)
        if user_id is None:
            return []
        sql = """
            SELECT USER_SESSION_ID, IP_ADDRESS, USER_AGENT, ISSUED_AT, EXPIRES_AT
              FROM USER_SESSION
             WHERE qbl_user_id = :user_id
               AND IS_ACTIVE   = 1
               AND EXPIRES_AT  > CURRENT_TIMESTAMP
             ORDER BY ISSUED_AT DESC
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, user_id=user_id)
            rows = cur.fetchall()
        return [
            {
                "session_id": int(r[0]),
                "ip_address": r[1],
                "user_agent": r[2],
                "issued_at":  str(r[3]),
                "expires_at": str(r[4]),
            }
            for r in rows
        ]

    def revoke_session_by_id(self, session_id: int, username: str) -> bool:
        """Deactivate a specific session, verifying ownership via *username*."""
        user_id = self._get_user_id(username)
        if user_id is None:
            return False
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE USER_SESSION_ID = :sid
               AND qbl_user_id     = :user_id
               AND IS_ACTIVE       = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, sid=session_id, user_id=user_id)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def is_session_active(self, token: str) -> bool:
        """Return True if the session exists, is active, and has not expired."""
        token_hash = _hash_token(token)
        sql = """
            SELECT IS_ACTIVE,
                   CASE WHEN EXPIRES_AT > CURRENT_TIMESTAMP THEN 1 ELSE 0 END
              FROM USER_SESSION
             WHERE TOKEN_HASH = :th
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, th=token_hash)
            row = cur.fetchone()
        if row is None:
            return False
        is_active, not_expired = row
        if is_active and not not_expired:
            self._deactivate_by_hash(token_hash)
            return False
        return bool(is_active and not_expired)

    def _deactivate_by_hash(self, token_hash: str) -> None:
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE TOKEN_HASH = :th
               AND IS_ACTIVE  = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, th=token_hash)
        con.commit()

    def deactivate_expired_sessions(self) -> int:
        """Bulk-deactivate all sessions whose EXPIRES_AT has passed. Returns row count."""
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE IS_ACTIVE  = 1
               AND EXPIRES_AT <= CURRENT_TIMESTAMP
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        con.commit()
        logger.info("deactivate_expired_sessions: deactivated %d expired session(s)", count)
        return count

    def get_session(self, token: str) -> dict[str, Any] | None:
        """Return the full session row as a dict (with username via join), or None."""
        token_hash = _hash_token(token)
        sql = """
            SELECT s.USER_SESSION_ID, u.USERNAME, s.ISSUED_AT, s.EXPIRES_AT,
                   s.IS_ACTIVE, s.IP_ADDRESS, s.USER_AGENT, s.SESSION_DATA,
                   s.created_at, s.updated_at
              FROM USER_SESSION s
              JOIN QBL_USERS    u ON u.qbl_user_id = s.qbl_user_id
             WHERE s.TOKEN_HASH = :th
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, th=token_hash)
            row = cur.fetchone()
        if row is None:
            return None
        keys = [
            "user_session_id", "username", "issued_at", "expires_at",
            "is_active", "ip_address", "user_agent", "session_data",
            "created_at", "updated_at",
        ]
        result = dict(zip(keys, row))
        if result["session_data"] and hasattr(result["session_data"], "read"):
            result["session_data"] = result["session_data"].read()
        return result

    def get_session_data(self, token: str) -> dict[str, Any]:
        session = self.get_session(token)
        if not session or not session.get("session_data"):
            return {}
        try:
            return json.loads(session["session_data"])
        except (json.JSONDecodeError, TypeError):
            return {}

    def set_session_data(self, token: str, data: dict[str, Any]) -> bool:
        import oracledb
        token_hash = _hash_token(token)
        sql = """
            UPDATE USER_SESSION
               SET SESSION_DATA = :data,
                   updated_at   = SYSTIMESTAMP,
                   updated_by   = 'SYSTEM'
             WHERE TOKEN_HASH   = :th
               AND IS_ACTIVE    = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.setinputsizes(data=oracledb.DB_TYPE_CLOB)
            cur.execute(sql, data=json.dumps(data), th=token_hash)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def merge_session_data(self, token: str, updates: dict[str, Any]) -> dict[str, Any]:
        current = self.get_session_data(token)
        current.update(updates)
        self.set_session_data(token, current)
        return current

    # ------------------------------------------------------------------
    # Sign-in audit
    # ------------------------------------------------------------------

    def log_sign_in(
        self,
        username: str,
        success: bool,
        ip_address: str | None = None,
        user_agent: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        """Append one row to USER_SIGN_IN.

        qbl_user_id is nullable — failed attempts for unknown usernames are
        recorded with qbl_user_id=NULL and the raw input in attempted_username.
        """
        user_id = self._get_user_id(username)
        sql = """
            INSERT INTO USER_SIGN_IN
                (qbl_user_id, attempted_username, IP_ADDRESS, USER_AGENT,
                 SUCCESS, FAILURE_REASON,
                 created_at, created_by, updated_at, updated_by)
            VALUES
                (:user_id, :username, :ip, :ua, :success, :reason,
                 SYSTIMESTAMP, 'SYSTEM', SYSTIMESTAMP, 'SYSTEM')
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(
                sql,
                user_id=user_id,
                username=username,
                ip=ip_address,
                ua=user_agent,
                success=1 if success else 0,
                reason=failure_reason,
            )
            con.commit()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def is_rate_limited(
        self,
        username: str,
        ip_address: str | None,
        window_minutes: int = 15,
        max_failures: int = 5,
    ) -> bool:
        """
        Return True if failed sign-in attempts for *username* OR *ip_address*
        within the last *window_minutes* minutes meet or exceed *max_failures*.
        """
        user_id = self._get_user_id(username)
        sql = """
            SELECT COUNT(*) FROM USER_SIGN_IN
             WHERE SUCCESS = 0
               AND created_at > (CURRENT_TIMESTAMP - NUMTODSINTERVAL(:window, 'MINUTE'))
               AND (:user_id IS NULL OR qbl_user_id       = :user_id)
               AND (:ip       IS NULL OR IP_ADDRESS        = :ip)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, window=window_minutes, user_id=user_id, ip=ip_address)
            row = cur.fetchone()
        count = row[0] if row else 0
        return count >= max_failures

    def failed_attempt_count(
        self,
        username: str,
        ip_address: str | None,
        window_minutes: int = 15,
    ) -> int:
        """Return the raw count of failed attempts in the window."""
        user_id = self._get_user_id(username)
        sql = """
            SELECT COUNT(*) FROM USER_SIGN_IN
             WHERE SUCCESS = 0
               AND created_at > (CURRENT_TIMESTAMP - NUMTODSINTERVAL(:window, 'MINUTE'))
               AND (:user_id IS NULL OR qbl_user_id = :user_id)
               AND (:ip       IS NULL OR IP_ADDRESS  = :ip)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, window=window_minutes, user_id=user_id, ip=ip_address)
            row = cur.fetchone()
        return row[0] if row else 0

    # ------------------------------------------------------------------
    # Refresh tokens
    # ------------------------------------------------------------------

    def create_refresh_token(
        self,
        username: str,
        token: str,
        session_id: int | None,
        expires_at: datetime,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> int:
        """Persist a new refresh-token row and return the generated ID."""
        import oracledb
        user_id = self._get_user_id(username)
        if user_id is None:
            raise ValueError(f"Cannot create refresh token: user '{username}' not found in QBL_USERS")
        token_hash = _hash_token(token)
        sql = """
            INSERT INTO USER_REFRESH_TOKEN
                (qbl_user_id, TOKEN_HASH, PARENT_SESSION_ID, ISSUED_AT, EXPIRES_AT, IS_ACTIVE,
                 IP_ADDRESS, USER_AGENT,
                 created_at, created_by, updated_at, updated_by)
            VALUES
                (:user_id, :token_hash, :session_id, CURRENT_TIMESTAMP, :expires_at, 1,
                 :ip_address, :user_agent,
                 SYSTIMESTAMP, :username, SYSTIMESTAMP, :username)
            RETURNING USER_REFRESH_TOKEN_ID INTO :token_id
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            token_id_var = cur.var(oracledb.NUMBER)
            cur.execute(
                sql,
                user_id=user_id,
                token_hash=token_hash,
                session_id=session_id,
                expires_at=expires_at,
                ip_address=ip_address,
                user_agent=user_agent,
                username=username,
                token_id=token_id_var,
            )
            con.commit()
        raw = token_id_var.getvalue()
        return int(raw[0]) if raw else -1

    def validate_refresh_token(self, token: str) -> dict | None:
        """
        Return a dict with username/is_active/not_expired, or None if the token
        does not exist in USER_REFRESH_TOKEN.
        """
        token_hash = _hash_token(token)
        sql = """
            SELECT u.USERNAME, rt.IS_ACTIVE,
                   CASE WHEN rt.EXPIRES_AT > CURRENT_TIMESTAMP THEN 1 ELSE 0 END
              FROM USER_REFRESH_TOKEN rt
              JOIN QBL_USERS          u  ON u.qbl_user_id = rt.qbl_user_id
             WHERE rt.TOKEN_HASH = :th
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, th=token_hash)
            row = cur.fetchone()
        if row is None:
            return None
        username, is_active, not_expired = row
        return {
            "username":    username,
            "is_active":   bool(is_active),
            "not_expired": bool(not_expired),
        }

    def rotate_refresh_token(
        self,
        old_token: str,
        username: str,
        new_token: str,
        expires_at: datetime,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> int:
        """Invalidate *old_token* and persist *new_token*. Returns new row ID."""
        self.revoke_refresh_token(old_token)
        return self.create_refresh_token(username, new_token, None, expires_at, ip_address, user_agent)

    def revoke_refresh_token(self, token: str) -> bool:
        """Deactivate a single refresh token. Returns True if a row was updated."""
        token_hash = _hash_token(token)
        sql = """
            UPDATE USER_REFRESH_TOKEN
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE TOKEN_HASH = :th
               AND IS_ACTIVE  = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, th=token_hash)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def revoke_all_refresh_tokens(self, username: str) -> int:
        """Deactivate every active refresh token for *username*. Returns count updated."""
        user_id = self._get_user_id(username)
        if user_id is None:
            return 0
        sql = """
            UPDATE USER_REFRESH_TOKEN
               SET IS_ACTIVE  = 0,
                   updated_at = SYSTIMESTAMP,
                   updated_by = 'SYSTEM'
             WHERE qbl_user_id = :user_id
               AND IS_ACTIVE   = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, user_id=user_id)
            count = cur.rowcount
            con.commit()
        return count
