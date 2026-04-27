"""
SessionService - manages USER_SESSION and USER_SIGN_IN records via OracleClient.

Responsibilities:
  - Create / invalidate / look-up JWT sessions
  - Log every sign-in attempt (success or failure)
  - Rate-limit check: count failed attempts per username + IP within a rolling window
  - Read / update the per-session JSON scratchpad (session_data)
  - Load the unified Catalog from CATALOG_REGISTRY for session bootstrap
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
    """SHA-256 hex digest of a raw JWT - what we store in the DB."""
    return hashlib.sha256(token.encode()).hexdigest()


class SessionService:

    _server_db: ServerDatabase

    def __init__(self, server_db: ServerDatabase):
        self._server_db = server_db

    # ------------------------------------------------------------------
    # Catalog session bootstrap
    # ------------------------------------------------------------------

    def load_session(self, username: str | None = None) -> Catalog:
        """
        Read SYSTEM-owned entries from CATALOG_REGISTRY (shared schema) plus,
        when *username* is provided, that user's own uploaded-file entries.
        Returns an empty Catalog if no rows exist yet.
        """
        if username:
            sql = "SELECT CATALOG_JSON FROM CATALOG_REGISTRY WHERE OWNER = 'SYSTEM' OR OWNER = :u"
            params: dict = {"u": username}
        else:
            sql = "SELECT CATALOG_JSON FROM CATALOG_REGISTRY WHERE OWNER = 'SYSTEM'"
            params = {}
        entities: list[Entity] = []
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, **params)
            for (json_val,) in cur:
                if json_val is None:
                    continue
                json_str: str = json_val.read() if hasattr(json_val, "read") else json_val
                try:
                    sub = Catalog.model_validate_json(json_str)
                    entities.extend(sub.entities)
                except Exception:
                    logger.warning("Skipping malformed catalog row in CATALOG_REGISTRY")
        return Catalog(entities=entities)

    def list_systems(self) -> list[str]:
        """Return sorted distinct plugin names for all SYSTEM-owned entries in CATALOG_REGISTRY.
        Plugin name is derived from the stored Catalog's source_type, falling back to the
        first entity's column locator. Never parses REGISTRY_KEY strings.
        """
        sql = "SELECT CATALOG_JSON FROM CATALOG_REGISTRY WHERE OWNER = 'SYSTEM'"
        plugins: set[str] = set()
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql)
            for (json_val,) in cur:
                if json_val is None:
                    continue
                json_str: str = json_val.read() if hasattr(json_val, "read") else json_val
                try:
                    sub = Catalog.model_validate_json(json_str)
                    if sub.source_type and sub.source_type != "federation":
                        plugins.add(sub.source_type)
                    else:
                        for entity in sub.entities:
                            if entity.locator and entity.locator.plugin:
                                plugins.add(entity.locator.plugin)
                                break
                except Exception:
                    logger.warning("Skipping malformed catalog row in list_systems")
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
        """
        Persist a new session row and return the generated USER_SESSION_ID.
        Any previous active sessions for this username are NOT invalidated here
        to support concurrent logins; call invalidate_all_sessions() if needed.
        """
        import oracledb
        token_hash = _hash_token(token)
        sql = """
            INSERT INTO USER_SESSION
                (USERNAME, TOKEN_HASH, ISSUED_AT, EXPIRES_AT, IS_ACTIVE,
                 IP_ADDRESS, USER_AGENT, SESSION_DATA,
                 CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
            VALUES
                (:username, :token_hash, CURRENT_TIMESTAMP, :expires_at, 1,
                 :ip_address, :user_agent, NULL,
                 CURRENT_TIMESTAMP, :username, CURRENT_TIMESTAMP, :username)
            RETURNING USER_SESSION_ID INTO :session_id
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            session_id_var = cur.var(oracledb.NUMBER)
            cur.execute(
                sql,
                username=username,
                token_hash=token_hash,
                expires_at=expires_at,
                ip_address=ip_address,
                user_agent=user_agent,
                session_id=session_id_var,
            )
            con.commit()
        raw = session_id_var.getvalue()
        return int(raw[0]) if raw else -1

    def invalidate_session(self, token: str) -> bool:
        """
        Mark the session for *token* as inactive.
        Returns True if a row was updated, False if not found or already inactive.
        """
        token_hash = _hash_token(token)
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
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
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
             WHERE USERNAME  = :username
               AND IS_ACTIVE = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, username=username)
            count = cur.rowcount
            con.commit()
        return count

    def list_active_sessions(self, username: str) -> list[dict]:
        """Return all non-expired, active sessions for *username*, newest first."""
        sql = """
            SELECT USER_SESSION_ID, IP_ADDRESS, USER_AGENT, ISSUED_AT, EXPIRES_AT
              FROM USER_SESSION
             WHERE USERNAME = :username
               AND IS_ACTIVE = 1
               AND EXPIRES_AT > CURRENT_TIMESTAMP
             ORDER BY ISSUED_AT DESC
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, username=username)
            rows = cur.fetchall()
        return [
            {
                "session_id": int(r[0]),
                "ip_address": r[1],
                "user_agent": r[2],
                "issued_at": str(r[3]),
                "expires_at": str(r[4]),
            }
            for r in rows
        ]

    def revoke_session_by_id(self, session_id: int, username: str) -> bool:
        """Deactivate a specific session, verifying ownership via *username*."""
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE = 0, UPDATED_DATE = CURRENT_TIMESTAMP, UPDATED_BY = 'SYSTEM'
             WHERE USER_SESSION_ID = :sid AND USERNAME = :username AND IS_ACTIVE = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, sid=session_id, username=username)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def is_session_active(self, token: str) -> bool:
        """Return True if the session exists, is marked active, and has not expired.
        If the session is found but has expired, opportunistically deactivates it.
        """
        token_hash = _hash_token(token)
        sql = """
            SELECT IS_ACTIVE, EXPIRES_AT > CURRENT_TIMESTAMP
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
            # Expired but still flagged active - clean it up now
            self._deactivate_by_hash(token_hash)
            return False
        return bool(is_active and not_expired)

    def _deactivate_by_hash(self, token_hash: str) -> None:
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
             WHERE TOKEN_HASH = :th
               AND IS_ACTIVE  = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, th=token_hash)
        con.commit()

    def deactivate_expired_sessions(self) -> int:
        """Bulk-deactivate all sessions whose EXPIRES_AT has passed but IS_ACTIVE is still 1.
        Returns the number of rows updated.
        """
        sql = """
            UPDATE USER_SESSION
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
             WHERE IS_ACTIVE  = 1
               AND EXPIRES_AT <= CURRENT_TIMESTAMP
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
        con.commit()
        logger.info(f"deactivate_expired_sessions: deactivated {count} expired session(s)")
        return count

    def get_session(self, token: str) -> dict[str, Any] | None:
        """Return the full session row as a dict, or None if not found."""
        token_hash = _hash_token(token)
        sql = """
            SELECT USER_SESSION_ID, USERNAME, ISSUED_AT, EXPIRES_AT,
                   IS_ACTIVE, IP_ADDRESS, USER_AGENT, SESSION_DATA,
                   CREATED_DATE, UPDATED_DATE
              FROM USER_SESSION
             WHERE TOKEN_HASH = :th
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, th=token_hash)
            row = cur.fetchone()
        if row is None:
            return None
        keys = [
            "user_session_id", "username", "issued_at", "expires_at",
            "is_active", "ip_address", "user_agent", "session_data",
            "created_date", "updated_date",
        ]
        result = dict(zip(keys, row))
        # Materialise CLOB if present
        if result["session_data"] and hasattr(result["session_data"], "read"):
            result["session_data"] = result["session_data"].read()
        return result

    # Session scratchpad (session_data JSON)
    def get_session_data(self, token: str) -> dict[str, Any]:
        """Return the parsed JSON scratchpad for the session, or {} if empty."""
        session = self.get_session(token)
        if not session or not session.get("session_data"):
            return {}
        try:
            return json.loads(session["session_data"])
        except (json.JSONDecodeError, TypeError):
            return {}

    def set_session_data(self, token: str, data: dict[str, Any]) -> bool:
        """Replace the entire session_data JSON blob. Returns True on success."""
        import oracledb
        token_hash = _hash_token(token)
        sql = """
            UPDATE USER_SESSION
               SET SESSION_DATA  = :data,
                   UPDATED_DATE  = CURRENT_TIMESTAMP,
                   UPDATED_BY    = USERNAME
             WHERE TOKEN_HASH    = :th
               AND IS_ACTIVE     = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.setinputsizes(data=oracledb.DB_TYPE_CLOB)
            cur.execute(sql, data=json.dumps(data), th=token_hash)
            updated = cur.rowcount > 0
            con.commit()
        return updated

    def merge_session_data(self, token: str, updates: dict[str, Any]) -> dict[str, Any]:
        """Shallow-merge *updates* into the existing session_data and persist."""
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
        """Append one row to USER_SIGN_IN."""
        sql = """
            INSERT INTO USER_SIGN_IN
                (USERNAME, IP_ADDRESS, USER_AGENT, SUCCESS, FAILURE_REASON,
                 CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
            VALUES
                (:username, :ip, :ua, :success, :reason,
                 CURRENT_TIMESTAMP, 'SYSTEM', CURRENT_TIMESTAMP, 'SYSTEM')
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(
                sql,
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
        Return True if the number of failed sign-in attempts for *username*
        OR *ip_address* within the last *window_minutes* minutes meets or
        exceeds *max_failures*.

        Either username or ip_address can be None; the query adapts accordingly.
        """
        sql = """
            SELECT COUNT(*) FROM USER_SIGN_IN
             WHERE SUCCESS = 0
               AND CREATED_DATE > (CURRENT_TIMESTAMP - NUMTODSINTERVAL(:window, 'MINUTE'))
               AND (:username IS NULL OR USERNAME   = :username)
               AND (:ip       IS NULL OR IP_ADDRESS = :ip)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, window=window_minutes, username=username, ip=ip_address)
            row = cur.fetchone()
        count = row[0] if row else 0
        return count >= max_failures

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
        token_hash = _hash_token(token)
        sql = """
            INSERT INTO USER_REFRESH_TOKEN
                (USERNAME, TOKEN_HASH, PARENT_SESSION_ID, ISSUED_AT, EXPIRES_AT, IS_ACTIVE,
                 IP_ADDRESS, USER_AGENT,
                 CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY)
            VALUES
                (:username, :token_hash, :session_id, CURRENT_TIMESTAMP, :expires_at, 1,
                 :ip_address, :user_agent,
                 CURRENT_TIMESTAMP, :username, CURRENT_TIMESTAMP, :username)
            RETURNING USER_REFRESH_TOKEN_ID INTO :token_id
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            token_id_var = cur.var(oracledb.NUMBER)
            cur.execute(
                sql,
                username=username,
                token_hash=token_hash,
                session_id=session_id,
                expires_at=expires_at,
                ip_address=ip_address,
                user_agent=user_agent,
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
            SELECT USERNAME, IS_ACTIVE, EXPIRES_AT > CURRENT_TIMESTAMP
              FROM USER_REFRESH_TOKEN
             WHERE TOKEN_HASH = :th
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, th=token_hash)
            row = cur.fetchone()
        if row is None:
            return None
        username, is_active, not_expired = row
        return {
            "username": username,
            "is_active": bool(is_active),
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
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
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
        sql = """
            UPDATE USER_REFRESH_TOKEN
               SET IS_ACTIVE    = 0,
                   UPDATED_DATE = CURRENT_TIMESTAMP,
                   UPDATED_BY   = 'SYSTEM'
             WHERE USERNAME  = :username
               AND IS_ACTIVE = 1
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, username=username)
            count = cur.rowcount
            con.commit()
        return count

    def failed_attempt_count(
        self,
        username: str,
        ip_address: str | None,
        window_minutes: int = 15,
    ) -> int:
        """Return the raw count of failed attempts in the window (useful for headers)."""
        sql = """
            SELECT COUNT(*) FROM USER_SIGN_IN
             WHERE SUCCESS = 0
               AND CREATED_DATE > (CURRENT_TIMESTAMP - NUMTODSINTERVAL(:window, 'MINUTE'))
               AND (:username IS NULL OR USERNAME   = :username)
               AND (:ip       IS NULL OR IP_ADDRESS = :ip)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, window=window_minutes, username=username, ip=ip_address)
            row = cur.fetchone()
        return row[0] if row else 0
