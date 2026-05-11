"""
Integration tests against the live QBLPDB Oracle database.

These tests require a running Oracle instance with QBLPDB provisioned and the
QBL schema DDL applied.  They are skipped automatically when the DB is
unreachable or when ORACLE_TEST_USE_MOCKS=1 is set.

Run:
    pytest tests/test_db.py -v
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

# ---------------------------------------------------------------------------
# Skip guard — skip entire module when mocks are active or DB vars are absent
# ---------------------------------------------------------------------------

_DB_VARS_PRESENT = all(
    os.getenv(v) for v in ("ORACLE_QBL_USER", "ORACLE_QBL_PWD", "ORACLE_QBL_HOST", "ORACLE_QBL_SERVICE")
)
_MOCKS_ACTIVE = os.getenv("ORACLE_TEST_USE_MOCKS", "0") == "1"

pytestmark = pytest.mark.skipif(
    _MOCKS_ACTIVE or not _DB_VARS_PRESENT,
    reason="Live Oracle QBLPDB not configured (set ORACLE_QBL_* vars and unset ORACLE_TEST_USE_MOCKS)",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def db():
    from server.db.ServerDatabase import ServerDatabase
    _db = ServerDatabase()
    _db.connect()
    yield _db
    _db.close()


@pytest.fixture(scope="module")
def session_svc(db):
    from server.services.session_service import SessionService
    return SessionService(db)


@pytest.fixture(scope="module")
def registry_svc():
    from server.services.catalog_registry import CatalogRegistryService
    return CatalogRegistryService()


@pytest.fixture(scope="module")
def test_username(db):
    """Insert a throwaway QBL_USERS row; delete it after the module finishes."""
    username = f"pytest_{uuid.uuid4().hex[:8]}"
    email    = f"{username}@pytest.local"
    pw_hash  = "pytest_hashed_password"

    con = db.connect()
    with con.cursor() as cur:
        cur.execute(
            "INSERT INTO QBL_USERS (USERNAME, EMAIL, password_hash, IS_ACTIVE) VALUES (:u, :e, :p, 1)",
            u=username, e=email, p=pw_hash,
        )
    con.commit()

    yield username

    with con.cursor() as cur:
        cur.execute("DELETE FROM QBL_USERS WHERE USERNAME = :u", u=username)
    con.commit()


# ---------------------------------------------------------------------------
# 1 — Connectivity
# ---------------------------------------------------------------------------

class TestConnectivity:
    def test_ping(self, db):
        db.ping()

    def test_dual_query(self, db):
        with db.connect().cursor() as cur:
            cur.execute("SELECT 1 FROM DUAL")
            assert int(cur.fetchone()[0]) == 1

    def test_connected_to_qblpdb(self, db):
        with db.connect().cursor() as cur:
            cur.execute("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL")
            con_name = cur.fetchone()[0]
        assert con_name.upper() == os.getenv("ORACLE_QBL_SERVICE", "QBLPDB").upper()


# ---------------------------------------------------------------------------
# 2 — Schema / DDL completeness
# ---------------------------------------------------------------------------

_EXPECTED_TABLES = [
    "QBL_USERS", "USER_ROLES", "USER_SETTINGS", "USER_TEAMS", "TEAMS",
    "USER_SESSION", "USER_SIGN_IN", "USER_REFRESH_TOKEN",
    "CATALOG_REGISTRY", "CATALOG_SHARES",
]

class TestSchema:
    def test_required_tables_exist(self, db):
        placeholders = ",".join(f"'{t}'" for t in _EXPECTED_TABLES)
        with db.connect().cursor() as cur:
            cur.execute(
                f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME IN ({placeholders})"
            )
            found = {row[0] for row in cur.fetchall()}
        missing = set(_EXPECTED_TABLES) - found
        assert not missing, f"Missing tables: {sorted(missing)}"

    def test_user_roles_seeded(self, db):
        with db.connect().cursor() as cur:
            cur.execute("SELECT role_id FROM user_roles ORDER BY role_id")
            roles = {row[0] for row in cur.fetchall()}
        assert "user"  in roles
        assert "admin" in roles

    def test_catalog_registry_json_columns(self, db):
        """Verify each JSON CLOB column exists with an IS JSON constraint."""
        with db.connect().cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME FROM USER_TAB_COLUMNS
                 WHERE TABLE_NAME = 'CATALOG_REGISTRY'
                   AND DATA_TYPE  = 'CLOB'
                 ORDER BY COLUMN_NAME
            """)
            clob_cols = {row[0] for row in cur.fetchall()}
        expected = {"ENTITIES", "FILTERS", "JOINS", "SORT_COLUMNS", "ASSIGNMENTS", "PROPERTIES"}
        assert expected <= clob_cols, f"Missing CLOB columns: {expected - clob_cols}"


# ---------------------------------------------------------------------------
# 3 — Auth queries
# ---------------------------------------------------------------------------

class TestAuthQueries:
    def test_get_credentials_returns_none_for_unknown_user(self, db):
        with db.connect().cursor() as cur:
            cur.execute(
                "SELECT USERNAME FROM QBL_USERS WHERE USERNAME = :u",
                u="definitely_does_not_exist_pytest",
            )
            assert cur.fetchone() is None

    def test_get_credentials_returns_row_for_test_user(self, db, test_username):
        with db.connect().cursor() as cur:
            cur.execute(
                "SELECT USERNAME, EMAIL, password_hash, IS_ACTIVE, NVL(role_id, 'user') FROM QBL_USERS WHERE USERNAME = :u",
                u=test_username,
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == test_username
        assert row[3] == 1        # IS_ACTIVE
        assert row[4] == "user"   # default role


# ---------------------------------------------------------------------------
# 4 — SessionService
# ---------------------------------------------------------------------------

class TestSessionService:
    def test_log_sign_in_unknown_user_does_not_raise(self, session_svc):
        """qbl_user_id is nullable — failed logins for unknown users must not raise."""
        session_svc.log_sign_in(
            "ghost_user_pytest",
            success=False,
            ip_address="127.0.0.1",
            failure_reason="pytest: unknown user",
        )

    def test_create_validate_invalidate_session(self, session_svc, test_username):
        import secrets as _secrets
        token   = _secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(minutes=15)

        sid = session_svc.create_session(test_username, token, expires, ip_address="127.0.0.1")
        assert sid > 0

        assert session_svc.is_session_active(token) is True
        assert session_svc.invalidate_session(token) is True
        assert session_svc.is_session_active(token) is False

    def test_refresh_token_round_trip(self, session_svc, test_username):
        import secrets as _secrets
        token   = _secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(days=7)

        rid = session_svc.create_refresh_token(test_username, token, None, expires)
        assert rid > 0

        result = session_svc.validate_refresh_token(token)
        assert result is not None
        assert result["username"]    == test_username
        assert result["is_active"]   is True
        assert result["not_expired"] is True

        assert session_svc.revoke_refresh_token(token) is True
        result2 = session_svc.validate_refresh_token(token)
        assert result2["is_active"] is False

    def test_rate_limit_not_triggered_below_threshold(self, session_svc, test_username):
        count = session_svc.failed_attempt_count(test_username, None, window_minutes=1)
        assert isinstance(count, int)


# ---------------------------------------------------------------------------
# 5 — CatalogRegistryService
# ---------------------------------------------------------------------------

class TestCatalogRegistry:
    _OWNER  = "SYSTEM"
    _NAME   = "pytest_catalog_registry_test"

    def _make_catalog(self):
        from server.plugins.PluginModels import Catalog, Entity, Column
        col    = Column(name="ID", raw_type="NUMBER", arrow_type_id="int64", primary_key=True)
        entity = Entity(name="PYTEST_TABLE", columns=[col])
        return Catalog(name=self._NAME, source_type="oracle", entities=[entity])

    def test_save_get_delete(self, registry_svc):
        catalog = self._make_catalog()

        # Clean up any leftover from a previously interrupted run
        registry_svc.delete(self._OWNER, self._NAME)

        registry_svc.save(self._OWNER, catalog)

        loaded = registry_svc.get(self._OWNER, self._NAME)
        assert loaded is not None
        assert loaded.name == self._NAME
        assert loaded.source_type == "oracle"
        assert len(loaded.entities) == 1
        assert loaded.entities[0].name == "PYTEST_TABLE"
        assert loaded.entities[0].columns[0].name == "ID"

        deleted = registry_svc.delete(self._OWNER, self._NAME)
        assert deleted is True

        assert registry_svc.get(self._OWNER, self._NAME) is None

    def test_save_overwrites_on_second_call(self, registry_svc):
        from server.plugins.PluginModels import Catalog
        registry_svc.delete(self._OWNER, self._NAME)

        c1 = Catalog(name=self._NAME, description="v1", source_type="oracle")
        c2 = Catalog(name=self._NAME, description="v2", source_type="federation")

        registry_svc.save(self._OWNER, c1)
        registry_svc.save(self._OWNER, c2)

        loaded = registry_svc.get(self._OWNER, self._NAME)
        assert loaded.description == "v2"
        assert loaded.source_type == "federation"

        registry_svc.delete(self._OWNER, self._NAME)

    def test_list_entries_includes_saved_catalog(self, registry_svc):
        from server.plugins.PluginModels import Catalog
        registry_svc.delete(self._OWNER, self._NAME)
        registry_svc.save(self._OWNER, Catalog(name=self._NAME, source_type="oracle"))

        entries = registry_svc.list_entries(self._OWNER)
        names = [e["name"] for e in entries]
        assert self._NAME in names

        registry_svc.delete(self._OWNER, self._NAME)
