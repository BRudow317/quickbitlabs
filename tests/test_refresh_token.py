"""Unit tests for the refresh-token lifecycle in SessionService.

No live Oracle connection is needed — the ServerDatabase is fully mocked.
"""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from server.services.session_service import SessionService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _svc() -> tuple[SessionService, MagicMock]:
    db = MagicMock()
    return SessionService(db), db


def _cursor(db: MagicMock) -> MagicMock:
    """Return the mock object used as `cur` inside `with db.connect().cursor() as cur`."""
    return db.connect.return_value.cursor.return_value.__enter__.return_value


# ---------------------------------------------------------------------------
# create_refresh_token
# ---------------------------------------------------------------------------

def test_create_refresh_token_returns_id():
    svc, db = _svc()
    cur = _cursor(db)
    cur.var.return_value.getvalue.return_value = [42]

    result = svc.create_refresh_token(
        username="alice",
        token=secrets.token_urlsafe(32),
        session_id=1,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    assert result == 42


def test_create_refresh_token_stores_hash_not_plaintext():
    svc, db = _svc()
    cur = _cursor(db)
    cur.var.return_value.getvalue.return_value = [1]

    raw_token = secrets.token_urlsafe(32)
    expected_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    svc.create_refresh_token(
        username="alice",
        token=raw_token,
        session_id=None,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )

    execute_call = str(cur.execute.call_args)
    assert expected_hash in execute_call, "Token hash should be passed to INSERT"
    assert raw_token not in execute_call, "Raw token must never reach the database"


# ---------------------------------------------------------------------------
# validate_refresh_token
# ---------------------------------------------------------------------------

def test_validate_refresh_token_valid():
    svc, db = _svc()
    cur = _cursor(db)
    cur.fetchone.return_value = ("alice", 1, 1)

    result = svc.validate_refresh_token(secrets.token_urlsafe(32))

    assert result is not None
    assert result["username"] == "alice"
    assert result["is_active"] is True
    assert result["not_expired"] is True


def test_validate_refresh_token_not_found_returns_none():
    svc, db = _svc()
    _cursor(db).fetchone.return_value = None

    assert svc.validate_refresh_token(secrets.token_urlsafe(32)) is None


def test_validate_refresh_token_expired():
    svc, db = _svc()
    _cursor(db).fetchone.return_value = ("alice", 1, 0)

    result = svc.validate_refresh_token(secrets.token_urlsafe(32))
    assert result is not None
    assert result["not_expired"] is False


def test_validate_refresh_token_inactive():
    svc, db = _svc()
    _cursor(db).fetchone.return_value = ("alice", 0, 1)

    result = svc.validate_refresh_token(secrets.token_urlsafe(32))
    assert result is not None
    assert result["is_active"] is False


# ---------------------------------------------------------------------------
# revoke_refresh_token
# ---------------------------------------------------------------------------

def test_revoke_refresh_token_returns_true_when_found():
    svc, db = _svc()
    cur = _cursor(db)
    cur.rowcount = 1

    assert svc.revoke_refresh_token(secrets.token_urlsafe(32)) is True


def test_revoke_refresh_token_returns_false_when_not_found():
    svc, db = _svc()
    _cursor(db).rowcount = 0

    assert svc.revoke_refresh_token(secrets.token_urlsafe(32)) is False


# ---------------------------------------------------------------------------
# revoke_all_refresh_tokens
# ---------------------------------------------------------------------------

def test_revoke_all_refresh_tokens_returns_count():
    svc, db = _svc()
    _cursor(db).rowcount = 5

    count = svc.revoke_all_refresh_tokens("alice")
    assert count == 5


def test_revoke_all_refresh_tokens_zero_when_none_active():
    svc, db = _svc()
    _cursor(db).rowcount = 0

    count = svc.revoke_all_refresh_tokens("alice")
    assert count == 0


# ---------------------------------------------------------------------------
# rotate_refresh_token (calls revoke then create internally)
# ---------------------------------------------------------------------------

def test_rotate_refresh_token_revokes_old_and_creates_new():
    svc, db = _svc()

    revoked: list[str] = []
    created: list[str] = []

    def _revoke(token: str) -> bool:
        revoked.append(token)
        return True

    def _create(username, token, session_id, expires_at, ip_address=None, user_agent=None) -> int:
        created.append(token)
        return 99

    svc.revoke_refresh_token = _revoke  # type: ignore[method-assign]
    svc.create_refresh_token = _create  # type: ignore[method-assign]

    old = secrets.token_urlsafe(32)
    new = secrets.token_urlsafe(32)
    result = svc.rotate_refresh_token(
        old_token=old,
        username="alice",
        new_token=new,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )

    assert result == 99
    assert old in revoked
    assert new in created
