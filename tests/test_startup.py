from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from server.configs.settings import Settings


# ---------------------------------------------------------------------------
# Settings: required-field validation
# ---------------------------------------------------------------------------

def test_settings_raises_when_jwt_secret_missing(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.setenv("UPLOAD_ENCRYPTION_KEY", "dGVzdC1rZXktMzItYnl0ZXMtbG9uZy1wYWRkZWQ=")
	with pytest.raises(ValidationError) as exc_info:
		Settings()  # type: ignore[call-arg]
	assert "jwt_secret" in str(exc_info.value).lower()


def test_settings_raises_when_upload_encryption_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.setenv("JWT_SECRET", "some-valid-secret")
	monkeypatch.delenv("UPLOAD_ENCRYPTION_KEY", raising=False)
	with pytest.raises(ValidationError) as exc_info:
		Settings()  # type: ignore[call-arg]
	assert "upload_encryption_key" in str(exc_info.value).lower()


def test_settings_raises_when_both_required_vars_missing(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.delenv("UPLOAD_ENCRYPTION_KEY", raising=False)
	with pytest.raises(ValidationError) as exc_info:
		Settings()  # type: ignore[call-arg]
	error_text = str(exc_info.value).lower()
	assert "jwt_secret" in error_text
	assert "upload_encryption_key" in error_text


def test_settings_loads_successfully_when_required_vars_present(monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.setenv("JWT_SECRET", "a-real-secret-for-testing")
	monkeypatch.setenv("UPLOAD_ENCRYPTION_KEY", "dGVzdC1rZXktMzItYnl0ZXMtbG9uZy1wYWRkZWQ=")
	s = Settings()  # type: ignore[call-arg]
	assert s.jwt_secret.get_secret_value() == "a-real-secret-for-testing"


# ---------------------------------------------------------------------------
# _ensure_jwt_secret: secret lifecycle
# ---------------------------------------------------------------------------

def test_ensure_jwt_secret_preserves_existing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
	import build.build_server as build_server
	monkeypatch.setenv("JWT_SECRET", "already-set-strong-secret")
	build_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "already-set-strong-secret"


def test_ensure_jwt_secret_replaces_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
	import build.build_server as build_server
	monkeypatch.setenv("JWT_SECRET", build_server._PLACEHOLDER)
	monkeypatch.setattr(build_server.secrets, "token_urlsafe", lambda _: "fresh-generated-key")
	build_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "fresh-generated-key"


def test_ensure_jwt_secret_generates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
	import build.build_server as build_server
	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.setattr(build_server.secrets, "token_urlsafe", lambda _: "fresh-generated-key")
	build_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "fresh-generated-key"


# ---------------------------------------------------------------------------
# build_process: control flow
# ---------------------------------------------------------------------------

def test_build_process_calls_check_db_and_npm_then_sets_sentinel(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	import build.build_server as build_server

	calls: list[tuple[str, str | None]] = []

	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.delenv("_BUILD_STEP_COMPLETED", raising=False)
	monkeypatch.setattr(build_server, "_check_db", lambda: calls.append(("db", None)))
	monkeypatch.setattr(build_server, "_npm", lambda script: calls.append(("npm", script)))
	monkeypatch.setattr(build_server.secrets, "token_urlsafe", lambda _: "ephemeral-test-secret")

	build_server.build_process(mode="development")

	assert os.environ["JWT_SECRET"] == "ephemeral-test-secret"
	assert os.environ["_BUILD_STEP_COMPLETED"] == "1"
	assert calls == [("db", None), ("npm", "build")]


def test_build_process_skips_all_steps_when_sentinel_is_set(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	import build.build_server as build_server

	monkeypatch.setenv("_BUILD_STEP_COMPLETED", "1")
	monkeypatch.setattr(
		build_server, "_check_db",
		lambda: (_ for _ in ()).throw(AssertionError("_check_db must not be called")),
	)
	monkeypatch.setattr(
		build_server, "_npm",
		lambda _: (_ for _ in ()).throw(AssertionError("_npm must not be called")),
	)

	build_server.build_process(mode="development")  # should return immediately, no assertions raised
