from __future__ import annotations

import os
import runpy
import subprocess
import sys
import types
from pathlib import Path

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
	import boot_server
	monkeypatch.setenv("JWT_SECRET", "already-set-strong-secret")
	boot_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "already-set-strong-secret"


def test_ensure_jwt_secret_replaces_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
	import boot_server
	monkeypatch.setenv("JWT_SECRET", boot_server._PLACEHOLDER)
	monkeypatch.setattr(boot_server.secrets, "token_urlsafe", lambda _: "fresh-generated-key")
	boot_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "fresh-generated-key"


def test_ensure_jwt_secret_generates_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
	import boot_server
	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.setattr(boot_server.secrets, "token_urlsafe", lambda _: "fresh-generated-key")
	boot_server._ensure_jwt_secret()
	assert os.environ["JWT_SECRET"] == "fresh-generated-key"


# ---------------------------------------------------------------------------
# Hot-reload sentinel: build step control flow
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_uvicorn(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
	calls: list[dict[str, object]] = []

	def fake_run(app: str, host: str, port: int, reload: bool) -> None:
		calls.append({"app": app, "host": host, "port": port, "reload": reload})

	monkeypatch.setitem(sys.modules, "uvicorn", types.SimpleNamespace(run=fake_run))
	return calls


def test_build_process_generates_ephemeral_secret_and_marks_completion(
	monkeypatch: pytest.MonkeyPatch,
) -> None:
	import boot_server

	calls: list[tuple[str, str | None]] = []

	monkeypatch.delenv("JWT_SECRET", raising=False)
	monkeypatch.delenv("_BUILD_STEP_COMPLETED", raising=False)
	monkeypatch.setattr(boot_server, "_check_db", lambda: calls.append(("db", None)) or True)
	monkeypatch.setattr(boot_server, "_npm", lambda script: calls.append(("npm", script)))
	monkeypatch.setattr(boot_server.secrets, "token_urlsafe", lambda _: "ephemeral-test-secret")

	boot_server.build_process(mode="development")

	assert os.environ["JWT_SECRET"] == "ephemeral-test-secret"
	assert os.environ["_BUILD_STEP_COMPLETED"] == "1"
	assert calls == [("db", None), ("npm", "build")]


def test_main_entry_skips_build_steps_when_hot_reload_sentinel_present(
	monkeypatch: pytest.MonkeyPatch,
	fake_uvicorn: list[dict[str, object]],
) -> None:
	main_path = Path(__file__).resolve().parents[1] / "boot_server.py"

	class FakeConnection:
		is_healthy = True

	class FakeServerDb:
		def connect(self) -> FakeConnection:
			raise AssertionError("build step should be skipped when sentinel is set")

	fake_server_module = types.ModuleType("server")
	fake_db_package = types.ModuleType("server.db")
	fake_db_module = types.ModuleType("server.db.db")
	setattr(fake_db_module, "server_db", FakeServerDb())

	monkeypatch.setitem(sys.modules, "server", fake_server_module)
	monkeypatch.setitem(sys.modules, "server.db", fake_db_package)
	monkeypatch.setitem(sys.modules, "server.db.db", fake_db_module)
	monkeypatch.setattr(
		subprocess,
		"run",
		lambda *args, **kwargs: (_ for _ in ()).throw(
			AssertionError("npm build should be skipped when sentinel is set")
		),
	)
	monkeypatch.setenv("_BUILD_STEP_COMPLETED", "1")
	monkeypatch.delenv("JWT_SECRET", raising=False)

	runpy.run_path(str(main_path), run_name="__main__")

	assert "JWT_SECRET" not in os.environ
	assert fake_uvicorn == [{
		"app": "server.start_server:app",
		"host": "0.0.0.0",
		"port": 8000,
		"reload": True,
	}]
