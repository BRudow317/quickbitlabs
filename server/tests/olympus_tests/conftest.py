"""Shared fixtures for olympus tests."""
from __future__ import annotations

import pytest
import os
from pathlib import Path


@pytest.fixture
def tmp_config(tmp_path: Path):
    """Write a temp config file and return its path."""
    def _write(content: str) -> Path:
        p = tmp_path / "test_config.dat"
        p.write_text(content, encoding="utf-8")
        return p
    return _write


@pytest.fixture
def clean_env(monkeypatch):
    """Remove olympus-related env vars so tests don't leak state."""
    for key in ("LOG_VAR", "LOG_LEVEL", "_OLYMPUS_REEXECED", "VIRTUAL_ENV"):
        monkeypatch.delenv(key, raising=False)