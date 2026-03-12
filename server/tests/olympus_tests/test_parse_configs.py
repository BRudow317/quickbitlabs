"""Tests for parse_config."""
from __future__ import annotations

import os
import pytest
from pathlib import Path

from olympus.utils.parse_config_file import parse_config_file


class TestParseConfigBasic:

    def test_empty_path_returns_empty_dict(self):
        # This is the bug we discussed — depends on whether you add the guard
        result = parse_config_file(config_path="")
        assert result == {}

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            parse_config_file(config_path=str(tmp_path / "nope.dat"))

    def test_simple_key_value(self, tmp_config):
        cfg = tmp_config("DB_HOST=localhost\nDB_PORT=5432")
        result = parse_config_file(config_path=str(cfg))
        assert result["DB_HOST"] == "localhost"
        assert result["DB_PORT"] == "5432"

    def test_strips_quotes(self, tmp_config):
        cfg = tmp_config('NAME="some value"\nOTHER=\'single\'')
        result = parse_config_file(config_path=str(cfg))
        assert result["NAME"] == "some value"
        assert result["OTHER"] == "single"

    def test_skips_comments(self, tmp_config):
        cfg = tmp_config("# this is a comment\nKEY=val\n! also skipped\n")
        result = parse_config_file(config_path=str(cfg))
        assert list(result.keys()) == ["KEY"]

    def test_skips_blank_lines(self, tmp_config):
        cfg = tmp_config("\n\n  \nKEY=val\n\n")
        result = parse_config_file(config_path=str(cfg))
        assert result == {"KEY": "val"}

    def test_skips_lines_without_equals(self, tmp_config):
        cfg = tmp_config("no_equals_here\nGOOD=yes")
        result = parse_config_file(config_path=str(cfg))
        assert "no_equals_here" not in result
        assert result["GOOD"] == "yes"

    def test_value_with_equals_sign(self, tmp_config):
        """partition on first '=' so value can contain '='."""
        cfg = tmp_config("CONN=host=db;port=5432")
        result = parse_config_file(config_path=str(cfg))
        assert result["CONN"] == "host=db;port=5432"


class TestParseConfigInterpolation:

    def test_plain_var_reference(self, tmp_config):
        cfg = tmp_config("BASE=/opt\nFULL=$BASE/app")
        result = parse_config_file(config_path=str(cfg))
        assert result["FULL"] == "/opt/app"

    def test_braced_var_reference(self, tmp_config):
        cfg = tmp_config("BASE=/opt\nFULL=${BASE}/app")
        result = parse_config_file(config_path=str(cfg))
        assert result["FULL"] == "/opt/app"

    def test_positional_env_substitution(self, tmp_config):
        cfg = tmp_config("TARGET=/servers/$1/config")
        result = parse_config_file(config_path=str(cfg), env="sit01")
        assert result["TARGET"] == "/servers/sit01/config"

    def test_env_special_scope(self, tmp_config):
        cfg = tmp_config("LOG=/logs/$env/output.log")
        result = parse_config_file(config_path=str(cfg), env="dev01")
        assert result["LOG"] == "/logs/dev01/output.log"

    def test_chained_resolution(self, tmp_config):
        cfg = tmp_config("A=hello\nB=$A\nC=$B")
        result = parse_config_file(config_path=str(cfg))
        assert result["C"] == "hello"

    def test_unresolved_var_left_as_is(self, tmp_config):
        cfg = tmp_config("VAL=$NONEXISTENT")
        result = parse_config_file(config_path=str(cfg), allow_os_env=False)
        assert result["VAL"] == "$NONEXISTENT"

    def test_os_env_lookup(self, tmp_config, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hunter2")
        cfg = tmp_config("PASS=$MY_SECRET")
        result = parse_config_file(config_path=str(cfg), allow_os_env=True)
        assert result["PASS"] == "hunter2"

    def test_os_env_disabled(self, tmp_config, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hunter2")
        cfg = tmp_config("PASS=$MY_SECRET")
        result = parse_config_file(config_path=str(cfg), allow_os_env=False)
        assert result["PASS"] == "$MY_SECRET"


class TestParseConfigEdgeCases:

    def test_cycle_does_not_hang(self, tmp_config):
        """Circular refs should resolve without infinite loop."""
        cfg = tmp_config("A=$B\nB=$A")
        # should return without hanging — value is best-effort
        result = parse_config_file(config_path=str(cfg), max_depth=20)
        assert "A" in result
        assert "B" in result

    def test_max_depth_exceeded(self, tmp_config):
        """Deeply nested indirection should stop at max_depth."""
        # A -> B -> C -> D ... each referencing the next
        lines = [f"V{i}=$V{i+1}" for i in range(25)]
        lines.append("V25=terminal")
        cfg = tmp_config("\n".join(lines))
        result = parse_config_file(config_path=str(cfg), max_depth=5)
        # V0 won't fully resolve to "terminal" because depth is capped
        assert "V0" in result

    def test_self_reference_does_not_hang(self, tmp_config):
        cfg = tmp_config("X=$X")
        result = parse_config_file(config_path=str(cfg), allow_os_env=False)
        assert "X" in result