"""Tests for parse_args."""
from __future__ import annotations

import pytest

from utils.parse_args import parse_args, _PROGRAM_NAME


class TestParseArgs:

    # --- basic --exec parsing ---

    def test_exec_simple_command(self):
        args, pt = parse_args(["--exec", "python", "my_script.py"])
        assert args.exec == ["python", "my_script.py"]
        assert pt == []

    def test_exec_single_token(self):
        args, _ = parse_args(["--exec", "run_job.sh"])
        assert args.exec == ["run_job.sh"]

    def test_missing_exec_raises(self):
        with pytest.raises(SystemExit):
            parse_args(["--env", "dev01"])

    def test_exec_with_no_args_raises(self):
        with pytest.raises(SystemExit):
            parse_args(["--exec"])

    # --- passthrough via -- ---

    def test_passthrough_args_split_on_double_dash(self):
        args, pt = parse_args(["--exec", "python", "job.py", "--", "--date", "2025-01-01"])
        assert args.exec == ["python", "job.py"]
        assert pt == ["--date", "2025-01-01"]

    def test_no_passthrough_when_no_double_dash(self):
        _, pt = parse_args(["--exec", "job.sh"])
        assert pt == []

    # --- olympus flags ---

    def test_env_flag(self):
        args, _ = parse_args(["--env", "sit01", "--exec", "job.sh"])
        assert args.env == "sit01"

    def test_env_defaults_empty(self):
        args, _ = parse_args(["--exec", "job.sh"])
        assert args.env == ""

    def test_config_flag(self):
        args, _ = parse_args(["--config", "/etc/config.dat", "--exec", "job.sh"])
        assert args.config == "/etc/config.dat"

    def test_venv_flag(self):
        args, _ = parse_args(["--venv", "/opt/venvs/py39", "--exec", "job.sh"])
        assert args.venv == "/opt/venvs/py39"

    def test_venv_mode_default_is_child(self):
        args, _ = parse_args(["--exec", "job.sh"])
        assert args.venv_mode == "child"

    def test_venv_mode_master(self):
        args, _ = parse_args(["--venv-mode", "master", "--exec", "job.sh"])
        assert args.venv_mode == "master"

    def test_venv_mode_invalid_choice_raises(self):
        with pytest.raises(SystemExit):
            parse_args(["--venv-mode", "bogus", "--exec", "job.sh"])

    def test_verbose_flag(self):
        args, _ = parse_args(["-v", "--exec", "job.sh"])
        assert args.verbose is True

    def test_verbose_default_false(self):
        args, _ = parse_args(["--exec", "job.sh"])
        assert args.verbose is False

    def test_log_dir_flag(self):
        args, _ = parse_args(["-l", "/tmp/logs", "--exec", "job.sh"])
        assert args.log_dir == "/tmp/logs"

    def test_log_dir_default(self):
        args, _ = parse_args(["--exec", "job.sh"])
        assert args.log_dir == "sys.stdout"

    # --- flag ordering ---

    def test_exec_before_flags(self):
        """--exec can appear before other olympus flags."""
        args, _ = parse_args(["--exec", "job.sh", "--env", "dev01"])
        assert args.exec == ["job.sh"]
        assert args.env == "dev01"

    def test_flags_interleaved_with_exec(self):
        args, _ = parse_args(["--env", "dev01", "--exec", "python", "job.py", "--config", "/cfg.dat"])
        assert args.env == "dev01"
        assert args.config == "/cfg.dat"
        assert args.exec == ["python", "job.py"]