"""Tests for setup_logging."""
from __future__ import annotations

import logging
from pathlib import Path

# Adjust this import to match your actual module location
from utils.setup_logging import setup_logging, _PROGRAM_NAME


class TestSetupLogging:

    def test_returns_logger_with_correct_name(self):
        logger = setup_logging()
        assert logger.name == _PROGRAM_NAME

    def test_custom_name(self):
        logger = setup_logging(program_name="apollo")
        assert logger.name == "apollo"

    def test_verbose_sets_debug(self):
        logger = setup_logging(verbose=True)
        assert logger.level == logging.DEBUG

    def test_non_verbose_sets_info(self):
        logger = setup_logging(verbose=False)
        assert logger.level == logging.INFO

    def test_stdout_only_has_one_handler(self):
        logger = setup_logging(log_dir="sys.stdout")
        # should only have the console StreamHandler
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_log_dir_creates_file_handler(self, tmp_path: Path):
        logger = setup_logging(log_dir=str(tmp_path), verbose=True)
        handler_types = [type(h) for h in logger.handlers]
        assert logging.StreamHandler in handler_types
        assert logging.FileHandler in handler_types

    def test_log_dir_creates_log_file(self, tmp_path: Path):
        setup_logging(log_dir=str(tmp_path), program_name="test_server")
        log_files = list(tmp_path.glob("*_test_server.log"))
        assert len(log_files) == 1

    def test_log_dir_creates_directory_if_missing(self, tmp_path: Path):
        nested = tmp_path / "sub" / "logs"
        assert not nested.exists()
        setup_logging(log_dir=str(nested))
        assert nested.exists()

    def test_clears_previous_handlers_on_reconfig(self):
        """Calling setup_logging twice shouldn't stack handlers."""
        setup_logging(log_dir="sys.stdout")
        logger = setup_logging(log_dir="sys.stdout")
        assert len(logger.handlers) == 1