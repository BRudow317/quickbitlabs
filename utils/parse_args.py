from __future__ import annotations

import argparse, logging

_PROGRAM_NAME='olympus'

logger = logging.getLogger(__name__)


def parse_args(argv)->tuple[argparse.Namespace, list[str]]:
    if "--" in argv:
        split_at = argv.index("--")
        cli_argv = argv[:split_at]
        passthrough_args = argv[split_at + 1:]
    else:
        cli_argv = argv
        passthrough_args = []
    CLI_FLAGS = ("--env", "--config", "--venv", "--venv-mode")

    def is_cli_flag(tok: str) -> bool:
        return any(tok == f or tok.startswith(f + "=") for f in CLI_FLAGS)
    exec_cmd = []
    if "--exec" in cli_argv:
        exec_idx = cli_argv.index("--exec")
        rest = cli_argv[exec_idx + 1:]
        for i, token in enumerate(rest):
            if is_cli_flag(token):
                exec_cmd = rest[:i]
                cli_argv = cli_argv[:exec_idx] + rest[i:]
                break
        else:
            exec_cmd = rest
            cli_argv = cli_argv[:exec_idx]

    _env_help_msg=f"Environment (dev01, mmdev, sit01, etc...) NOTE: This is not for .env files, use --config for those."
    _config_help_msg="Path to environment config file with key=value pairs. Values can reference other keys with $KEY or ${KEY} syntax, and can also reference environment variables. See README for details."
    _venv_help_msg = f"Path to venv directory for the child or parent process (default: None, no venv applied)"
    _verbose_help_msg = f"Enable debug logging (default: errors and info only)"
    _venv_mode_help_msg = f"If venv directory is specified; Apply venv to child only (default) or re-exec {_PROGRAM_NAME}.py in venv. Warning this will cause an interrupt for a parent process calling {_PROGRAM_NAME}.py, only use child mode if {_PROGRAM_NAME} is not the originator."
    _log_help_msg = f"The folder where the log should be written (default: sys.stdout)"

    parser = argparse.ArgumentParser(prog=_PROGRAM_NAME, description=f"{_PROGRAM_NAME}.py - universal pipeline orchestrator", allow_abbrev=False)
    parser.add_argument("--env", dest="env", required=False, type=str, help=_env_help_msg, default="")
    parser.add_argument("--config", "--config_file", "--config-file", dest="config", required=False, type=str, default="", help=_config_help_msg)
    parser.add_argument("--venv", "--venv_dir", "--venv-dir", dest="venv", required=False, type=str, default="", help=_venv_help_msg)
    parser.add_argument("--venv-mode", "--venv_mode", dest="venv_mode", choices=["child", "master", _PROGRAM_NAME], default="child", help=_venv_mode_help_msg)
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help=_verbose_help_msg)
    parser.add_argument("-l", "--log", type=str, dest="log_dir", default="sys.stdout",  required=False, help=_log_help_msg)
    args = parser.parse_args(cli_argv)
    
    _exec_cmd_parser_error_msg=f"The child python program to run is required after `--exec` Usage: {_PROGRAM_NAME}... -- --exec python your_script.py"
    if not exec_cmd: parser.error(_exec_cmd_parser_error_msg)

    args.exec = exec_cmd

    return args, passthrough_args