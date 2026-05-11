#!/usr/bin/env python3
"""
python ./boot.py -v  -l ./.logs --env homelab --config ../.secrets/.env --exec python ./main.py
"""
from __future__ import annotations

import sys, subprocess, threading, os, argparse, re, logging
from pathlib import Path
from datetime import datetime

from typing import IO, Any, TextIO

program_name: str = "boot"
_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'

def set_roots(args: argparse.Namespace) -> tuple[str, str]:
    global program_name
    script_path = next((Path(t).resolve() for t in getattr(args,"exec", []) if Path(t).is_file()), None)

    current_dir: str|Path = str(script_path.parent) if script_path else Path.cwd()
    project_root: str|Path = current_dir

    if script_path and script_path.suffix == ".py":
        temp_root: Path = Path(current_dir)
        while (temp_root / "__init__.py").exists() and temp_root.parent != temp_root:
            temp_root = temp_root.parent
        project_root = str(temp_root)

    if project_root not in sys.path:
        sys.path.insert(0, str(project_root))
    os.environ["PROJECT_ROOT"] = str(project_root)

    if getattr(args, "program_name", None) is not None and getattr(args, "program_name") != program_name:
        prg_name: str = getattr(args, "program_name")
    else:
        prg_name = os.getenv("PROGRAM_NAME", "null")
    if prg_name == "null":
        prg_name = Path(project_root).name

    os.environ["PROGRAM_NAME"] = str(prg_name)
    program_name = str(prg_name)

    return os.environ["PROJECT_ROOT"], os.environ["PROGRAM_NAME"]



def get_args(argv) -> argparse.Namespace:
    _env_help_msg=f"Environment (dev01, prod, stage01, etc...) NOTE: This is not for .env file paths, use --config for those."
    _config_help_msg="Path to environment config file with key=value pairs. Values can reference other keys with $KEY or ${KEY} syntax, and can also reference environment variables. See README for details."
    _venv_help_msg = f"Path to venv for the child process (default: inherit caller's environment)"
    _verbose_help_msg = f"Enable debug logging (default: errors and info only)"
    _log_help_msg = f"The folder where the log should be written (default: sys.stdout)"
    _exec_help_msg = f"Child command to run. Must follow all master flags. Usage: {program_name} [flags] --exec python script.py [child args...]"

    parser = argparse.ArgumentParser(prog=program_name, description=f"{program_name}.py - universal pipeline orchestrator", allow_abbrev=False)
    parser.add_argument("--env", "--environment", dest="env", required=False, type=str, help=_env_help_msg, default="")
    parser.add_argument("--config", "--dotenv", "--config-file", dest="config", required=False, type=str, default=os.getenv("SECRETS_ENV", "./.env"), help=_config_help_msg)
    parser.add_argument("--venv", "--venv_dir", "--venv-dir", dest="venv", required=False, type=str, default="", help=_venv_help_msg)
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help=_verbose_help_msg)
    parser.add_argument("-l", "--log", "--log_dir", "--log-dir", type=str, dest="log_dir", default="sys.stdout",  required=False, help=_log_help_msg)
    parser.add_argument("--exec", nargs=argparse.REMAINDER, default=[], dest="exec", help=_exec_help_msg)
    args: argparse.Namespace = parser.parse_args(argv)
    if not args.exec:
        parser.error(f"Child command required after --exec. Usage:\n  {program_name} [flags] --exec python your_script.py")
    return args

def setup_logging(
        log_dir: str = "sys.stdout",
        verbose: bool = True,
        program_name: str = program_name
        ) -> tuple[logging.Logger, Path | None]:
    log_format = _LOG_FORMAT
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter(log_format)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    logfile: Path | None = None
    if log_dir and log_dir != 'sys.stdout':
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        logfile = path / f"{datetime.now():%Y_%m_%d_%H_%M_%S}_{program_name}.log"
        fh = logging.FileHandler(logfile)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    
    return logging.getLogger(program_name), logfile

def parse_config_file(config_path: str | Path = "", env: str = "") -> dict[str, str]:
    _VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("!") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            raw[key.strip()] = val.strip().strip('"').strip("'")

    lookup: dict[str, Any] = {**os.environ, **raw, "env": env, "ENV": env}

    def interpolate(val: str) -> str:
        previous = None
        loops = 0
        while val != previous and loops < 10:
            previous = val
            def repl(m: re.Match) -> str:
                name = m.group(1) or m.group(2)
                return lookup.get(name, m.group(0))
            
            val = _VAR.sub(repl, val)
            loops += 1
        return val

    resolved = {k: interpolate(v) for k, v in raw.items()}
    for k, v in list(resolved.items()):
        if v in resolved:
            resolved[k] = resolved[v]
    return resolved

def configure_environment(
    args,
    config_vars: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, str], str]:
    _PYTHON_BASENAMES = {
        "python", "python3", "python3.11", "python.exe", "python3.exe", "py", "py.exe", "pythonw.exe",
    }
    config_vars = config_vars or {}
    venv = getattr(args, "venv", "")
    is_python = False
    project_root = os.getenv("PROJECT_ROOT", "")
    cwd = os.getenv("PROJECT_ROOT", "")
    

    # checking named venv and building with env
    python = sys.executable
    env = {**os.environ, **config_vars}
    env["PYTHONUNBUFFERED"] = "1"
    if venv:
        bin_dir = Path(venv) / ("Scripts" if sys.platform == "win32" else "bin")
        venv_python = bin_dir / ("python.exe" if sys.platform == "win32" else "python")
        if not venv_python.exists(): raise FileNotFoundError(f"venv python not found: {venv_python}")
        python = str(venv_python)
        env["VIRTUAL_ENV"] = str(Path(venv).resolve())
        env.pop("PYTHONHOME", None)
        env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"

    # Prepend project_root and cwd (deduplicated) to PYTHONPATH
    existing = [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
    new_paths = [p for p in dict.fromkeys([project_root, cwd]) if p not in existing]
    env["PYTHONPATH"] = os.pathsep.join(new_paths + existing)

    # Build cmd: resolve file tokens to absolute, swap python basename for venv python
    cmd = [str(Path(t).resolve()) if Path(t).is_file() else t for t in args.exec]
    if os.path.basename(cmd[0]).lower() in _PYTHON_BASENAMES:
        cmd[0] = python
        is_python = True
    elif Path(cmd[0]).suffix == ".py" and os.path.isfile(cmd[0]):
        cmd = [python] + cmd
        is_python = True

    # Inject child env logging bootstrap
    log_level = "INFO"
    if args.verbose: log_level = "DEBUG"
    logging_bootstrap = (
        "import sys,os,logging,runpy;"
        "logging.basicConfig("
        f"level=os.environ.get('LOG_LEVEL','{log_level}').upper(),"
        f"format={_LOG_FORMAT!r});"
        "sys.argv=sys.argv[1:];"
        "runpy.run_path(sys.argv[0],run_name='__main__')"
    )
    if is_python: cmd = [cmd[0], "-c", logging_bootstrap] + cmd[1:]

    return cmd, env, cwd

def main():
    args = get_args(sys.argv[1:])
    _, prog_name = set_roots(args)
    logger, logfile = setup_logging(args.log_dir, args.verbose, prog_name)
    logger.debug(f"\nStarting {prog_name} with args: {args}\n\n\n")
    config_vars = parse_config_file(args.config, env=args.env) if args.config else {}
    cmd, child_env, child_cwd = configure_environment(args, config_vars)
    logger.debug(f"Child working directory: {child_cwd}")

    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        env=child_env, 
        cwd=child_cwd
        )
    assert process.stdout is not None
    assert process.stderr is not None

    log_lock = threading.Lock()
    lf = open(logfile, "a", encoding="utf-8") if logfile else None

    def stream_pipe(pipe: IO[bytes], out_stream: TextIO) -> None:
        for line in iter(pipe.readline, b""):
            text = line.decode("utf-8", errors="replace")
            out_stream.write(text)
            out_stream.flush()
            if lf:
                with log_lock:
                    lf.write(text)
                    lf.flush()
        pipe.close()

    t_out = threading.Thread(target=stream_pipe, args=(process.stdout, sys.stdout))
    t_err = threading.Thread(target=stream_pipe, args=(process.stderr, sys.stderr))
    t_out.start(); t_err.start(); t_out.join(); t_err.join()
    if lf: lf.close()
    process.wait()
    sys.exit( process.returncode )

if __name__ == '__main__':
    main()