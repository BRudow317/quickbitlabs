"""
python "Q:/scripts/boot.py" -v  -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./main.py
"""
from __future__ import annotations
from datetime import datetime
import logging, os, sys, subprocess, re
from pathlib import Path

PY_PROJECT_ROOT = Path(__file__).resolve().parent
if str(PY_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_PROJECT_ROOT))

FRONTEND_DIR = PY_PROJECT_ROOT / "frontend"

# Cold start: create the log file, store its path for reload children to inherit.
# Reload children: skip pre-checks and attach to the same log file via _QBL_LOGFILE.
_IS_RELOAD_CHILD = os.environ.get("_QBL_PRECHECKS_DONE") == "1"

logger = logging.getLogger(__name__)
 
def dotenv_loader(config_path: str | Path = "", env: str = "") -> None:
    """
    #!/usr/bin/env bash
    env=$1 
    example_homelab_user=ExampleUserName
    EXAMPLE_USER=example_${env}_user
    """
    _var = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")

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

    lookup = {**os.environ, **raw, "env": env, "ENV": env}

    def interpolate(val: str) -> str:
        previous = None
        loops = 0
        while val != previous and loops < 10:
            previous = val
            def repl(m: re.Match) -> str:
                name = m.group(1) or m.group(2)
                return lookup.get(name, m.group(0))
            
            val = _var.sub(repl, val)
            loops += 1
        return val

    resolved = {k: interpolate(v) for k, v in raw.items()}
    for k, v in list(resolved.items()):
        if v in resolved:
            resolved[k] = resolved[v]
    os.environ.update(resolved)

def _npm(script: str) -> None:
    result = subprocess.run(
        ["npm", "run", script],
        cwd=str(FRONTEND_DIR),
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("npm run %s failed:\n%s", script, result.stderr.strip())
        sys.exit(1)
    
    logger.debug("npm run build - OK")\

def _check_db() -> bool:
    try:
        from server.db.db import server_db
        server_db.connect().is_healthy
        return True
    except Exception as exc:
        logger.critical("Pre Check: Server database unreachable: %s", exc)
        sys.exit(1)
        return False

def build_process(mode="prod") -> None:
    try:
        if _IS_RELOAD_CHILD:
            return
       
        # _secrets_path = os.environ.get("SECRETS_ENV")
        # dotenv_loader(
        #     config_path=os.environ.get("SECRETS_ENV", ""),
        #     env="homelab"
        # )

        _check_db()

        if mode == "development":
            _npm("build")

        # Mark done so uvicorn reload children skip all of the above
        os.environ["_QBL_PRECHECKS_DONE"] = "1"
    except Exception as e:
        logger.critical("Pre-checks failed: %s", e)
        sys.exit(1)
    
def start_app(mode="prod") -> None:
    reload = False
    if mode == "development":
        reload = True
    import uvicorn
    uvicorn.run("server.start_server:app", host="0.0.0.0", port=8000, reload=reload)

if __name__ == "__main__":
    mode="development"
    build_process(mode=mode)
    start_app(mode=mode)
    # Load secrets BEFORE server imports - ServerDatabase reads env vars at init time
    
    # NOTE: run `npm run generate` here after the server is serving /openapi.json
    # (server must be running first, so this needs a two-step startup or a separate script)