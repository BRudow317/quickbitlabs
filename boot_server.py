"""

logging, .env parsing, venv, and other generic bootloading are handled by boot.py - main.py is app specific configuration.

python "Q:/scripts/boot.py" -v  -l ./.logs --env homelab --exec ./main.py
"""
from __future__ import annotations
from datetime import datetime
import logging, os, sys, subprocess, re, secrets
from pathlib import Path

PY_PROJECT_ROOT = Path(__file__).resolve().parent
if str(PY_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_PROJECT_ROOT))

FRONTEND_DIR = PY_PROJECT_ROOT / "frontend"

# Cold start: create the log file, store its path for reload children to inherit.
# Reload children: skip pre-checks and attach to the same log file via _QBL_LOGFILE.

# 
logger = logging.getLogger(__name__)

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

_PLACEHOLDER = "CHANGE_ME_FOR_PRODUCTION"

def _ensure_jwt_secret() -> None:
    """
    Guarantee a cryptographically strong JWT_SECRET is in the environment before
    uvicorn starts. Hot-reload children inherit the env var, so the key stays
    stable across reloads.

    Priority order:
      1. JWT_SECRET already set in environment (e.g. from .env / boot.py loader) — used as-is.
      2. Not set or still the placeholder — generate a fresh 256-bit random key.

    In production, set JWT_SECRET in .env so restarts don't invalidate sessions.
    In development, the ephemeral key is regenerated each time the server process starts;
    sessions naturally invalidate on restart, which is acceptable.
    """
    current = os.environ.get("JWT_SECRET", "")
    if current and current != _PLACEHOLDER:
        return
    key = secrets.token_urlsafe(32)   # 256-bit URL-safe base64
    os.environ["JWT_SECRET"] = key
    logger.info("JWT_SECRET: generated ephemeral 256-bit key (set JWT_SECRET in .env for persistence)")


def build_process(mode="prod") -> None:
    try:       
        # _secrets_path = os.environ.get("SECRETS_ENV")

        _check_db()
        _ensure_jwt_secret()

        # Uncomment for production: deep-hydrates CATALOG_REGISTRY from all plugins.
        # Safe to run here — _IS_BUILD_STEP_COMPLETED guard ensures it runs once per server start,
        # not on every uvicorn hot reload. Keep commented in development.
        # from server.services.sync_systems import sync_all
        # sync_all()

        if mode == "development":
            _npm("build")
        
        # Mark done so uvicorn reload children skip all of the above
        os.environ["_BUILD_STEP_COMPLETED"] = "1"
    except Exception as e:
        logger.critical("Build process failed: %s", e)
        sys.exit(1)
    
def start_app(mode="prod") -> None:
    reload = False
    if mode == "development":
        reload = True
    import uvicorn
    uvicorn.run("server.start_server:app", host="0.0.0.0", port=8000, reload=reload)

if __name__ == "__main__":
    mode="development"
    if not (os.environ.get("_BUILD_STEP_COMPLETED") == "1"):   
        build_process(mode=mode)
    start_app(mode=mode)
    # Load secrets BEFORE server imports - ServerDatabase reads env vars at init time
    
    # NOTE: run `npm run generate` here after the server is serving /openapi.json
    # (server must be running first, so this needs a two-step startup or a separate script)