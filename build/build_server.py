"""
logging, .env parsing, venv, and other generic bootloading are handled by boot.py.

python "Q:/quickbitlabs/build/boot.py" -v  -l ./.logs --env homelab --exec ./main.py
"""
from __future__ import annotations
import logging, os, sys, subprocess, secrets
from pathlib import Path
from typing import Literal

# Project root is one level above the build/ package.
PY_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PY_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_PROJECT_ROOT))
os.environ.setdefault("PY_PROJECT_ROOT", str(PY_PROJECT_ROOT))

logger = logging.getLogger(__name__)

# Sentinel used in .env templates and Docker env files.
# _ensure_jwt_secret replaces it with a real key at build time.
_PLACEHOLDER = "CHANGE_ME"


def _npm(script: str) -> None:
    result = subprocess.run(
        ["npm", "run", script],
        cwd=str(PY_PROJECT_ROOT / "frontend"),
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("npm run %s failed:\n%s", script, result.stderr.strip())
        sys.exit(1)
    logger.debug("npm run %s - OK", script)


def _check_db() -> None:
    """Connect to the server database. Raises on any connection failure."""
    from server.db.db import server_db
    server_db.connect()
    logger.info("Server database connection: OK")


def _ensure_jwt_secret() -> None:
    """
    Guarantee a cryptographically strong JWT_SECRET before uvicorn starts.
    Hot-reload children inherit the env var, keeping the key stable across reloads.

    Priority:
      1. JWT_SECRET is set and is not the placeholder value — used as-is.
      2. JWT_SECRET is missing or equals _PLACEHOLDER — generate a fresh 256-bit key.

    Set JWT_SECRET in .env for production so restarts don't invalidate sessions.
    In development the ephemeral key is acceptable.
    """
    current = os.environ.get("JWT_SECRET")
    if current and current != _PLACEHOLDER:
        return
    key = secrets.token_urlsafe(32)
    os.environ["JWT_SECRET"] = key
    logger.info("JWT_SECRET: generated ephemeral 256-bit key (set JWT_SECRET in .env for persistence)")


def build_process(mode: Literal["development", "staging", "production"]) -> None:
    if os.environ.get("_BUILD_STEP_COMPLETED"):
        logger.debug("Build step already completed — skipping (uvicorn reload child).")
        return

    try:
        _check_db()
        _ensure_jwt_secret()

        # Uncomment for production: deep-hydrates CATALOG_REGISTRY from all plugins.
        # Safe here — _BUILD_STEP_COMPLETED guard ensures it runs once per server start.

        if mode == "development":
            _npm("build")

        elif mode == "production":
            from server.services.sync_systems import sync_all
            sync_all()

        os.environ["_BUILD_STEP_COMPLETED"] = "1"
        logger.info("Build process completed.")
    except Exception as e:
        logger.critical("Build process failed: %s", e)
        sys.exit(1)
