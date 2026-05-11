from __future__ import annotations
import logging, os, sys, subprocess
from pathlib import Path

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT",".")).resolve()

def npm_run(script: str) -> None:
    result = subprocess.run(
        ["npm", "run", script],
        cwd=str(PROJECT_ROOT / "frontend"),
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("npm run %s failed:\n%s", script, result.stderr.strip())
        sys.exit(1)
    logger.debug("npm run %s - OK", script)