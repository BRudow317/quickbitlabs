from __future__ import annotations
import logging, os, sys, subprocess, secrets
from pathlib import Path
from typing import Literal



# export PY_PROJECT_ROOT=$(git rev-parse --show-toplevel)

logger = logging.getLogger(__name__)
PROJECT_ROOT: Path = Path(os.getenv("PROJECT_ROOT","")).resolve()


def build_server(mode: str|None = None, force_rebuild: bool = False, pgdb_base_name="QBL") -> None:
    
    if not mode:
        mode = os.environ.get("ENV_MODE", "production")
        if mode == "production":
            os.environ["_BUILD_STEP_COMPLETED"] = "1"
        else:
            # run npm build for non-production modes
            from build.npm_run import npm_run
            npm_run("build")

    if os.environ.get("_BUILD_STEP_COMPLETED"):
        logger.debug("Build Step Sentinel Exit.")
        return
    
    if mode == "development":
        if force_rebuild:
            from build.pdb_setup import orchestrate_pdb
            orchestrate_pdb(pgdb_base_name=pgdb_base_name, force_rebuild=force_rebuild)
            from build.db_setup import build_db
            build_db(sql_dir=str(PROJECT_ROOT / "build" / "sql"))
            from build.app_setup import user_setup
            user_setup(username="admin")
            from server.services.sync_systems import sync_all
            sync_all()
        
    if mode == "production":
        from server.services.sync_systems import sync_all
        sync_all()

    os.environ["_BUILD_STEP_COMPLETED"] = "1"
    logger.info("Build process completed.")
