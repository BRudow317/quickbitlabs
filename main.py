"""clear; python "./build/boot.py" -v  -l ./.logs --env QBL  --exec ./main.py"""
from __future__ import annotations
import os
from typing import Literal
import logging

logger: logging.Logger = logging.getLogger(__name__)
ENV_MODE = Literal["development", "staging", "production"]

def build(mode: ENV_MODE|None = None, force_rebuild: bool = False):
    from build.builder import build_server
    if not mode:
        mode = cast(ENV_MODE, os.environ.get("ENV_MODE", "production"))
    build_server(mode=mode, force_rebuild=force_rebuild)

def release():
    mode = os.environ.get("ENV_MODE", "production")
    if mode == "production":
        return
    import pytest
    pytest.main(["-v", "tests/"])

def run():
    from server.app import start_app
    start_app()

if __name__ == "__main__":
    """clear; python ./boot.py -v  -l ./.logs --env qbl  --exec ./main.py --mode d -r """
    import argparse
    from typing import Literal, cast
    parser = argparse.ArgumentParser(description="Build, release, or run the application")
    parser.add_argument(
        "--mode", nargs="?",
        choices=["d","dev","development",
                "s","stg","staging",
                "p","prod","production"],
        default="production", help="Mode to run the application in (default: production)"
    )
    parser.add_argument("-r", "--rebuild", action="store_true", default=False, help="Force rebuild of the database, mode must also be development or staging")
    args = parser.parse_args()
    mode_map = {
        "development": "development", 
        "dev": "development",
        "d": "development",
        "staging": "staging",
        "stg": "staging", 
        "s": "staging",
        "production": "production",
        "prod": "production",
        "p": "production",
        }
    arg_mode = cast(ENV_MODE, mode_map[args.mode])
    os.environ["ENV_MODE"] = arg_mode
    build(mode=arg_mode, force_rebuild=args.rebuild)
    release()
    run()
