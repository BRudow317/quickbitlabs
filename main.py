"""clear; python "./build/boot.py" -v  -l ./.logs --env homelab  --exec ./main.py"""
from __future__ import annotations
from typing import Literal

def build(mode: Literal["development", "staging", "production"] = "development"):
    from build.build_server import build_process
    build_process(mode=mode)

def release():
    import pytest
    pytest.main(["-v", "server/tests/"])

def run(mode: Literal["development", "staging", "production"] = "development"):
    from server.app import start_app
    start_app(mode=mode)

if __name__ == "__main__":
    build('development')
    release()
    run('development')