"""
python Q:/scripts/boot.py -v -l ./.logs --env homelab --config ../.secrets/.env --exec ./main.py
"""
from __future__ import annotations
import sys
from pathlib import Path

# Resolve project root dynamically and add to sys.path before imports
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn
from server.start_server import app

def main():
    # app = server.start_server.create_app()
    uvicorn.run("server.start_server:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    main()
    # from server.services.sync_systems import sync_all
    # counts = sync_all()