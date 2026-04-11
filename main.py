"""
python ./scripts/boot.py -v -l ./.logs --env homelab --config ../.secrets/.env --exec ./main.py
"""
from __future__ import annotations
import uvicorn

from server.services.FullMigration import run

def main():
    uvicorn.run("server.start_server:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    run()