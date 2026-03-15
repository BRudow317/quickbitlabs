from __future__ import annotations

from server.services.migration import run_migration

if __name__ == "__main__":
    run_migration()

# python master.py --config ./.env -l ./logs -v --exec python -m run