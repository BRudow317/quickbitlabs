from __future__ import annotations

from server.tools.sync_systems_to_db import sync_all

if __name__ == "__main__":
    counts = sync_all()
    print("Sync results:", counts)
