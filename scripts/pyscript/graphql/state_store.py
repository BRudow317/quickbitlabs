from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any


@dataclass
class ProcessedRecord:
    etag: str
    last_modified: str


class StateStore:
    """
    Minimal state:
      - item_id -> (etag, last_modified)

    This supports:
      - "compare to local processed state"
      - ignore already-processed versions on restart
    """

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / "processed_items.json"
        self.log = logging.getLogger("StateStore")
        self.items: Dict[str, ProcessedRecord] = {}

    def load(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.items = {}
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            items = {}
            for item_id, rec in (data.get("items") or {}).items():
                items[item_id] = ProcessedRecord(
                    etag=str(rec.get("etag") or ""),
                    last_modified=str(rec.get("last_modified") or ""),
                )
            self.items = items
        except Exception:
            self.log.exception("Failed loading state file; starting fresh.")
            self.items = {}

    def save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        data = {
            "items": {
                item_id: {"etag": rec.etag, "last_modified": rec.last_modified}
                for item_id, rec in self.items.items()
            }
        }
        tmp = self.path.with_suffix(".json.part")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def is_processed(self, item_id: str, etag: str, last_modified: str) -> bool:
        rec = self.items.get(item_id)
        if not rec:
            return False
        # Prefer etag match; fallback to last_modified match
        if rec.etag and etag and rec.etag == etag:
            return True
        if rec.last_modified and last_modified and rec.last_modified == last_modified:
            return True
        return False

    def mark_processed(self, item_id: str, etag: str, last_modified: str) -> None:
        self.items[item_id] = ProcessedRecord(etag=etag, last_modified=last_modified)
