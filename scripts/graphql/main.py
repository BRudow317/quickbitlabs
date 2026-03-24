#!/usr/bin/env python3
"""
Orchestrates:
1) SharePoint folder monitoring (Graph)
2) XLSX parsing -> per-sheet table plan
3) Oracle load into new physical table
4) Atomic swap of logical name (view or synonym)
5) State update + local processed copy

Environment variables (minimum):
  TENANT_ID, CLIENT_ID
  SP_DRIVE_ID, SP_FOLDER_ITEM_ID
  ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD

Optional:
  STATE_DIR=.state
  LANDING_DIR=landing
  PROCESSED_DIR=processed
  LOG_DIR=logs
  POLL_SECONDS=30
  INITIAL_MODE=process_existing|ignore_existing   (default process_existing)
  ORACLE_SWAP_MODE=view|synonym                  (default view)
  ORACLE_IDENT_MAX=30                            (default 30)
  ORACLE_VARCHAR2_LEN=4000                       (default 4000)
  ORACLE_GRANT_TO=ROLE1,ROLE2                    (optional)
  RETAIN_VERSIONS=3                              (default 3)
  KEEP_PROCESSED_HISTORY=1                       (default 0)
  TRUNCATE_OVERFLOW=truncate|error               (default truncate)
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from graph_watcher import GraphWatcher, ChangedItem
from state_store import StateStore
from excel_introspect import WorkbookPlan, build_workbook_plan
from oracle_loader import OracleLoader, OracleConfig


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "ingest.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def safe_copy_processed(
    src_file: Path,
    processed_dir: Path,
    dataset_key: str,
    keep_history: bool,
) -> None:
    ds_dir = processed_dir / dataset_key
    ds_dir.mkdir(parents=True, exist_ok=True)

    latest = ds_dir / "latest.xlsx"
    tmp = ds_dir / "latest.xlsx.part"
    shutil.copy2(src_file, tmp)
    tmp.replace(latest)

    if keep_history:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hist = ds_dir / f"{stamp}.xlsx"
        shutil.copy2(src_file, hist)


def process_item(
    loader: OracleLoader,
    state: StateStore,
    landing_dir: Path,
    processed_dir: Path,
    keep_processed_history: bool,
    truncate_overflow: str,
    item: ChangedItem,
) -> None:
    log = logging.getLogger("process_item")

    # Decide if we should process based on stored etag/mtime
    if state.is_processed(item_id=item.item_id, etag=item.etag, last_modified=item.last_modified):
        log.info("Skip (already processed): %s", item.name)
        return

    landing_dir.mkdir(parents=True, exist_ok=True)
    local_path = landing_dir / item.name
    log.info("Downloading: %s", item.name)
    item.download_to(local_path)

    # Build workbook plan (per visible, non-blank sheet)
    plan: WorkbookPlan = build_workbook_plan(
        xlsx_path=local_path,
        truncate_overflow=truncate_overflow,
    )
    if not plan.sheets:
        log.warning("No visible non-blank sheets found: %s", item.name)
        state.mark_processed(item_id=item.item_id, etag=item.etag, last_modified=item.last_modified)
        safe_copy_processed(local_path, processed_dir, plan.dataset_key, keep_processed_history)
        return

    # Load each sheet into its own logical table name (filename or filename_sheet)
    for sheet in plan.sheets:
        log.info("Loading sheet '%s' -> logical '%s'", sheet.sheet_name, sheet.logical_name)
        loader.load_sheet_atomic(
            sheet_plan=sheet,
            source_file=item.name,
            source_item_id=item.item_id,
        )

    # Mark processed and keep a local processed copy for operator sanity / diffing
    state.mark_processed(item_id=item.item_id, etag=item.etag, last_modified=item.last_modified)
    safe_copy_processed(local_path, processed_dir, plan.dataset_key, keep_processed_history)

    # Optional: remove landing file
    try:
        local_path.unlink(missing_ok=True)
    except Exception:
        log.exception("Failed to remove landing file: %s", local_path)


def main() -> int:
    state_dir = Path(os.getenv("STATE_DIR", ".state"))
    landing_dir = Path(os.getenv("LANDING_DIR", "landing"))
    processed_dir = Path(os.getenv("PROCESSED_DIR", "processed"))
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    setup_logging(log_dir)

    log = logging.getLogger("main")

    tenant_id = _env("TENANT_ID")
    client_id = _env("CLIENT_ID")
    drive_id = _env("SP_DRIVE_ID")
    folder_item_id = _env("SP_FOLDER_ITEM_ID")

    poll_seconds = int(os.getenv("POLL_SECONDS", "30"))
    initial_mode = os.getenv("INITIAL_MODE", "process_existing").strip().lower()
    keep_processed_history = os.getenv("KEEP_PROCESSED_HISTORY", "0") == "1"
    truncate_overflow = os.getenv("TRUNCATE_OVERFLOW", "truncate").strip().lower()

    oracle_cfg = OracleConfig(
        dsn=_env("ORACLE_DSN"),
        user=_env("ORACLE_USER"),
        password=_env("ORACLE_PASSWORD"),
        swap_mode=os.getenv("ORACLE_SWAP_MODE", "view").strip().lower(),
        ident_max=int(os.getenv("ORACLE_IDENT_MAX", "30")),
        varchar2_len=int(os.getenv("ORACLE_VARCHAR2_LEN", "4000")),
        grant_to=[x.strip() for x in os.getenv("ORACLE_GRANT_TO", "").split(",") if x.strip()],
        retain_versions=int(os.getenv("RETAIN_VERSIONS", "3")),
    )

    state = StateStore(state_dir=state_dir)
    state.load()

    watcher = GraphWatcher(
        tenant_id=tenant_id,
        client_id=client_id,
        drive_id=drive_id,
        folder_item_id=folder_item_id,
        state_dir=state_dir,
        poll_seconds=poll_seconds,
        initial_mode=initial_mode,
    )

    with OracleLoader(cfg=oracle_cfg) as loader:
        log.info("Watcher started (poll=%ss, initial_mode=%s)", poll_seconds, initial_mode)
        for changed in watcher.iter_changed_items(state=state):
            try:
                process_item(
                    loader=loader,
                    state=state,
                    landing_dir=landing_dir,
                    processed_dir=processed_dir,
                    keep_processed_history=keep_processed_history,
                    truncate_overflow=truncate_overflow,
                    item=changed,
                )
                state.save()
            except Exception:
                log.exception("Failed processing item: %s", changed.name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
