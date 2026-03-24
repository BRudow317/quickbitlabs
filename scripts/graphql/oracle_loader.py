from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Optional

import oracledb

from excel_introspect import SheetPlan, iter_sheet_rows, sanitize_identifier


log = logging.getLogger("oracle_loader")


@dataclass(frozen=True)
class OracleConfig:
    dsn: str
    user: str
    password: str
    swap_mode: str = "view"          # view|synonym
    ident_max: int = 30
    varchar2_len: int = 4000
    grant_to: List[str] = None
    retain_versions: int = 3


class OracleLoader:
    def __init__(self, cfg: OracleConfig) -> None:
        self.cfg = cfg
        self.conn: Optional[oracledb.Connection] = None

    def __enter__(self) -> "OracleLoader":
        # Thin mode by default
        self.conn = oracledb.connect(user=self.cfg.user, password=self.cfg.password, dsn=self.cfg.dsn)
        self.conn.autocommit = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self.conn:
                if exc:
                    self.conn.rollback()
                else:
                    self.conn.commit()
        finally:
            if self.conn:
                self.conn.close()

    def _exec(self, sql: str, params=None) -> None:
        assert self.conn is not None
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params or {})
        finally:
            cur.close()

    def _query(self, sql: str, params=None) -> List[Tuple]:
        assert self.conn is not None
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params or {})
            return cur.fetchall()
        finally:
            cur.close()

    def _physical_name(self, logical_name: str) -> str:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw = f"PHYS_{logical_name}_{stamp}"
        return sanitize_identifier(raw, max_len=self.cfg.ident_max, prefix="T")

    def _logical_name(self, logical_name: str) -> str:
        # Keep logical stable; optionally prefix to avoid collisions
        return sanitize_identifier(f"LOG_{logical_name}", max_len=self.cfg.ident_max, prefix="T")

    def _create_table(self, table_name: str, columns: List[str], varchar2_len: int) -> None:
        cols = ", ".join([f"{c} VARCHAR2({varchar2_len})" for c in columns])
        sql = f"CREATE TABLE {table_name} ({cols})"
        self._exec(sql)

    def _drop_table_if_exists(self, table_name: str) -> None:
        # Best-effort drop
        try:
            self._exec(f"DROP TABLE {table_name} PURGE")
        except oracledb.DatabaseError as e:
            msg = str(e)
            if "ORA-00942" in msg:  # table or view does not exist
                return
            raise

    def _swap_logical(self, logical: str, physical: str, columns: List[str]) -> None:
        mode = (self.cfg.swap_mode or "view").lower()
        if mode == "synonym":
            self._exec(f"CREATE OR REPLACE SYNONYM {logical} FOR {physical}")
        else:
            # View is usually the safest: privileges remain stable and Tableau can query it.
            self._exec(f"CREATE OR REPLACE VIEW {logical} AS SELECT * FROM {physical}")

    def _grant_select(self, object_name: str) -> None:
        grantees = self.cfg.grant_to or []
        for g in grantees:
            self._exec(f"GRANT SELECT ON {object_name} TO {g}")

    def _cleanup_old_versions(self, logical_name: str) -> None:
        """
        Keep newest N physical tables for this logical base.
        """
        keep = max(0, int(self.cfg.retain_versions or 0))
        if keep == 0:
            return

        # Physical tables are prefixed PHYS_<logical>_YYYYMMDD_HHMMSS (sanitized).
        # We select by LIKE 'PHYS_<logical>%'
        prefix = sanitize_identifier(f"PHYS_{logical_name}_", max_len=self.cfg.ident_max, prefix="T")
        like = prefix + "%"

        rows = self._query(
            "SELECT table_name FROM user_tables WHERE table_name LIKE :like ORDER BY table_name DESC",
            {"like": like},
        )
        names = [r[0] for r in rows]
        for old in names[keep:]:
            try:
                self._exec(f"DROP TABLE {old} PURGE")
            except Exception:
                log.exception("Failed dropping old table: %s", old)

    def load_sheet_atomic(self, sheet_plan: SheetPlan, source_file: str, source_item_id: str) -> None:
        """
        Atomic replacement:
          1) create physical table
          2) load data
          3) swap logical (view/synonym)
          4) grant select (mode-dependent)
          5) cleanup old physical versions
        """
        assert self.conn is not None
        cur = self.conn.cursor()

        logical = self._logical_name(sheet_plan.logical_name)
        physical = self._physical_name(sheet_plan.logical_name)

        try:
            log.info("Create physical table: %s", physical)
            self._create_table(physical, sheet_plan.columns, sheet_plan.varchar2_len)

            # Insert
            col_list = ", ".join(sheet_plan.columns)
            bind_list = ", ".join([f":{i+1}" for i in range(len(sheet_plan.columns))])
            insert_sql = f"INSERT INTO {physical} ({col_list}) VALUES ({bind_list})"

            total_rows = 0
            try:
                for batch in iter_sheet_rows(sheet_plan, batch_size=5000):
                    cur.executemany(insert_sql, batch)
                    total_rows += len(batch)
                self.conn.commit()
            except oracledb.DatabaseError:
                log.exception("Oracle insert failed (file=%s sheet=%s).", source_file, sheet_plan.sheet_name)
                raise

            log.info("Loaded rows=%s into %s", total_rows, physical)

            # Swap logical to new physical
            self._swap_logical(logical=logical, physical=physical, columns=sheet_plan.columns)

            # Grants:
            # - if view: grant on logical once (still safe to re-run)
            # - if synonym: need to grant on *physical* because synonym doesn't carry privilege
            if (self.cfg.swap_mode or "view").lower() == "synonym":
                self._grant_select(physical)
            else:
                self._grant_select(logical)

            self.conn.commit()

            # Cleanup older physical tables
            self._cleanup_old_versions(sheet_plan.logical_name)

        except Exception:
            # On failure, do not touch logical; drop physical if created
            try:
                self.conn.rollback()
            except Exception:
                pass
            try:
                self._drop_table_if_exists(physical)
            except Exception:
                log.exception("Failed cleaning up physical table after error: %s", physical)
            raise
        finally:
            cur.close()
