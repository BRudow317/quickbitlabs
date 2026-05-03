import oracledb
import logging
logger: logging.Logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def table_exists(cur: oracledb.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :tn",
        {"tn": table_name.upper()},
    )
    return (cur.fetchone() or (0,))[0] > 0


def column_exists(cur: oracledb.Cursor, table_name: str, column_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :t AND COLUMN_NAME = :c",
        {"t": table_name.upper(), "c": column_name.upper()},
    )
    return (cur.fetchone() or (0,))[0] > 0


def index_exists(cur: oracledb.Cursor, index_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM USER_INDEXES WHERE INDEX_NAME = :idx",
        {"idx": index_name.upper()},
    )
    return (cur.fetchone() or (0,))[0] > 0


def constraint_exists(cur: oracledb.Cursor, constraint_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE CONSTRAINT_NAME = :cn",
        {"cn": constraint_name.upper()},
    )
    return (cur.fetchone() or (0,))[0] > 0


def run(cur: oracledb.Cursor, sql: str, label: str) -> None:
    try:
        cur.execute(sql)
        logger.info(f"  OK  {label}")
    except oracledb.DatabaseError as exc:
        (error,) = exc.args
        logger.warning(f"  SKIP {label} - {error.message.strip()}")
