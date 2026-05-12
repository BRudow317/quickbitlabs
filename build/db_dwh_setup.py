from __future__ import annotations
import json
import logging
import sys
from pathlib import Path

from build.db_setup import sql_runner

logger = logging.getLogger(__name__)

MOCK_DATA_DIR = Path(__file__).parent / "sql" / "mock_data"

LOAD_ORDER = [
    "mq_lookup.json",
    "account.json",
    "employee.json",
    "demographic.json",
    "phone.json",
    "address.json",
    "employment.json",
]

TABLE_CONFIG: dict[str, dict] = {
    "mq_lookup.json": {
        "table": "dwh.mq_lookup",
        "columns": ["lookup_id", "category", "lookup_desc"],
    },
    "account.json": {
        "table": "dwh.account",
        "columns": ["account_id", "account_code", "account_name", "account_status_id", "account_type_id"],
    },
    "employee.json": {
        "table": "dwh.employee",
        "columns": ["employee_id", "ssn"],
    },
    "demographic.json": {
        "table": "dwh.demographic",
        "columns": [
            "demographic_id", "employee_id", "first_name", "middle_name", "last_name",
            "prefix_id", "suffix_id", "birthdate", "dt_of_death", "gender_id",
            "mar_status_id", "email_addr", "deleted_status",
        ],
    },
    "phone.json": {
        "table": "dwh.phone",
        "columns": ["phone_id", "demographic_id", "phone", "phone_ext", "phone_type_id"],
    },
    "address.json": {
        "table": "dwh.address",
        "columns": [
            "address_id", "demographic_id", "address1", "address2", "address3", "address4",
            "city", "county", "state", "postal", "country", "address_type_id",
        ],
    },
    "employment.json": {
        "table": "dwh.employment",
        "columns": ["employment_id", "demographic_id", "account_code", "hire_dt", "termination_dt"],
    },
}

DATE_FIELDS = {"birthdate", "dt_of_death", "hire_dt", "termination_dt"}


def _fmt(val, col: str) -> str:
    if val is None:
        return "NULL"
    if col in DATE_FIELDS:
        return f"DATE '{val}'"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


_EXTRA_TRUNCATE_FIRST = ["dwh.census", "dwh.mq_pkg_log"]

def _truncate_sql() -> str:
    extra = "\n".join(f"TRUNCATE TABLE {t};" for t in _EXTRA_TRUNCATE_FIRST)
    main  = "\n".join(f"TRUNCATE TABLE {TABLE_CONFIG[f]['table']};" for f in reversed(LOAD_ORDER))
    return f"{extra}\n{main}"


def _insert_sql(fname: str) -> str:
    cfg = TABLE_CONFIG[fname]
    table = cfg["table"]
    columns = cfg["columns"]
    col_list = ", ".join(columns)

    data: list[dict] = json.loads((MOCK_DATA_DIR / fname).read_text(encoding="utf-8"))
    lines = []
    for row in data:
        vals = ", ".join(_fmt(row.get(col), col) for col in columns)
        lines.append(f"INSERT INTO {table} ({col_list}) VALUES ({vals});")
    lines.append("COMMIT;")
    return "\n".join(lines)


def _run(raw_sql: str, label: str) -> None:
    result = sql_runner(raw_sql, username="SYSDBA")
    stdout_upper = result.stdout.upper()
    if result.returncode != 0 or "ORA-" in stdout_upper or "SP2-" in stdout_upper:
        logger.error("%s failed:\n%s", label, result.stdout.strip())
        sys.exit(1)


def _load_mock_data() -> None:
    logger.info("Truncating DWH mock tables...")
    _run(_truncate_sql(), "truncate")

    logger.info("Loading mock data into DWH...")
    for fname in LOAD_ORDER:
        table = TABLE_CONFIG[fname]["table"]
        _run(_insert_sql(fname), fname)
        logger.info("  %s loaded.", table)


def dwh_setup() -> None:
    from build.sql.mock_data.generate_mock_data import generate_full_mock_data

    logger.info("Generating mock data...")
    generate_full_mock_data(MOCK_DATA_DIR)
    _load_mock_data()


if __name__ == "__main__":
    dwh_setup()
