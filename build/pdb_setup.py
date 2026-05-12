import os
import sys
import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def _run_sysdba_query(sql_command: str, container: str | None = None) -> str:
    sys_user = os.getenv("ORACLE_SYSDBA_USER", default="SYS")
    sys_pass = os.getenv("ORACLE_SYSDBA_PWD")
    sys_host = os.getenv("ORACLE_SYSDBA_HOST", default="localhost")
    sys_port = os.getenv("ORACLE_SYSDBA_PORT", default="1521")
    sys_sid = os.getenv("ORACLE_SYSDBA_SID")

    logger.debug("SYSDBA Connection Details: user=%s, host=%s, port=%s, sid=%s", sys_user, sys_host, sys_port, sys_sid)

    if not sys_pass:
        logger.error("Missing 'ORACLE_SYSDBA_PWD' environment variable.")
        sys.exit(1)

    conn_str = f"{sys_user}/\"{sys_pass}\"@{sys_host}:{sys_port}/{sys_sid} AS SYSDBA"
    
    session_setup = f"ALTER SESSION SET CONTAINER = {container};\n" if container else ""
    
    safe_sql = f"""
    SET PAGESIZE 0 
    SET FEEDBACK OFF 
    SET VERIFY OFF 
    SET HEADING OFF 
    SET ECHO OFF 
    SET TRIMSPOOL ON
    SET SQLBLANKLINES ON
    WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;
    {session_setup}
    {sql_command}
    EXIT;
    """
    #logger.debug("Executing SYSDBA Query:\n%s", safe_sql)
    logger.debug("Executing SYSDBA Query...")
    try:
        result: subprocess.CompletedProcess = subprocess.run(
            ["sqlplus", "-s", conn_str],
            input=safe_sql,
            text=True,
            capture_output=True,
            check=False
        )
        logger.debug("SYSDBA Query Return Code: %d", result.returncode)
        if result.returncode != 0 or "ORA-" in result.stdout:
            logger.error("Error: SYSDBA Execution Failed:\n%s", result.stdout.strip())
            sys.exit(1)
        logger.info("Success: SYSDBA Query Executed.")
        return result.stdout.strip()
    except FileNotFoundError:
        logger.error("sqlplus not found on system PATH.")
        sys.exit(1)

def create_pdb_from_seed(pdb_name: str, admin_user: str, admin_pwd: str) -> None:
    create_sql = f"""
    CREATE PLUGGABLE DATABASE {pdb_name}
    ADMIN USER {admin_user} IDENTIFIED BY "{admin_pwd}";

    ALTER PLUGGABLE DATABASE {pdb_name} OPEN;
    ALTER PLUGGABLE DATABASE {pdb_name} SAVE STATE;
    ALTER SYSTEM REGISTER;
    """
    _run_sysdba_query(create_sql)

    storage_sql = """
    CREATE TABLESPACE USERS;
    ALTER DATABASE DEFAULT TABLESPACE USERS;
    """
    _run_sysdba_query(storage_sql, container=pdb_name)

    logger.info("PDB '%s' created and opened successfully.", pdb_name)

def orchestrate_user(user_name: str, is_admin: bool = False) -> None:
    pdb_name = os.getenv("ORACLE_PDB", "QBLPDB").upper()
    app_user = os.getenv(f"ORACLE_{user_name.upper()}_USER")
    app_pwd = os.getenv(f"ORACLE_{user_name.upper()}_PWD")

    if not app_user or not app_pwd:
        logger.error("Missing app user credentials in environment variables.")
        sys.exit(1)
    # Every user needs at least CREATE SESSION to log in
    grants = f"GRANT CREATE SESSION TO {app_user};"
    
    if is_admin:
        grants += f"\n    GRANT DBA TO {app_user};"
    else:
        # Give standard users the ability to create tables and use tablespace storage
        grants += f"""
            GRANT RESOURCE TO {app_user};
            GRANT CREATE VIEW TO {app_user};
            GRANT CREATE SYNONYM TO {app_user};
            GRANT CREATE MATERIALIZED VIEW TO {app_user};
            GRANT CREATE ANY DIRECTORY TO {app_user};
            ALTER USER {app_user} QUOTA UNLIMITED ON USERS;"""

    user_sql = f"""
    DECLARE
       v_count NUMBER;
    BEGIN
       SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = UPPER('{app_user}');
       IF v_count = 0 THEN
          EXECUTE IMMEDIATE 'CREATE USER {app_user} IDENTIFIED BY "{app_pwd}"';
       ELSE
          EXECUTE IMMEDIATE 'ALTER USER {app_user} IDENTIFIED BY "{app_pwd}"';
       END IF;
    END;
    /

    {grants}
    """
    _run_sysdba_query(user_sql, container=pdb_name)

def orchestrate_pdb(pgdb_base_name: str, force_rebuild: bool = False) -> None:
    logger.debug("Starting PDB orchestration with base name '%s' and force_rebuild=%s", pgdb_base_name, force_rebuild)
    pdb_name = f"{pgdb_base_name}PDB".upper()
    admin_user = os.getenv(f"ORACLE_{pdb_name}_ADMIN_USER", "admin")
    admin_pwd = os.getenv(f"ORACLE_{pdb_name}_ADMIN_PWD", "password")

    count_output = _run_sysdba_query(f"SELECT COUNT(*) FROM V$PDBS WHERE UPPER(NAME) = '{pdb_name}';")
    exists = count_output == "1"

    if exists:
        if force_rebuild:
            logger.info("PDB '%s' already exists. Force rebuild enabled, dropping existing PDB.", pdb_name)
            _run_sysdba_query(f"""
                ALTER PLUGGABLE DATABASE {pdb_name} CLOSE IMMEDIATE;
                DROP PLUGGABLE DATABASE {pdb_name} INCLUDING DATAFILES;
            """)
            logger.info("PDB '%s' dropped successfully.", pdb_name)
            create_pdb_from_seed(pdb_name, admin_user, admin_pwd)
        else:
            logger.info("PDB '%s' already exists. Skipping creation.", pdb_name)
    else:
        create_pdb_from_seed(pdb_name, admin_user, admin_pwd)

    orchestrate_user("QBL")
    orchestrate_user("DWH")
    # orchestrate_user("QBLPDB_ADMIN", is_admin=True)

if __name__ == "__main__":
    """clear; python ./boot.py -v  -l ./.logs --env qbl  --exec ./build/pdb_setup.py -n "QBL" -f"""
    import argparse
    parser = argparse.ArgumentParser(prog="pdb_setup", description="PDB Setup Script")
    parser.add_argument("-n", "--pgdb", type=str, default="", help="Base name for the PDB (default: QBL)")
    parser.add_argument("-f", "--force", action="store_true", default=False, help="Force rebuild of PDB if it already exists")
    args: argparse.Namespace = parser.parse_args()
    logger.debug("Parsed arguments: pgdb=%s, force=%s", args.pgdb, args.force)
    if not args.pgdb:
        logger.error("Base name for PDB is required. Use -n or --pgdb to specify it.")
        sys.exit(1)
    orchestrate_pdb(pgdb_base_name=args.pgdb, force_rebuild=args.force)