from __future__ import annotations
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Run a SQL function from the command line.")
    parser.add_argument("oracle_user", help="Database username", default='')
    parser.add_argument("oracle_pass", help="Database password", default='')
    parser.add_argument("oracle_sid", help="Oracle SID", default='')
    parser.add_argument("oracle_service", help="Oracle Service", default='')
    parser.add_argument("oracle_host", help="Oracle Host", default='')
    parser.add_argument("oracle_port", help="Oracle Port", default=1521)
    parser.add_argument("package", help="Package name", default='')
    parser.add_argument("function", help="Function name", default='')
    parser.add_argument("file", help="File name to pass as an argument to the SQL function", default='')
    args = parser.parse_args()

    result = subprocess.run([
            args.oracle_user,
            args.oracle_pass,
            args.oracle_sid,
            args.oracle_service,
            args.oracle_host,
            args.oracle_port,
            args.package,
            args.function,
            args.file,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    else:
        print(f"Output: {result.stdout}")