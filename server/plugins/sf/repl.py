"""
python master.py --config ./.env -l ./logs --exec server/connectors/sf/repl.py
"""
from __future__ import annotations

import cmd
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

# locals
from server.plugins.sf.Salesforce import Salesforce
from server.plugins.sf.engines.SfBulk2Engine import Bulk2SObject

# logging
import logging
logger = logging.getLogger(__name__)

class Repl(cmd.Cmd):
    intro = 'Welcome to the Project quickbitlabs Salesforce Console. Type help or ? to list commands.\n'

    def __init__(self, sf_instance: Salesforce, stdout: Any = None) -> None:
        super().__init__(stdout=stdout)
        self.sf = sf_instance
        self.api_mode = 'rest'        # 'rest' | 'bulk'
        self.return_type = 'records'  # 'records' | 'arrow'
        self.verbose = False
        self._last_result: pa.Table | None = None
        self._update_prompt()

    def _update_prompt(self) -> None:
        """Dynamically update the prompt to show the active API mode, return type, and current time."""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rt_flag = ' ARROW' if self.return_type == 'arrow' else ''
        v_flag = ' VERBOSE' if self.verbose else ''
        self.prompt = f'[{current_time}] Salesforce [{self.api_mode.upper()}{rt_flag}{v_flag}] ) '

    def postcmd(self, stop: bool, line: str) -> bool:
        """Hook method executed just after a command finishes running."""
        self._update_prompt()
        return stop

    def emptyline(self) -> bool:
        """Prevents the REPL from repeating the last command on an empty Enter press."""
        self._update_prompt()
        return False

    # -------------------------------------------------------------------------
    # Mode / settings commands
    # -------------------------------------------------------------------------

    def do_mode(self, arg: str) -> None:
        """Switch between standard REST and Bulk 2.0 API.
        Usage: mode rest
               mode bulk
        """
        new_mode = arg.strip().lower()
        if not new_mode:
            print(f"*** Current mode: {self.api_mode.upper()}", file=self.stdout)
            return
        if new_mode in ('rest', 'bulk'):
            self.api_mode = new_mode
            self._update_prompt()
            print(f"*** Switched to {self.api_mode.upper()} mode.", file=self.stdout)
        else:
            print(f"*** Error: Unknown mode '{arg}'. Available modes: rest, bulk", file=self.stdout)

    def do_returntype(self, arg: str) -> None:
        """Switch between Records (dicts) and ArrowStream return types.
        Usage: returntype records
               returntype arrow
        """
        new_rt = arg.strip().lower()
        if not new_rt:
            print(f"*** Current return type: {self.return_type.upper()}", file=self.stdout)
            return
        if new_rt in ('records', 'arrow'):
            self.return_type = new_rt
            self._update_prompt()
            print(f"*** Return type set to {self.return_type.upper()}.", file=self.stdout)
        else:
            print(f"*** Error: Unknown return type '{arg}'. Available: records, arrow", file=self.stdout)

    def do_verbose(self, arg: str) -> None:
        """Toggle verbose mode to show full raw requests and responses.
        Usage: verbose
               verbose on
               verbose off
        """
        a = arg.strip().lower()
        if a == 'on':
            self.verbose = True
        elif a == 'off':
            self.verbose = False
        else:
            self.verbose = not self.verbose
        self._update_prompt()
        print(f"*** Verbose {'ON' if self.verbose else 'OFF'}.", file=self.stdout)

    # -------------------------------------------------------------------------
    # Query
    # -------------------------------------------------------------------------

    def do_query(self, arg: str) -> None:
        """Execute a SOQL Query using the active mode and return type.
        Usage: query SELECT Id, Name FROM Account LIMIT 10
        """
        if not arg:
            print("*** Error: Query string required.", file=self.stdout)
            return

        self._last_result = None

        if self.verbose:
            print(f"\n--- REQUEST ---", file=self.stdout)
            print(f"Mode:        {self.api_mode.upper()}", file=self.stdout)
            print(f"Return Type: {self.return_type.upper()}", file=self.stdout)
            print(f"SOQL:        {arg}", file=self.stdout)

        print(f'Submitting {self.api_mode.upper()} Job...', file=self.stdout)

        try:
            if self.api_mode in ('bulk', 'bulk2'):
                self._do_bulk_query(arg)
            else:
                self._do_rest_query(arg)
        except Exception as e:
            print(f"*** SalesforceClient Error: {e}", file=self.stdout)

    def _do_rest_query(self, soql: str) -> None:
        if self.verbose:
            raw = self.sf.rest.query(soql)
            done_label = 'done' if raw.get('done') else 'more pages follow'
            print(f"\n--- RAW RESPONSE (page 1, {done_label}) ---", file=self.stdout)
            print(json.dumps(raw, indent=2, default=str), file=self.stdout)

        if self.return_type == 'arrow':
            stream = self.sf.service.rest_query(soql, return_type='ArrowStream')
            table = pa.Table.from_batches(list(stream))
            self._last_result = table if len(table) > 0 else None
            print(f"\n--- REST Results (Arrow, {len(table)} rows) ---", file=self.stdout)
            self._print_table_head(table)
        else:
            records = list(self.sf.service.rest_query(soql, return_type='Records'))
            self._last_result = pa.Table.from_pylist(records) if records else None
            print(f"\n--- REST Results ({len(records)} records) ---", file=self.stdout)
            print(json.dumps(records[:50], indent=2, default=str), file=self.stdout)
            if len(records) > 50:
                print(f"... and {len(records) - 50} more records.", file=self.stdout)

    def _do_bulk_query(self, soql: str) -> None:
        match = re.search(r'from\s+(\w+)', soql, re.IGNORECASE)
        if not match:
            print("*** Error: Could not parse SObject from query.", file=self.stdout)
            return
        sobject = match.group(1)

        print("Waiting for Salesforce to process the bulk job (usually 5-15 seconds)...", file=self.stdout)

        all_tables: list[pa.Table] = []
        sf_obj = getattr(self.sf.bulk2, sobject)

        for i, csv_bytes in enumerate(sf_obj.query(soql), 1):
            if self.verbose:
                print(f"\n--- RAW RESPONSE (Batch {i}, CSV) ---", file=self.stdout)
                preview = csv_bytes.decode('utf-8', errors='replace')
                if len(preview) > 2000:
                    print(preview[:2000], file=self.stdout)
                    print(f"... ({len(csv_bytes)} bytes total)", file=self.stdout)
                else:
                    print(preview, file=self.stdout)

            batch_table = Bulk2SObject.csv_bytes_to_arrow(csv_bytes)
            all_tables.append(batch_table)

            print(f"\n--- Batch {i} ({len(batch_table)} rows) ---", file=self.stdout)
            if self.return_type == 'arrow':
                self._print_table_head(batch_table)
            else:
                records = batch_table.to_pylist()
                print(json.dumps(records[:50], indent=2, default=str), file=self.stdout)
                if len(records) > 50:
                    print(f"... and {len(records) - 50} more rows.", file=self.stdout)

            if not self._prompt_continue():
                break

        if all_tables:
            try:
                self._last_result = pa.concat_tables(all_tables, promote_options='default')
            except Exception:
                self._last_result = all_tables[-1]

    def _print_table_head(self, table: pa.Table, n: int = 10) -> None:
        print(f"Schema: {table.schema}", file=self.stdout)
        if len(table) > 0:
            head = table.slice(0, min(n, len(table)))
            print(json.dumps(head.to_pylist(), indent=2, default=str), file=self.stdout)
            if len(table) > n:
                print(f"... and {len(table) - n} more rows.", file=self.stdout)

    def _prompt_continue(self) -> bool:
        """Prompt user to continue to next batch. Returns True to continue."""
        prompt_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.stdout.write(f"\n[{prompt_time}] Fetch next batch? (y/n): ")
        self.stdout.flush()
        cont = sys.stdin.readline().strip().lower()
        if cont in ('exit', 'quit'):
            print("\n*** Exited query pagination.", file=self.stdout)
            return False
        return cont in ('y', '')

    # -------------------------------------------------------------------------
    # Result inspection & export
    # -------------------------------------------------------------------------

    def do_result(self, arg: str) -> None:
        """Show info about the last stored query result.
        Usage: result
               result schema
               result head [n]
        """
        if self._last_result is None:
            print("*** No result in memory. Run a query first.", file=self.stdout)
            return

        table = self._last_result
        sub = arg.strip().lower()

        if sub == 'schema':
            print(f"{table.schema}", file=self.stdout)
        elif sub.startswith('head'):
            parts = sub.split()
            n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 10
            self._print_table_head(table, n)
        else:
            print(f"Rows:   {len(table)}", file=self.stdout)
            print(f"Schema: {table.schema}", file=self.stdout)

    def do_export(self, arg: str) -> None:
        """Export the last query result to a file.
        Usage: export csv /path/to/file.csv
               export json /path/to/file.json
               export parquet /path/to/file.parquet
        """
        if self._last_result is None:
            print("*** No result to export. Run a query first.", file=self.stdout)
            return

        parts = arg.strip().split(maxsplit=1)
        if len(parts) < 2:
            print("*** Error: Provide format and path. Example: export csv output.csv", file=self.stdout)
            return

        fmt, path_str = parts[0].lower(), parts[1].strip()
        path = Path(path_str)
        table = self._last_result

        try:
            if fmt == 'csv':
                pa_csv.write_csv(table, str(path))
                print(f"*** Exported {len(table)} rows to CSV: {path}", file=self.stdout)
            elif fmt == 'json':
                records = table.to_pylist()
                path.write_text(json.dumps(records, indent=2, default=str), encoding='utf-8')
                print(f"*** Exported {len(table)} rows to JSON: {path}", file=self.stdout)
            elif fmt == 'parquet':
                pq.write_table(table, str(path), compression='snappy')
                print(f"*** Exported {len(table)} rows to Parquet: {path}", file=self.stdout)
            else:
                print(f"*** Error: Unknown format '{fmt}'. Available: csv, json, parquet", file=self.stdout)
        except Exception as e:
            print(f"*** Export error: {e}", file=self.stdout)

    # -------------------------------------------------------------------------
    # Existing commands (insert, delete, describe)
    # -------------------------------------------------------------------------

    def do_insert(self, arg: str) -> None:
        """Insert a new record (or records).
        Usage: insert Contact {"FirstName": "John", "LastName": "Doe"}
               insert Account [{"Name": "Acme"}, {"Name": "Globex"}] (Bulk Mode)
        """
        if not arg:
            print("*** Error: Object name and JSON payload required.", file=self.stdout)
            return

        parts = arg.split(maxsplit=1)
        if len(parts) < 2:
            print("*** Error: Please provide both an SObject name and a JSON payload.", file=self.stdout)
            return

        object_name = parts[0].strip().title()
        json_str = parts[1].strip()

        try:
            payload = json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"*** Error: Invalid JSON format. {e}", file=self.stdout)
            return

        print(f'Submitting {self.api_mode.upper()} Insert for {object_name}...', file=self.stdout)

        try:
            if self.api_mode in ('bulk', 'bulk2'):
                records = [payload] if isinstance(payload, dict) else payload
                bulk_obj_handler = getattr(self.sf.bulk2, object_name)
                results = bulk_obj_handler.insert(records=records)
                print(f"\n--- BULK Insert Results ---", file=self.stdout)
                print(json.dumps(results, indent=2), file=self.stdout)
            else:
                if not isinstance(payload, dict):
                    print("*** Error: REST insert requires a single JSON object (dictionary).", file=self.stdout)
                    return
                rest_obj_handler = getattr(self.sf.rest, object_name)
                result = rest_obj_handler.create(payload)
                print(f"\n--- REST Insert Result ---", file=self.stdout)
                print(json.dumps(result, indent=2), file=self.stdout)
        except Exception as e:
            print(f"*** SalesforceClient Error: {e}", file=self.stdout)

    def do_delete(self, arg: str) -> None:
        """Delete a record (or records).
        Usage: delete Contact {"Id": "003fj00000jJV30AAG"}
               delete Account [{"Id": "001..."}, {"Id": "002..."}] (Bulk Mode)
        """
        if not arg:
            print("*** Error: Object name and JSON payload required.", file=self.stdout)
            return

        parts = arg.split(maxsplit=1)
        if len(parts) < 2:
            print("*** Error: Please provide both an SObject name and a JSON payload.", file=self.stdout)
            return

        object_name = parts[0].strip().title()
        json_str = parts[1].strip()

        try:
            payload = json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"*** Error: Invalid JSON format. {e}", file=self.stdout)
            return

        print(f'Submitting {self.api_mode.upper()} Delete for {object_name}...', file=self.stdout)

        try:
            if self.api_mode in ('bulk', 'bulk2'):
                records = [payload] if isinstance(payload, dict) else payload
                cleaned_records = []
                for r in records:
                    rec_id = r.get('Id') or r.get('id')
                    if not rec_id:
                        print(f"*** Error: Record missing 'Id' field: {r}", file=self.stdout)
                        return
                    cleaned_records.append({"Id": rec_id})
                bulk_obj_handler = getattr(self.sf.bulk2, object_name)
                results = bulk_obj_handler.delete(records=cleaned_records)
                print(f"\n--- BULK Delete Results ---", file=self.stdout)
                print(json.dumps(results, indent=2), file=self.stdout)
            else:
                if not isinstance(payload, dict):
                    print("*** Error: REST delete requires a single JSON object.", file=self.stdout)
                    return
                record_id = payload.get('Id') or payload.get('id')
                if not record_id:
                    print("*** Error: JSON payload must contain an 'Id' or 'id' key.", file=self.stdout)
                    return
                rest_obj_handler = getattr(self.sf.rest, object_name)
                status_code = rest_obj_handler.delete(record_id)
                print(f"\n--- REST Delete Result ---", file=self.stdout)
                if status_code == 204:
                    print(f"Success! (HTTP 204: Record Deleted)", file=self.stdout)
                else:
                    print(f"Unexpected Status: HTTP {status_code}", file=self.stdout)
        except Exception as e:
            print(f"*** SalesforceClient Error: {e}", file=self.stdout)

    def do_describe(self, arg: str) -> None:
        """Describe an object's metadata. Usage: describe Account"""
        if not arg:
            print("*** Error: Object name required.", file=self.stdout)
            return
        try:
            meta: Any = getattr(self.sf.rest, arg).describe()
            print(json.dumps(meta, indent=2), file=self.stdout)
        except Exception as e:
            print(f"*** Error: {e}", file=self.stdout)

    def do_exit(self, arg: str) -> bool:
        """Exit the console."""
        print("Closing session...", file=self.stdout)
        return True

    def do_EOF(self, arg: str) -> bool:
        print("", file=self.stdout)
        return True

def main() -> None:
    sf = Salesforce()
    Repl(sf, stdout=sys.stdout).cmdloop()

if __name__ == '__main__':
    main()
