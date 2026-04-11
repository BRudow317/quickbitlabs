"""
python master.py --config ./.env -l ./logs --exec server/connectors/sf/repl.py
"""
from __future__ import annotations

import cmd
import sys
import json
import re
from datetime import datetime
from typing import Any
from collections.abc import Iterable

# locals
from server.plugins.sf.Salesforce import Salesforce

# logging
import logging
logger = logging.getLogger(__name__)

class Repl(cmd.Cmd):
    intro = 'Welcome to the Project quickbitlabs Salesforce Console. Type help or ? to list commands.\n'
    
    def __init__(self, sf_instance: Salesforce, stdout: Any = None) -> None:
        super().__init__(stdout=stdout)
        self.sf = sf_instance
        self.api_mode = 'rest'  # Default mode
        self._update_prompt()

    def _update_prompt(self) -> None:
        """Dynamically update the prompt to show the active API mode and current time."""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.prompt = f'[{current_time}] Salesforce [{self.api_mode.upper()}] ) '

    def postcmd(self, stop: bool, line: str) -> bool:
        """Hook method executed just after a command finishes running."""
        self._update_prompt()
        return stop

    def emptyline(self) -> bool:
        """Prevents the REPL from repeating the last command on an empty Enter press."""
        self._update_prompt()
        return False

    def do_mode(self, arg: str) -> None:
        """Switch between standard REST and Bulk 2.0 API.
        Usage: mode rest
               mode bulk
        """
        new_mode = arg.strip().lower()
        if not new_mode:
            print(f"*** Current mode is: {self.api_mode.upper()}", file=self.stdout)
            return
        if new_mode in ('rest', 'bulk'):
            self.api_mode = new_mode
            self._update_prompt()
            print(f"*** Switched to {self.api_mode.upper()} mode.", file=self.stdout)
        else:
            print(f"*** Error: Unknown mode '{arg}'. Available modes: rest, bulk", file=self.stdout)

    def do_query(self, arg: str) -> None:
        """Execute a SOQL Query using the active mode.
        Usage: query SELECT Id, Name FROM Account LIMIT 10
        """
        if not arg:
            print("*** Error: Query string required.", file=self.stdout)
            return

        print(f'Submitting {self.api_mode.upper()} Job...', file=self.stdout)
        
        if self.api_mode in ('bulk', 'bulk2'):
            print("Waiting for Salesforce to process the bulk job in the cloud (this usually takes 5-15 seconds)...", file=self.stdout)

        try:
            if self.api_mode in ('bulk', 'bulk2'):
                # Access the explicit bulk2 property
                bulk2_handler = self.sf.bulk2
                
                match = re.search(r'from\s+(\w+)', arg, re.IGNORECASE)
                if not match:
                    print("*** Error: Could not parse SObject from query.", file=self.stdout)
                    return
                sobject = match.group(1)
                
                bulk_obj_handler = getattr(bulk2_handler, sobject)
                results: Iterable[Any] = bulk_obj_handler.query(arg)
                
                for i, chunk in enumerate(results, start=1):
                    print(f"\n--- Batch {i} ---", file=self.stdout)
                    chunk_text = str(chunk)
                    suffix = '...' if len(chunk_text) > 1000 else ''
                    print(chunk_text[:1000] + suffix, file=self.stdout)
                    
                    prompt_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    self.stdout.write(f"\n[{prompt_time}] Fetch next batch? (y/n): ")
                    self.stdout.flush()
                    cont = sys.stdin.readline().strip().lower()
                    
                    if cont in ('exit', 'quit'):
                        print("\n*** Exited query pagination. Returning to main menu.", file=self.stdout)
                        break
                    
                    if cont not in ('y', ''):
                        break

            else:
                # REST Mode: explicitly target sf.rest.query
                results = self.sf.rest.query(arg)
                records = results.get('records', [])
                print(f"\n--- REST Results (Total: {results.get('totalSize')}) ---", file=self.stdout)
                print(json.dumps(records[:50], indent=2), file=self.stdout)
                if len(records) > 50:
                    print(f"... and {len(records) - 50} more records.", file=self.stdout)

        except Exception as e:
            print(f"*** SalesforceClient Error: {e}", file=self.stdout)
    
    
    def do_insert(self, arg: str) -> None:
        """Insert a new record (or records).
        Usage: insert Contact {"FirstName": "John", "LastName": "Doe"}
               insert Account [{"Name": "Acme"}, {"Name": "Globex"}] (Bulk Mode)
        """
        if not arg:
            print("*** Error: Object name and JSON payload required.", file=self.stdout)
            return

        # Split the argument into two pieces: ObjectName and the JSON string
        parts = arg.split(maxsplit=1)
        if len(parts) < 2:
            print("*** Error: Please provide both an SObject name and a JSON payload.", file=self.stdout)
            return

        # Capitalize the object name (e.g., 'contact' -> 'Contact') to match Salesforce API specs
        object_name = parts[0].strip().title()
        json_str = parts[1].strip()

        try:
            # Safely parse the user's JSON string into a Python dictionary or list
            payload = json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"*** Error: Invalid JSON format. {e}", file=self.stdout)
            return

        print(f'Submitting {self.api_mode.upper()} Insert for {object_name}...', file=self.stdout)

        try:
            if self.api_mode in ('bulk', 'bulk2'):
                # Bulk V2 expects a list of dictionaries. Wrap it if the user only provided one.
                records = [payload] if isinstance(payload, dict) else payload
                
                bulk_obj_handler = getattr(self.sf.bulk2, object_name)
                results = bulk_obj_handler.insert(records=records)
                
                print(f"\n--- BULK Insert Results ---", file=self.stdout)
                print(json.dumps(results, indent=2), file=self.stdout)

            else:
                # REST mode expects a single dictionary
                if not isinstance(payload, dict):
                    print("*** Error: REST insert requires a single JSON object (dictionary).", file=self.stdout)
                    return
                
                # Dynamically call self.sf.rest.ObjectName.create()
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
                
                # Bulk V2 STRICTLY requires the key to be "Id" (not "id"). 
                # We normalize it here so the user's JSON doesn't cause a CSV header error.
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
                # REST Mode
                if not isinstance(payload, dict):
                    print("*** Error: REST delete requires a single JSON object.", file=self.stdout)
                    return
                
                # Extract the ID from the payload
                record_id = payload.get('Id') or payload.get('id')
                if not record_id:
                    print("*** Error: JSON payload must contain an 'Id' or 'id' key.", file=self.stdout)
                    return
                
                rest_obj_handler = getattr(self.sf.rest, object_name)
                status_code = rest_obj_handler.delete(record_id)
                
                # Salesforce returns 204 No Content on a successful REST delete
                if status_code == 204:
                    print(f"\n--- REST Delete Result ---", file=self.stdout)
                    print(f"Success! (HTTP 204: Record Deleted)", file=self.stdout)
                else:
                    print(f"\n--- REST Delete Result ---", file=self.stdout)
                    print(f"Unexpected Status: HTTP {status_code}", file=self.stdout)

        except Exception as e:
            print(f"*** SalesforceClient Error: {e}", file=self.stdout)

    def do_describe(self, arg: str) -> None:
        """Describe an object's metadata. Usage: describe Account"""
        if not arg:
            print("*** Error: Object name required.", file=self.stdout)
            return
        try:
            # Target sf.rest for SObject magic attribute access
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