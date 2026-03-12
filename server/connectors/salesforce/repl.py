from __future__ import annotations
import cmd
import logging
import sys
from typing import Any, Iterable, TYPE_CHECKING
import json
from datetime import datetime

if TYPE_CHECKING:
    from api import SfSession

logger = logging.getLogger(__name__)

class Repl(cmd.Cmd):
    intro = 'Welcome to the Project SfSession Console. Type help or ? to list commands.\n'
    
    # Accept a custom stdout stream (defaults to None, which cmd resolves to sys.stdout)
    def __init__(self, sf_instance: SfSession, stdout: Any = None) -> None:
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
        
        if self.api_mode == 'bulk':
            print("Waiting for Salesforce to process the bulk job in the cloud (this usually takes 5-15 seconds)...", file=self.stdout)

        try:
            if self.api_mode == 'bulk':
                bulk2_handler: Any = getattr(self.sf, 'bulk2')
                
                import re
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
                    # Note: input() always writes its prompt to sys.stdout. 
                    # If you need this strictly routed to self.stdout, we write it explicitly.
                    self.stdout.write(f"\n[{prompt_time}] Fetch next batch? (y/n): ")
                    self.stdout.flush()
                    cont = sys.stdin.readline().strip().lower()
                    
                    if cont in ('exit', 'quit'):
                        print("\n*** Exited query pagination. Returning to main menu.", file=self.stdout)
                        break
                    
                    if cont not in ('y', ''):
                        break

            else:
                # REST Mode
                results = self.sf.query(arg)
                records = results.get('records', [])
                print(f"\n--- REST Results (Total: {results.get('totalSize')}) ---", file=self.stdout)
                print(json.dumps(records[:50], indent=2), file=self.stdout)
                if len(records) > 50:
                    print(f"... and {len(records) - 50} more records.", file=self.stdout)

        except Exception as e:
            print(f"*** SfSession Error: {e}", file=self.stdout)
            # Optional: Send exceptions directly to your logger as well
            # logger.error(f"SfSession Error: {e}", exc_info=True)

    def do_describe(self, arg: str) -> None:
        """Describe an object's metadata. Usage: describe Account"""
        if not arg:
            print("*** Error: Object name required.", file=self.stdout)
            return
        try:
            meta: Any = getattr(self.sf, arg).describe()
            # fields = [f['name'] for f in meta['fields']]
            # print(f"Fields in {arg}: {', '.join(fields[:20])}...",file=self.stdout)
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
    from api import SfSession
    sf = SfSession()
    # If your bootstrap overrides sys.stdout, passing it here wires the REPL to it.
    Repl(sf, stdout=sys.stdout).cmdloop()

if __name__ == '__main__':
    main()