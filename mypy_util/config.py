"""
Consider loading secrets from environment or a dedicated secrets manager 
instead of a flat JSON file on disk; keep .secrets.json out of repos and 
static directories (confirm in .gitignore/server static config).


Add basic logging/monitoring around config load failures, but never log secret values.
"""
import json
import os
import sys
from pathlib import Path

# Initialize secrets as the environment is loaded.
_secrets = {}

# Load secrets once when the module is imported
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
    _SECRETS_FILE = BASE_DIR / ".secrets.json"

    with open(_SECRETS_FILE, "r") as f:
        file_data = json.load(f)
        for category, items in file_data.items():
            _secrets[category] = {}
            for key, value in items.items():
                _secret = value
                if isinstance(_secret, str) and os.path.isfile(_secret):
                    with open(_secret, "r") as f2:
                        _secret = f2.read()
                _secrets[category][key] = _secret
except FileNotFoundError:
    print(f"Secrets file not found: {_SECRETS_FILE}", file=sys.stderr)
    sys.exit(1)
except json.JSONDecodeError:
    print("JSONDecodeError")
    sys.exit(1)
except Exception as e:
    print(f"Failed: {e}")
    sys.exit(1)

def get_secret(group: str, key):
    """Retrieve a secret value by group and key, case-insensitive."""
    return _secrets.get(group, {}).get(key)