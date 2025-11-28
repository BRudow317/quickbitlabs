""" Uvicorn server runner for QuickBitLabs project. """
import uvicorn
import os
import sys
import json

# Add the project root to the python path so we can import mypy_util
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mypy_util import config

if __name__ == "__main__":
    print("Starting QuickBitLabs Server (Uvicorn)...")
    
    # Load settings using your centralized config
    # We use defaults (or "127.0.0.1") to prevent crashes if JSON is missing keys
    host = config.get_secret("uvicorn", "host") or "127.0.0.1"
    
    # Type conversion: Strings from JSON need to be int/bool
    port_str = config.get_secret("uvicorn", "port")
    port = int(port_str) if port_str else 8000
    
    log_level = config.get_secret("uvicorn", "log_level") or "info"
    
    reload_str = config.get_secret("uvicorn", "reload")
    # robust boolean check (handles "True", "true", "TRUE")
    reload_val = str(reload_str).lower() == "true"
    
    workers_str = config.get_secret("uvicorn", "workers")
    workers = int(workers_str) if workers_str else 1

    print(f"Config: Loaded via mypy_util")
    print(f"Target: http://{host}:{port}")

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=reload_val,
        workers=workers
    )
