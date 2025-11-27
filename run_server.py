""" Uvicorn server runner for QuickBitLabs project. """
import uvicorn
import os
import sys
import json

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    """Loads Uvicorn settings from .secrets.json"""
    secrets_path = ".\\.secrets.json"
    with open(secrets_path, 'r') as f:
        json_data = json.load(f)
        return json_data.get("Uvicorn")

if __name__ == "__main__":
    # Load settings
    config = load_config()

    # Parse settings with type conversion (JSON strings -> Python types)
    host = config.get("host")
    port = int(config.get("port"))
    log_level = config.get("log_level")
    reload_val = (config.get("reload").lower() == "true")
    workers = int(config.get("workers"))

    # Console output for testing.
    print("Starting QuickBitLabs Server (Uvicorn)...")
    print(f"Config: Loaded from .secrets.json")
    uvicorn.run(
        "main:app", 
        host=host, 
        port=port, 
        log_level=log_level,
        reload=reload_val,
        workers=workers 
    )
    print(f"Local URL: http://{host}:{port}")
    print("Server is running")
