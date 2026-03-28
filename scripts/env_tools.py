from typing import Any, Type
import os
def env(key: str, default: Any = None, cast: Type = str) -> Any:
    """
    45. env - Get environment variable with optional type casting
    
    Args:
        key: Environment variable name
        default: Default value if not set
        cast: Type to cast to
    
    Returns:
        Environment variable value
    
    Example:
        port = env("PORT", 8000, int)
        debug = env("DEBUG", False, bool)
    """
    value = os.environ.get(key)
    
    if value is None:
        return default
    
    if cast is bool:
        return value.lower() in ("true", "1", "yes", "on")
    
    return cast(value)


def load_dotenv(path: str = ".env") -> dict[str, str]:
    """
    load_dotenv - Load environment variables from .env file
    
    Args:
        path: Path to .env file
    
    Returns:
        Dict of loaded variables
    
    Example:
        load_dotenv()
        load_dotenv(".env.local")
    """
    loaded = {}
    
    if not os.path.exists(path):
        return loaded
    
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                
                # Remove quotes
                if value and value[0] in "\"'" and value[-1] == value[0]:
                    value = value[1:-1]
                
                os.environ[key] = value
                loaded[key] = value
    
    return loaded