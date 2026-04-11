from __future__ import annotations
import importlib
from typing import Any
import logging

# 1. Import the universal contract!
from .PluginProtocol import Plugin

logger = logging.getLogger(__name__)

# Maps plugin facade name -> (module_path, class_name)
PLUGIN_REGISTRY: dict[str, tuple[str, str]] = {
    'salesforce': ('server.plugins.sf.Salesforce', 'Salesforce'),
    # 'postgres':   ('server.plugins.postgres.Postgres', 'Postgres'),
    'oracle':     ('server.plugins.oracle.Oracle', 'Oracle'),
    # 'csv':        ('server.plugins.readers.Csv', 'Csv'),
    # 'json':       ('server.plugins.readers.Json', 'Json'),
    # 'excel':      ('server.plugins.readers.Excel', 'Excel'),
}

def get_plugin(name: str, **kwargs: Any) -> Plugin:
    """Lazy import and instantiate a plugin, ensuring it matches the universal contract."""
    entry = PLUGIN_REGISTRY.get(name.lower())
    if not entry:
        available = ', '.join(sorted(PLUGIN_REGISTRY.keys()))
        raise ValueError(f"Unknown plugin '{name}'. Available: {available}")

    module_path, class_name = entry
    
    # 3. Safe Dependency Loading
    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
    except ImportError as e:
        logger.error(f"Failed to load dependencies for plugin '{name}': {e}")
        raise RuntimeError(f"Plugin '{name}' is registered, but its dependencies are missing. Did you install them? Error: {e}")
    except AttributeError:
        logger.error(f"Class '{class_name}' not found in module '{module_path}'")
        raise RuntimeError(f"Plugin '{name}' is broken. Class '{class_name}' is missing.")
        
    # Instantiate and return the Facade, passing credentials/paths via kwargs
    return cls(**kwargs)

def list_plugins() -> list[str]:
    return sorted(PLUGIN_REGISTRY.keys())