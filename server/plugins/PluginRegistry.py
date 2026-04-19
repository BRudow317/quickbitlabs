from __future__ import annotations
import importlib
from typing import Any, Literal, get_args, TYPE_CHECKING

if TYPE_CHECKING:
    from server.plugins.PluginProtocol import Plugin
    from server.plugins.PluginModels import Catalog, Entity, OperatorGroup

import logging
logger = logging.getLogger(__name__)

PLUGIN = Literal[
    'salesforce', 
    'oracle',
    ]

# Maps plugin facade name -> (module_path, class_name)
PLUGIN_REGISTRY: dict[PLUGIN, tuple[str, str]] = {
    'salesforce': ('server.plugins.sf.Salesforce', 'Salesforce'),
    'oracle':     ('server.plugins.oracle.Oracle', 'Oracle'),
}

_expected_keys = set(get_args(PLUGIN))
_actual_keys = set(PLUGIN_REGISTRY.keys())

if _expected_keys != _actual_keys:
    missing_in_registry = _expected_keys - _actual_keys
    missing_in_literal = _actual_keys - _expected_keys
    raise RuntimeError(
        f"Plugin configuration mismatch!\n"
        f"Missing in PLUGIN_REGISTRY: {missing_in_registry}\n"
        f"Missing in SystemName Literal: {missing_in_literal}"
    )

    # 'postgres':   ('server.plugins.postgres.Postgres', 'Postgres'),
    # 'csv':        ('server.plugins.readers.Csv', 'Csv'),
    # 'json':       ('server.plugins.readers.Json', 'Json'),
    # 'excel':      ('server.plugins.readers.Excel', 'Excel'),

def get_plugin(plugin: PLUGIN, **kwargs: Any) -> Plugin:
    """Lazy import and instantiate a plugin, ensuring it matches the universal contract."""

    entry = PLUGIN_REGISTRY.get(plugin)
    if not entry:
        available = ', '.join(sorted(PLUGIN_REGISTRY.keys()))
        raise ValueError(f"Unknown plugin '{plugin}'. Available: {available}")

    module_path, class_name = entry
    
    # 3. Safe Dependency Loading
    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
    except ImportError as e:
        logger.error(f"Failed to load dependencies for plugin '{plugin}': {e}")
        raise RuntimeError(f"Plugin '{plugin}' is registered, but its dependencies are missing. Did you install them? Error: {e}")
    except AttributeError:
        logger.error(f"Class '{class_name}' not found in module '{module_path}'")
        raise RuntimeError(f"Plugin '{plugin}' is broken. Class '{class_name}' is missing.")
        
    # Instantiate and return the Facade, passing credentials/paths via kwargs
    return cls(**kwargs)

def list_plugins() -> list[str]:
    return sorted(PLUGIN_REGISTRY.keys())


