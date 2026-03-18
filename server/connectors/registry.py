from __future__ import annotations
import importlib
from typing import Any

import logging
logger = logging.getLogger(__name__)

# Maps connector name -> (module_path, class_name)
CONNECTOR_REGISTRY: dict[str, tuple[str, str]] = {
    'salesforce': ('server.connectors.sf.SalesforceConnector', 'SalesforceConnector'),
    'postgres':   ('server.connectors.postgres.PostgresConnector', 'PostgresConnector'),
    'oracle':     ('server.connectors.oracle.OracleConnector', 'OracleConnector'),
    'csv':        ('server.connectors.readers.CsvConnector', 'CsvConnector'),
    
}


def get_connector(name: str, **kwargs: Any):
    """Lazy import and instantiate a connector"""
    entry = CONNECTOR_REGISTRY.get(name.lower())
    if not entry:
        available = ', '.join(sorted(CONNECTOR_REGISTRY.keys()))
        raise ValueError(f"Unknown connector '{name}'. Available: {available}")

    module_path, class_name = entry
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**kwargs)


def list_connectors() -> list[str]:
    return sorted(CONNECTOR_REGISTRY.keys())