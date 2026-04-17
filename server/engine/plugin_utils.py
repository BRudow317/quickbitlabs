from server.plugins.PluginRegistry import get_plugin
from server.plugins.PluginModels import Catalog, Entity
from server.plugins.PluginProtocol import Plugin
def resolve_plugin_from_catalog(catalog: Catalog) -> Plugin:
    """Inspects the Catalog to determine the target system and instances the plugin."""
    if not catalog.entities:
        raise ValueError("Catalog must contain at least one entity to resolve the target system.")
    
    # Assuming your FQN/Locator pattern puts the system name first: e.g., ["oracle", "dev01", ...]
    first_entity = catalog.entities[0]
    if not first_entity.parent_names:
        raise ValueError(f"Entity {first_entity.name} is missing parent_names to identify the system.")
        
    system_name = first_entity.parent_names[0]
    return get_plugin(system_name)