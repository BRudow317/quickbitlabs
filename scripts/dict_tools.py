from typing import Any

# Dictionary utilities
def deep_merge(base: dict, override: dict) -> dict:
    """
    deep_merge - Recursively merge two dictionaries
    
    Args:
        base: Base dictionary
        override: Dictionary to merge on top
    
    Returns:
        Merged dictionary
    
    Example:
        config = deep_merge(default_config, user_config)
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def flatten_dict(
    d: dict,
    separator: str = ".",
    parent_key: str = ""
) -> dict[str, Any]:
    """
    flatten_dict - Flatten nested dictionary
    
    Args:
        d: Dictionary to flatten
        separator: Key separator
        parent_key: Prefix for keys
    
    Returns:
        Flattened dictionary
    
    Example:
        flatten_dict({"a": {"b": {"c": 1}}})  # {"a.b.c": 1}
    """
    items = []
    
    for k, v in d.items():
        new_key = f"{parent_key}{separator}{k}" if parent_key else k
        
        if isinstance(v, dict):
            items.extend(flatten_dict(v, separator, new_key).items())
        else:
            items.append((new_key, v))
    
    return dict(items)


def unflatten_dict(d: dict[str, Any], separator: str = ".") -> dict:
    """
    unflatten_dict - Unflatten dictionary with dot notation keys
    
    Args:
        d: Flattened dictionary
        separator: Key separator
    
    Returns:
        Nested dictionary
    
    Example:
        unflatten_dict({"a.b.c": 1})  # {"a": {"b": {"c": 1}}}
    """
    result = {}
    
    for key, value in d.items():
        parts = key.split(separator)
        current = result
        
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
    
    return result

def safe_get(
    obj: Any,
    path: str,
    default: Any = None,
    separator: str = "."
) -> Any:
    """
    safe_get - Safely get nested value from dict/object
    
    Args:
        obj: Source object
        path: Dot-notation path
        default: Default if not found
        separator: Path separator
    
    Returns:
        Value or default
    
    Example:
        safe_get(data, "user.profile.name", "Anonymous")
        safe_get(config, "database.host", "localhost")
    """
    keys = path.split(separator)
    
    for key in keys:
        if obj is None:
            return default
        
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, (list, tuple)):
            try:
                obj = obj[int(key)]
            except (ValueError, IndexError):
                return default
        elif hasattr(obj, key):
            obj = getattr(obj, key)
        else:
            return default
    
    return obj if obj is not None else default