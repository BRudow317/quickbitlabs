import inspect
import functools
from typing import TypeVar, Any, Type, Callable, get_type_hints, get_origin, get_args, Union, overload
T = TypeVar("T")


def is_type(value: Any, expected_type: Type) -> bool:
    """
    is_type - Check if value matches expected type (supports generics)
    
    Args:
        value: Value to check
        expected_type: Expected type (can be generic like List[str])
    
    Returns:
        bool: Whether value matches type
    
    Example:
        is_type([1, 2, 3], List[int])  # True
        is_type({"a": 1}, Dict[str, int])  # True
    """
    origin = get_origin(expected_type)
    
    if origin is None:
        # Simple type
        if expected_type is Any:
            return True
        return isinstance(value, expected_type)
    
    # Generic type
    args = get_args(expected_type)
    
    # Check container type first
    if origin is Union:
        return any(is_type(value, arg) for arg in args)
    
    if not isinstance(value, origin):
        return False
    
    if not args:
        return True
    
    # Check element types
    if origin in (list, set, frozenset):
        return all(is_type(item, args[0]) for item in value)
    
    if origin is tuple:
        if len(args) == 2 and args[1] is ...:
            return all(is_type(item, args[0]) for item in value)
        if len(value) != len(args):
            return False
        return all(is_type(item, arg) for item, arg in zip(value, args))
    
    if origin is dict:
        key_type, val_type = args
        return all(
            is_type(k, key_type) and is_type(v, val_type)
            for k, v in value.items()
        )
    
    return True


def validate_type(value: Any, expected_type: Type, name: str = "value") -> None:
    """
    validate_type - Validate type and raise TypeError if mismatch
    
    Args:
        value: Value to validate
        expected_type: Expected type
        name: Variable name for error message
    
    Raises:
        TypeError: If type doesn't match
    
    Example:
        validate_type(user_id, int, "user_id")
        validate_type(items, List[str], "items")
    """
    if not is_type(value, expected_type):
        raise TypeError(
            f"{name} must be {expected_type}, got {type(value).__name__}: {repr(value)[:100]}"
        )


def type_checked(func: Callable[..., T]) -> Callable[..., T]:
    """
    type_checked - Decorator to enforce type hints at runtime
    
    Example:
        @type_checked
        def greet(name: str, times: int = 1) -> str:
            return f"Hello, {name}! " * times
        
        greet("John", 3)  # OK
        greet(123, 3)  # Raises TypeError
    """
    hints = get_type_hints(func)
    sig = inspect.signature(func)
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Bind arguments to parameters
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        
        # Check each argument
        for param_name, value in bound.arguments.items():
            if param_name in hints:
                expected = hints[param_name]
                if not is_type(value, expected):
                    raise TypeError(
                        f"Argument '{param_name}' must be {expected}, "
                        f"got {type(value).__name__}"
                    )
        
        result = func(*args, **kwargs)
        
        # Check return type
        if "return" in hints and hints["return"] is not type(None):
            if not is_type(result, hints["return"]):
                raise TypeError(
                    f"Return value must be {hints['return']}, "
                    f"got {type(result).__name__}"
                )
        
        return result
    
    return wrapper


def get_type_name(obj: Any) -> str:
    """
    get_type_name - Get detailed type name of an object
    
    Args:
        obj: Any object
    
    Returns:
        str: Descriptive type name
    
    Example:
        get_type_name([1, 2, 3])  # "list[int] (3 items)"
        get_type_name({"a": 1})  # "dict[str, int] (1 items)"
    """
    t = type(obj)
    name = t.__name__
    
    if isinstance(obj, (list, tuple, set, frozenset)):
        if not obj:
            return f"{name} (empty)"
        
        types = set(type(item).__name__ for item in obj)
        if len(types) == 1:
            elem_type = types.pop()
        else:
            elem_type = " | ".join(sorted(types))
        
        return f"{name}[{elem_type}] ({len(obj)} items)"
    
    if isinstance(obj, dict):
        if not obj:
            return "dict (empty)"
        
        key_types = set(type(k).__name__ for k in obj.keys())
        val_types = set(type(v).__name__ for v in obj.values())
        
        key_type = " | ".join(sorted(key_types)) if len(key_types) > 1 else key_types.pop()
        val_type = " | ".join(sorted(val_types)) if len(val_types) > 1 else val_types.pop()
        
        return f"dict[{key_type}, {val_type}] ({len(obj)} items)"
    
    if isinstance(obj, str):
        return f"str ({len(obj)} chars)"
    
    if isinstance(obj, bytes):
        return f"bytes ({len(obj)} bytes)"
    
    return name


@overload
def coerce_type(value: Any, target_type: Type[T]) -> T:
    ...


@overload
def coerce_type(value: Any, target_type: Any) -> Any:
    ...


def coerce_type(value: Any, target_type: Any) -> Any:
    """
    coerce_type - Attempt to convert value to target type
    
    Args:
        value: Value to convert
        target_type: Target type
    
    Returns:
        Converted value
    
    Example:
        coerce_type("123", int)  # 123
        coerce_type("true", bool)  # True
        coerce_type("1,2,3", List[int])  # [1, 2, 3]
    """
    if isinstance(value, target_type):
        return value
    
    origin = get_origin(target_type)
    args = get_args(target_type)
    
    # Handle Optional
    if origin is Union:
        if type(None) in args and value is None:
            return None
        for arg in args:
            if arg is not type(None):
                try:
                    return coerce_type(value, arg)
                except (ValueError, TypeError):
                    continue
        raise TypeError(f"Cannot coerce {value!r} to {target_type}")
    
    # Handle List
    if origin is list:
        if isinstance(value, str):
            value = [v.strip() for v in value.split(",")]
        if args:
            return [coerce_type(v, args[0]) for v in value]
        return list(value)
    
    # Handle bool specially (before int, since bool is subclass of int)
    if target_type is bool:
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
    
    # Handle basic types
    if target_type in (int, float, str):
        return target_type(value)
    
    raise TypeError(f"Cannot coerce {type(value).__name__} to {target_type}")


def is_none_or_empty(value: Any) -> bool:
    """
    is_none_or_empty - Check if value is None, empty string, or empty collection
    
    Args:
        value: Value to check
    
    Returns:
        bool: True if None or empty
    
    Example:
        is_none_or_empty(None)  # True
        is_none_or_empty("")  # True
        is_none_or_empty([])  # True
        is_none_or_empty("hello")  # False
    """
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict, set, tuple)) and len(value) == 0:
        return True
    return False