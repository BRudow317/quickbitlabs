import functools, inspect, os, sys
from dataclasses import dataclass, field, is_dataclass, fields
from typing import Any, Callable
# PRETTY PRINTING & DEBUGGING


def pp(obj: Any, max_depth: int = 4, indent: int = 2) -> None:
    """
    pp - Pretty print any object
    
    Args:
        obj: Object to print
        max_depth: Maximum nesting depth
        indent: Indentation spaces
    
    Example:
        pp(complex_dict)
        pp(my_object, max_depth=2)
    """
    try:
        from rich import print as rprint
        from rich.pretty import Pretty
        rprint(Pretty(obj, max_depth=max_depth, indent_size=indent))
    except ImportError:
        import pprint
        pprint.pprint(obj, depth=max_depth, indent=indent)


def debug(*args, **kwargs) -> None:
    """
    debug - Debug print with file/line info
    
    Example:
        debug("value is", value)
        debug(user=user, status=status)
    """
    frame = inspect.currentframe().f_back
    filename = os.path.basename(frame.f_code.co_filename)
    lineno = frame.f_lineno
    func = frame.f_code.co_name
    
    prefix = f"[{filename}:{lineno} in {func}]"
    
    if args and kwargs:
        print(f"{prefix}", *args, kwargs)
    elif args:
        print(f"{prefix}", *args)
    elif kwargs:
        parts = [f"{k}={v!r}" for k, v in kwargs.items()]
        print(f"{prefix}", ", ".join(parts))


def log_call(func: Callable) -> Callable:
    """
    log_call - Decorator to log function calls with arguments
    
    Example:
        @log_call
        def process(data, flag=True):
            return data
        
        process([1, 2, 3], flag=False)
        # Logs: process([1, 2, 3], flag=False) -> [1, 2, 3]
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        
        print(f"→ {func.__name__}({signature})")
        
        try:
            result = func(*args, **kwargs)
            print(f"← {func.__name__} returned {result!r}")
            return result
        except Exception as e:
            print(f"✗ {func.__name__} raised {type(e).__name__}: {e}")
            raise
    
    return wrapper


def describe(obj: Any) -> dict[str, Any]:
    """
    describe - Get detailed description of any object
    
    Args:
        obj: Object to describe
    
    Returns:
        dict with type info, methods, attributes, etc.
    
    Example:
        info = describe(my_object)
        pp(info)
    """
    info = {
        "type": type(obj).__name__,
        "module": type(obj).__module__,
        "id": id(obj),
        "size": sys.getsizeof(obj),
    }
    
    # String representation
    try:
        info["repr"] = repr(obj)[:200]
    except Exception:
        info["repr"] = "<error>"
    
    # For collections, add length
    if hasattr(obj, "__len__"):
        info["length"] = len(obj)
    
    # For dataclasses
    if is_dataclass(obj):
        info["fields"] = {f.name: getattr(obj, f.name) for f in fields(obj)}
    
    # List attributes and methods
    attrs = []
    methods = []
    
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            val = getattr(obj, name)
            if callable(val):
                methods.append(name)
            else:
                attrs.append(name)
        except Exception:
            pass
    
    if attrs:
        info["attributes"] = attrs[:20]
    if methods:
        info["methods"] = methods[:20]
    
    return info