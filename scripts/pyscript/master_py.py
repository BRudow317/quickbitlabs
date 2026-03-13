#!/usr/bin/env python3
"""
PYTHON HELPER FUNCTIONS

A comprehensive collection of utility functions for common Python operations.

Usage:
    from python_helpers import *
    # or
    import python_helpers as helpers

Setup (run once):
    python python_helpers.py --setup
    # Creates venv and installs dependencies
"""

import os
import sys
import subprocess
import json
import csv
import re
import hashlib
import base64
import logging
import functools
import inspect
import threading
import time
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import (
    Any, Dict, List, Optional, Union, Callable, TypeVar, Type,
    get_type_hints, get_origin, get_args, Tuple
)
from dataclasses import dataclass, fields, is_dataclass, asdict
from collections.abc import Mapping, Sequence
from io import StringIO, BytesIO
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.parse



# VENV & ENVIRONMENT SETUP


def setup_venv(
    venv_path: str = ".venv",
    packages: Optional[List[str]] = None,
    requirements_file: Optional[str] = None,
    python_version: Optional[str] = None
) -> bool:
    """
    1. setup_venv - Create virtual environment and install packages
    
    Args:
        venv_path: Path for the virtual environment
        packages: List of packages to install
        requirements_file: Path to requirements.txt
        python_version: Specific Python version (e.g., "python3.11")
    
    Returns:
        bool: Success status
    
    Example:
        setup_venv(".venv", ["fastapi", "uvicorn", "flask"])
        setup_venv(requirements_file="requirements.txt")
    """
    default_packages = [
        "fastapi",
        "uvicorn[standard]",
        "flask",
        "requests",
        "pydantic",
        "python-dotenv",
        "pyyaml",
        "toml",
        "xmltodict",
        "pandas",
        "openpyxl",
        "markdown",
        "beautifulsoup4",
        "lxml",
        "rich",
        "httpx",
        "python-multipart",
    ]
    
    packages = packages or default_packages
    python_cmd = python_version or sys.executable
    
    print(f"🐍 Creating virtual environment at {venv_path}...")
    
    try:
        # Create venv
        subprocess.run([python_cmd, "-m", "venv", venv_path], check=True)
        
        # Determine pip path
        if sys.platform == "win32":
            pip_path = os.path.join(venv_path, "Scripts", "pip")
            python_path = os.path.join(venv_path, "Scripts", "python")
        else:
            pip_path = os.path.join(venv_path, "bin", "pip")
            python_path = os.path.join(venv_path, "bin", "python")
        
        # Upgrade pip
        print("📦 Upgrading pip...")
        subprocess.run([pip_path, "install", "--upgrade", "pip"], check=True)
        
        # Install from requirements file if provided
        if requirements_file and os.path.exists(requirements_file):
            print(f"📋 Installing from {requirements_file}...")
            subprocess.run([pip_path, "install", "-r", requirements_file], check=True)
        
        # Install packages
        if packages:
            print(f"📦 Installing {len(packages)} packages...")
            subprocess.run([pip_path, "install"] + packages, check=True)
        
        print(f"""
 Virtual environment created successfully!

To activate:
  Windows:   {venv_path}\\Scripts\\activate
  Linux/Mac: source {venv_path}/bin/activate

Python path: {python_path}
Pip path:    {pip_path}
""")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f" Error setting up venv: {e}")
        return False


def generate_requirements(
    output_file: str = "requirements.txt",
    include_versions: bool = True,
    exclude: Optional[List[str]] = None
) -> str:
    """
    2. generate_requirements - Generate requirements.txt from current environment
    
    Args:
        output_file: Output file path
        include_versions: Include version pinning
        exclude: Packages to exclude
    
    Returns:
        str: Contents of requirements file
    
    Example:
        generate_requirements("requirements.txt")
        generate_requirements(include_versions=False)
    """
    exclude = exclude or []
    
    result = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"],
        capture_output=True,
        text=True
    )
    
    lines = []
    for line in result.stdout.strip().split("\n"):
        if not line or line.startswith("#"):
            continue
        
        pkg_name = line.split("==")[0].lower()
        if pkg_name in [e.lower() for e in exclude]:
            continue
        
        if include_versions:
            lines.append(line)
        else:
            lines.append(pkg_name)
    
    content = "\n".join(sorted(lines))
    
    with open(output_file, "w") as f:
        f.write(content)
    
    print(f" Generated {output_file} with {len(lines)} packages")
    return content


def check_dependencies(packages: List[str]) -> Dict[str, bool]:
    """
    3. check_dependencies - Check if packages are installed
    
    Args:
        packages: List of package names to check
    
    Returns:
        Dict mapping package names to installed status
    
    Example:
        status = check_dependencies(["fastapi", "flask", "numpy"])
        missing = [pkg for pkg, installed in status.items() if not installed]
    """
    import importlib.util
    
    results = {}
    for package in packages:
        # Handle packages with different import names
        import_name = package.split("[")[0].replace("-", "_").lower()
        spec = importlib.util.find_spec(import_name)
        results[package] = spec is not None
    
    return results


def install_package(package: str, upgrade: bool = False) -> bool:
    """
    4. install_package - Install a package at runtime
    
    Args:
        package: Package name (can include version specifier)
        upgrade: Whether to upgrade if already installed
    
    Returns:
        bool: Success status
    
    Example:
        install_package("requests>=2.28.0")
        install_package("numpy", upgrade=True)
    """
    cmd = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        cmd.append("--upgrade")
    cmd.append(package)
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f" Installed {package}")
        return True
    except subprocess.CalledProcessError as e:
        print(f" Failed to install {package}: {e}")
        return False











# TYPE CHECKING & VALIDATION


T = TypeVar("T")


def is_type(value: Any, expected_type: Type) -> bool:
    """
    22. is_type - Check if value matches expected type (supports generics)
    
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
    23. validate_type - Validate type and raise TypeError if mismatch
    
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
    24. type_checked - Decorator to enforce type hints at runtime
    
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
    25. get_type_name - Get detailed type name of an object
    
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


def coerce_type(value: Any, target_type: Type[T]) -> T:
    """
    26. coerce_type - Attempt to convert value to target type
    
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
    27. is_none_or_empty - Check if value is None, empty string, or empty collection
    
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



# UTILITY FUNCTIONS


def deep_merge(base: Dict, override: Dict) -> Dict:
    """
    28. deep_merge - Recursively merge two dictionaries
    
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
    d: Dict,
    separator: str = ".",
    parent_key: str = ""
) -> Dict[str, Any]:
    """
    29. flatten_dict - Flatten nested dictionary
    
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


def unflatten_dict(d: Dict[str, Any], separator: str = ".") -> Dict:
    """
    30. unflatten_dict - Unflatten dictionary with dot notation keys
    
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


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    31. retry - Decorator for automatic retry with exponential backoff
    
    Args:
        max_attempts: Maximum retry attempts
        delay: Initial delay between retries
        backoff: Multiplier for delay after each attempt
        exceptions: Exception types to catch
    
    Example:
        @retry(max_attempts=3, delay=1.0)
        def fetch_data():
            return requests.get(url).json()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def memoize(
    maxsize: int = 128,
    ttl: Optional[float] = None
) -> Callable:
    """
    32. memoize - Decorator for caching function results
    
    Args:
        maxsize: Maximum cache size
        ttl: Time-to-live in seconds (None for no expiration)
    
    Example:
        @memoize(maxsize=100, ttl=300)
        def expensive_calculation(n):
            return n ** n
    """
    def decorator(func: Callable) -> Callable:
        cache = {}
        timestamps = {}
        lock = threading.Lock()
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            
            with lock:
                # Check if cached and not expired
                if key in cache:
                    if ttl is None or time.time() - timestamps[key] < ttl:
                        return cache[key]
                    else:
                        del cache[key]
                        del timestamps[key]
                
                # Compute and cache
                result = func(*args, **kwargs)
                
                # Evict oldest if at capacity
                if len(cache) >= maxsize:
                    oldest = min(timestamps, key=timestamps.get)
                    del cache[oldest]
                    del timestamps[oldest]
                
                cache[key] = result
                timestamps[key] = time.time()
                
                return result
        
        wrapper.cache_clear = lambda: (cache.clear(), timestamps.clear())
        wrapper.cache_info = lambda: {"size": len(cache), "maxsize": maxsize}
        
        return wrapper
    return decorator


def debounce(wait: float) -> Callable:
    """
    33. debounce - Decorator to debounce function calls
    
    Args:
        wait: Wait time in seconds
    
    Example:
        @debounce(0.5)
        def save_to_disk(data):
            with open("data.json", "w") as f:
                json.dump(data, f)
    """
    def decorator(func: Callable) -> Callable:
        last_call = [0.0]
        timer = [None]
        lock = threading.Lock()
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            def call_func():
                func(*args, **kwargs)
            
            with lock:
                if timer[0]:
                    timer[0].cancel()
                
                timer[0] = threading.Timer(wait, call_func)
                timer[0].start()
        
        return wrapper
    return decorator


def throttle(rate: float) -> Callable:
    """
    34. throttle - Decorator to limit function call rate
    
    Args:
        rate: Minimum seconds between calls
    
    Example:
        @throttle(1.0)  # Max once per second
        def send_notification(msg):
            api.send(msg)
    """
    def decorator(func: Callable) -> Callable:
        last_call = [0.0]
        lock = threading.Lock()
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                now = time.time()
                if now - last_call[0] >= rate:
                    last_call[0] = now
                    return func(*args, **kwargs)
            return None
        
        return wrapper
    return decorator


def timed(func: Callable) -> Callable:
    """
    35. timed - Decorator to measure function execution time
    
    Example:
        @timed
        def slow_function():
            time.sleep(1)
        
        # Prints: slow_function took 1.001s
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"{func.__name__} took {elapsed:.3f}s")
        return result
    return wrapper


@contextmanager
def timer(label: str = "Operation"):
    """
    36. timer - Context manager for timing code blocks
    
    Example:
        with timer("Database query"):
            results = db.query(sql)
        # Prints: Database query took 0.123s
    """
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"{label} took {elapsed:.3f}s")


def parallel_map(
    func: Callable,
    items: List,
    max_workers: int = 4
) -> List:
    """
    37. parallel_map - Execute function on items in parallel
    
    Args:
        func: Function to apply
        items: Items to process
        max_workers: Maximum concurrent workers
    
    Returns:
        List of results in order
    
    Example:
        urls = ["http://example.com/1", "http://example.com/2"]
        responses = parallel_map(requests.get, urls, max_workers=4)
    """
    results = [None] * len(items)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): i for i, item in enumerate(items)}
        
        for future in as_completed(futures):
            index = futures[future]
            results[index] = future.result()
    
    return results


def chunk_list(lst: List, size: int) -> List[List]:
    """
    38. chunk_list - Split list into chunks of given size
    
    Args:
        lst: List to split
        size: Chunk size
    
    Returns:
        List of chunks
    
    Example:
        chunk_list([1,2,3,4,5], 2)  # [[1,2], [3,4], [5]]
    """
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def safe_get(
    obj: Any,
    path: str,
    default: Any = None,
    separator: str = "."
) -> Any:
    """
    39. safe_get - Safely get nested value from dict/object
    
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


def hash_string(
    s: str,
    algorithm: str = "sha256"
) -> str:
    """
    40. hash_string - Generate hash of string
    
    Args:
        s: String to hash
        algorithm: Hash algorithm (md5, sha1, sha256, sha512)
    
    Returns:
        Hex digest string
    
    Example:
        hash_string("password123")  # SHA-256 hash
        hash_string("data", algorithm="md5")
    """
    h = hashlib.new(algorithm)
    h.update(s.encode("utf-8"))
    return h.hexdigest()


def generate_id(length: int = 16) -> str:
    """
    41. generate_id - Generate random ID string
    
    Args:
        length: Length of ID
    
    Returns:
        Random alphanumeric string
    
    Example:
        user_id = generate_id()  # e.g., "a7f3b2c9d1e4f5a6"
        short_id = generate_id(8)
    """
    import secrets
    import string
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def slugify(text: str) -> str:
    """
    42. slugify - Convert text to URL-safe slug
    
    Args:
        text: Text to convert
    
    Returns:
        URL-safe slug
    
    Example:
        slugify("Hello World!")  # "hello-world"
        slugify("Ünïcödé Têxt")  # "unicode-text"
    """
    import unicodedata
    
    # Normalize unicode
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    
    # Convert to lowercase and replace spaces/special chars
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    
    return text.strip("-")


def truncate_string(
    s: str,
    max_length: int,
    suffix: str = "..."
) -> str:
    """
    43. truncate_string - Truncate string to max length
    
    Args:
        s: String to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add if truncated
    
    Returns:
        Truncated string
    
    Example:
        truncate_string("Hello World", 8)  # "Hello..."
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def format_bytes(size: int) -> str:
    """
    44. format_bytes - Format byte size as human readable
    
    Args:
        size: Size in bytes
    
    Returns:
        Formatted string
    
    Example:
        format_bytes(1024)  # "1.0 KB"
        format_bytes(1234567)  # "1.2 MB"
    """
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} EB"


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


def load_dotenv(path: str = ".env") -> Dict[str, str]:
    """
    46. load_dotenv - Load environment variables from .env file
    
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



# PRETTY PRINTING & DEBUGGING


def pp(obj: Any, max_depth: int = 4, indent: int = 2) -> None:
    """
    47. pp - Pretty print any object
    
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
    48. debug - Debug print with file/line info
    
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
    49. log_call - Decorator to log function calls with arguments
    
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


def describe(obj: Any) -> Dict[str, Any]:
    """
    50. describe - Get detailed description of any object
    
    Args:
        obj: Object to describe
    
    Returns:
        Dict with type info, methods, attributes, etc.
    
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



# MAIN - Setup Script


def list_helpers():
    """Print list of all available helper functions."""
    help_text = """
PYTHON HELPERS - Available Functions
====================================

VENV & ENVIRONMENT:
  setup_venv(path, packages, req_file)     - Create venv & install packages
  generate_requirements(output, versions)  - Generate requirements.txt
  check_dependencies(packages)             - Check if packages installed
  install_package(package, upgrade)        - Install package at runtime

FORMAT CONVERSION:
  to_json(data, pretty)                    - Object to JSON
  from_json(json_str, cls)                 - JSON to object
  json_to_xml(json, root_name)             - JSON → XML
  xml_to_json(xml, strip_root)             - XML → JSON/dict
  json_to_yaml(json)                       - JSON → YAML
  yaml_to_json(yaml)                       - YAML → JSON/dict
  json_to_toml(json)                       - JSON → TOML
  toml_to_json(toml)                       - TOML → JSON/dict
  json_to_csv(json, output_file)           - JSON → CSV
  csv_to_json(csv, delimiter)              - CSV → JSON
  json_to_excel(json, output)              - JSON → Excel
  excel_to_json(file, sheet, all_sheets)   - Excel → JSON
  convert_format(content, from, to)        - Universal converter

MARKDOWN & HTML:
  markdown_to_html(md, extensions)         - Markdown → HTML
  html_to_markdown(html, strip_tags)       - HTML → Markdown
  markdown_to_html_doc(md, title, css)     - Markdown → complete HTML doc
  extract_markdown_toc(md)                 - Extract table of contents

TYPE CHECKING:
  is_type(value, expected_type)            - Check type (supports generics)
  validate_type(value, type, name)         - Validate or raise TypeError
  @type_checked                            - Decorator for runtime type checking
  get_type_name(obj)                       - Get detailed type description
  coerce_type(value, target_type)          - Convert to target type
  is_none_or_empty(value)                  - Check None/empty

UTILITIES:
  deep_merge(base, override)               - Recursive dict merge
  flatten_dict(d, separator)               - Flatten nested dict
  unflatten_dict(d, separator)             - Unflatten dot-notation dict
  @retry(attempts, delay, backoff)         - Retry decorator
  @memoize(maxsize, ttl)                   - Caching decorator
  @debounce(wait)                          - Debounce decorator
  @throttle(rate)                          - Throttle decorator
  @timed                                   - Measure execution time
  timer(label)                             - Context manager for timing
  parallel_map(func, items, workers)       - Parallel execution
  chunk_list(lst, size)                    - Split list into chunks
  safe_get(obj, path, default)             - Safe nested access
  hash_string(s, algorithm)                - Generate hash
  generate_id(length)                      - Random ID generator
  slugify(text)                            - URL-safe slug
  truncate_string(s, max_length)           - Truncate with suffix
  format_bytes(size)                       - Human readable size
  env(key, default, cast)                  - Get env var with casting
  load_dotenv(path)                        - Load .env file

DEBUGGING:
  pp(obj, max_depth)                       - Pretty print
  debug(*args, **kwargs)                   - Debug with file/line info
  @log_call                                - Log function calls
  describe(obj)                            - Detailed object description
"""
    print(help_text)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Python Helper Functions")
    parser.add_argument("--setup", action="store_true", help="Setup virtual environment")
    parser.add_argument("--venv-path", default=".venv", help="Virtual environment path")
    parser.add_argument("--requirements", help="Requirements file to install")
    parser.add_argument("--list", action="store_true", help="List all helper functions")
    
    args = parser.parse_args()
    
    if args.list:
        list_helpers()
    elif args.setup:
        setup_venv(args.venv_path, requirements_file=args.requirements)
    else:
        parser.print_help()