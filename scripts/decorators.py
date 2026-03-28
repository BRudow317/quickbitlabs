import functools
import threading
import time
from typing import (
    Any, Callable, Type, Tuple
)
from contextlib import contextmanager

CacheKey = tuple[tuple[Any, ...], tuple[tuple[str, Any], ...]]

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
) -> Callable:
    """
    retry - Decorator for automatic retry with exponential backoff
    
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
            last_exception: Exception | None = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            if last_exception is not None:
                raise last_exception
            raise RuntimeError("retry exhausted without capturing an exception")
        
        return wrapper
    return decorator


def memoize(
    maxsize: int = 128,
    ttl: float | None = None
) -> Callable:
    """
    memoize - Decorator for caching function results
    
    Args:
        maxsize: Maximum cache size
        ttl: Time-to-live in seconds (None for no expiration)
    
    Example:
        @memoize(maxsize=100, ttl=300)
        def expensive_calculation(n):
            return n ** n
    """
    def decorator(func: Callable) -> Callable:
        cache: dict[CacheKey, Any] = {}
        timestamps: dict[CacheKey, float] = {}
        lock = threading.Lock()
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key: CacheKey = (args, tuple(sorted(kwargs.items())))
            
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
                    oldest = min(timestamps, key=lambda k: timestamps[k])
                    del cache[oldest]
                    del timestamps[oldest]
                
                cache[key] = result
                timestamps[key] = time.time()
                
                return result
        
        setattr(wrapper, "cache_clear", lambda: (cache.clear(), timestamps.clear()))
        setattr(wrapper, "cache_info", lambda: {"size": len(cache), "maxsize": maxsize})
        
        return wrapper
    return decorator


def debounce(wait: float) -> Callable:
    """
    debounce - Decorator to debounce function calls
    
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
        timer: list[threading.Timer | None] = [None]
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
    throttle - Decorator to limit function call rate
    
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
    timed - Decorator to measure function execution time
    
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
    timer - Context manager for timing code blocks
    
    Example:
        with timer("Database query"):
            results = db.query(sql)
        # Prints: Database query took 0.123s
    """
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"{label} took {elapsed:.3f}s")
