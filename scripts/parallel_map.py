from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable
def parallel_map(
    func: Callable,
    items: list,
    max_workers: int = 4
) -> list:
    """
    parallel_map - Execute function on items in parallel
    
    Args:
        func: Function to apply
        items: Items to process
        max_workers: Maximum concurrent workers
    
    Returns:
        list of results in order
    
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