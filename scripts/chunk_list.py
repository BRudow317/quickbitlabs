

def chunk_list(lst: list, size: int) -> list[list]:
    """
    chunk_list - Split list into chunks of given size
    
    Args:
        lst: List to split
        size: Chunk size
    
    Returns:
        List of chunks
    
    Example:
        chunk_list([1,2,3,4,5], 2)  # [[1,2], [3,4], [5]]
    """
    return [lst[i:i + size] for i in range(0, len(lst), size)]