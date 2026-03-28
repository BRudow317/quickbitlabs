def format_bytes(size: int | float) -> str:
    """
    format_bytes - Format byte size as human readable
    
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