def substr(
    s: str,
    max_length: int,
    suffix: str = "..."
) -> str:
    """
    truncate_string - Truncate string to max length
    
    Args:
        s: String to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add if truncated
    
    Returns:
        Truncated string
    
    Example:
        substr("Hello World", 8)  # "Hello..."
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix
