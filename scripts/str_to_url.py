import re
def escape_url(text: str) -> str:
    """
    escape_url - Convert text to URL-safe slug
    
    Args:
        text: Text to convert
    
    Returns:
        URL-safe slug
    
    Example:
        escape_url("Hello World!")  # "hello-world"
        escape_url("Ünïcödé Têxt")  # "unicode-text"
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