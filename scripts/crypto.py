import hashlib
def hash_string(
    s: str,
    algorithm: str = "sha256"
) -> str:
    """
    hash_string - Generate hash of string
    
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
    generate_id - Generate random ID string
    
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