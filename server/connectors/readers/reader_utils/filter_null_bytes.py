from typing import AnyStr

def filter_null_bytes(b: AnyStr) -> AnyStr:
        """https://github.com/airbytehq/airbyte/issues/8300"""
        if isinstance(b, str):
            return b.replace("\x00", "")
        if isinstance(b, bytes):
            return b.replace(b"\x00", b"")
        raise TypeError("Expected str or bytes")