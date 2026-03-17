from __future__ import annotations
import re
from typing import Iterable
from ..oracle_config import _ORACLE_RESERVED

def to_oracle_snake(value: str, max_len: int = 128, reserved: Iterable[str] = _ORACLE_RESERVED) -> str:
    s = str(value).strip()
    if not s:
        return 'COL'
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = re.sub(r'([A-Za-z])([0-9])', r'\1_\2', s)
    s = re.sub(r'([0-9])([A-Za-z])', r'\1_\2', s)
    s = re.sub(r'[^A-Za-z0-9_]+', '_', s)
    s = re.sub(r'_{2,}', '_', s).strip('_').upper()
    if not s:
        return 'COL'
    if s[0].isdigit():
        s = 'C_' + s
    if s in reserved:
        s = f'{s}_COL'
    if len(s) > max_len:
        if s.endswith('_COL'):
            base = s[:max_len - 4].rstrip('_')
            s = f'{base}_COL'
        else:
            s = s[:max_len].rstrip('_')
    return s if s else 'COL'
