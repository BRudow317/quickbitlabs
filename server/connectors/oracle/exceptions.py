from __future__ import annotations
import logging, re, shutil
from pathlib import Path
logger = logging.getLogger(__name__)

class IngestionError(Exception):
    """Base class for all pipeline errors."""

class QuarantineError(IngestionError):
    def __init__(self, message: str = '', source_path: str | None = None):
        super().__init__(message)
        self.source_path = source_path
    def __str__(self):
        base = super().__str__()
        return f"{base} | source={self.source_path}" if self.source_path else base

class AlignmentError(QuarantineError):
    def __init__(self, message: str, source_path: str | None = None, row_number: int | None = None, expected: int | None = None, got: int | None = None):
        super().__init__(message, source_path)
        self.row_number = row_number; self.expected = expected; self.got = got
    def __str__(self):
        base = super().__str__(); parts=[]
        if self.row_number is not None: parts.append(f'row={self.row_number}')
        if self.expected is not None: parts.append(f'expected={self.expected}')
        if self.got is not None: parts.append(f'got={self.got}')
        return f"{base} | {' '.join(parts)}" if parts else base

class SizeBreachError(QuarantineError):
    def __init__(self, message: str, source_path: str | None = None, column_name: str | None = None, char_length: int | None = None, limit: int = 4000):
        super().__init__(message, source_path)
        self.column_name = column_name; self.char_length = char_length; self.limit = limit
    def __str__(self):
        base = super().__str__(); parts=[]
        if self.column_name: parts.append(f'column={self.column_name}')
        if self.char_length is not None: parts.append(f'char_length={self.char_length}')
        parts.append(f'limit={self.limit}')
        return f"{base} | {' '.join(parts)}"

class DDLError(IngestionError):
    def __init__(self, message: str, ddl: str | None = None):
        super().__init__(message); self.ddl = ddl
    def __str__(self):
        base = super().__str__(); return f"{base} | ddl={self.ddl}" if self.ddl else base

def quarantine_file(reason: str | Exception = '', job=None):
    source = ''
    dest_dir = ''
    if not job or not job.error_dir or not job.csv.source_path:
        raise ValueError('Path error on source_path or error_dir')
    source = Path(job.csv.source_path)
    dest_dir = Path(job.error_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_dest = dest_dir / source.name
    if file_dest.exists():
        stem = source.stem; suffix = source.suffix; counter = 1
        while file_dest.exists():
            file_dest = dest_dir / f'{stem}_{counter}{suffix}'
            counter += 1
    try:
        shutil.move(str(source), str(file_dest))
    except OSError as e:
        raise ValueError(f'Failed to quarantine {source} -> {file_dest}: {e}') from e
    return job

def extract_ora_code(message: str) -> str:
    match = re.search(r'ORA-\d+', message)
    return match.group(0) if match else 'ORA_UNKNOWN'

def count_errors_in_log(log_dir: Path | str, program_name: str = 'apollo') -> int:
    path_obj = Path(log_dir)
    if not path_obj.exists(): return 0
    log_files = list(path_obj.glob(f'*{program_name}.log'))
    if not log_files: return 0
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    with latest_log.open('r', encoding='utf-8') as f:
        return sum(1 for line in f if 'source=' in line)
