from __future__ import annotations
import csv
from pathlib import Path
from dataclasses import dataclass, field
from typing import Iterator,  Union, Type, cast #TYPE_CHECKING, Optional
from .base import AbstractSource
from .exceptions import AlignmentError
import logging
logger = logging.getLogger(__name__)

@dataclass
class Csv(AbstractSource):
    source_path: Path
    file_headers: list[str] = field(default_factory=lambda: [])
    file_name: str = ''
    col_count: int = 0
    row_count: int = 0
    dialect: Union[Type[csv.Dialect], str] = 'excel'
    encoding: str = 'utf-8'
    has_headers: bool = True
    alignment_validated: bool = False
    def __post_init__(self):
        logger.debug('Enter: Csv.__post_init__')
        self.source_path = Path(self.source_path).expanduser().resolve()
        if not self.source_path.is_file():
            raise FileNotFoundError(f'Source file not found: {self.source_path}')
        self.file_name = self.source_path.name
        self.csv_sniff()
        with self.open() as f:
            reader = csv.reader(f, dialect=self.dialect)
            self.file_headers = next(reader, cast(list[str], []))
        if not self.file_headers:
            raise AlignmentError('CSV file has no headers.', source_path=str(self.source_path), row_number=1, expected=1, got=0)
        for i, h in enumerate(self.file_headers):
            if not h.strip():
                raise AlignmentError(f'Header at position {i} is blank.', source_path=str(self.source_path), row_number=1, expected=len(self.file_headers), got=len(self.file_headers))
        self.col_count = len(self.file_headers)
        self.validate_row_alignment()
    def header_idx_map(self) -> dict[str, int]:
        return {name: i for i, name in enumerate(self.headers())}
    def open(self):
        return self.source_path.open(mode='r', newline='', encoding=self.encoding)
    def headers(self) -> list[str]: return self.file_headers
    def rows(self) -> Iterator[list[str]]:
        with self.open() as f:
            reader = csv.reader(f, dialect=self.dialect)
            if self.has_headers: next(reader, None)
            yield from reader
    def close(self) -> None: return None
    def validate_file_structure(self):
        expected = len(self.file_headers)
        for line_no, row in enumerate(self.rows(), start=2):
            if len(row) != expected:
                raise ValueError(f'Data shift at line {line_no}: expected {expected}, got {len(row)}')
    def validate_row_alignment(self) -> None:
        line_no = 1
        for line_no, row in enumerate(self.rows(), start=2):
            if len(row) != self.col_count:
                raise AlignmentError(f'Row {line_no} has {len(row)} fields, expected {self.col_count}.', source_path=str(self.source_path), row_number=line_no, expected=self.col_count, got=len(row))
        self.row_count = max(line_no - 1, 0)
        self.alignment_validated = True
    def csv_sniff(self):
        with self.open() as f:
            sample = f.read(4096)
        try:
            self.dialect = csv.Sniffer().sniff(sample, delimiters=',;	')
        except csv.Error:
            self.dialect = 'excel'
