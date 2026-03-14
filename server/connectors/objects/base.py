from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, TYPE_CHECKING
if TYPE_CHECKING:
    from io import TextIOWrapper

class AbstractSource(ABC):
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
    @abstractmethod
    def open(self) -> 'TextIOWrapper': ...
    @abstractmethod
    def headers(self) -> list[str]: ...
    @abstractmethod
    def rows(self) -> Iterator[list[str]]: ...
    @abstractmethod
    def close(self) -> None: ...
    def __enter__(self):
        self.open(); return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(); return None
