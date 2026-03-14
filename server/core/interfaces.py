# olympus/core/interfaces.py

from abc import ABC, abstractmethod
from typing import Iterator, Iterable, Dict, Any, List
from server.models.schema import UniversalTable

class SourceConnector(ABC):
    """The strict contract for any system we extract data FROM."""

    @abstractmethod
    def discover_schema(self, streams: List[str] | None = None) -> List[UniversalTable]:
        """
        Queries the source (e.g., Salesforce Describe API) and translates 
        its specific metadata into our agnostic UniversalTable Pydantic models.
        """
        pass

    @abstractmethod
    def read_data(self, stream_name: str) -> Iterator[Dict[str, Any]]:
        """
        Fetches data from the source and yields it row by row (or batch by batch).
        Using an Iterator prevents memory limits when pulling 10 million rows.
        """
        pass


class DestinationConnector(ABC):
    """The strict contract for any system we load data INTO."""

    @abstractmethod
    def apply_schema(self, table: UniversalTable) -> None:
        """
        Takes a UniversalTable and translates it into the destination's native 
        DDL (e.g., Postgres CREATE TABLE or SQLAlchemy metadata), then executes it.
        """
        pass

    @abstractmethod
    def write_data(self, stream_name: str, records: Iterable[Dict[str, Any]]) -> None:
        """
        Takes a stream of raw dictionaries and inserts/upserts them into the 
        destination database.
        """
        pass