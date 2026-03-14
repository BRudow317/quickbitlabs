from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, NamedTuple, TypedDict
    from collections.abc import Mapping, MutableMapping
    from enum import Enum

API_VERSION: str = '63.0'
OAUTH_URI: str = '/services/oauth2/token'

# --- Common Types ---
Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, Any]]
BulkDataStr = list[Mapping[str, str]]

# --- REST Models ---
class Usage(NamedTuple):
    """Usage information for a Salesforce org"""
    used: int
    total: int

class PerAppUsage(NamedTuple):
    """Per App Usage information for a Salesforce org"""
    used: int
    total: int
    name: str

# --- Bulk API 2.0 Enums ---
class Operation(str, Enum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"

class JobState(str, Enum):
    open = "Open"
    aborted = "Aborted"
    failed = "Failed"
    upload_complete = "UploadComplete"
    in_progress = "InProgress"
    job_complete = "JobComplete"

class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"          # (^)
    COMMA = "COMMA"          # (,)
    PIPE = "PIPE"            # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"              # (\t)

class LineEnding(str, Enum):
    LF = "LF"
    CRLF = "CRLF"

class ResultsType(str, Enum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"

# --- Bulk API 2.0 Types ---
class QueryParameters(TypedDict, total=False):
    maxRecords: int
    locator: str

class QueryRecordsResult(TypedDict):
    locator: str
    number_of_records: int
    records: str

class QueryFileResult(TypedDict):
    locator: str
    number_of_records: int
    file: str

QueryResult = QueryRecordsResult | QueryFileResult