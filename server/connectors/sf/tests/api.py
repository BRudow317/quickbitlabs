from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, field
import base64
import json
import logging
import os
import re
from collections import OrderedDict
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import (
    Any, Callable, Iterator, Mapping, MutableMapping, NamedTuple, Optional,
    Union, cast
)
# Dependencies
from urllib.parse import urljoin, urlparse, quote_plus
import requests
# Local imports
from utils.date_to_iso8601 import date_to_iso8601

logger = logging.getLogger(__name__)

API_VERSION = '63.0'
OAUTH_URI = '/services/oauth2/token'

class Usage(NamedTuple):
    """Usage information for a Salesforce org"""
    used: int
    total: int

class PerAppUsage(NamedTuple):
    """Per App Usage information for a Salesforce org"""
    used: int
    total: int
    name: str

Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, Any]]
BulkDataStr = list[Mapping[str, str]]

##################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

# https://docs.python.org/3/library/json.html
#  SfSession 
@dataclass
class SfSession:
    """Salesforce session — manages authentication and provides REST helpers.

    Construct via one of two paths:
    1. **Direct** - supply session_id and (instance or instance_url).
    2. **Client-credentials** - supply consumer_key, consumer_secret,
       and domain.
    """

    # init params
    session_id: str | None = None
    instance: str | None = None
    instance_url: str | None = None
    session: requests.Session = field(default_factory=requests.Session)
    domain: str = 'login'
    consumer_key: str | None = None
    consumer_secret: str | None = None
    sf_version: str = API_VERSION
    parse_float: Optional[Callable[[str], Any]] = None
    object_pairs_hook: Optional[Callable[[list[tuple[Any, Any]]], Any]] = field(
        default=OrderedDict
    )

    #  derived (computed in __post_init__)
    auth_type: str = field(init=False, default='')
    sf_instance: str = field(init=False, default='')
    headers: Headers = field(
        init=False,
        default_factory=lambda: cast(Headers, {}),
    )
    base_url: str = field(init=False, default='')
    apex_url: str = field(init=False, default='')
    bulk_url: str = field(init=False, default='')
    bulk2_url: str = field(init=False, default='')
    tooling_url: str = field(init=False, default='')
    oauth2_url: str = field(init=False, default='')
    auth_site: str = field(init=False, default='')
    api_usage: MutableMapping[str, Usage | PerAppUsage] = field(
        init=False,
        default_factory=lambda: cast(
            MutableMapping[str, Usage | PerAppUsage], {}
        ),
    )
    # Aliases used internally so parse helpers don't need extra indirection
    _parse_float: Optional[Callable[[str], Any]] = field(
        init=False, default=None, repr=False
    )
    _object_pairs_hook: Optional[Callable[..., Any]] = field(
        init=False, default=None, repr=False
    )

    def __post_init__(self) -> None:
        self._parse_float = self.parse_float
        self._object_pairs_hook = self.object_pairs_hook

        if all(
            arg is not None
            for arg in (self.session_id, self.instance or self.instance_url)
        ):
            self._init_direct()

        else:
            self._init_client_credentials()

        self.auth_site = f'https://{self.domain}.salesforce.com'
        self._generate_headers()
        self._build_urls()

    # private init helpers
    def _init_direct(self) -> None:
        self.auth_type = 'direct'
        self.session_id = cast(str, self.session_id)

        if self.instance_url is not None:
            parsed = urlparse(self.instance_url)
            self.sf_instance = parsed.hostname or ''
            if parsed.port not in (None, 443):
                self.sf_instance += f':{parsed.port}'
        else:
            self.sf_instance = cast(str, self.instance)

    def _init_client_credentials(self) -> None:
        self.auth_type = 'client-credentials'
        if not self.instance_url:
            self.instance_url = os.getenv('BASE_URL')
        if not self.consumer_key:
            self.consumer_key = os.getenv('CONSUMER_KEY')
        if not self.consumer_secret:
            self.consumer_secret = os.getenv('CONSUMER_SECRET')
        self.refresh_session()

    def _generate_headers(self) -> None:
        """(Re)build Authorization headers from the current session_id."""
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.session_id}',
            'X-PrettyPrint': '1',
        }

    def _build_urls(self) -> None:
        """Derive all service URLs from sf_instance + sf_version."""
        base = f'https://{self.sf_instance}'
        ver = self.sf_version
        self.base_url = f'{base}/services/data/v{ver}/'
        self.apex_url = f'{base}/services/apexrest/'
        self.bulk_url = f'{base}/services/async/{ver}/'
        self.bulk2_url = f'{base}/services/data/v{ver}/jobs/'
        self.tooling_url = f'{self.base_url}tooling/'
        self.oauth2_url = f'{base}/services/oauth2/'

    def refresh_session(self) -> None:
        """Re-authenticate using stored credentials."""
        if self.auth_type != 'client-credentials':
            raise RuntimeError('Cannot refresh a direct session')
        self.session_id, self.sf_instance = self._fetch_token()
        self._generate_headers()
        self._build_urls()

    def _fetch_token(self) -> tuple[str, str]:
        """Fetch an OAuth access token and return (access_token, sf_instance)."""
        base = self.instance_url or f'https://{self.domain}.salesforce.com'
        response = self.session.post(
            f'{base}{OAUTH_URI}',
            data={
                'grant_type': 'client_credentials',
                'client_id': self.consumer_key,
                'client_secret': self.consumer_secret,
            },
        )
        try:
            data = response.json()
        except JSONDecodeError as exc:
            raise Exception(response.status_code, response.text) from exc
        if response.status_code != 200:
            raise Exception(data.get('error'), data.get('error_description'))
        return data['access_token'], data['instance_url'].removeprefix('https://')
    
    # SObject attribute access
    def __getattr__(self, name: str) -> Union[SfBulk2Handler, 'SfObjType', Any]:
        if name.startswith('__'):
            return super().__getattribute__(name)
        if name == 'bulk2':
            return SfBulk2Handler(self, self.bulk2_url)

        return SfObjType(
            object_name=name,
            sf_session=self,
            object_pairs_hook=self._object_pairs_hook or OrderedDict,
        )

    #  REST helpers

    def describe(self, **kwargs: Any) -> Optional[Any]:
        """Describe all available objects."""
        url = self.base_url + 'sobjects'
        result = self.call_salesforce('GET', url, **kwargs)
        json_result = self.parse_result_to_json(result)
        return json_result or None

    def is_sandbox(self) -> Optional[bool]:
        """Return whether the org is a sandbox."""
        if not self.session_id:
            return None
        return (
            self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
            .get('records', [{'IsSandbox': None}])[0]
            .get('IsSandbox')
        )

    def restful(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        method: str = 'GET',
        **kwargs: Any,
    ) -> Optional[Any]:
        """Make a direct REST call by relative path."""
        url = self.base_url + path
        result = self.call_salesforce(
            method, url, params=params, **kwargs
        )
        if result.status_code == 204:
            return None
        json_result = self.parse_result_to_json(result)
        return json_result or None

    #  Search / Query 
    def search(self, search: str) -> dict[str, Any]:
        """Execute a SOSL search."""
        url = self.base_url + 'search/'
        result = self.call_salesforce(
            'GET', url, params={'q': search}
        )
        json_result = self.parse_result_to_json(result)
        return json_result if json_result else {}

    def quick_search(self, search: str) -> dict[str, Any]:
        """Convenience wrapper: wraps *search* in FIND {…}."""
        return self.search(f'FIND {{{search}}}')

    def limits(self, **kwargs: Any) -> dict[str, Any]:
        """Return org limits."""
        url = self.base_url + 'limits/'
        result = self.call_salesforce('GET', url, **kwargs)
        if result.status_code != 200:
            raise Exception(result)
        return self.parse_result_to_json(result)

    def query(
        self,
        query: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute a SOQL query."""
        endpoint = 'queryAll/' if include_deleted else 'query/'
        url = self.base_url + endpoint
        result = self.call_salesforce(
            'GET', url, params={'q': query}, **kwargs
        )
        return self.parse_result_to_json(result)

    def query_more(
        self,
        next_records_identifier: str,
        identifier_is_url: bool = False,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Fetch the next page of query results."""
        if identifier_is_url:
            url = f'https://{self.sf_instance}{next_records_identifier}'
        else:
            endpoint = 'queryAll' if include_deleted else 'query'
            url = f'{self.base_url}{endpoint}/{next_records_identifier}'
        result = self.call_salesforce('GET', url, **kwargs)
        return self.parse_result_to_json(result)

    def query_all_iter(
        self,
        query: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Lazily iterate over all records for a query."""
        result = self.query(query, include_deleted=include_deleted, **kwargs)
        while True:
            yield from result['records']
            if not result['done']:
                result = self.query_more(
                    result['nextRecordsUrl'], identifier_is_url=True, **kwargs
                )
            else:
                return

    def query_all(
        self,
        query: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Eagerly fetch all records for a query."""
        all_records = list(
            self.query_all_iter(query, include_deleted=include_deleted, **kwargs)
        )
        return {
            'records': all_records,
            'totalSize': len(all_records),
            'done': True,
        }

    # HTTP layer
    def call_salesforce(
        self,
        method: str,
        url: str,
        retries: int = 0,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> requests.Response:
        """Perform an HTTP request to Salesforce with auto-retry on 401."""
        headers: dict[str, str] = dict(self.headers)
        headers.update(kwargs.pop('headers', {}) or {})

        result = self.session.request(method, url, headers=headers, **kwargs)

        if (
            self.auth_type == 'client-credentials'
            and result.status_code == 401
        ):
            error_details = result.json()[0]
            if error_details['errorCode'] == 'INVALID_SESSION_ID':
                self.refresh_session()
                retries += 1
                if retries > max_retries:
                    raise Exception(result, 'Max retries exceeded')
                return self.call_salesforce(
                    method, url, retries=retries, **kwargs
                )

        if result.status_code >= 300:
            raise Exception(f'HTTP {result.status_code} {method} {url}: {result.text}')

        sforce_limit_info = result.headers.get('Sforce-Limit-Info')
        if sforce_limit_info:
            self.api_usage = self.parse_api_usage(sforce_limit_info)

        return result

    # Parsing helpers
    @staticmethod
    def parse_api_usage(
        sforce_limit_info: str,
    ) -> MutableMapping[str, Union[Usage, PerAppUsage]]:
        """Parse the Sforce-Limit-Info response header."""
        result: MutableMapping[str, Union[Usage, PerAppUsage]] = {}

        api_usage = re.match(
            r'[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)', sforce_limit_info
        )
        pau = re.match(
            r'.+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)',
            sforce_limit_info,
        )

        if api_usage and api_usage.groups():
            g = api_usage.groups()
            result['api-usage'] = Usage(used=int(g[0]), total=int(g[1]))
        if pau and pau.groups():
            g = pau.groups()
            result['per-app-api-usage'] = PerAppUsage(
                used=int(g[0]), total=int(g[1]), name=g[2]
            )
        return result

    def parse_result_to_json(self, result: requests.Response) -> Any:
        """Parse JSON from a Response, respecting float/pairs hooks."""
        return result.json(
            object_pairs_hook=self._object_pairs_hook,
            parse_float=self._parse_float,
        )


# SfObjType
@dataclass
class SfObjType:
    """Interface to a specific Salesforce SObject type (e.g. Lead, Contact)."""

    object_name: str
    sf_session: SfSession
    sf_version: str = API_VERSION
    parse_float: Optional[Callable[[str], Any]] = None
    object_pairs_hook: Callable[[list[tuple[Any, Any]]], Any] = field(
        default=OrderedDict
    )

    #  derived 
    base_url: str = field(init=False, default='')
    api_usage: MutableMapping[str, Usage | PerAppUsage] = field(
        init=False,
        default_factory=lambda: cast(
            MutableMapping[str, Usage | PerAppUsage], {}
        ),
    )
    _parse_float: Optional[Callable[[str], Any]] = field(
        init=False, default=None, repr=False
    )
    _object_pairs_hook: Optional[Callable[..., Any]] = field(
        init=False, default=None, repr=False
    )

    def __post_init__(self) -> None:
        self._parse_float = self.parse_float
        self._object_pairs_hook = self.object_pairs_hook
        self.base_url = (
            f'https://{self.sf_session.sf_instance}/services/data/'
            f'v{self.sf_version}/sobjects/{self.object_name}/'
        )

    #  metadata / describe
    def metadata(self, headers: Optional[Headers] = None) -> dict[str, Any]:
        result = self.call_salesforce('GET', self.base_url, headers=headers)
        return self.parse_result_to_json(result)

    def describe(self, headers: Optional[Headers] = None) -> dict[str, Any]:
        result = self.call_salesforce(
            'GET', urljoin(self.base_url, 'describe'), headers=headers
        )
        return self.parse_result_to_json(result)

    def describe_layout(
        self, record_id: str, headers: Optional[Headers] = None
    ) -> dict[str, Any]:
        result = self.call_salesforce(
            'GET',
            urljoin(self.base_url, f'describe/layouts/{record_id}'),
            headers=headers,
        )
        return self.parse_result_to_json(result)

    #  CRUD 
    def get(
        self,
        record_id: str,
        headers: Optional[Headers] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        result = self.call_salesforce(
            'GET', urljoin(self.base_url, record_id), headers=headers, **kwargs
        )
        return self.parse_result_to_json(result)

    def get_by_custom_id(
        self,
        custom_id_field: str,
        custom_id: str,
        headers: Optional[Headers] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        url = urljoin(
            self.base_url, f'{custom_id_field}/{quote_plus(custom_id)}'
        )
        result = self.call_salesforce(
            'GET', url, headers=headers, **kwargs
        )
        return self.parse_result_to_json(result)

    def create(
        self,
        data: dict[str, Any],
        headers: Optional[Headers] = None,
    ) -> dict[str, Any]:
        result = self.call_salesforce(
            'POST', self.base_url, data=json.dumps(data), headers=headers
        )
        return self.parse_result_to_json(result)

    def upsert(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ) -> Union[int, requests.Response]:
        result = self.call_salesforce(
            'PATCH',
            urljoin(self.base_url, record_id),
            data=json.dumps(data),
            headers=headers,
        )
        return self._raw_response(result, raw_response)

    def update(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ) -> Union[int, requests.Response]:
        result = self.call_salesforce(
            'PATCH',
            urljoin(self.base_url, record_id),
            data=json.dumps(data),
            headers=headers,
        )
        return self._raw_response(result, raw_response)

    def delete(
        self,
        record_id: str,
        raw_response: bool = False,
        headers: Optional[Headers] = None,
    ) -> Union[int, requests.Response]:
        result = self.call_salesforce(
            'DELETE', urljoin(self.base_url, record_id), headers=headers
        )
        return self._raw_response(result, raw_response)

    def deleted(
        self,
        start: datetime,
        end: datetime,
        headers: Optional[Headers] = None,
    ) -> dict[str, Any]:
        url = urljoin(
            self.base_url,
            f'deleted/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}',
        )
        result = self.call_salesforce('GET', url, headers=headers)
        return self.parse_result_to_json(result)

    def updated(
        self,
        start: datetime,
        end: datetime,
        headers: Optional[Headers] = None,
    ) -> dict[str, Any]:
        url = urljoin(
            self.base_url,
            f'updated/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}',
        )
        result = self.call_salesforce('GET', url, headers=headers)
        return self.parse_result_to_json(result)

    #  base64 file operations 
    def upload_base64(
        self,
        file_path: str,
        base64_field: str = 'Body',
        headers: Optional[Headers] = None,
        **kwargs: Any,
    ) -> requests.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        return self.call_salesforce(
            'POST', self.base_url, headers=headers,
            json={base64_field: body}, **kwargs,
        )

    def update_base64(
        self,
        record_id: str,
        file_path: str,
        base64_field: str = 'Body',
        headers: Optional[Headers] = None,
        raw_response: bool = False,
        **kwargs: Any,
    ) -> Union[int, requests.Response]:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        result = self.call_salesforce(
            'PATCH',
            urljoin(self.base_url, record_id),
            json={base64_field: body},
            headers=headers,
            **kwargs,
        )
        return self._raw_response(result, raw_response)

    def get_base64(
        self,
        record_id: str,
        base64_field: str = 'Body',
        data: Optional[Any] = None,
        headers: Optional[Headers] = None,
        **kwargs: Any,
    ) -> bytes:
        result = self.call_salesforce(
            'GET',
            urljoin(self.base_url, f'{record_id}/{base64_field}'),
            data=data,
            headers=headers,
            **kwargs,
        )
        return result.content

    #  HTTP layer 
    def call_salesforce(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        return self.sf_session.call_salesforce(method, url, **kwargs)

    # Utilities 
    @staticmethod
    def _raw_response(
        response: requests.Response, body_flag: bool
    ) -> Union[int, requests.Response]:
        return response if body_flag else response.status_code

    def parse_result_to_json(self, result: requests.Response) -> Any:
        return result.json(
            object_pairs_hook=self._object_pairs_hook,
            parse_float=self._parse_float,
        )


##################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
import csv
# import datetime
import http.client as http
import io
# import json
import math
# import os
import sys
import tempfile
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from enum import Enum
from functools import partial
from time import sleep
from typing import Any, AnyStr, Generator, Iterator, \
    Optional, Union, Literal, TypedDict


_QuotingType = Literal[0, 1, 2, 3, 4, 5]

# import requests
from more_itertools import chunked

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
    CARET = "CARET"  # (^)
    COMMA = "COMMA"  # (,)
    PIPE = "PIPE"  # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"  # (\t)

_delimiter_char = {
    ColumnDelimiter.BACKQUOTE: "`",
    ColumnDelimiter.CARET: "^",
    ColumnDelimiter.COMMA: ",",
    ColumnDelimiter.PIPE: "|",
    ColumnDelimiter.SEMICOLON: ";",
    ColumnDelimiter.TAB: "\t",
    }

class LineEnding(str,Enum):
    LF = "LF"
    CRLF = "CRLF"

_line_ending_char = {LineEnding.LF: "\n", LineEnding.CRLF: "\r\n"}

class ResultsType(str, Enum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"

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

QueryResult = Union[QueryRecordsResult, QueryFileResult]


# https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_prepare_csv.htm
MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024
MAX_INGEST_JOB_PARALLELISM = 10  # TODO: ? Salesforce limits
DEFAULT_QUERY_PAGE_SIZE = 50000


def _split_csv(
        filename: str | None = None,
        records: str | None = None,
        max_records: Optional[int] = None,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: _QuotingType = csv.QUOTE_MINIMAL
        ) -> Generator[tuple[int, str], None, None]:
    """Split a CSV file into chunks to avoid exceeding the Salesforce
    bulk 2.0 API limits.

    Arguments:
        * filename -- csv file
        * max_records -- the number of records per chunk, None for auto size
    """
    if filename:
        total_records = _count_csv(filename=filename,
                                   skip_header=True,
                                   line_ending=line_ending,
                                   column_delimiter=column_delimiter,
                                   quoting=quoting
                                   )
    else:
        total_records = _count_csv(data=records,
                                   skip_header=True,
                                   line_ending=line_ending,
                                   column_delimiter=column_delimiter,
                                    quoting=quoting
                                    )
    csv_data_size = os.path.getsize(filename) if filename else sys.getsizeof(records)
    _max_records: int = max_records or total_records
    _max_records = min(_max_records,total_records)
    max_bytes = min(csv_data_size,
        MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
        )  # -1 MB for sentinel

    dl = _delimiter_char[column_delimiter]
    le = _line_ending_char[line_ending]

    def _flush(header: list[str], records: list[list[str]]) -> io.StringIO:
        buffer = io.StringIO()
        writer = csv.writer(
            buffer,
            delimiter=dl,
            lineterminator=le,
            quoting=quoting,
        )
        writer.writerow(header)
        writer.writerows(records)
        return buffer

    def _split(csv_reader: Iterator[list[str]]) -> Generator[tuple[int, str], None, None]:
        fieldnames = next(csv_reader)
        records_size = 0
        bytes_size = 0
        buff: list[list[str]] = []
        for line in csv_reader:
            line_data_size = len(f"{dl}".join(line).encode("utf-8"))  # rough estimate
            records_size += 1
            bytes_size += line_data_size
            if records_size > _max_records or bytes_size > max_bytes:
                if buff:
                    yield records_size - 1, _flush(fieldnames, buff).getvalue()
                records_size = 1
                bytes_size = line_data_size
                buff = [line]
            else:
                buff.append(line)
        if buff:
            yield records_size, _flush(fieldnames, buff).getvalue()

    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            yield from _split(reader)
    elif records:
        reader = csv.reader(
            io.StringIO(records), delimiter=dl, lineterminator=le, quoting=quoting
        )
        yield from _split(reader)
    else:
        raise ValueError("Either filename or records must be provided")


def _count_csv(
        filename: str | None = None,
        data: str | None = None,
        skip_header: bool = False,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: _QuotingType = csv.QUOTE_MINIMAL
        ) -> int:
    """Count the number of records in a CSV file."""
    dl = _delimiter_char[column_delimiter]
    le = _line_ending_char[line_ending]
    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            if skip_header: next(reader)
            count = sum(1 for _ in reader)
    elif data:
        reader = csv.reader(
            io.StringIO(data), delimiter=dl, lineterminator=le, quoting=quoting
        )
        if skip_header: next(reader)
        count = sum(1 for _ in reader)
    else:
        raise ValueError("Either filename or data must be provided")

    return count


def _convert_dict_to_csv(
        data: Optional[list[dict[str, str]]],
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        quoting: _QuotingType = csv.QUOTE_MINIMAL,
        sort_keys: bool = False,
        ) -> str | None:
    """Converts list of dicts to CSV like object."""
    if not data:
        return None
    dl = _delimiter_char[column_delimiter]
    le = _line_ending_char[line_ending]
    keys = set(i for s in [d.keys() for d in data] for i in s)
    if sort_keys:
        keys = list(sorted(keys))
    dict_to_csv_file = io.StringIO()
    writer = csv.DictWriter(dict_to_csv_file,
                            fieldnames=keys,
                            delimiter=dl,
                            lineterminator=le,
                            quoting=quoting
                            )
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return dict_to_csv_file.getvalue()


def _get_csv_fieldnames(
        filename: str | None = None,
        records: Optional[list[dict[str, str]]] = None,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: _QuotingType = csv.QUOTE_MINIMAL
) -> list[str]:
    """Get fieldnames from a CSV file or list of records."""
    dl = _delimiter_char[column_delimiter]
    le = _line_ending_char[line_ending]
    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            filenames = next(reader)
    elif records:
        filenames = list(records[0].keys())
    else:
        raise ValueError("Either filename or records must be provided")
    return filenames


class SfBulk2Handler:
    """Bulk 2.0 API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk2.Contacts.insert(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(self, sf_session: 'SfSession', bulk2_url: str):
        """Initialize the instance with the given parameters.

        Arguments:

        * sf_session -- the active SfSession used for authenticated requests
        * bulk2_url -- 2.0 API endpoint set in Salesforce instance
        """
        self.sf_session = sf_session
        self.bulk2_url = bulk2_url

    def query(self, soql: str, **kwargs: Any) -> Any:
        """Run a Bulk 2.0 query without needing an object name (taken from SOQL)."""
        return SfBulk2Type(
            object_name='_query',
            bulk2_url=self.bulk2_url,
            sf_session=self.sf_session,
        ).query(soql, **kwargs)

    def query_all(self, soql: str, **kwargs: Any) -> Any:
        """Run a Bulk 2.0 queryAll without needing an object name."""
        return SfBulk2Type(
            object_name='_query',
            bulk2_url=self.bulk2_url,
            sf_session=self.sf_session,
        ).query_all(soql, **kwargs)

    def __getattr__(self, name: str) -> "SfBulk2Type":
        return SfBulk2Type(
            object_name=name,
            bulk2_url=self.bulk2_url,
            sf_session=self.sf_session,
            )

############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

class _Bulk2Client:
    """Bulk 2.0 API client"""
    JSON_CONTENT_TYPE = "application/json"
    CSV_CONTENT_TYPE = "text/csv; charset=UTF-8"

    DEFAULT_WAIT_TIMEOUT_SECONDS = 86400  # 24-hour bulk job running time
    MAX_CHECK_INTERVAL_SECONDS = 60.0

    def __init__(
            self,
            object_name: str,
            bulk2_url: str,
            sf_session: 'SfSession',
            ):
        """
        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. Lead or Contact
        * bulk2_url -- 2.0 API endpoint set in Salesforce instance
        * sf_session -- the active SfSession used for authenticated requests
        """
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self.sf_session = sf_session

    def _get_headers(
            self,
            request_content_type: str | None = None,
            accept_content_type: str | None = None
            ) -> dict[str, str]:
        """Get content-type/accept override headers for bulk 2.0 API request"""
        return {
            "Content-Type": request_content_type or self.JSON_CONTENT_TYPE,
            "Accept": accept_content_type or self.JSON_CONTENT_TYPE,
        }

    def _construct_request_url(
            self,
            job_id: str | None,
            is_query: bool
            ) -> str:
        """Construct bulk 2.0 API request URL"""
        if not job_id:
            job_id = ""
        url: str
        if is_query:
            url = self.bulk2_url + "query"
        else:
            url = self.bulk2_url + "ingest"
        if job_id:
            url = f"{url}/{job_id}"
        return url

    def create_job(
            self,
            operation: Operation,
            query: str | None = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            external_id_field: str | None = None,
            ) -> Any:
        """Create job

        Arguments:
        * operation -- Bulk operation to be performed by job
        * query -- SOQL query to be performed by job
        * column_delimiter -- The column delimiter used for CSV job data
        * line_ending -- The line ending used for CSV job data
        * external_id_field -- The external ID field in the object being updated
        """
        payload: dict[str, Any] = {
            "operation": operation,
            "columnDelimiter": column_delimiter,
            "lineEnding": line_ending,
            }
        if external_id_field:
            payload["externalIdFieldName"] = external_id_field

        is_query = operation in (Operation.query, Operation.query_all)
        url = self._construct_request_url(None,
                                          is_query
                                          )
        if is_query:
            headers = self._get_headers(
                self.JSON_CONTENT_TYPE,
                self.CSV_CONTENT_TYPE
                )
            if not query:
                raise Exception("Query is required for query jobs")
            payload["query"] = query
        else:
            headers = self._get_headers(
                self.JSON_CONTENT_TYPE,
                self.JSON_CONTENT_TYPE
                )
            payload["object"] = self.object_name
            payload["contentType"] = "CSV"
        result = self.sf_session.call_salesforce(
            "POST", url,
            headers=headers,
            data=json.dumps(payload, allow_nan=False),
            )
        return result.json(object_pairs_hook=OrderedDict)

    def wait_for_job(
            self,
            job_id: str,
            is_query: bool,
            wait: float = 0.5
            ) -> Literal[JobState.job_complete]:
        """Wait for job completion or timeout"""
        import datetime
        expiration_time: datetime = (
            datetime.datetime.now() +
            datetime.timedelta(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
            )
        job_status = JobState.in_progress if is_query else JobState.open
        delay_timeout = 0.0
        delay_cnt = 0
        sleep(wait)
        while datetime.datetime.now() < expiration_time:
            job_info = self.get_job(job_id,
                                    is_query
                                    )
            job_status = job_info["state"]
            if job_status in [
                JobState.job_complete,
                JobState.aborted,
                JobState.failed,
                ]:
                if job_status != JobState.job_complete:
                    error_message = job_info.get("errorMessage") or job_info
                    raise Exception(f"Job failure. Response content: {error_message}")
                return job_status  # JobComplete

            if delay_timeout < self.MAX_CHECK_INTERVAL_SECONDS:
                delay_timeout = wait + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1
            sleep(delay_timeout)
        raise Exception(f"Job timeout. Job status: {job_status}")

    def abort_job(self,job_id: str,is_query: bool) -> Any:
        """Abort query/ingest job"""
        return self._set_job_state(job_id,is_query,JobState.aborted)

    def close_job(self,job_id: str) -> Any:
        """Close ingest job"""
        return self._set_job_state(
            job_id,
            False,
            JobState.upload_complete
            )

    def delete_job(self, job_id: str, is_query: bool) -> Any:
        """Delete query/ingest job"""
        url = self._construct_request_url(job_id,
                                          is_query
                                          )
        result = self.sf_session.call_salesforce(
            "DELETE", url,
            headers=self._get_headers(),
            )
        return result.json(object_pairs_hook=OrderedDict)

    def _set_job_state(self, job_id: str, is_query: bool, state: str) -> Any:
        """Set job state"""
        url = self._construct_request_url(job_id, is_query)
        payload = {"state": state}
        result = self.sf_session.call_salesforce(
            "PATCH", url,
            headers=self._get_headers(),
            data=json.dumps(payload, allow_nan=False),
            )
        return result.json(object_pairs_hook=OrderedDict)

    def get_job(self,
                job_id: str,
                is_query: bool
                ) -> Any:
        """Get job info"""
        url = self._construct_request_url(job_id, is_query)
        result = self.sf_session.call_salesforce("GET", url)
        return result.json(object_pairs_hook=OrderedDict)

    def filter_null_bytes(self, b: AnyStr) -> AnyStr:
        """https://github.com/airbytehq/airbyte/issues/8300"""
        if isinstance(b, str):
            return b.replace("\x00", "")
        if isinstance(b, bytes):
            return b.replace(b"\x00", b"")
        raise TypeError("Expected str or bytes")

    def get_query_results(
            self,
            job_id: str,
            locator: str = "",
            max_records: int = DEFAULT_QUERY_PAGE_SIZE
            ) -> QueryRecordsResult:
        """Get results for a query job"""
        url = self._construct_request_url(job_id,  True) + "/results"
        params: QueryParameters = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator
        headers = self._get_headers(
            self.JSON_CONTENT_TYPE,
            self.CSV_CONTENT_TYPE
            )
        result = self.sf_session.call_salesforce(
            "GET", url,
            headers=headers,
            params=params,
            )
        locator = result.headers.get("Sforce-Locator", "")
        if locator == "null":
            locator = ""
        number_of_records = int(result.headers["Sforce-NumberOfRecords"])
        return {
            "locator": locator,
            "number_of_records": number_of_records,
            "records": self.filter_null_bytes(result.content.decode('utf-8')),
            }

    def download_job_data(
            self,
            path: str,
            job_id: str,
            locator: str = "",
            max_records: int = DEFAULT_QUERY_PAGE_SIZE,
            chunk_size: int = 1024,
            ) -> QueryFileResult:
        """Get results for a query job"""
        if not os.path.exists(path):
            raise Exception(f"Path does not exist: {path}")

        url = self._construct_request_url(job_id, True) + "/results"
        params: QueryParameters = {"maxRecords": max_records}
        if locator and locator != "null":
            params["locator"] = locator
        headers = self._get_headers(
            self.JSON_CONTENT_TYPE,
            self.CSV_CONTENT_TYPE
            )
        with closing(
                self.sf_session.call_salesforce(
                    "GET", url,
                    headers=headers,
                    params=params,
                    stream=True,
                    )
                ) as result, tempfile.NamedTemporaryFile(
            "wb",
            dir=path,
            suffix=".csv",
            delete=False
            ) as bos:
            locator = result.headers.get("Sforce-Locator", "")
            if locator == "null":
                locator = ""
            number_of_records = int(result.headers["Sforce-NumberOfRecords"])
            for chunk in result.iter_content(chunk_size=chunk_size):
                bos.write(self.filter_null_bytes(chunk))
            # check the file exists
            if os.path.isfile(bos.name):
                return {
                    "locator": locator,
                    "number_of_records": number_of_records,
                    "file": bos.name,
                    }
            raise Exception(f"The IO/Error occured while verifying binary data. File {bos.name} doesn't exist, url: {url}, ")

    def upload_job_data(
            self,
            job_id: str,
            data: str,
            content_url: str | None = None
            ) -> None:
        """Upload job data"""
        if not data:
            raise Exception("Data is required for ingest jobs")

        # performance reduction here
        data_size = len(data.encode("utf-8"))
        if data_size > MAX_INGEST_JOB_FILE_SIZE:
            raise Exception(f"Data size {data_size} exceeds the max file size accepted by Bulk V2 (100 MB)")

        url = (content_url or self._construct_request_url(job_id, False) + "/batches"
        )
        headers = self._get_headers(
            self.CSV_CONTENT_TYPE,
            self.JSON_CONTENT_TYPE
            )
        result = self.sf_session.call_salesforce(
            "PUT", url,
            headers=headers,
            data=data.encode("utf-8"),
            )
        if result.status_code != http.CREATED:
            raise Exception(
                f"Failed to upload job data. Error Code {result.status_code}. "
                f"Response content: {result.content.decode()}"
                )

    def get_ingest_results(self, job_id: str, results_type: str) -> str:
        """Get record results"""
        url = self._construct_request_url(job_id, False) + "/" + results_type
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        result = self.sf_session.call_salesforce("GET", url, headers=headers)
        return result.text

    def download_ingest_results(
            self,
            file: str,
            job_id: str,
            results_type: str,
            chunk_size: int = 1024
            ) -> None:
        """Download record results to a file"""
        url = self._construct_request_url(job_id, False) + "/" + results_type
        headers = self._get_headers(self.JSON_CONTENT_TYPE, self.CSV_CONTENT_TYPE)
        with closing(
                self.sf_session.call_salesforce("GET", url, headers=headers)
                ) as result, open(file, "wb") as bos:
            
            for chunk in result.iter_content(chunk_size=chunk_size):
                bos.write(self.filter_null_bytes(chunk))

        if not os.path.exists(file):
            raise Exception( f"The IO/Error occured while verifying binary data. File {file} doesn't exist, url: {url}, ")


class SfBulk2Type:
    """Interface to Bulk 2.0 API functions"""

    def __init__(
            self,
            object_name: str,
            bulk2_url: str,
            sf_session: 'SfSession',
            ):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. Lead or Contact
        * bulk2_url -- API endpoint set in Salesforce instance
        * sf_session -- the active SfSession used for authenticated requests
        """
        self.object_name = object_name
        self.bulk2_url = bulk2_url
        self.sf_session = sf_session
        self._client = _Bulk2Client(object_name, bulk2_url, sf_session)

    def _upload_data(
            self,
            operation: Operation,
            data: Union[str, tuple[int, str]],
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            external_id_field: str | None = None,
            wait: int = 5,
            ) -> dict[str, int]:
        """Upload data to Salesforce"""
        unpacked_data: str
        if isinstance(data,tuple):
            total, unpacked_data = data
        else:
            total = _count_csv(
                data=data,
                line_ending=line_ending,
                skip_header=True
                )
            unpacked_data = data
        res = self._client.create_job(
            operation,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            external_id_field=external_id_field,
            )
        job_id = res["id"]
        try:
            if res["state"] == JobState.open:
                self._client.upload_job_data(job_id, unpacked_data)
                self._client.close_job(job_id)
                self._client.wait_for_job(job_id,False, wait)
                res = self._client.get_job(job_id,False)
                return {
                    "numberRecordsFailed": int(res["numberRecordsFailed"]),
                    "numberRecordsProcessed": int(res["numberRecordsProcessed"]),
                    "numberRecordsTotal": int(total),
                    "job_id": job_id,
                    }
            raise Exception(f"Failed to upload job data. Response content: {res}")
        except Exception:
            res = self._client.get_job(job_id, False)
            if res["state"] in (
                    JobState.upload_complete,
                    JobState.in_progress,
                    JobState.open,
                    ):
                self._client.abort_job(job_id, False)
            raise

    def _constrain_id_only(
            self,
            csv_file: str | None = None,
            records: list[dict[str, str]] | None = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
    ) -> None:
        header = _get_csv_fieldnames(
            filename=csv_file,
            records=records,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting
        )
        if header != ["Id"]:
            raise Exception(f"InvalidBatch: The 'delete/hard_delete' batch must contain only 'Id', {header}")


    def _upload_file(
            self,
            operation: Operation,
            csv_file: str | None = None,
            records: str | None = None,
            batch_size: Optional[int] = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            external_id_field: str | None = None,
            concurrency: int = 1,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """Upload csv file to Salesforce"""
        if csv_file and records:
            raise Exception("Cannot include both file and records")
        elif csv_file:
            if not os.path.exists(csv_file):
                raise Exception(csv_file + " not found.")
        elif records:
            pass
        else:
            raise Exception("Must include either file or records")

        results: list[dict[str, int]] = []
        workers = min(concurrency, MAX_INGEST_JOB_PARALLELISM)
        split_data = _split_csv(filename=csv_file,
                                max_records=batch_size,
                                line_ending=line_ending,
                                column_delimiter=column_delimiter,
                                quoting=quoting
                                ) \
            if \
            csv_file else _split_csv(records=records,
                                     max_records=batch_size,
                                     line_ending=line_ending,
                                     column_delimiter=column_delimiter,
                                     quoting=quoting
                                     )
        if workers == 1:
            for data in split_data:
                result = self._upload_data(
                    operation,
                    data,
                    column_delimiter,
                    line_ending,
                    external_id_field,
                    wait,
                    )
                results.append(result)
        else:
            # OOM is possible if the file is too large
            for chunks in chunked(split_data, n=workers):
                workers = min(workers, len(chunks))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    multi_thread_worker = partial(
                        self._upload_data,
                        operation,
                        column_delimiter=column_delimiter,
                        line_ending=line_ending,
                        external_id_field=external_id_field,
                        wait=wait,
                        )
                    _results = pool.map(multi_thread_worker, chunks)
                results.extend(list(_results))
        return results

    def delete(
            self,
            csv_file: str | None = None,
            records: Optional[list[dict[str, str]]] = None,
            batch_size: Optional[int] = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            external_id_field: str | None = None,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """soft delete records"""
        self._constrain_id_only(
            csv_file=csv_file,
            records=records,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
        )
        return self._upload_file(
            Operation.delete,
            csv_file=csv_file,
            records=_convert_dict_to_csv(
                records,
                column_delimiter=column_delimiter,
                line_ending=line_ending,
                quoting=quoting
                ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
            external_id_field=external_id_field,
            wait=wait,
            )

    def insert(
            self,
            csv_file: str | None = None,
            records: Optional[list[dict[str, str]]] = None,
            batch_size: Optional[int] = None,
            concurrency: int = 1,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """insert records"""
        return self._upload_file(
            Operation.insert,
            csv_file=csv_file,
            records=_convert_dict_to_csv(
                records,
                column_delimiter=column_delimiter,
                line_ending=line_ending,
                quoting=quoting
                ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
            concurrency=concurrency,
            wait=wait,
            )

    def upsert(
            self,
            csv_file: str | None = None,
            records: Optional[list[dict[str, str]]] = None,
            external_id_field: str = 'Id',
            batch_size: Optional[int] = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """upsert records based on a unique identifier"""
        return self._upload_file(
            Operation.upsert,
            csv_file=csv_file,
            records=_convert_dict_to_csv(
                records,
                column_delimiter=column_delimiter,
                line_ending=line_ending,
                quoting=quoting
                ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
            external_id_field=external_id_field,
            wait=wait,
            )

    def update(
            self,
            csv_file: str | None = None,
            records: Optional[list[dict[str, str]]] = None,
            batch_size: Optional[int] = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """update records"""
        return self._upload_file(
            Operation.update,
            csv_file=csv_file,
            records=_convert_dict_to_csv(
                records,
                column_delimiter=column_delimiter,
                line_ending=line_ending,
                quoting=quoting
                ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
            wait=wait,
            )

    def hard_delete(
            self,
            csv_file: str | None = None,
            records: Optional[list[dict[str, str]]] = None,
            batch_size: Optional[int] = None,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            quoting: _QuotingType = csv.QUOTE_MINIMAL,
            wait: int = 5,
            ) -> list[dict[str, int]]:
        """hard delete records"""
        self._constrain_id_only(
            csv_file=csv_file,
            records=records,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
        )
        return self._upload_file(
            Operation.hard_delete,
            csv_file=csv_file,
            records=_convert_dict_to_csv(
                records,
                column_delimiter=column_delimiter,
                line_ending=line_ending
                ),
            batch_size=batch_size,
            column_delimiter=column_delimiter,
            line_ending=line_ending,
            quoting=quoting,
            wait=wait,
            )

    def query(
            self,
            query: str,
            max_records: int = DEFAULT_QUERY_PAGE_SIZE,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            wait: int = 5,
            ) -> Generator[Union[str, int], None, None]:
        """bulk 2.0 query

        Arguments:
        * query -- SOQL query
        * max_records -- max records to retrieve per batch, default 50000

        Returns:
        * locator  -- the locator for the next set of results
        * number_of_records -- the number of records in this set
        * records -- records in this set
        """
        res = self._client.create_job(
            Operation.query,
            query,
            column_delimiter,
            line_ending
            )
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.get_query_results(
                job_id,
                locator,
                max_records
                )
            locator = result["locator"]
            yield result["records"]

    def query_all(
            self,
            query: str,
            max_records: int = DEFAULT_QUERY_PAGE_SIZE,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            wait: int = 5,
            ) -> Generator[str, None, None]:
        """bulk 2.0 query_all
        Arguments:
        * query -- SOQL query
        * max_records -- max records to retrieve per batch, default 50000

        Returns:
        * locator  -- the locator for the next set of results
        * number_of_records -- the number of records in this set
        * records -- records in this set
        """
        res = self._client.create_job(
            Operation.query_all,
            query,
            column_delimiter,
            line_ending
            )
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.get_query_results(
                job_id,
                locator,
                max_records
                )
            locator = result["locator"]
            yield result["records"]

    def download(
            self,
            query: str,
            path: str,
            max_records: int = DEFAULT_QUERY_PAGE_SIZE,
            column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
            line_ending: LineEnding = LineEnding.LF,
            wait: int = 5,
            ) -> list[QueryFileResult]:
        """bulk 2.0 query stream to file, avoiding high memory usage

        Arguments:
        * query -- SOQL query
        * max_records -- max records to retrieve per batch, default 50000

        Returns:
        * locator  -- the locator for the next set of results
        * number_of_records -- the number of records in this set
        * file -- downloaded file
        """
        if not os.path.exists(path):
            raise Exception(f"Path does not exist: {path}")

        res = self._client.create_job(
            Operation.query,
            query,
            column_delimiter,
            line_ending
            )
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        results: list[QueryFileResult] = []
        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.download_job_data(
                path,
                job_id,
                locator,
                max_records
                )
            locator = result["locator"]
            results.append(result)
        return results

    def _retrieve_ingest_records(self,job_id: str, results_type: str,file: str | None = None) -> str:
        """ Retrieves the Salesforce records for a given job and result type, either returning the results as a string or downloading them to a file if a file path is provided. """
        if not file:
            return self._client.get_ingest_results(job_id, results_type)
        self._client.download_ingest_results(file, job_id, results_type)
        return ""

    def get_failed_records(self, job_id: str,file: str | None = None) -> str:
        """Retrieve the records that failed in a specific batch job, optionally downloading them to a file if a file path is provided."""
        return self._retrieve_ingest_records(job_id, ResultsType.failed, file)

    def get_unprocessed_records(self, job_id: str, file: str | None = None) -> str:
        """Retrieve the records that were not processed in a specific batch job, optionally downloading them to a file if a file path is provided."""
        return self._retrieve_ingest_records(job_id, ResultsType.unprocessed, file)

    def get_successful_records(self, job_id: str, file: str | None = None) -> str:
        """Retrieve the records that were successfully processed in a specific batch job, optionally downloading them to a file if a file path is provided."""
        return self._retrieve_ingest_records(job_id, ResultsType.successful, file)

    def get_all_ingest_records(
            self,
            job_id: str,
            file: str | None = None
            ) -> dict[str, list[Any]]:
        successful_records = csv.DictReader(
            self.get_successful_records(
                job_id=job_id,
                file=file
                ).splitlines(),
            delimiter=',',
            lineterminator='\n', )
        failed_records = csv.DictReader(
            self.get_failed_records(
                job_id=job_id,
                file=file
                ).splitlines(),
            delimiter=',',
            lineterminator='\n', )
        unprocessed_records = csv.DictReader(
            self.get_unprocessed_records(
                job_id=job_id,
                file=file
            ).splitlines(),
            delimiter=',',
            lineterminator='\n', )
        return {
            'successfulRecords': list(successful_records),
            'failedRecords': list(failed_records),
            'unprocessedRecords': list(unprocessed_records)
            }
