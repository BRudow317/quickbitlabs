from __future__ import annotations
import base64
from datetime import datetime
from pathlib import Path
from typing import Any
from collections.abc import Iterator

# dependencies
from urllib.parse import quote_plus
import requests

# locals
from server.connectors.sf.utils.date_to_iso8601 import date_to_iso8601
from server.connectors.sf.models import SKIP_SUFFIXES, SKIP_NAMES

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.connectors.sf.HttpClient import HttpClient

import logging
logger = logging.getLogger(__name__)


class SfRest:
    """
    Handles global REST API operations (SOQL, Limits, Global Describes) 
    and acts as a factory for specific SObject operations.
    """
    _http: HttpClient

    def __init__(self, http_client: HttpClient):
        self._http = http_client

    def __getattr__(self, name: str) -> "SfObjType":
        """Allows dot-notation access to SObjects.
        Example: sf.rest.Contact.get('003...')
        """
        if name.startswith('__'):
            return super().__getattribute__(name)
        return SfObjType(object_name=name, http_client=self._http)

    def describe(self, **kwargs: Any) -> dict[str, Any]:
        """Describe all available objects (Global Describe)."""
        response = self._http.request('GET', 'sobjects', **kwargs)
        return self._http.parse_json(response)

    def is_sandbox(self) -> bool:
        """Return whether the org is a sandbox."""
        result = self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
        records = result.get('records', [{'IsSandbox': False}])
        return records[0].get('IsSandbox', False)

    def request(self, 
                path: str,
                params: dict[str, Any] | None = None, 
                method: str = 'GET', 
                **kwargs: Any
                ) -> dict[str, Any] | None:
        """Make a direct REST call by relative path."""
        response = self._http.request(method, path, params=params, **kwargs)
        return self._http.parse_json(response)

    def search(self, search_str: str) -> dict[str, Any]:
        """Execute a SOSL search."""
        response = self._http.request('GET', 'search/', params={'q': search_str})
        return self._http.parse_json(response) or {}

    def quick_search(self, search_str: str) -> dict[str, Any]:
        """Convenience wrapper: wraps search in FIND {…}."""
        return self.search(f'FIND {{{search_str}}}')

    def limits(self, **kwargs: Any) -> dict[str, Any]:
        """Return org REST limits."""
        response = self._http.request('GET', 'limits/', **kwargs)
        return self._http.parse_json(response)

    def query(self, query_str: str, include_deleted: bool = False, **kwargs: Any) -> dict[str, Any]:
        """Execute a SOQL query."""
        endpoint = 'queryAll/' if include_deleted else 'query/'
        response = self._http.request('GET', endpoint, params={'q': query_str}, **kwargs)
        return self._http.parse_json(response)

    def query_more(self, next_records_identifier: str, identifier_is_url: bool = False, include_deleted: bool = False, **kwargs: Any) -> dict[str, Any]:
        """Fetch the next page of query results."""
        if identifier_is_url:
            # If it's a full URL, pass it directly (HttpClient handles absolute URLs)
            endpoint = next_records_identifier
        else:
            base_endpoint = 'queryAll' if include_deleted else 'query'
            endpoint = f"{base_endpoint}/{next_records_identifier}"
            
        response = self._http.request('GET', endpoint, **kwargs)
        return self._http.parse_json(response)

    def query_all_iter(self, query_str: str, include_deleted: bool = False, **kwargs: Any) -> Iterator[Any]:
        """Lazily iterate over all records for a query."""
        result = self.query(query_str, include_deleted=include_deleted, **kwargs)
        while True:
            yield from result['records']
            if not result['done']:
                result = self.query_more(result['nextRecordsUrl'], identifier_is_url=True, **kwargs)
            else:
                return

    def query_all(self, query_str: str, include_deleted: bool = False, **kwargs: Any) -> dict[str, Any]:
        """Eagerly fetch all records for a query into memory."""
        all_records = list(self.query_all_iter(query_str, include_deleted=include_deleted, **kwargs))
        return {
            'records': all_records,
            'totalSize': len(all_records),
            'done': True,
        }
    
    def describe_all_fields(self) -> dict[str, list[dict[str, Any]]]:
        """Single SOQL call returning full field metadata for all queryable+layoutable objects.

        Returns a dict keyed by object name, value is the list of FieldDefinition records
        for that object. Replaces 183 individual /sobjects/{name}/describe calls with one
        paginated query.
        """
        soql = (
            "SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, DataType, "
            "IsNillable, IsUnique, IsIdLookup, Label, Length, Precision, Scale, "
            "IsCreatable, IsUpdatable, DefaultValue "
            "FROM FieldDefinition "
            "WHERE EntityDefinition.IsQueryable = true "
            "AND EntityDefinition.IsLayoutable = true"
        )
        fields_by_object: dict[str, list[dict[str, Any]]] = {}
        for record in self.query_all_iter(soql):
            obj_name = (record.get('EntityDefinition') or {}).get('QualifiedApiName')
            if obj_name:
                fields_by_object.setdefault(obj_name, []).append(record)
        return fields_by_object

    def describe_migratable(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Return only business-data objects from the global describe."""
        return [
            obj for obj in self.describe(**kwargs).get('sobjects', [])
            if self._is_migratable(obj)
        ]

    @staticmethod
    def _is_migratable(obj: dict[str, Any]) -> bool:
        if not obj.get('queryable') or not obj.get('retrieveable'):
            return False
        if not obj.get('layoutable') and not obj.get('searchable'):
            return False

        name = obj.get('name', '')

        if any(name.endswith(s) for s in SKIP_SUFFIXES):
            return False
        if name in SKIP_NAMES:
            return False

        return True

class SfObjType:
    """Interface to a specific Salesforce SObject type (e.g., Lead, Contact)."""
    object_name: str
    object_map: dict[str, Any] | None
    _http: HttpClient
    _base_endpoint: str
    
    def __init__(self, object_name: str, http_client: HttpClient):
        self.object_name = object_name or ""
        self.object_map = None
        self._http = http_client
        self._base_endpoint = f"sobjects/{self.object_name}"

    # Metadata / Describe
    def metadata(self, headers: dict | None = None) -> dict[str, Any]:
        response = self._http.request('GET', self._base_endpoint, headers=headers)
        return self._http.parse_json(response)

    def describe(self, headers: dict | None = None) -> dict[str, Any]:
        response = self._http.request('GET', f"{self._base_endpoint}/describe", headers=headers)
        return self._http.parse_json(response)

    def describe_layout(self, record_id: str, headers: dict | None = None) -> dict[str, Any]:
        response = self._http.request('GET', f"{self._base_endpoint}/describe/layouts/{record_id}", headers=headers)
        return self._http.parse_json(response)

    # CRUD
    def get(self, record_id: str, headers: dict | None = None, **kwargs: Any) -> dict[str, Any]:
        response = self._http.request('GET', f"{self._base_endpoint}/{record_id}", headers=headers, **kwargs)
        return self._http.parse_json(response)

    def get_by_custom_id(self, custom_id_field: str, custom_id: str, headers: dict | None = None, **kwargs: Any) -> dict[str, Any]:
        endpoint = f"{self._base_endpoint}/{custom_id_field}/{quote_plus(custom_id)}"
        response = self._http.request('GET', endpoint, headers=headers, **kwargs)
        return self._http.parse_json(response)

    def create(self, data: dict[str, Any], headers: dict | None = None) -> dict[str, Any]:
        response = self._http.request('POST', f"{self._base_endpoint}/", json=data, headers=headers)
        return self._http.parse_json(response)

    def upsert(self, record_id: str, data: dict[str, Any], raw_response: bool = False, headers: dict | None = None) -> int | requests.Response:
        response = self._http.request('PATCH', f"{self._base_endpoint}/{record_id}", json=data, headers=headers)
        return self._raw_response(response, raw_response)

    def update(self, record_id: str, data: dict[str, Any], raw_response: bool = False, headers: dict | None = None) -> int | requests.Response:
        response = self._http.request('PATCH', f"{self._base_endpoint}/{record_id}", json=data, headers=headers)
        return self._raw_response(response, raw_response)

    def delete(self, record_id: str, raw_response: bool = False, headers: dict | None = None) -> int | requests.Response:
        response = self._http.request('DELETE', f"{self._base_endpoint}/{record_id}", headers=headers)
        return self._raw_response(response, raw_response)

    # Get Deleted/Updated
    def deleted(self, start: datetime, end: datetime, headers: dict | None = None) -> dict[str, Any]:
        endpoint = f"{self._base_endpoint}/deleted/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}"
        response = self._http.request('GET', endpoint, headers=headers)
        return self._http.parse_json(response)

    def updated(self, start: datetime, end: datetime, headers: dict | None = None) -> dict[str, Any]:
        endpoint = f"{self._base_endpoint}/updated/?start={date_to_iso8601(start)}&end={date_to_iso8601(end)}"
        response = self._http.request('GET', endpoint, headers=headers)
        return self._http.parse_json(response)

    # Base64 File Operations
    def upload_base64(
            self, 
            file_path: str, 
            base64_field: str = 'Body', 
            headers: dict | None = None, 
            **kwargs: Any
            ) -> requests.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        return self._http.request('POST', f"{self._base_endpoint}/", headers=headers, json={base64_field: body}, **kwargs)

    def update_base64(self, record_id: str, file_path: str, base64_field: str = 'Body', headers: dict | None = None, raw_response: bool = False, **kwargs: Any) -> int | requests.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        response = self._http.request('PATCH', f"{self._base_endpoint}/{record_id}", json={base64_field: body}, headers=headers, **kwargs)
        return self._raw_response(response, raw_response)

    def get_base64(self, record_id: str, base64_field: str = 'Body', data: Any = None, headers: dict | None = None, **kwargs: Any) -> bytes:
        response = self._http.request('GET', f"{self._base_endpoint}/{record_id}/{base64_field}", data=data, headers=headers, **kwargs)
        return response.content

    # Utilities
    @staticmethod
    def _raw_response(response: requests.Response, return_raw: bool) -> int | requests.Response:
        return response if return_raw else response.status_code