from __future__ import annotations
import base64, json
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import quote_plus
import httpx
from server.plugins.sf.models.SfModels import SKIP_SUFFIXES, SKIP_NAMES, SF_BASE_URL, API_VERSION

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.plugins.sf.engines.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)


class SfRest:
    """
    Global REST API operations - SOQL, limits, global describe.
    Acts as a factory for SfObjType via dot-notation attribute access.
    """
    _http: SfClient

    def __init__(self, http_client: SfClient) -> None:
        self._http = http_client

    def __getattr__(self, name: str) -> "SfObjType":
        """Dot-notation access to SObjects. Example: sf.rest.Contact.get('003...')"""
        if name.startswith("__"):
            return super().__getattribute__(name)
        return SfObjType(object_name=name, http_client=self._http)

    async def describe(self, **kwargs: Any) -> dict[str, Any]:
        """Global describe - all available SObjects."""
        response = await self._http.request("GET", "sobjects", **kwargs)
        return response.json()

    async def is_sandbox(self) -> bool:
        """Return whether the org is a sandbox."""
        result = await self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
        records = result.get("records", [{"IsSandbox": False}])
        return records[0].get("IsSandbox", False)

    async def request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        method: str = "GET",
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Direct REST call by relative path."""
        response = await self._http.request(method, path, params=params, **kwargs)
        return response.json() if response.status_code != 204 else None

    async def search(self, search_str: str) -> dict[str, Any]:
        """Execute a SOSL search."""
        response = await self._http.request("GET", "search/", params={"q": search_str})
        return response.json() or {}

    async def quick_search(self, search_str: str) -> dict[str, Any]:
        """Wraps search string in FIND {…}."""
        return await self.search(f"FIND {{{search_str}}}")

    async def limits(self, **kwargs: Any) -> dict[str, Any]:
        """Org REST API limits."""
        response = await self._http.request("GET", "limits/", **kwargs)
        return response.json()

    async def query(
        self,
        query_str: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute a SOQL query. Returns first page."""
        endpoint = "queryAll/" if include_deleted else "query/"
        response = await self._http.request(
            "GET", endpoint, params={"q": query_str}, **kwargs
        )
        return response.json()

    async def query_more(
        self,
        next_records_identifier: str,
        identifier_is_url: bool = False,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Fetch the next page of SOQL results via nextRecordsUrl."""
        if identifier_is_url:
            endpoint = next_records_identifier
        else:
            base = "queryAll" if include_deleted else "query"
            endpoint = f"{base}/{next_records_identifier}"

        response = await self._http.request("GET", endpoint, **kwargs)
        return response.json()

    async def query_all_iter(
        self,
        query_str: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """
        Async generator - lazily yields individual records across all pages.
        Follows nextRecordsUrl automatically. Never loads all pages into memory.
        """
        result = await self.query(query_str, include_deleted=include_deleted, **kwargs)
        while True:
            for record in result["records"]:
                yield record
            if result["done"]:
                return
            result = await self.query_more(
                result["nextRecordsUrl"], identifier_is_url=True, **kwargs
            )

    async def query_all(
        self,
        query_str: str,
        include_deleted: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Eagerly fetch all records across all pages into memory.
        Use query_all_iter for large result sets.
        """
        records = [
            record
            async for record in self.query_all_iter(
                query_str, include_deleted=include_deleted, **kwargs
            )
        ]
        return {"records": records, "totalSize": len(records), "done": True}

    async def describe_migratable(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Global describe filtered to business-data objects only."""
        all_objects = (await self.describe(**kwargs)).get("sobjects", [])
        return [obj for obj in all_objects if self._is_migratable(obj)]

    @staticmethod
    def _is_migratable(obj: dict[str, Any]) -> bool:
        if not obj.get("queryable") or not obj.get("retrieveable"):
            return False
        if not obj.get("layoutable") and not obj.get("searchable"):
            return False
        name = obj.get("name", "")
        if any(name.endswith(s) for s in SKIP_SUFFIXES):
            return False
        if name in SKIP_NAMES:
            return False
        return True
    


    async def apex_execute(
            self,
            action: str | None = None,
            method: str = 'GET',
            data: dict[str, Any] | None = None,
            **kwargs: Any
            ) -> Any:
        """Makes an HTTP request to an APEX REST endpoint
        Arguments:
        * action -- The REST endpoint for the request.
        * method -- HTTP method for the request (default GET)
        * data -- A dict of parameters to send in a POST / PUT request
        * kwargs -- Additional kwargs to pass to `requests.request`
        """
        SF_APEX_URL: str = f"{SF_BASE_URL}/services/apexrest"
        # If data is None, we should send an empty body, not "null", which is
        # None in json.
        json_data = json.dumps(data) if data is not None else None
        result = await self._http.request(
            method = method,
            url = SF_APEX_URL,
            name="apex_execute",
            data=json_data,
            **kwargs
            )
        try:
            response_content = result.json()
        except Exception:
            response_content = result.text

        return response_content


class SfObjType:
    """
    Interface to a specific Salesforce SObject type.
    Constructed by SfRest.__getattr__ - not instantiated directly.
    """
    object_name: str
    _http: SfClient
    _base_endpoint: str

    def __init__(self, object_name: str, http_client: SfClient) -> None:
        self.object_name = object_name
        self._http = http_client
        self._base_endpoint = f"sobjects/{object_name}"

    # --- Describe ---

    async def metadata(self, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request("GET", self._base_endpoint, **kwargs)
        return response.json()

    async def describe(self, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            "GET", f"{self._base_endpoint}/describe", **kwargs
        )
        return response.json()

    async def describe_layout(self, record_id: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            "GET", f"{self._base_endpoint}/describe/layouts/{record_id}", **kwargs
        )
        return response.json()

    # --- CRUD ---

    async def get(self, record_id: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            "GET", f"{self._base_endpoint}/{record_id}", **kwargs
        )
        return response.json()

    async def get_by_custom_id(
        self, custom_id_field: str, custom_id: str, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = f"{self._base_endpoint}/{custom_id_field}/{quote_plus(custom_id)}"
        response = await self._http.request("GET", endpoint, **kwargs)
        return response.json()

    async def create(self, data: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        response = await self._http.request(
            "POST", f"{self._base_endpoint}/", json=data, **kwargs
        )
        return response.json()

    async def upsert(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        response = await self._http.request(
            "PATCH", f"{self._base_endpoint}/{record_id}", json=data, **kwargs
        )
        return self._raw_response(response, raw_response)

    async def update(
        self,
        record_id: str,
        data: dict[str, Any],
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        response = await self._http.request(
            "PATCH", f"{self._base_endpoint}/{record_id}", json=data, **kwargs
        )
        return self._raw_response(response, raw_response)

    async def delete(
        self,
        record_id: str,
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        response = await self._http.request(
            "DELETE", f"{self._base_endpoint}/{record_id}", **kwargs
        )
        return self._raw_response(response, raw_response)

    # --- Deleted / Updated Ranges ---

    async def deleted(
        self, start: datetime, end: datetime, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = (
            f"{self._base_endpoint}/deleted/"
            f"?start={start.isoformat()}&end={end.isoformat()}"
        )
        response = await self._http.request("GET", endpoint, **kwargs)
        return response.json()

    async def updated(
        self, start: datetime, end: datetime, **kwargs: Any
    ) -> dict[str, Any]:
        endpoint = (
            f"{self._base_endpoint}/updated/"
            f"?start={start.isoformat()}&end={end.isoformat()}"
        )
        response = await self._http.request("GET", endpoint, **kwargs)
        return response.json()

    # --- Base64 File Operations ---

    async def upload_base64(
        self,
        file_path: str,
        base64_field: str = "Body",
        **kwargs: Any,
    ) -> httpx.Response:
        # Note: Path.read_bytes() is blocking - acceptable for prototype phase
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        return await self._http.request(
            "POST", f"{self._base_endpoint}/", json={base64_field: body}, **kwargs
        )

    async def update_base64(
        self,
        record_id: str,
        file_path: str,
        base64_field: str = "Body",
        raw_response: bool = False,
        **kwargs: Any,
    ) -> int | httpx.Response:
        body = base64.b64encode(Path(file_path).read_bytes()).decode()
        response = await self._http.request(
            "PATCH",
            f"{self._base_endpoint}/{record_id}",
            json={base64_field: body},
            **kwargs,
        )
        return self._raw_response(response, raw_response)

    async def get_base64(
        self,
        record_id: str,
        base64_field: str = "Body",
        **kwargs: Any,
    ) -> bytes:
        response = await self._http.request(
            "GET",
            f"{self._base_endpoint}/{record_id}/{base64_field}",
            **kwargs,
        )
        return response.content

    # --- Utilities ---

    @staticmethod
    def _raw_response(response: httpx.Response, return_raw: bool) -> int | httpx.Response:
        return response if return_raw else response.status_code