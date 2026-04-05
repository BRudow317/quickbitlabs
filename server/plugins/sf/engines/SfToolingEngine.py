from __future__ import annotations
import base64, json
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import quote_plus
import httpx
from server.plugins.sf.models.SfExceptions import SfException
from server.plugins.sf.models.SfModels import SKIP_SUFFIXES, SKIP_NAMES, SF_BASE_URL, API_VERSION

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.plugins.sf.engines.SfClient import SfClient

import logging
logger = logging.getLogger(__name__)

class SfToolingApi:
    def __init__(self, http_client: SfClient) -> None:
        self._http = http_client
    
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

    async def tooling_execute(
            self,
            action: str | None = None,
            method: str = 'GET',
            data: dict[str, Any] | None = None,
            **kwargs: Any
            ) -> Any:
        """Makes an HTTP request to an TOOLING REST endpoint
        Arguments:
        * action -- The REST endpoint for the request.
        * method -- HTTP method for the request (default GET)
        * data -- A dict of parameters to send in a POST / PUT request
        * kwargs -- Additional kwargs to pass to `requests.request`
        """
        
        SF_TOOLING_URL: str = f"{SF_BASE_URL}/services/data/v{API_VERSION}/tooling"
        # If data is None, we should send an empty body, not "null", which is
        # None in json.
        json_data = json.dumps(data) if data is not None else None
        result = await self._http.request(
            method = method,
            url = SF_TOOLING_URL,
            name="tooling_execute",
            data=json_data,
            **kwargs
            )
        try:
            response_content = result.json()
        # pylint: disable=broad-except
        except Exception:
            response_content = result.text

        return response_content
    

# ---------------------------------------------------------------------------
# Test raw query
# ---------------------------------------------------------------------------
# QualifiedApiName, Label
async def test_tooling_query(rest):
    response_dict = {}
    sobj_list = ["Account", "Contact", "Case", "User"]
    for sobj in sobj_list:
        soql = f"""SELECT Id, DurableId, QualifiedapiName, 
        DeveloperName, MasterLabel, NamespacePrefix, 
        EditUrl, NewUrl, DetailUrl, EditDefinitionUrl,
        IsCustomizable, IsRetrievable, IsQueryable, 
        IsSearchable, IsReplicatable, IsEverCreatable,
        IsEverUpdatable, IsEverDeletable, IsDeprecatedAndHidden, 
        IsInterface, ImplementsInterfaces, ImplementedBy, ExtendsInterfaces, ExtendsInterfaces, ExtendedBy,
        DefaultImplementation, IsTriggerable, IsCustomSetting, 
        IsCustomSetting
        FROM EntityDefinition 
        WHERE DeveloperName like '%{sobj}%' 
        LIMIT 1
        """
        response = await rest.toolingexecute(soql) 
        response_dict[sobj] = response
    
        # 1. Check HTTP status before parsing
        # This will raise an exception for 4xx or 5xx errors
        response.raise_for_status() 
        
        jsponse = response.json()

        # 2. Salesforce specific: Check if it's an error list vs success dict
        if isinstance(jsponse, list):
            # Salesforce often returns errors as a list: [{"message": "...", "errorCode": "..."}]
            error = jsponse[0]
            raise SfException(f"SF API Error {error.get('errorCode')}: {error.get('message')}")

        # 3. Use .get() to avoid KeyErrors during debugging
        records = jsponse.get("records", [])
        total_size = jsponse.get("totalSize", 0)
        is_done = jsponse.get("done", True)

        assert "records" in jsponse, f"Response missing 'records' key. Keys found: {list(jsponse.keys())}"
        assert len(records) > 0, "Expected at least one queryable EntityDefinition"

        print(f"Fetched {len(records)}/{total_size} records.")
        if records:
            print(f"Sample record keys: {list(records[0].keys())}")
        
        if not is_done:
            print("Note: There are more pages of data available (done=false).")