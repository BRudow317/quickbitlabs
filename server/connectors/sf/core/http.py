from __future__ import annotations

import re
import requests 
from collections import OrderedDict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from collections.abc import MutableMapping

import logging
logger = logging.getLogger(__name__)

class Usage(NamedTuple):
    used: int
    total: int

class PerAppUsage(NamedTuple):
    used: int
    total: int
    name: str

class HttpClient:
    """A resilient HTTP wrapper that handles retries, headers, and JSON parsing."""
    base_url: str
    access_token: str
    refresh_callback: Callable[[], str] | None
    parse_float: Callable[[str], Any] | None
    object_pairs_hook: Callable

    def __init__(
        self, 
        base_url, 
        access_token, 
        refresh_callback = None,
        parse_float = None,
        object_pairs_hook: Callable = OrderedDict
        ) -> None:

        self.base_url = base_url
        self.session = requests.Session()
        self.refresh_callback = refresh_callback
        
        # Parsing hooks
        self._parse_float = parse_float
        self._object_pairs_hook = object_pairs_hook
        
        # State
        self.api_usage: MutableMapping[str, Usage | PerAppUsage] = {}
        self.set_token(access_token)

    def set_token(self, token: str) -> None:
        """Update the session headers with a new bearer token."""
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}',
            'X-PrettyPrint': '1',
        })

    def request(
        self, 
        method: str, 
        endpoint: str, 
        retries: int = 0, 
        max_retries: int = 3, 
        **kwargs: Any
    ) -> requests.Response:
        """Execute HTTP request with auto-retry for 401 Unauthorized."""
        
        # If endpoint is a full URL, use it. Otherwise, append to base_url.
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}/{endpoint.lstrip('/')}"
        
        response = self.session.request(method, url, **kwargs)

        # Handle Expired Token
        if response.status_code == 401 and self.refresh_callback:
            try:
                error_details = response.json()[0]
                if error_details.get('errorCode') == 'INVALID_SESSION_ID':
                    logger.info("Session invalid/expired. Refreshing token...")
                    new_token = self.refresh_callback()
                    self.set_token(new_token)
                    
                    retries += 1
                    if retries > max_retries:
                        raise Exception('Max retries exceeded while refreshing token.')
                    
                    return self.request(method, endpoint, retries=retries, max_retries=max_retries, **kwargs)
            except (ValueError, KeyError, IndexError):
                pass # Not a standard SF auth error, proceed to standard error handling

        if response.status_code >= 300:
            raise Exception(f'HTTP {response.status_code} {method} {url}: {response.text}')

        # Parse Salesforce Limits Header
        sforce_limit_info = response.headers.get('Sforce-Limit-Info')
        if sforce_limit_info:
            self._parse_api_usage(sforce_limit_info)

        return response

    def parse_json(self, response: requests.Response) -> Any:
        """Parse JSON respecting float/pairs hooks."""
        if response.status_code == 204:
            return None
        return response.json(
            object_pairs_hook=self._object_pairs_hook,
            parse_float=self._parse_float,
        )

    def _parse_api_usage(self, sforce_limit_info: str) -> None:
        """Parse the Sforce-Limit-Info response header into internal state."""
        api_usage = re.match(r'[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)', sforce_limit_info)
        pau = re.match(r'.+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)', sforce_limit_info)

        if api_usage and api_usage.groups():
            g = api_usage.groups()
            self.api_usage['api-usage'] = Usage(used=int(g[0]), total=int(g[1]))
        if pau and pau.groups():
            g = pau.groups()
            self.api_usage['per-app-api-usage'] = PerAppUsage(used=int(g[0]), total=int(g[1]), name=g[2])