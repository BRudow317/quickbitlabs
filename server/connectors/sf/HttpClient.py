from __future__ import annotations
import os
import re
from collections import OrderedDict 
from collections.abc import Callable
from models import API_VERSION
from typing import Any, NamedTuple
from collections.abc import MutableMapping

# dependencies
import requests

# locals
from server.connectors.sf.auth import fetch_client_credentials
from server.connectors.sf.models import API_VERSION, SF_BASE_URL, SF_QUERY_URI, SF_BASE_DOMAIN

# logging
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
    session: requests.Session
    base_url: str | None # Needed for reverse compatibility with basic auth logins
    instance_url: str | None # Doesn't contain the https:// prefix
    services_url: str | None
    query_url: str | None
    consumer_key: str | None
    consumer_secret: str | None
    access_token: str | None
    parse_float: Callable[[str], Any] | None
    object_pairs_hook: Callable
    api_version: str = API_VERSION
    retries: int = 0
    max_retries: int = 1
    methods: tuple[str, ...] = ('delete', 'get', 'head', 'options', 'patch', 'post', 'put', 'request')

    def __init__(
        self, 
        base_url: str | None = None,
        consumer_key: str | None = None, 
        consumer_secret: str | None = None,
        parse_float: Callable[[str], Any] | None = None,
        object_pairs_hook: Callable = OrderedDict,
        access_token: str | None = None
        ) -> None:
        
        self.base_url = base_url or SF_BASE_URL \
        or f"https://{SF_BASE_DOMAIN}.salesforce.com" \
        or "https://salesforce.com"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret

        if not access_token:
            self.access_token, self.instance_url = fetch_client_credentials(
                consumer_key = self.consumer_key, 
                consumer_secret = self.consumer_secret, 
                base_url = self.base_url
            )
        else:
            self.access_token = access_token
            self.instance_url = self.base_url.split("//")[1].split("/")[0]
        self.services_url = f"{self.base_url}/services/data/v{self.api_version}"
        self.query_url = f"{self.base_url}{SF_QUERY_URI}"
        
        self.session = requests.Session()
        self.refresh_callback = self._refresh_token_callback
        
        # Parsing hooks
        self._parse_float = parse_float
        self._object_pairs_hook = object_pairs_hook
        
        # State
        self.api_usage: MutableMapping[str, Usage | PerAppUsage] = {}
        self.set_token(self.access_token)

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
        max_retries: int = 1, 
        **kwargs: Any
    ) -> requests.Response:
        """Execute HTTP request with auto-retry for 401 Unauthorized."""
        # if endpoint is a full URL, use it. Otherwise, append to the versioned services URL.
        url = endpoint if endpoint.startswith("https") else f"{self.services_url}/{endpoint.lstrip('/')}"
        
        response = self.session.request(method, url, **kwargs)

        # Handle Expired Token
        if response.status_code == 401 and self.refresh_callback:
            try:
                error_details = response.json()[0]
                if error_details.get('errorCode') == 'INVALID_SESSION_ID':
                    logger.info("Session invalid/expired. Refreshing token...")
                    self.set_token(self._refresh_token_callback())
                    return self.request(method, endpoint, retries=retries, max_retries=max_retries, **kwargs)
            except (ValueError, KeyError, IndexError):
                pass # Not a standard SF auth error

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

    def _refresh_token_callback(self):
        """Can be retried up to the max retries before flagging an error"""
        self.retries += 1
        if self.retries > self.max_retries:
            raise Exception('Max retries exceeded while refreshing token.')
        
        new_token, _ = fetch_client_credentials(
                consumer_key=self.consumer_key, 
                consumer_secret=self.consumer_secret, 
                base_url=self.base_url
                )
        if new_token == self.access_token:
            raise Exception("Failed to refresh token: No token returned from auth service.")
        
        self.retries = 0
        return new_token
