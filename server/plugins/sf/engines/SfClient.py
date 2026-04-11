from __future__ import annotations

import re
from typing import Any
from collections.abc import MutableMapping

import httpx

from server.plugins.sf.engines.SfAuth import fetch_client_credentials, SalesforceAuthError
from server.plugins.sf.models.SfModels import API_VERSION, SF_BASE_URL, Usage, PerAppUsage

import logging
logger = logging.getLogger(__name__)


class SfClient:
    """
    Sync HTTP client for Salesforce REST and Bulk 2.0 APIs.
    Always construct via SfClient.create() - not __init__ directly.
    """

    base_url: str
    services_url: str
    access_token: str
    api_version: str
    api_usage: MutableMapping[str, Usage | PerAppUsage]
    _session: httpx.Client
    _max_retries: int

    def __init__(
        self,
        base_url: str,
        access_token: str,
        api_version: str = API_VERSION,
        max_retries: int = 1,
    ) -> None:
        self.base_url = base_url
        self.access_token = access_token
        self.api_version = api_version
        self.services_url = f"{base_url}/services/data/v{api_version}"
        self.api_usage = {}
        self._max_retries = max_retries
        self._session = httpx.Client(
            headers=self._auth_headers(access_token),
            timeout=httpx.Timeout(30.0, connect=10.0),
        )

    @classmethod
    def create(
        cls,
        base_url: str | None = None,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        access_token: str | None = None,
        api_version: str = API_VERSION,
        max_retries: int = 1,
    ) -> SfClient:
        """Factory - use this instead of calling SfClient() directly."""
        resolved_url = base_url or SF_BASE_URL
        if not resolved_url: raise SalesforceAuthError("SF_BASE_URL is required.")
        if access_token is None:
            access_token = fetch_client_credentials(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                base_url=resolved_url,
            )
        return cls(
            base_url=resolved_url,
            access_token=access_token,
            api_version=api_version,
            max_retries=max_retries,
        )

    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def _update_token(self, token: str) -> None:
        """Replace the bearer token on the live session."""
        self.access_token = token
        self._session.headers.update(self._auth_headers(token))

    def request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Execute a sync HTTP request.
        Handles token refresh on 401 INVALID_SESSION_ID.
        Full URL or relative endpoint both accepted.
        """
        url = (
            endpoint
            if endpoint.startswith("https")
            else f"{self.services_url}/{endpoint.lstrip('/')}"
        )
        response = self._session.request(method, url, **kwargs)
        if response.status_code == 401:
            self._handle_401(response)
            response = self._session.request(method, url, **kwargs)
        if response.status_code >= 300: raise Exception (f"HTTP {response.status_code} {method} {url}: {response.text}")
        limit_info = response.headers.get("Sforce-Limit-Info")
        if limit_info: self._parse_api_usage(limit_info)
        return response

    def _handle_401(self, response: httpx.Response) -> None:
        """Refresh the token on INVALID_SESSION_ID."""
        try: error_code = response.json()[0].get("errorCode")
        except Exception: return
        if error_code != "INVALID_SESSION_ID": return
        logger.info("Session expired. Refreshing token...")
        for attempt in range(1, self._max_retries + 1):
            new_token = fetch_client_credentials(base_url=self.base_url)
            if new_token and new_token != self.access_token:
                self._update_token(new_token)
                return
            logger.warning(f"Token refresh attempt {attempt} returned same or empty token.")
        raise Exception("Max retries exceeded: could not refresh Salesforce token.")

    def _parse_api_usage(self, sforce_limit_info: str) -> None:
        api_usage = re.match(r"[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)", sforce_limit_info)
        pau = re.match(
            r".+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)",
            sforce_limit_info,
        )
        if api_usage:
            g = api_usage.groups()
            self.api_usage["api-usage"] = Usage(used=int(g[0]), total=int(g[1]))
        if pau:
            g = pau.groups()
            self.api_usage["per-app-api-usage"] = PerAppUsage(
                used=int(g[0]), total=int(g[1]), name=g[2]
            )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> SfClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
    
