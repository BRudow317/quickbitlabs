from __future__ import annotations

# locals
from core.http import HttpClient
from auth import fetch_client_credentials
from services.rest import SfRest
from services.bulk2 import SfBulk2Handler
from models import API_VERSION

# logging
import logging
logger = logging.getLogger(__name__)

class SalesforceClient:
    consumer_key: str | None
    consumer_secret: str | None
    instance_url: str | None
    api_version: str
    def __init__(
        self, 
        consumer_key: str | None = None, 
        consumer_secret: str | None = None, 
        instance_url: str | None = None,
        api_version: str = API_VERSION
    ):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.instance_url = instance_url
        self.api_version = api_version
        
        token, sf_instance = fetch_client_credentials(
            self.consumer_key, 
            self.consumer_secret, 
            instance_url=self.instance_url
        )
        
        base_url = f"https://{sf_instance}/services/data/v{self.api_version}"
        
        self.http = HttpClient(
            base_url=base_url,
            access_token=token,
            refresh_callback=self._refresh_token_callback
        )

        self.rest = SfRest(self.http)
        self.bulk2 = SfBulk2Handler(self.http, bulk2_url=f"{base_url}/jobs/")

    def _refresh_token_callback(self) -> str:
        """
        Passed to the HttpClient. When the HTTP client gets a 401, 
        it calls this to get a new token automatically.
        """
        token, _ = fetch_client_credentials(
            self.consumer_key, 
            self.consumer_secret, 
            instance_url=self.instance_url
        )
        return token