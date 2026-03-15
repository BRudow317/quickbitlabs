from __future__ import annotations

import os
import requests
from json.decoder import JSONDecodeError

from server.connectors.sf.models import SF_AUTH_URI

import logging
logger = logging.getLogger(__name__)

oath_uri = SF_AUTH_URI

class SalesforceAuthError(Exception):
    """Custom exception for authentication failures."""
    pass

def fetch_client_credentials(
    consumer_key: str | None = None,
    consumer_secret: str | None = None,
    domain: str = 'login',
    base_url: str | None = None
) -> tuple[str, str]:
    """
    Fetch an OAuth access token using the Client Credentials flow.
    Returns: (access_token, sf_instance_hostname)
    """
    key: str | None = consumer_key or os.getenv('SF_CONSUMER_KEY', None)
    secret: str | None = consumer_secret or os.getenv('SF_CONSUMER_SECRET', None)
    # ex) https://empathetic-narwhal-8eqg8r-dev-ed.trailblaze.my.salesforce.com
    base_url_: str | None = base_url \
        or os.getenv('SF_BASE_URL', None) \
        or f'https://{domain}.salesforce.com' 
    
    base = base_url_ or f'https://{domain}.salesforce.com'
    
    response = requests.post(
        f'{base}{oath_uri}',
        data={
            'grant_type': 'client_credentials',
            'client_id': key,
            'client_secret': secret,
        },
    )
    
    try:
        json_data = response.json()
    except JSONDecodeError as exc:
        raise SalesforceAuthError(f"HTTP {response.status_code}: {response.text}") from exc
        
    if response.status_code != 200:
        print(f"[auth debug] {response.status_code}: {json_data}")
        raise SalesforceAuthError(f"{json_data.get('error')}: {json_data.get('error_description')}")
        
    sf_instance = json_data['instance_url'].removeprefix('https://')
    return json_data['access_token'], sf_instance

