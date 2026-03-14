from __future__ import annotations

import logging
import os
import requests
from json.decoder import JSONDecodeError

logger = logging.getLogger(__name__)

OAUTH_URI = '/services/oauth2/token'

class SalesforceAuthError(Exception):
    """Custom exception for authentication failures."""
    pass

def fetch_client_credentials(
    consumer_key: str | None = None,
    consumer_secret: str | None = None,
    domain: str = 'login',
    instance_url: str | None = None
) -> tuple[str, str]:
    """
    Fetch an OAuth access token using the Client Credentials flow.
    Returns: (access_token, sf_instance_hostname)
    """
    key = consumer_key or os.getenv('CONSUMER_KEY')
    secret = consumer_secret or os.getenv('CONSUMER_SECRET')
    url = instance_url or os.getenv('BASE_URL')
    
    base = url or f'https://{domain}.salesforce.com'
    
    response = requests.post(
        f'{base}{OAUTH_URI}',
        data={
            'grant_type': 'client_credentials',
            'client_id': key,
            'client_secret': secret,
        },
    )
    
    try:
        data = response.json()
    except JSONDecodeError as exc:
        raise SalesforceAuthError(f"HTTP {response.status_code}: {response.text}") from exc
        
    if response.status_code != 200:
        raise SalesforceAuthError(f"{data.get('error')}: {data.get('error_description')}")
        
    sf_instance = data['instance_url'].removeprefix('https://')
    return data['access_token'], sf_instance