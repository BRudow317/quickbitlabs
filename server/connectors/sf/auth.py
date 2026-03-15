from __future__ import annotations


import os
import requests
from json.decoder import JSONDecodeError

import logging
logger = logging.getLogger(__name__)

OAUTH_URI = '/services/oauth2/token'

class SalesforceAuthError(Exception):
    """Custom exception for authentication failures."""
    logger.error("Salesforce authentication failed.")
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
    key = consumer_key or os.getenv('CONSUMER_KEY')
    secret = consumer_secret or os.getenv('CONSUMER_SECRET')
    base_url = base_url or os.getenv('BASE_URL') # ex) https://empathetic-narwhal-8eqg8r-dev-ed.trailblaze.my.salesforce.com
    
    base = base_url or f'https://{domain}.salesforce.com'
    
    response = requests.post(
        f'{base}{OAUTH_URI}',
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
        raise SalesforceAuthError(f"{json_data.get('error')}: {json_data.get('error_description')}")
        
    sf_instance = json_data['instance_url'].removeprefix('https://')
    return json_data['access_token'], sf_instance

