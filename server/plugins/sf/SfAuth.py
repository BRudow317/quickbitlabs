from __future__ import annotations
import os, logging
import httpx
from server.plugins.sf.SfExceptions import SfException

logger = logging.getLogger(__name__)
SF_AUTH_URI: str = os.getenv('SF_AUTH_URI', '/services/oauth2/token')

class SalesforceAuthError(SfException):
    """Custom exception for authentication failures."""
    pass

async def fetch_client_credentials(
    consumer_key: str | None = None,
    consumer_secret: str | None = None,
    base_url: str | None = None,
    access_token: str | None = None
) -> str:
    """Fetch an OAuth access token using the Client Credentials flow.
        Returns: access token string
        Raises: SalesforceAuthError on failure
    """
    if access_token:
        return access_token
    if consumer_key is None and consumer_secret is None:
        consumer_key = os.getenv('SF_CONSUMER_KEY', None)
        consumer_secret = os.getenv('SF_CONSUMER_SECRET', None)
    if base_url is None:
        base_url = os.getenv('SF_BASE_URL', None)

    # ex) https://empathetic-narwhal-8eqg8r-dev-ed.trailblaze.my.salesforce.com
    
    try: 
        if not all([consumer_key, consumer_secret, base_url]):
            env_debug = {
                k: ("*" * len(v) if v else "[EMPTY STRING]") 
                for k, v in os.environ.items() 
                if k.startswith("SF_")
            }
            print(f"DEBUG SF Vars: {env_debug}")
            raise SalesforceAuthError("Missing required environment variables for authentication.")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{base_url}{SF_AUTH_URI}",
                data={
                    "grant_type": "client_credentials",
                    "client_id": consumer_key,
                    "client_secret": consumer_secret,
                },
            )

        payload = response.json()

        if response.status_code != 200:
            raise SalesforceAuthError(f"{payload.get('error')}: {payload.get('error_description')}")

        return str(payload['access_token'])

    except SalesforceAuthError:
        raise
    except Exception as exc:
        raise SalesforceAuthError("An unexpected error occurred while fetching client credentials.") from exc

