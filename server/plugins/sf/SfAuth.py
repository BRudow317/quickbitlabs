from __future__ import annotations
import os, logging
import httpx
logger = logging.getLogger(__name__)
SF_AUTH_URI: str = os.getenv('SF_AUTH_URI', '/services/oauth2/token')

class SalesforceAuthError(Exception):
    """Custom exception for authentication failures."""
    pass

async def fetch_client_credentials(
    consumer_key: str | None = os.getenv('SF_CONSUMER_KEY', None),
    consumer_secret: str | None = os.getenv('SF_CONSUMER_SECRET', None),
    base_url: str | None = os.getenv('SF_BASE_URL', None)
) -> str:
    """Fetch an OAuth access token using the Client Credentials flow.
        Returns: access token string
        Raises: SalesforceAuthError on failure
    """
    # ex) https://empathetic-narwhal-8eqg8r-dev-ed.trailblaze.my.salesforce.com
    try: 
        if not all([consumer_key, consumer_secret, base_url]):
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

