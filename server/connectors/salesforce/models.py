
API_VERSION = '63.0'
OAUTH_URI = '/services/oauth2/token'

class Usage(NamedTuple):
    """Usage information for a Salesforce org"""
    used: int
    total: int

class PerAppUsage(NamedTuple):
    """Per App Usage information for a Salesforce org"""
    used: int
    total: int
    name: str

Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, Any]]
BulkDataStr = list[Mapping[str, str]]