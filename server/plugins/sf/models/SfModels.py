from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from enum import Enum
import os
from typing import NamedTuple, TypedDict

API_VERSION: str = os.getenv('SF_API_VERSION', '66.0')
SF_EXTERNAL_CLIENT_APP_NAME: str = os.getenv('SF_EXTERNAL_CLIENT_APP_NAME', 'automation')
SF_BASE_URL: str = os.getenv('SF_BASE_URL') or f"https://{os.getenv('SF_BASE_DOMAIN')}.salesforce.com"
SF_CALLBACK_URL: str = os.getenv('SF_CALLBACK_URL', 'http://localhost:1717/OauthRedirect')


class SfFieldMeta(TypedDict):
    name:              str
    sf_type:           str
    arrow_type:        str
    filterable:        bool
    createable:        bool
    updateable:        bool
    nillable:          bool
    reference_to:      list[str]
    relationship_name: str | None

# --- Common Types ---
Headers = MutableMapping[str, str]
BulkDataAny = list[Mapping[str, any]]
BulkDataStr = list[Mapping[str, str]]

# --- http methods ---
class HttpMethod(str, Enum):
    delete = 'DELETE'
    get = 'GET'
    head = 'HEAD'
    options = 'OPTIONS'
    patch = 'PATCH'
    post = 'POST'
    put = 'PUT'
    request = 'REQUEST'

# --- REST Models ---
class Usage(NamedTuple):
    """Usage information for a Salesforce org"""
    used: int
    total: int

class PerAppUsage(NamedTuple):
    """Per App Usage information for a Salesforce org"""
    used: int
    total: int
    name: str

# --- Bulk API 2.0 Enums ---
class Operation(str, Enum):
    insert = "insert"
    upsert = "upsert"
    update = "update"
    delete = "delete"
    hard_delete = "hardDelete"
    query = "query"
    query_all = "queryAll"

class JobState(str, Enum):
    open = "Open"
    aborted = "Aborted"
    failed = "Failed"
    upload_complete = "UploadComplete"
    in_progress = "InProgress"
    job_complete = "JobComplete"

class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"          # (^)
    COMMA = "COMMA"          # (,)
    PIPE = "PIPE"            # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"              # (\t)

class LineEnding(str, Enum):
    LF = "LF"
    CRLF = "CRLF"

class ResultsType(str, Enum):
    failed = "failedResults"
    successful = "successfulResults"
    unprocessed = "unprocessedRecords"

# --- Bulk API 2.0 Types ---
class QueryParameters(TypedDict, total=False):
    maxRecords: int
    locator: str

class QueryRecordsResult(TypedDict):
    locator: str
    number_of_records: int
    records: str

QueryResult = QueryRecordsResult

# Objects excluded from migratable describe
SKIP_SUFFIXES = (
    '__History', '__Feed', '__Share', '__Tag',
    '__ChangeEvent', '__e', '__mdt', '__b',
)

SKIP_NAMES = {
    # Feeds
    'AccountFeed', 'ContactFeed', 'CaseFeed', 'LeadFeed',
    'OpportunityFeed', 'UserFeed', 'CollaborationGroupFeed',
    # History
    'AccountHistory', 'ContactHistory', 'CaseHistory',
    'LeadHistory', 'OpportunityHistory', 'OpportunityFieldHistory',
    # Shares
    'AccountShare', 'CaseShare', 'LeadShare', 'OpportunityShare',
    # Apex / Dev
    'ApexClass', 'ApexTrigger', 'ApexLog', 'ApexTestResult',
    'AsyncApexJob', 'CronTrigger', 'CronJobDetail',
    # Content (binary blobs - break bulk migrations)
    'ContentVersion', 'ContentDocument', 'ContentDocumentLink',
    # Metadata / Definitions
    'EntityDefinition', 'FieldDefinition', 'FieldPermissions',
    # Auth / Sessions
    'OauthToken', 'AuthSession', 'SessionPermSetActivation',
    'TwoFactorInfo', 'VerificationHistory', 'LoginHistory', 'LoginGeo',
    # Platform
    'StaticResource', 'AuraDefinition', 'AuraDefinitionBundle',
    'FlowDefinitionView', 'FlowInterview',
    'PlatformEventChannel', 'PlatformEventChannelMember',
    'DataStatistics', 'BackgroundOperation', 'SetupAuditTrail',
    # Permissions
    'PermissionSet', 'PermissionSetAssignment',
    'GroupMember', 'UserRole', 'UserLicense',
}
