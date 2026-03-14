import pytest
from unittest.mock import patch, MagicMock

# locals
from connectors.sf.client import SalesforceClient
from connectors.sf.core.http import HttpClient
from connectors.sf.auth import fetch_client_credentials
from connectors.sf.services.bulk2 import JobState

@pytest.fixture
def sf() -> SalesforceClient:
    """Creates a direct SfSession so we bypass OAuth."""
    return SalesforceClient(
        consumer_key="dummy_bulk_session_token",
        consumer_secret="dummy_bulk_session_secret",
        instance_url="https://test-bulk-org.salesforce.com"
    )

@pytest.fixture
def mock_sleep():
    """Mocks the sleep function so our tests don't actually pause while waiting for jobs."""
    with patch("hades.api.sleep") as mock:
        yield mock

@pytest.fixture
def mock_bulk_request():
    """
    A dynamic mock that returns different responses based on the Bulk V2 
    lifecycle step (Create Job, Upload Data, Close Job, Check Status).
    """
    def bulk_router(method, url, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        
        # 1. INGEST JOBS (Insert, Update, Upsert, Delete)
        if "ingest" in url:
            if method == "POST":
                # Create Job
                mock_response.json.return_value = {"id": "750_ingest_123", "state": JobState.open}
            elif method == "PUT" and "batches" in url:
                # Upload Data
                mock_response.status_code = 201 
            elif method == "PATCH":
                # Close Job
                mock_response.json.return_value = {"id": "750_ingest_123", "state": JobState.upload_complete}
            elif method == "GET":
                # Poll Status (Simulate immediate success)
                mock_response.json.return_value = {
                    "id": "750_ingest_123", 
                    "state": JobState.job_complete,
                    "numberRecordsProcessed": 2, 
                    "numberRecordsFailed": 0
                }
                
        # 2. QUERY JOBS
        elif "query" in url:
            if method == "POST":
                # Create Query Job
                mock_response.json.return_value = {"id": "750_query_123", "state": JobState.open}
            elif method == "GET" and "results" not in url:
                # Poll Query Status
                mock_response.json.return_value = {"id": "750_query_123", "state": JobState.job_complete}
            elif method == "GET" and "results" in url:
                # Get Query Results
                mock_response.headers = {
                    "Sforce-Locator": "null", 
                    "Sforce-NumberOfRecords": "2"
                }
                # Simulate the CSV byte stream returned by Salesforce
                mock_response.content = b'"Id","Name"\n"003AAA","Alice"\n"003BBB","Bob"'
                
        return mock_response

    with patch("requests.Session.request", side_effect=bulk_router) as mock:
        yield mock

def test_bulk_insert(sf, mock_bulk_request, mock_sleep):
    """Demo: Bulk inserting records from a list of dicts."""
    records_to_insert = [
        {"FirstName": "Alice", "LastName": "Smith"},
        {"FirstName": "Bob", "LastName": "Jones"}
    ]
    
    results = sf.bulk2.Contact.insert(records=records_to_insert)
    
    # Assert we get the correct summary output mapped from our mock
    assert len(results) == 1
    summary = results[0]
    assert summary["job_id"] == "750_ingest_123"
    assert summary["numberRecordsProcessed"] == 2
    assert summary["numberRecordsFailed"] == 0
    assert summary["numberRecordsTotal"] == 2
    
    # Verify the sequence of HTTP calls: POST (create) -> PUT (upload) -> PATCH (close) -> GET (poll)
    assert mock_bulk_request.call_count >= 4

def test_bulk_upsert(sf, mock_bulk_request, mock_sleep):
    """Demo: Bulk upserting records using an External ID."""
    records_to_upsert = [
        {"External_ID__c": "EXT123", "Title": "Manager"},
        {"External_ID__c": "EXT456", "Title": "Director"}
    ]
    
    results = sf.bulk2.Contact.upsert(
        records=records_to_upsert, 
        external_id_field="External_ID__c"
    )
    
    summary = results[0]
    assert summary["job_id"] == "750_ingest_123"
    
    # Inspect the first call (POST) to ensure the externalIdFieldName was passed correctly
    first_call_kwargs = mock_bulk_request.call_args_list[0][1]
    assert "External_ID__c" in first_call_kwargs["data"]
    assert "upsert" in first_call_kwargs["data"]

def test_bulk_delete(sf, mock_bulk_request, mock_sleep):
    """Demo: Bulk deleting records. Note that delete jobs only accept the 'Id' field."""
    records_to_delete = [
        {"Id": "003000000000001AAA"},
        {"Id": "003000000000002AAA"}
    ]
    
    results = sf.bulk2.Contact.delete(records=records_to_delete)
    
    summary = results[0]
    assert summary["job_id"] == "750_ingest_123"
    assert summary["numberRecordsProcessed"] == 2

def test_bulk_delete_validation_error(sf, mock_bulk_request, mock_sleep):
    """Demo: Ensure the client raises an exception if passing fields other than 'Id' to delete."""
    invalid_records = [
        {"Id": "003000000000001AAA", "Name": "Should Not Be Here"}
    ]
    
    with pytest.raises(Exception, match="InvalidBatch: The 'delete/hard_delete' batch must contain only 'Id'"):
        sf.bulk2.Contact.delete(records=invalid_records)

def test_bulk_query(sf, mock_bulk_request, mock_sleep):
    """Demo: Bulk querying records. Returns a generator yielding CSV strings."""
    soql = "SELECT Id, Name FROM Contact"
    
    # The query method yields batches of results
    result_generator = sf.bulk2.Contact.query(soql)
    batches = list(result_generator)
    
    assert len(batches) == 1
    csv_result = batches[0]
    
    # Assert the CSV string matches our mock content
    assert '"Id","Name"' in csv_result
    assert '"003AAA","Alice"' in csv_result