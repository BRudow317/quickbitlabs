import pytest
from unittest.mock import patch, MagicMock

# locals
from server.connectors.sf.SalesforceConnector import SalesforceClient
from server.connectors.sf.HttpClient import HttpClient
from connectors.sf.auth import fetch_client_credentials

@pytest.fixture
def sf():
    """
    Creates an SfSession using the 'direct' authentication path. 
    This allows us to test the REST methods without triggering the OAuth flow.
    """
    return fetch_client_credentials()

@pytest.fixture
def mock_request():
    """
    Mocks the requests.Session.request method.
    We return a MagicMock that simulates a successful HTTP 200/201 response.
    """
    with patch("requests.Session.request") as mock:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "003000000000001AAA", "success": True}
        # Simulate an empty header dict to avoid KeyError on 'Sforce-Limit-Info'
        mock_response.headers = {}  
        mock.return_value = mock_response
        yield mock

def test_insert_record(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Inserting (Creating) a new record."""
    contact_data = {
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com"
    }
    
    # Using the magic __getattr__ to access the Contact SObject type
    response = sf.rest.Contact.create(contact_data)
    
    # Assert the method returns the mocked JSON
    assert response["success"] is True
    assert response["id"] == "003000000000001AAA"
    
    # Verify the correct HTTP method and URL were used
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert args[0] == "POST"
    assert "sobjects/Contact/" in args[1]
    assert '{"FirstName": "John"' in kwargs["data"]

def test_get_record(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Fetching a record by its Salesforce ID."""
    record_id = "003000000000001AAA"
    
    sf.rest.Contact.get(record_id)
    
    mock_request.assert_called_once()
    args, _ = mock_request.call_args
    assert args[0] == "GET"
    assert args[1].endswith(f"sobjects/Contact/{record_id}")

def test_update_record(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Updating an existing record by its Salesforce ID."""
    record_id = "003000000000001AAA"
    update_data = {"Title": "Senior Developer"}
    
    # Update returns the status code by default (200, 201, 204)
    sf.rest.Contact.update(record_id, update_data)
    
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert args[0] == "PATCH"
    assert args[1].endswith(f"sobjects/Contact/{record_id}")
    assert '{"Title": "Senior Developer"}' in kwargs["data"]

def test_upsert_record(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Upserting a record."""
    record_id = "003000000000001AAA"
    upsert_data = {"Email": "new.email@example.com"}
    
    # Note: In your specific implementation, 'upsert' utilizes the exact 
    # same base_url and record_id endpoint as 'update' (PATCH to /ID).
    sf.rest.Contact.upsert(record_id, upsert_data)
    
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert args[0] == "PATCH"
    assert args[1].endswith(f"sobjects/Contact/{record_id}")

def test_delete_record(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Deleting a record by its Salesforce ID."""
    record_id = "003000000000001AAA"
    
    # Set the mock to return a 204 No Content (standard Salesforce delete response)
    mock_request.return_value.status_code = 204
    
    status_code = sf.rest.Contact.delete(record_id)
    
    assert status_code == 204
    mock_request.assert_called_once()
    args, _ = mock_request.call_args
    assert args[0] == "DELETE"
    assert args[1].endswith(f"sobjects/Contact/{record_id}")

def test_soql_query(sf: SalesforceClient, mock_request: MagicMock):
    """Demo: Running a standard SOQL query."""
    # Override the mock JSON to simulate a query result
    mock_request.return_value.json.return_value = {
        "totalSize": 1,
        "done": True,
        "records": [{"Id": "003000000000001AAA", "Name": "John Doe"}]
    }
    
    result = sf.rest.query("SELECT Id, Name FROM Contact LIMIT 1")
    
    assert result["totalSize"] == 1
    assert result["records"][0]["Name"] == "John Doe"
    
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert args[0] == "GET"
    assert "query/" in args[1]
    assert kwargs["params"]["q"] == "SELECT Id, Name FROM Contact LIMIT 1"