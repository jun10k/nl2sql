import pytest
from fastapi.testclient import TestClient
from bizops.main import app

# Create a test client
client = TestClient(app)

def test_completion_api():
    """
    Comprehensive test for the NL2SQL completion API
    - Validates successful response
    - Checks response structure
    - Verifies context preservation
    """
    # Prepare test request with full payload
    request_payload = {
        "prompt": "Generate a SQL query to find top 5 customers by total purchase value",
        "context": {
            "database": "sales",
            "request_source": "test_suite",
            "user_id": "test_user_123"
        }
    }
    
    # Send request to completion endpoint
    response = client.post("/api/v1/nl2sql/completion", json=request_payload)
    
    # Assert response status code
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    
    # Parse response
    response_data = response.json()
    
    # Validate response structure
    assert "text" in response_data, "Response missing 'text' field"
    assert "data" in response_data, "Response missing 'data' field"
    assert "context" in response_data, "Response missing 'context' field"
    assert "metadata" in response_data, "Response missing 'metadata' field"
    
    # Check that response text is not empty
    assert len(response_data["text"]) > 0, "Response text is empty"
    
    # Verify context preservation
    assert response_data["context"] == request_payload.get("context"), "Context not preserved correctly"
    
    # Additional checks on metadata
    assert "timestamp" in response_data["metadata"], "Timestamp missing from metadata"
