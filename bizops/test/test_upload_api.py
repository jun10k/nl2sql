import os
import pytest
from fastapi.testclient import TestClient
from bizops.main import app

client = TestClient(app)

# Get the absolute path to the sample CSV directory
SAMPLE_CSV_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../assets/sample_cvs'))

def test_upload_database_info():
    """Test uploading database information via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'db_info_assets_maintenance.cvs')
    
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/symantic/database/assets_maintenance/info",
            files={"file": ("db_info_assets_maintenance.cvs", f, "text/csv")}
        )
    
    assert response.status_code == 200
    assert response.json() == {"message": "Database information updated successfully"}

def test_upload_table_info():
    """Test uploading table information via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'tb_info_assets_maintenance.cvs')
    
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/symantic/database/assets_maintenance/table",
            files={"file": ("tb_info_assets_maintenance.cvs", f, "text/csv")}
        )
    
    assert response.status_code == 200
    assert response.json() == {"message": "Table information updated successfully"}

def test_upload_table_details():
    """Test uploading table details via CSV"""
    test_cases = [
        ('Assets', 'tb_details_Assets.cvs'),
        ('Maintenance_Contracts', 'tb_details_Maintenance_Contracts.cvs'),
        ('Third_Party_Companies', 'tb_details_Third_Party_Companies.cvs')
    ]
    
    for table_name, csv_file in test_cases:
        csv_path = os.path.join(SAMPLE_CSV_DIR, csv_file)
        
        with open(csv_path, 'rb') as f:
            response = client.post(
                f"/symantic/database/assets_maintenance/table/{table_name}/details",
                files={"file": (csv_file, f, "text/csv")}
            )
        
        assert response.status_code == 200
        assert response.json() == {"message": "Table details updated successfully"}

def test_upload_query_examples():
    """Test uploading query examples via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'query_samples_asset_maintenance.cvs')
    
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/symantic/database/assets_maintenance/query_examples",
            files={"file": ("query_samples_asset_maintenance.cvs", f, "text/csv")}
        )
    
    assert response.status_code == 200
    assert response.json() == {"message": "Query examples updated successfully"}

def test_invalid_file_prefix():
    """Test uploading files with invalid prefixes"""
    test_cases = [
        # (endpoint, file_name, expected_error)
        (
            "/symantic/database/assets_maintenance/info",
            "tb_info_assets_maintenance.cvs",
            "File name must start with 'db_'"
        ),
        (
            "/symantic/database/assets_maintenance/table",
            "db_info_assets_maintenance.cvs",
            "File name must start with 'tb_'"
        ),
        (
            "/symantic/database/assets_maintenance/table/Assets/details",
            "tb_info_assets_maintenance.cvs",
            "File name must start with 'tb_details_'"
        ),
        (
            "/symantic/database/assets_maintenance/query_examples",
            "tb_info_assets_maintenance.cvs",
            "File name must start with 'sample_'"
        ),
    ]
    
    for endpoint, file_name, expected_error in test_cases:
        csv_path = os.path.join(SAMPLE_CSV_DIR, file_name)
        
        with open(csv_path, 'rb') as f:
            response = client.post(
                endpoint,
                files={"file": (file_name, f, "text/csv")}
            )
        
        assert response.status_code == 400
        assert response.json() == {"message": expected_error}

def test_list_endpoints():
    """Test the list endpoints"""
    # Test list databases
    response = client.get("/symantic/databases")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert "assets_maintenance" in response.json()
    
    # Test list tables
    response = client.get("/symantic/database/assets_maintenance/tables")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert all(table in response.json() for table in ["Assets", "Maintenance_Contracts", "Third_Party_Companies"])
    
    # Test list table details
    response = client.get("/symantic/database/assets_maintenance/table/Assets/details")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert all(isinstance(item, dict) for item in response.json())
    
    # Test list query examples
    response = client.get("/symantic/database/assets_maintenance/query_examples")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert all(isinstance(item, dict) for item in response.json())
