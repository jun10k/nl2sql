import os

from fastapi.testclient import TestClient

from bizops.main import app

client = TestClient(app)

# Get the absolute path to the sample CSV directory
SAMPLE_CSV_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../assets/sample_csv'))

def test_upload_database_info():
    """Test uploading database information via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'db_info_assets_maintenance.csv')
    
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/update/database-info",
            files={"file": ("db_info_assets_maintenance.csv", f, "text/csv")}
        )
    
    assert response.status_code == 201
    assert response.json() == {"message": "Database information updated successfully"}

def test_upload_table_info():
    """Test uploading table information via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'tb_info_assets_maintenance.csv')
    
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/assets_maintenance/update/table-info",
            files={"file": ("tb_info_assets_maintenance.csv", f, "text/csv")}
        )
    
    assert response.status_code == 201
    assert response.json() == {"message": "Table information updated successfully"}

def test_upload_table_details():
    """Test uploading table details via CSV"""
    # Test with Assets table details
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'tb_details_Assets.csv')
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/assets_maintenance/table/Assets/update/table-details",
            files={"file": ("tb_details_Assets.csv", f, "text/csv")}
        )
    assert response.status_code == 201
    assert response.json() == {"message": "Table details updated successfully"}
    
    # Test with Maintenance Contracts table details
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'tb_details_Maintenance_Contracts.csv')
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/assets_maintenance/table/Maintenance_Contracts/update/table-details",
            files={"file": ("tb_details_Maintenance_Contracts.csv", f, "text/csv")}
        )
    assert response.status_code == 201
    assert response.json() == {"message": "Table details updated successfully"}
    
    # Test with Third Party Companies table details
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'tb_details_Third_Party_Companies.csv')
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/assets_maintenance/table/Third_Party_Companies/update/table-details",
            files={"file": ("tb_details_Third_Party_Companies.csv", f, "text/csv")}
        )
    assert response.status_code == 201
    assert response.json() == {"message": "Table details updated successfully"}

def test_upload_query_examples():
    """Test uploading query examples via CSV"""
    csv_path = os.path.join(SAMPLE_CSV_DIR, 'query_examples_asset_maintenance.csv')
    with open(csv_path, 'rb') as f:
        response = client.post(
            "/api/v1/symantic-layer/database/assets_maintenance/update/query-examples",
            files={"file": ("query_examples_asset_maintenance.csv", f, "text/csv")}
        )
    assert response.status_code == 201
    assert response.json() == {"message": "Query examples updated successfully"}
