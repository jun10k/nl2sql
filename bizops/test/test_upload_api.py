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
