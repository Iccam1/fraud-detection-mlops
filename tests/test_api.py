import sys
import os
import unittest.mock as mock

# Add serving directory to path BEFORE any imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src', 'serving'))

# Mock boto3 before importing api
mock_boto3 = mock.MagicMock()
sys.modules['boto3'] = mock_boto3

# Now patch load_model
with mock.patch('builtins.open', mock.mock_open()):
    import api
    api.load_model = mock.MagicMock()

from fastapi.testclient import TestClient
client = TestClient(api.app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["message"] == "Fraud Detection API"

def test_type_encoding():
    assert api.TYPE_ENCODING["TRANSFER"] == 4
    assert api.TYPE_ENCODING["CASH_OUT"] == 1
    assert api.TYPE_ENCODING["PAYMENT"] == 3
