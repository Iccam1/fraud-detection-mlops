import sys
import os
import unittest.mock as mock

# Add the serving directory to path relative to project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'serving'))

with mock.patch.dict('sys.modules', {'boto3': mock.MagicMock()}):
    with mock.patch('api.load_model'):
        from api import app, TYPE_ENCODING

from fastapi.testclient import TestClient
client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["message"] == "Fraud Detection API"

def test_type_encoding():
    assert TYPE_ENCODING["TRANSFER"] == 4
    assert TYPE_ENCODING["CASH_OUT"] == 1
    assert TYPE_ENCODING["PAYMENT"] == 3
