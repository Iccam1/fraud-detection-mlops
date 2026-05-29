from fastapi.testclient import TestClient
import sys
sys.path.insert(0, '/home/vielficker/projects/fraud-detection-mlops/src/serving')

# Mock the model loading so tests don't need MinIO
import unittest.mock as mock
with mock.patch('api.load_model'):
    from api import app, model, TYPE_ENCODING
    import pickle
    import numpy as np

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
