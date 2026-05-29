from fastapi import FastAPI, Request
from pydantic import BaseModel
import pickle
import boto3
import numpy as np
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Fraud Detection API", version="1.0.0")

# Prometheus metrics
REQUEST_COUNT = Counter("fraud_api_requests_total", "Total requests", ["endpoint", "method"])
REQUEST_LATENCY = Histogram("fraud_api_request_latency_seconds", "Request latency", ["endpoint"])
FRAUD_DETECTED = Counter("fraud_api_fraud_detected_total", "Total fraud detected")
PREDICTION_COUNT = Counter("fraud_api_predictions_total", "Total predictions made")
MODEL_LOADED = Gauge("fraud_api_model_loaded", "Whether model is loaded")

MODEL_PATH = "/tmp/fraud_model.pkl"
model = None

def load_model():
    global model
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    s3.download_file(
        "mlflow",
        "artifacts/1/906c4e8b01d447328f84580c2d63f5c2/artifacts/xgboost_tuned.pkl",
        MODEL_PATH
    )
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    MODEL_LOADED.set(1)
    print("✅ Model loaded successfully")

@app.on_event("startup")
async def startup_event():
    load_model()

class Transaction(BaseModel):
    type: str
    amount: float
    oldbalanceOrg: float
    newbalanceOrig: float
    oldbalanceDest: float
    newbalanceDest: float
    balance_diff_orig: float
    balance_diff_dest: float

class PredictionResponse(BaseModel):
    transaction_type: str
    amount: float
    fraud_probability: float
    is_fraud: bool
    risk_level: str

TYPE_ENCODING = {"CASH_IN": 0, "CASH_OUT": 1, "DEBIT": 2, "PAYMENT": 3, "TRANSFER": 4}

@app.get("/health")
def health():
    REQUEST_COUNT.labels(endpoint="/health", method="GET").inc()
    return {"status": "healthy", "model_loaded": model is not None}

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return generate_latest()

@app.post("/predict", response_model=PredictionResponse)
def predict(transaction: Transaction):
    start = time.time()
    REQUEST_COUNT.labels(endpoint="/predict", method="POST").inc()
    PREDICTION_COUNT.inc()

    type_encoded = TYPE_ENCODING.get(transaction.type, 0)
    features = np.array([[
        type_encoded,
        transaction.amount,
        transaction.oldbalanceOrg,
        transaction.newbalanceOrig,
        transaction.oldbalanceDest,
        transaction.newbalanceDest,
        transaction.balance_diff_orig,
        transaction.balance_diff_dest
    ]])
    prob = float(model.predict_proba(features)[0][1])
    is_fraud = prob > 0.5

    if is_fraud:
        FRAUD_DETECTED.inc()

    if prob > 0.8:
        risk = "HIGH"
    elif prob > 0.5:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    REQUEST_LATENCY.labels(endpoint="/predict").observe(time.time() - start)

    return PredictionResponse(
        transaction_type=transaction.type,
        amount=transaction.amount,
        fraud_probability=round(prob, 4),
        is_fraud=is_fraud,
        risk_level=risk
    )

@app.get("/")
def root():
    return {"message": "Fraud Detection API", "docs": "/docs"}
