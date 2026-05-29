from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import boto3
import numpy as np
import os

app = FastAPI(title="Fraud Detection API", version="1.0.0")

# Load model at startup
MODEL_PATH = "/tmp/fraud_model.pkl"
model = None

def load_model():
    global model
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    s3.download_file(
        'mlflow',
        'artifacts/1/906c4e8b01d447328f84580c2d63f5c2/artifacts/xgboost_tuned.pkl',
        MODEL_PATH
    )
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
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
    return {"status": "healthy", "model_loaded": model is not None}

@app.post("/predict", response_model=PredictionResponse)
def predict(transaction: Transaction):
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
    if prob > 0.8:
        risk = "HIGH"
    elif prob > 0.5:
        risk = "MEDIUM"
    else:
        risk = "LOW"
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
