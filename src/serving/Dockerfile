FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn boto3 xgboost scikit-learn numpy pydantic

COPY api.py .

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
