# Fraud Detection MLOps — Lambda Architecture

An end-to-end Big Data platform for detecting fraudulent e-commerce transactions, built as a Master's project. Combines a batch pipeline (Airflow + Spark) with a real-time streaming pipeline (Kafka + Spark Structured Streaming) following the Lambda Architecture pattern.

![CI](https://github.com/Iccam1/fraud-detection-mlops/actions/workflows/ci.yml/badge.svg)

## Results

| Model | PR-AUC | F1 | Recall |
|---|---|---|---|
| Logistic Regression | 0.6491 | 0.1647 | 0.89 |
| XGBoost baseline | 0.9738 | 0.7501 | 0.99 |
| LightGBM | 0.9775 | 0.6425 | 1.00 |
| **XGBoost tuned (production)** | **0.9741** | **0.7731** | **0.99** |

## Stack

| Category | Tools |
|---|---|
| Batch processing | Apache Spark, Apache Airflow |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Storage | MinIO + Delta Lake |
| Feature store | Feast + Redis |
| ML tracking | MLflow |
| Serving | FastAPI |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |

## Quickstart

```bash
git clone https://github.com/Iccam1/fraud-detection-mlops.git
cd fraud-detection-mlops
docker compose up -d
```

## Service URLs

| Service | URL |
|---|---|
| Jupyter / Spark | http://localhost:8888 |
| Airflow | http://localhost:8081 |
| MinIO | http://localhost:9001 |
| MLflow | http://localhost:5000 |
| Fraud API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## API Usage

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "type": "TRANSFER",
    "amount": 181.0,
    "oldbalanceOrg": 181.0,
    "newbalanceOrig": 0.0,
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
    "balance_diff_orig": -181.0,
    "balance_diff_dest": 0.0
  }'
```

## Phases

- ✅ Phase 1 — Docker stack (11 services)
- ✅ Phase 2 — EDA on 3 datasets (7M+ rows)
- ✅ Phase 3 — Data Lake (MinIO + Delta Lake, medallion architecture)
- ✅ Phase 4 — Airflow pipelines (4 DAGs)
- ✅ Phase 5 — Kafka streaming + real-time fraud alerts
- ✅ Phase 6 — Feast feature store with Redis
- ✅ Phase 7 — ML training (XGBoost tuned, PR-AUC 0.9741)
- ✅ Phase 8 — FastAPI serving API
- ✅ Phase 9 — Prometheus + Grafana monitoring
- ✅ Phase 10 — GitHub Actions CI pipeline
- ✅ Phase 11 — Documentation