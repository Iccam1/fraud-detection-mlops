# Data Lake — Medallion Architecture

## Storage
- **Object store:** MinIO (S3-compatible), running at `localhost:9001`
- **Format:** Delta Lake (ACID, schema enforcement, time travel)
- **Access:** Spark via S3A protocol (`s3a://`)

## Buckets & Layers

| Bucket | Layer | Purpose |
|--------|-------|---------|
| `bronze` | Raw | Immutable raw data + audit columns |
| `silver` | Cleaned | Deduplicated, typed, derived features |
| `gold` | Aggregated | Business-level aggregates for ML |

## Partition Strategy

| Layer | Partition Keys |
|-------|---------------|
| Bronze | `transaction_date`, `source` |
| Silver | `transaction_date`, `source` |
| Gold | `transaction_date`, `type` |

## Schemas
See `src/schemas.py` for full PySpark schema definitions.

## Table Paths
- Bronze: `s3a://bronze/transactions/`
- Silver: `s3a://silver/transactions/`
- Gold: `s3a://gold/aggregates/`
