from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64

# Entity
card = Entity(
    name="card_id",
    description="Card or account identifier (nameOrig)"
)

# Offline source — card-level aggregates
card_stats_source = FileSource(
    path="/home/vielficker/projects/fraud-detection-mlops/data/feast/card_stats.parquet",
    timestamp_field="event_timestamp",
)

# Feature view
card_features = FeatureView(
    name="card_transaction_features",
    entities=[card],
    ttl=timedelta(days=7),
    schema=[
        Field(name="tx_count", dtype=Int64),
        Field(name="total_amount", dtype=Float32),
        Field(name="avg_amount", dtype=Float32),
        Field(name="fraud_count", dtype=Int64),
        Field(name="fraud_rate", dtype=Float32),
        Field(name="avg_balance_diff", dtype=Float32),
    ],
    source=card_stats_source,
)
