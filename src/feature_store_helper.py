"""
Feature store helper — used by training pipeline and serving API.
"""
from feast import FeatureStore

FEATURE_REPO_PATH = "/home/vielficker/projects/fraud-detection-mlops/feature_store/feature_repo"

CARD_FEATURES = [
    "card_transaction_features:tx_count",
    "card_transaction_features:total_amount",
    "card_transaction_features:avg_amount",
    "card_transaction_features:fraud_count",
    "card_transaction_features:fraud_rate",
    "card_transaction_features:avg_balance_diff",
]

def get_online_features(card_ids: list[str]) -> dict:
    """Fetch online features for a list of card IDs from Redis."""
    store = FeatureStore(repo_path=FEATURE_REPO_PATH)
    entity_rows = [{"card_id": cid} for cid in card_ids]
    return store.get_online_features(
        features=CARD_FEATURES,
        entity_rows=entity_rows
    ).to_dict()

def get_offline_features(entity_df) -> "pd.DataFrame":
    """Fetch offline features for training (point-in-time correct)."""
    import pandas as pd
    store = FeatureStore(repo_path=FEATURE_REPO_PATH)
    return store.get_historical_features(
        entity_df=entity_df,
        features=CARD_FEATURES
    ).to_df()

if __name__ == "__main__":
    result = get_online_features(["C1000000639", "C1000001337"])
    print("✅ Online features:")
    for k, v in result.items():
        print(f"  {k}: {v}")
