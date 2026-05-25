from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "fraud-detection",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def ingest_paysim():
    import pandas as pd
    import boto3
    from io import BytesIO

    print("📂 Reading PaySim CSV...")
    df = pd.read_csv("/opt/airflow/data/paysim1/PS_20174392719_1491204439457_log.csv")
    print(f"✅ Loaded {len(df):,} rows")

    df["transaction_id"]   = df["nameOrig"]
    df["ingestion_ts"]     = datetime.utcnow().isoformat()
    df["source"]           = "paysim"
    df["transaction_date"] = "2024-01-01"

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="bronze",
                  Key="transactions/source=paysim/transaction_date=2024-01-01/paysim.parquet",
                  Body=buffer.getvalue())
    print(f"✅ PaySim bronze ingestion complete: {len(df):,} rows")

def ingest_creditcard():
    import pandas as pd
    import boto3
    from io import BytesIO

    print("📂 Reading CreditCard CSV...")
    df = pd.read_csv("/opt/airflow/data/creditcardfraud/creditcard.csv")
    print(f"✅ Loaded {len(df):,} rows")

    df["transaction_id"]   = df.index.astype(str)
    df["ingestion_ts"]     = datetime.utcnow().isoformat()
    df["source"]           = "creditcard"
    df["transaction_date"] = "2024-01-01"
    df = df.rename(columns={"Class": "isFraud", "Amount": "amount"})

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="bronze",
                  Key="transactions/source=creditcard/transaction_date=2024-01-01/creditcard.parquet",
                  Body=buffer.getvalue())
    print(f"✅ CreditCard bronze ingestion complete: {len(df):,} rows")

def ingest_ieee():
    import pandas as pd
    import boto3
    from io import BytesIO

    print("📂 Reading IEEE transaction CSV...")
    tx = pd.read_csv("/opt/airflow/data/ieee-fraud-detection/train_transaction.csv")
    print(f"✅ Loaded {len(tx):,} transaction rows")

    print("📂 Reading IEEE identity CSV...")
    identity = pd.read_csv("/opt/airflow/data/ieee-fraud-detection/train_identity.csv")
    print(f"✅ Loaded {len(identity):,} identity rows")

    print("🔗 Joining transaction + identity on TransactionID...")
    df = tx.merge(identity, on="TransactionID", how="left")
    print(f"✅ Joined dataset: {len(df):,} rows")

    df["transaction_id"]   = df["TransactionID"].astype(str)
    df["ingestion_ts"]     = datetime.utcnow().isoformat()
    df["source"]           = "ieee"
    df["transaction_date"] = "2024-01-01"
    df = df.rename(columns={"TransactionAmt": "amount"})

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="bronze",
                  Key="transactions/source=ieee/transaction_date=2024-01-01/ieee.parquet",
                  Body=buffer.getvalue())
    print(f"✅ IEEE bronze ingestion complete: {len(df):,} rows")

with DAG(
    dag_id="bronze_ingestion",
    default_args=default_args,
    description="Ingest raw data into bronze layer",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    t1 = PythonOperator(task_id="ingest_paysim",     python_callable=ingest_paysim)
    t2 = PythonOperator(task_id="ingest_creditcard", python_callable=ingest_creditcard)
    t3 = PythonOperator(task_id="ingest_ieee",       python_callable=ingest_ieee)

    t1 >> t2 >> t3