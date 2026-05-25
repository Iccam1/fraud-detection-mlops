from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "fraud-detection",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def clean_paysim():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")

    print("📂 Reading PaySim bronze from MinIO...")
    obj = s3.get_object(Bucket="bronze",
                        Key="transactions/source=paysim/transaction_date=2024-01-01/paysim.parquet")
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    print(f"✅ Loaded {len(df):,} rows")

    before = len(df)
    df = df.dropna(subset=["transaction_id", "type", "amount", "nameOrig", "isFraud"])
    print(f"🧹 Dropped {before - len(df):,} null rows")

    df["balance_diff_orig"] = df["newbalanceOrig"] - df["oldbalanceOrg"]
    df["balance_diff_dest"] = df["newbalanceDest"] - df["oldbalanceDest"]
    df["transaction_date"]  = pd.to_datetime("2024-01-01").date()
    df["processed_ts"]      = datetime.utcnow().isoformat()

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="silver",
                  Key="transactions/source=paysim/transaction_date=2024-01-01/paysim.parquet",
                  Body=buffer.getvalue())
    print(f"✅ PaySim silver complete: {len(df):,} rows")

def clean_creditcard():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")

    print("📂 Reading CreditCard bronze from MinIO...")
    obj = s3.get_object(Bucket="bronze",
                        Key="transactions/source=creditcard/transaction_date=2024-01-01/creditcard.parquet")
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    print(f"✅ Loaded {len(df):,} rows")

    before = len(df)
    df = df.dropna(subset=["transaction_id", "amount", "isFraud"])
    print(f"🧹 Dropped {before - len(df):,} null rows")

    df["transaction_date"] = pd.to_datetime("2024-01-01").date()
    df["processed_ts"]     = datetime.utcnow().isoformat()

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="silver",
                  Key="transactions/source=creditcard/transaction_date=2024-01-01/creditcard.parquet",
                  Body=buffer.getvalue())
    print(f"✅ CreditCard silver complete: {len(df):,} rows")

def clean_ieee():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client("s3", endpoint_url="http://minio:9000",
                      aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")

    print("📂 Reading IEEE bronze from MinIO...")
    obj = s3.get_object(Bucket="bronze",
                        Key="transactions/source=ieee/transaction_date=2024-01-01/ieee.parquet")
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    print(f"✅ Loaded {len(df):,} rows")

    before = len(df)
    df = df.dropna(subset=["transaction_id", "amount", "isFraud"])
    print(f"🧹 Dropped {before - len(df):,} null rows")

    df["transaction_date"] = pd.to_datetime("2024-01-01").date()
    df["processed_ts"]     = datetime.utcnow().isoformat()

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket="silver",
                  Key="transactions/source=ieee/transaction_date=2024-01-01/ieee.parquet",
                  Body=buffer.getvalue())
    print(f"✅ IEEE silver complete: {len(df):,} rows")

with DAG(
    dag_id="silver_cleaning",
    default_args=default_args,
    description="Clean and enrich bronze data into silver layer",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "cleaning"],
) as dag:

    t1 = PythonOperator(task_id="clean_paysim",      python_callable=clean_paysim)
    t2 = PythonOperator(task_id="clean_creditcard",  python_callable=clean_creditcard)
    t3 = PythonOperator(task_id="clean_ieee",        python_callable=clean_ieee)

    t1 >> t2 >> t3