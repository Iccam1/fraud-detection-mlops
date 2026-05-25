from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
from io import BytesIO

default_args = {
    "owner": "fraud-detection",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_checks(bucket, key, name):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    print(f"\n📊 {name}")

    # 1. row check
    print("Rows:", len(df))
    assert len(df) > 0

    # 2. null check
    nulls = df.isnull().sum().sum()
    print("Total nulls:", nulls)
    assert nulls < len(df) * 0.5  # basic safety

    # 3. fraud validity
    if "isFraud" in df.columns:
        assert df["isFraud"].isin([0, 1]).all()

    # 4. fraud rate sanity
    if "isFraud" in df.columns:
        rate = df["isFraud"].mean()
        print("Fraud rate:", rate)
        assert 0 <= rate <= 0.2  # sanity range


with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality"]
) as dag:

    t1 = PythonOperator(
        task_id="check_paysim",
        python_callable=run_checks,
        op_kwargs={
            "bucket": "silver",
            "key": "transactions/source=paysim/transaction_date=2024-01-01/paysim.parquet",
            "name": "PaySim"
        }
    )

    t2 = PythonOperator(
        task_id="check_creditcard",
        python_callable=run_checks,
        op_kwargs={
            "bucket": "silver",
            "key": "transactions/source=creditcard/transaction_date=2024-01-01/creditcard.parquet",
            "name": "CreditCard"
        }
    )

    t3 = PythonOperator(
        task_id="check_ieee",
        python_callable=run_checks,
        op_kwargs={
            "bucket": "silver",
            "key": "transactions/source=ieee/transaction_date=2024-01-01/ieee.parquet",
            "name": "IEEE"
        }
    )

    t1 >> t2 >> t3