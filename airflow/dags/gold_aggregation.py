from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "fraud-detection",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------
# PAYSIM GOLD
# ---------------------------
def aggregate_paysim():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    print("📂 Reading PaySim silver...")
    obj = s3.get_object(
        Bucket="silver",
        Key="transactions/source=paysim/transaction_date=2024-01-01/paysim.parquet"
    )

    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    base = df.groupby(["transaction_date", "type"]).agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        fraud_count=("isFraud", "sum"),
    ).reset_index()

    base["fraud_rate"] = base["fraud_count"] / base["total_transactions"]

    fraud_avg = df[df["isFraud"] == 1].groupby(
        ["transaction_date", "type"]
    )["amount"].mean().reset_index(name="avg_fraud_amount")

    gold = base.merge(fraud_avg, on=["transaction_date", "type"], how="left")
    gold["avg_fraud_amount"] = gold["avg_fraud_amount"].fillna(0)

    gold["computed_ts"] = datetime.utcnow().isoformat()
    gold["source"] = "paysim"

    buffer = BytesIO()
    gold.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket="gold",
        Key="fraud_summary/source=paysim/transaction_date=2024-01-01/paysim.parquet",
        Body=buffer.getvalue()
    )

    print(f"✅ PaySim gold done: {len(gold)} rows")


# ---------------------------
# CREDITCARD GOLD
# ---------------------------
def aggregate_creditcard():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    print("📂 Reading CreditCard silver...")
    obj = s3.get_object(
        Bucket="silver",
        Key="transactions/source=creditcard/transaction_date=2024-01-01/creditcard.parquet"
    )

    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    gold = df.groupby(["transaction_date"]).agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        fraud_count=("isFraud", "sum"),
        avg_amount=("amount", "mean"),
    ).reset_index()

    gold["fraud_rate"] = gold["fraud_count"] / gold["total_transactions"]

    gold["avg_fraud_amount"] = (
        df[df["isFraud"] == 1]["amount"].mean()
        if (df["isFraud"] == 1).any()
        else 0
    )

    gold["type"] = "CREDIT_CARD"
    gold["source"] = "creditcard"
    gold["computed_ts"] = datetime.utcnow().isoformat()

    buffer = BytesIO()
    gold.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket="gold",
        Key="fraud_summary/source=creditcard/transaction_date=2024-01-01/creditcard.parquet",
        Body=buffer.getvalue()
    )

    print(f"✅ CreditCard gold done: {len(gold)} rows")


# ---------------------------
# IEEE GOLD
# ---------------------------
def aggregate_ieee():
    import pandas as pd
    import boto3
    from io import BytesIO

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    print("📂 Reading IEEE silver...")
    obj = s3.get_object(
        Bucket="silver",
        Key="transactions/source=ieee/transaction_date=2024-01-01/ieee.parquet"
    )

    df = pd.read_parquet(BytesIO(obj["Body"].read()))

    base = df.groupby(["transaction_date", "ProductCD"]).agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        fraud_count=("isFraud", "sum"),
    ).reset_index()

    base["fraud_rate"] = base["fraud_count"] / base["total_transactions"]

    fraud_avg = df[df["isFraud"] == 1].groupby(
        ["transaction_date", "ProductCD"]
    )["amount"].mean().reset_index(name="avg_fraud_amount")

    gold = base.merge(fraud_avg, on=["transaction_date", "ProductCD"], how="left")
    gold["avg_fraud_amount"] = gold["avg_fraud_amount"].fillna(0)

    gold = gold.rename(columns={"ProductCD": "type"})
    gold["source"] = "ieee"
    gold["computed_ts"] = datetime.utcnow().isoformat()

    buffer = BytesIO()
    gold.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket="gold",
        Key="fraud_summary/source=ieee/transaction_date=2024-01-01/ieee.parquet",
        Body=buffer.getvalue()
    )

    print(f"✅ IEEE gold done: {len(gold)} rows")


# ---------------------------
# DAG
# ---------------------------
with DAG(
    dag_id="gold_aggregation",
    default_args=default_args,
    description="Aggregate silver data into gold layer",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "aggregation"],
) as dag:

    t1 = PythonOperator(task_id="aggregate_paysim", python_callable=aggregate_paysim)
    t2 = PythonOperator(task_id="aggregate_creditcard", python_callable=aggregate_creditcard)
    t3 = PythonOperator(task_id="aggregate_ieee", python_callable=aggregate_ieee)

    t1 >> t2 >> t3