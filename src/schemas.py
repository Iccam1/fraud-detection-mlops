from pyspark.sql.types import *

bronze_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("ingestion_ts", TimestampType(), True),
    StructField("source", StringType(), True),
])

silver_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("step", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("nameOrig", StringType(), False),
    StructField("oldbalanceOrg", DoubleType(), False),
    StructField("newbalanceOrig", DoubleType(), False),
    StructField("nameDest", StringType(), False),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), False),
    StructField("isFlaggedFraud", IntegerType(), False),
    StructField("balance_diff_orig", DoubleType(), True),
    StructField("balance_diff_dest", DoubleType(), True),
    StructField("transaction_date", DateType(), False),
    StructField("ingestion_ts", TimestampType(), False),
    StructField("source", StringType(), False),
    StructField("processed_ts", TimestampType(), False),
])

gold_schema = StructType([
    StructField("transaction_date", DateType(), False),
    StructField("type", StringType(), False),
    StructField("total_transactions", LongType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("fraud_count", LongType(), False),
    StructField("fraud_rate", DoubleType(), False),
    StructField("avg_fraud_amount", DoubleType(), True),
    StructField("computed_ts", TimestampType(), False),
])
