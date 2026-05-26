import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TRANSACTION_TYPES = ["TRANSFER", "CASH_OUT", "PAYMENT", "CASH_IN", "DEBIT"]

def generate_transaction():
    is_fraud = 1 if random.random() < 0.01 else 0
    amount = round(random.uniform(5000, 500000), 2) if is_fraud else round(random.uniform(1, 5000), 2)
    return {
        "transaction_id": f"T{random.randint(100000, 999999)}",
        "type": random.choice(TRANSACTION_TYPES),
        "amount": amount,
        "nameOrig": f"C{random.randint(1000000, 9999999)}",
        "nameDest": f"C{random.randint(1000000, 9999999)}",
        "oldbalanceOrg": round(random.uniform(0, 1000000), 2),
        "newbalanceOrig": round(random.uniform(0, 1000000), 2),
        "isFraud": is_fraud,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "synthetic"
    }

print("🚀 Starting Kafka producer... Press Ctrl+C to stop.")
count = 0
while True:
    tx = generate_transaction()
    producer.send("transactions", value=tx)
    count += 1
    if count % 100 == 0:
        print(f"📤 Sent {count} transactions | Last: {tx['transaction_id']} | Fraud: {tx['isFraud']}")
    time.sleep(0.1)
