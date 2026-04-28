from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer initialized...")

# store recent transactions per user
user_transactions = defaultdict(list)

# thresholds
HIGH_AMOUNT = 1500
TIME_WINDOW = timedelta(minutes=1)
MAX_TXN_COUNT = 5

for message in consumer:
    event = message.value
    user = event['user_id']
    amount = event['amount']
    timestamp = datetime.fromisoformat(event['timestamp'])

    # store transaction
    user_transactions[user].append((timestamp, amount))

    # remove old transactions
    user_transactions[user] = [
        txn for txn in user_transactions[user]
        if timestamp - txn[0] <= TIME_WINDOW
    ]

    # 1. Hight value transaction
    if amount > HIGH_AMOUNT:
        print(f"ALERT: High value transaction detected for {event}")
    
    # 2. Too many transactions in short time
    if len(user_transactions[user]) > MAX_TXN_COUNT:
        print(f"ALERT: Too many transactions detected for user {event}")