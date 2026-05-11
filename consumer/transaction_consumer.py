from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json
import logging
from utils.db import get_connection

# logging setup
logging.basicConfig(level=logging.INFO)

# store last known info
user_last_location = {}
user_last_device = {}

# store transactions per user
user_transactions = defaultdict(list)

# thresholds
HIGH_AMOUNT = 1500
TIME_WINDOW = timedelta(minutes=1)
MAX_TXN_COUNT = 5
SPENDING_SPIKE = 3000

# Kafka consumer
consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logging.info("Fraud Detection Consumer initialized...")

# save alerts to file
def save_alert(alert):
    # with open("data/fraud_alerts.json", "a") as f:
    #     f.write(json.dumps(alert) + "\n")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO fraud_alerts (alert_type, event) VALUES (%s, %s)",
        (alert['alert_type'], json.dumps(alert['event']))
    )

    conn.commit()
    cur.close()
    conn.close()

# Process messages
for message in consumer:
    event = message.value
    user = event['user_id']
    amount = event['amount']
    country = event['country']
    device = event['device_id']
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
        alert = {
            "alert_type": "HIGH_VALUE_TRANSACTION",
            "event": event
        }
        logging.warning(f"High-value transaction detected: {event}")
        save_alert(alert)
    
    # 2. Too many transactions in short time
    if len(user_transactions[user]) > MAX_TXN_COUNT:
        alert = {
            "alert_type": "VELOCITY_FRAUD",
            "event": event,
        }
        logging.warning(f"Too many transactions detected: {event}")
        save_alert(alert)

    # 3. Impossible travel
    if user in user_last_location:
        if user_last_location[user] != country:
            alert = {
                "alert_type": "LOCATION_CHANGE",
                "event": event
            }
            logging.warning(f"Location change detected: {event}")
            save_alert(alert)
    user_last_location[user] = country

    # 4. Device change
    if user in user_last_device:
        if user_last_device[user] != device:
            alert = {
                "alert_type": "DEVICE_CHANGE",
                "event": event
            }

            logging.warning(f"Device change detected: {event}")
            save_alert(alert)
    user_last_device[user] = device

    # 5. Spending spike
    total_amount = sum(txn[1] for txn in user_transactions[user])
    if total_amount > SPENDING_SPIKE:
        alert = {
            "alert_type": "SPENDING_SPIKE",
            "event": event
        }
        logging.warning(f"Spending spike detected: {event}")
        save_alert(alert)