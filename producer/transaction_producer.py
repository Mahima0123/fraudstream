from faker import Faker
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = [f"U{i}" for i in range(1, 51)]
devices = ["mobile", "web", "tablet"]
countries = ["US", "IN", "UK", "CA", "DE"]
merchants = ["Amazon", "Walmart", "Netflix", "Uber", "Apple"]

def generate_transaction():
    return {
        "user_id": random.choice(users),
        "amount": round(random.uniform(5, 2000), 2),
        "country": random.choice(countries),
        "device_id": f"{random.choice(devices)}_{random.randint(1, 10)}",
        "timestamp": datetime.utcnow().isoformat(),
        "merchant": random.choice(merchants)
    }

while True:
    event = generate_transaction()

    producer.send('transactions', value=event)
    print(f"Produced: {event}")
    time.sleep(1)