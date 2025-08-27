from kafka import KafkaConsumer
from config import get_kafka_secret
import json

# Load secret once
secret = get_kafka_secret()

# Kafka consumer setup
consumer = KafkaConsumer(
    "topic-1",
    bootstrap_servers="d2m7vo4hu0bcm3tve3sg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=secret["username"],
    sasl_plain_password=secret["password"],
    auto_offset_reset="earliest",   # start from beginning if no commits
    enable_auto_commit=True,        # commit offsets automatically
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📡 Listening for messages...")
try:
    for message in consumer:
        print(f"📥 Received Message: {message.value}")
except KeyboardInterrupt:
    print("👋 Consumer stopped manually")
finally:
    consumer.close()
