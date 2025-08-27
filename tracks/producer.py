import pandas as pd
from kafka import KafkaProducer
import json
from config import get_kafka_secret

# Load secret once
secret = get_kafka_secret()

producer = KafkaProducer(
    bootstrap_servers="d2m7vo4hu0bcm3tve3sg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=secret["username"],
    sasl_plain_password=secret["password"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

tracks = pd.read_csv("track.csv")  # 1000 records

for record in tracks.to_dict(orient='records'):
    try:
        result= producer.send('tracks', record)  # async send
        print(f"Message Produced: {result}")

    except Exception as e:
        print(f"Error producing message: {e}")
        

producer.flush()  # ensure all messages are sent
producer.close()
print("All records sent successfully!")
