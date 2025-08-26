import pandas as pd
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="d2m7vo4hu0bcm3tve3sg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
  security_protocol="SASL_SSL",
  sasl_mechanism="SCRAM-SHA-256",
  sasl_plain_username="darshilshah",
  sasl_plain_password="4a4HL3lMWY202piQVhBjiIneE3x7U4",
  value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

albums = pd.read_csv("albums.csv")  # 1000 records

for record in albums.to_dict(orient='records'):
    try:
        result= producer.send('albums', record)  # async send
        print(f"Message Produced: {result}")

    except Exception as e:
        print(f"Error producing message: {e}")
        

producer.flush()  # ensure all messages are sent
producer.close()
print("All records sent successfully!")
