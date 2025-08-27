import socket, json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import get_kafka_secret

# Load secret once
secret = get_kafka_secret()

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="d2m7vo4hu0bcm3tve3sg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=secret["username"],
    sasl_plain_password=secret["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

hostname = str.encode(socket.gethostname())

def on_success(metadata):
    print(f"✅ Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
    print(f"❌ Error sending message: {e}")

# Produce some messages
for i in range(5):
    msg = f"asynchronous message #{i}"
    future = producer.send(
        "topic-1",
        key=hostname,
        value={"msg": msg}  # sending JSON instead of plain string
    )
    future.add_callback(on_success)
    future.add_errback(on_error)

# Ensure all messages are sent
producer.flush()
producer.close()
print("✅ All records sent successfully!")
