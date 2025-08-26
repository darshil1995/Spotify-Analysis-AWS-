import pandas as pd
import json
import boto3
from io import StringIO
from kafka import KafkaConsumer

# -----------------------------
# Configuration
# -----------------------------
TOPIC = 'artists'
BATCH_SIZE = 10
BUCKET_NAME = 'ds-de-project-kafka'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="d2m7vo4hu0bcm3tve3sg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="darshilshah",
    sasl_plain_password="4a4HL3lMWY202piQVhBjiIneE3x7U4",
    auto_offset_reset="earliest",  # start from earliest if no committed offset
    enable_auto_commit=True,       # commit offsets automatically
    group_id="artists-consumer-group",   #persist offsets under this group
    consumer_timeout_ms=10000      # stop if no new messages in 10s
)

# Initialize S3 client
s3_client = boto3.client("s3")

# -----------------------------
# Processing loop
# -----------------------------
DATA = []
batch_number = 1

try:
    for message in consumer:
        try:
            record = json.loads(message.value.decode("utf-8"))
            DATA.append(record)
        except Exception as e:
            print(f"❌ Error decoding message: {e}")
            continue  # skip bad message

        # Upload batch if reached BATCH_SIZE
        if len(DATA) >= BATCH_SIZE:
            try:
                df = pd.DataFrame(DATA)
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                s3_key = f"staging/artists/artists_batch_{batch_number}.csv"
                s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
                print(f"✅ Uploaded batch {batch_number} with {len(DATA)} records to S3 as {s3_key}")
                DATA = []
                batch_number += 1
            except Exception as e:
                print(f"❌ Error uploading batch {batch_number} to S3: {e}")

    # Upload any remaining records after consuming all messages
    if DATA:
        try:
            df = pd.DataFrame(DATA)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_key = f"staging/artists/artists_batch_{batch_number}.csv"
            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
            print(f"✅ Uploaded final batch {batch_number} with {len(DATA)} records to S3 as {s3_key}")
        except Exception as e:
            print(f"❌ Error uploading final batch to S3: {e}")

except KeyboardInterrupt:
    print("⚠️ Consumer stopped manually")

finally:
    consumer.close()
    print("Consumer closed. All messages processed once.")
