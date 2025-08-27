import boto3, json

def get_kafka_secret():
    """
    Fetch Kafka credentials from AWS Secrets Manager.
    Make sure you have a secret called 'kafka/producer' with fields:
    {
      "username": "...",
      "password": "..."
    }
    """
    client = boto3.client("secretsmanager", region_name="us-east-1")
    secret = client.get_secret_value(SecretId="kafka/producer")
    return json.loads(secret["SecretString"])
🚀 producer.py