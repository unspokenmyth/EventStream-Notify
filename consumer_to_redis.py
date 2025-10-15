import yaml
import json
import logging
import redis
from kafka import KafkaConsumer

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("consumer.log"), logging.StreamHandler()]
)

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

KAFKA_BOOTSTRAP = config['kafka']['bootstrap_servers']
TOPICS = config['kafka']['topics']

REDIS_HOST = config['redis']['host']
REDIS_PORT = config['redis']['port']
REDIS_DB = config['redis']['db']

# Redis client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Kafka consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def run_consumer():
    for message in consumer:
        key = f"event:{message.value['id']}"
        try:
            r.set(key, json.dumps(message.value))
            logging.info(f"Stored in Redis: {message.value}")
        except Exception as e:
            logging.error(f"Redis store failed: {e}")

if __name__ == "__main__":
    run_consumer()
