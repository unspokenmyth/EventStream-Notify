import yaml
import json
import logging
from kafka import KafkaProducer
import time

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("producer.log"), logging.StreamHandler()]
)

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

KAFKA_BOOTSTRAP = config['kafka']['bootstrap_servers']
TOPICS = config['kafka']['topics']

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def run_producer():
    for i in range(1, 11):
        for topic in TOPICS:
            value = {"id": i, "value": f"event-{i}"}
            try:
                producer.send(topic, value)
                logging.info(f"Sent to {topic}: {value}")
            except Exception as e:
                logging.error(f"Failed to send message: {e}")
            time.sleep(0.1)  # slight delay

if __name__ == "__main__":
    run_producer()
    producer.flush()
