"""
event_automation.py
Resilient Kafka <-> Redis automation with:
- topic auto-create
- producer (simulated)
- consumer with checkpointing, dedup, rate-limit, batching
- publishes processed events to Redis Pub/Sub channel "events" for real-time dashboard

Usage:
  1. Ensure Kafka (KRaft) is running and listening on 127.0.0.1:9092.
  2. Ensure Redis server is running (default 127.0.0.1:6379).
  3. Put config.yaml next to this script or rely on defaults.
  4. python event_automation.py
"""

import threading
import time
import json
import logging
import random
import signal
import sys
from typing import Dict, List

import redis
import yaml
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("event_automation.log"), logging.StreamHandler()],
)

# ----------------------------
# Load config (optional)
# ----------------------------
DEFAULT_CONFIG = {
    "kafka": {
        "bootstrap_servers": "127.0.0.1:9092",
        "topic": "test_topic",
        "num_partitions": 1,
        "replication_factor": 1,
    },
    "redis": {"host": "127.0.0.1", "port": 6379, "db": 0},
    "producer": {"enabled": True, "produce_interval_s": 0.05},  # 20 msgs/sec
    "consumer": {"batch_size": 32, "dedup_ttl_s": 60, "rate_limit_count": 5, "rate_limit_window_s": 60},
}

try:
    with open("config.yaml", "r") as f:
        cfg_file = yaml.safe_load(f)
        cfg = DEFAULT_CONFIG.copy()
        # shallow merge (good enough for this script)
        for k, v in (cfg_file or {}).items():
            if isinstance(v, dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
except FileNotFoundError:
    logging.info("config.yaml not found — using default configuration.")
    cfg = DEFAULT_CONFIG

KAFKA_BOOTSTRAP = cfg["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = cfg["kafka"]["topic"]
NUM_PARTITIONS = cfg["kafka"].get("num_partitions", 1)
REPLICATION_FACTOR = cfg["kafka"].get("replication_factor", 1)

REDIS_HOST = cfg["redis"]["host"]
REDIS_PORT = cfg["redis"]["port"]
REDIS_DB = cfg["redis"]["db"]

# Producer/consumer params
PRODUCER_ENABLED = cfg["producer"].get("enabled", True)
PRODUCE_INTERVAL = cfg["producer"].get("produce_interval_s", 0.05)

BATCH_SIZE = cfg["consumer"].get("batch_size", 32)
DEDUP_TTL = cfg["consumer"].get("dedup_ttl_s", 60)
RATE_LIMIT_COUNT = cfg["consumer"].get("rate_limit_count", 5)
RATE_LIMIT_WINDOW = cfg["consumer"].get("rate_limit_window_s", 60)

# Redis keys
OFFSET_HASH_KEY = f"offsets:{KAFKA_TOPIC}"  # hash: partition -> last_offset
DEDUP_PREFIX = "dedup:"  # use setnx on dedup:{event_id}
RATE_PREFIX = "rate:"  # rate:{user_id}
PUBSUB_CHANNEL = "events"  # channel for dashboard frontend
RUN_DURATION = 10  # seconds, adjust as needed
start_time = time.time()


# ----------------------------
# Graceful shutdown handling
# ----------------------------
stop_event = threading.Event()


def handle_sigint(sig, frame):
    logging.info("SIGINT received — shutting down...")
    stop_event.set()


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


# ----------------------------
# Wait helpers
# ----------------------------


def wait_for_redis(timeout: int = 30):
    start = time.time()
    while True:
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)
            client.ping()
            logging.info("Connected to Redis.")
            return client
        except Exception as e:
            if time.time() - start > timeout:
                raise TimeoutError("Redis not available within timeout") from e
            logging.info("Waiting for Redis... retry in 1s.")
            time.sleep(1)


def wait_for_kafka(timeout: int = 30):
    start = time.time()
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="waiter")
            admin.close()
            logging.info("Kafka broker is reachable.")
            return True
        except NoBrokersAvailable:
            if time.time() - start > timeout:
                raise TimeoutError("Kafka not available within timeout")
            logging.info("Waiting for Kafka... retry in 1s.")
            time.sleep(1)


# ----------------------------
# Topic management
# ----------------------------
def ensure_topic_exists():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="topic-creator", request_timeout_ms=2000)
        topics = admin.list_topics()
        if KAFKA_TOPIC in topics:
            logging.info(f"Topic '{KAFKA_TOPIC}' already exists.")
        else:
            new_topic = NewTopic(name=KAFKA_TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
            admin.create_topics([new_topic])
            logging.info(f"Created topic '{KAFKA_TOPIC}'.")
        admin.close()
    except TopicAlreadyExistsError:
        logging.info("Topic already exists (race).")
    except Exception as e:
        logging.error(f"Failed to ensure topic exists: {e}")
        raise


# ----------------------------
# Producer thread (simulated events)
# ----------------------------
def producer_thread_main():
    """Simulated producer. Sends events continuously to the topic."""
    # resilient Kafka producer connect
    producer = None
    start_connect = time.time()
    while not stop_event.is_set():
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_block_ms=5000,
            )
            logging.info("Producer connected to Kafka.")
            break
        except Exception as e:
            if time.time() - start_connect > 30:
                logging.error("Producer failed to connect to Kafka within 30s.")
                return
            logging.info("Producer waiting for Kafka... retry in 1s")
            time.sleep(1)

    id_counter = 1
    start_time = time.time()  # <-- track producer runtime

    while not stop_event.is_set():
        # stop after RUN_DURATION seconds
        if time.time() - start_time > RUN_DURATION:
            logging.info(f"⏱️ Stopping producer after {RUN_DURATION} seconds.")
            stop_event.set()
            break

        # create a sample event; include optional user_id to show rate-limiting
        user_id = f"user{random.randint(1,5)}"  # 5 users for demo
        event = {
            "id": id_counter,
            "value": f"event-{id_counter}",
            "user_id": user_id,
            "timestamp": time.time()
        }

        try:
            producer.send(KAFKA_TOPIC, value=event)
        except Exception as e:
            logging.error(f"Producer send error: {e}")

        id_counter += 1
        if id_counter > 1000000:
            id_counter = 1

        time.sleep(PRODUCE_INTERVAL)

    try:
        producer.flush(5)
        producer.close()
    except Exception:
        pass
    logging.info("Producer thread exiting.")

# ----------------------------
# Consumer helper: restore offsets from Redis
# ----------------------------
def restore_offsets(consumer: KafkaConsumer, r: redis.Redis):
    """
    After subscription, wait for assignment, then seek partitions to offsets stored in Redis.
    Stored offsets are the last processed offset; consumer should start at last+1.
    """
    # wait for assignment
    retries = 0
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)
        retries += 1
        if retries > 200:  # ~20s
            logging.warning("Timeout waiting for assignment.")
            break

    assignments = consumer.assignment()
    if not assignments:
        logging.warning("No partition assignment; cannot restore offsets.")
        return

    # read stored offsets from Redis hash
    stored = r.hgetall(OFFSET_HASH_KEY)  # returns bytes -> bytes
    stored_offsets = {}
    for part_b, off_b in stored.items():
        try:
            part = int(part_b.decode() if isinstance(part_b, bytes) else part_b)
            off = int(off_b.decode() if isinstance(off_b, bytes) else off_b)
            stored_offsets[part] = off
        except Exception:
            continue

    for tp in assignments:
        part = tp.partition
        if part in stored_offsets:
            # seek to stored_offset + 1 (next message after last processed)
            seek_to = stored_offsets[part] + 1
            try:
                consumer.seek(tp, seek_to)
                logging.info(f"Seeking partition {part} to offset {seek_to}")
            except Exception as e:
                logging.warning(f"Failed to seek partition {part} to {seek_to}: {e}")


# ----------------------------
# Consumer thread
# ----------------------------
def consumer_thread_main(r: redis.Redis):
    """
    Robust timed consumer:
    - subscribes
    - restores offsets
    - processes messages in batches
    - dedup via Redis SETNX with TTL
    - rate-limit per user
    - writes processed events to Redis
    - publishes events on Redis Pub/Sub channel
    - checkpoints offsets to Redis
    Stops automatically after RUN_DURATION seconds.
    """
    start_time = time.time()
    consumer = None

    # connect consumer with retries
    retry_start = time.time()
    while not stop_event.is_set():
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=2000,
                client_id="consumer-event-automation",
            )
            logging.info("Consumer connected to Kafka.")
            break
        except Exception as e:
            if time.time() - retry_start > 30:
                logging.error("Consumer failed to connect within 30s.")
                return
            logging.info("Consumer waiting for Kafka... retry in 1s")
            time.sleep(1)

    restore_offsets(consumer, r)

    batch: list = []
    last_offsets_to_commit: dict = {}

    try:
        while not stop_event.is_set():
            for msg in consumer:
                if msg is None:
                    continue

                # Stop after RUN_DURATION
                if time.time() - start_time > RUN_DURATION:
                    logging.info(f"⏱️ Stopping consumer after {RUN_DURATION} seconds.")
                    stop_event.set()
                    break

                ev = msg.value
                # dedup
                ev_id = ev.get("id")
                ev_key = f"{DEDUP_PREFIX}{ev_id}" if ev_id else f"noid:{msg.partition}:{msg.offset}"

                try:
                    added = r.setnx(ev_key, 1)
                    if added:
                        r.expire(ev_key, DEDUP_TTL)
                    else:
                        last_offsets_to_commit[msg.partition] = msg.offset
                        continue
                except Exception as e:
                    logging.error(f"Redis dedup failed: {e}")
                    last_offsets_to_commit[msg.partition] = msg.offset
                    continue

                # rate-limit
                user_id = ev.get("user_id")
                rate_allowed = True
                if user_id:
                    rate_key = f"{RATE_PREFIX}{user_id}"
                    try:
                        count = r.incr(rate_key)
                        if count == 1:
                            r.expire(rate_key, RATE_LIMIT_WINDOW)
                        if count > RATE_LIMIT_COUNT:
                            rate_allowed = False
                            logging.info(f"Rate-limited user {user_id} (count={count})")
                    except Exception as e:
                        logging.error(f"Redis rate-limit failed: {e}")

                if rate_allowed:
                    batch.append((msg.partition, msg.offset, ev))
                else:
                    last_offsets_to_commit[msg.partition] = msg.offset

                # batch processing
                if len(batch) >= BATCH_SIZE:
                    pipe = r.pipeline()
                    for part, offset, event in batch:
                        key = f"event:{event.get('id')}"
                        pipe.set(key, json.dumps(event))
                        pipe.publish(PUBSUB_CHANNEL, json.dumps(event))
                        last_offsets_to_commit[part] = offset
                    try:
                        pipe.execute()
                        logging.info(f"Processed batch of {len(batch)} events.")
                    except Exception as e:
                        logging.error(f"Redis pipeline failed: {e}")
                    batch = []

            # flush remaining batch
            if batch:
                pipe = r.pipeline()
                for part, offset, event in batch:
                    key = f"event:{event.get('id')}"
                    pipe.set(key, json.dumps(event))
                    pipe.publish(PUBSUB_CHANNEL, json.dumps(event))
                    last_offsets_to_commit[part] = offset
                try:
                    pipe.execute()
                    logging.info(f"Flushed remaining batch of {len(batch)} events.")
                except Exception as e:
                    logging.error(f"Redis pipeline failed: {e}")
                batch = []

            # checkpoint offsets
            if last_offsets_to_commit:
                try:
                    mapping = {str(part): str(off) for part, off in last_offsets_to_commit.items()}
                    r.hset(OFFSET_HASH_KEY, mapping=mapping)
                    logging.debug(f"Checkpointed offsets: {mapping}")
                except Exception as e:
                    logging.error(f"Offset checkpoint failed: {e}")

            # if no messages, brief sleep
            time.sleep(0.5)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logging.info("Consumer thread exiting.")
        # Stop producer if running
        stop_event.set()

# ----------------------------
# Main entry — orchestrate threads
# ----------------------------
def main():
    logging.info("Starting event automation...")

    # Wait for Redis and Kafka
    r = wait_for_redis(timeout=60)
    wait_for_kafka(timeout=60)

    # Ensure topic
    ensure_topic_exists()

    # Start consumer thread first (so it can restore offsets and be ready)
    consumer_thread = threading.Thread(target=consumer_thread_main, args=(r,), daemon=True)
    consumer_thread.start()
    logging.info("Started consumer thread.")

    # Give consumer a half-second to set up assignment & seek
    time.sleep(0.5)

    # Start producer (if enabled)
    if PRODUCER_ENABLED:
        prod_thread = threading.Thread(target=producer_thread_main, daemon=True)
        prod_thread.start()
        logging.info("Started producer thread.")
    else:
        logging.info("Producer disabled by config.")

    # Wait until interrupted
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    logging.info("Main loop exiting. Waiting for threads to finish.")
    # threads are daemons — they will exit with process
    time.sleep(1)
    logging.info("Shutdown complete.")


if __name__ == "__main__":
    main()

