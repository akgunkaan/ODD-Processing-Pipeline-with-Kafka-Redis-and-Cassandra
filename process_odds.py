import glob
import json
import os
import time
import warnings

import numpy as np
import yaml
from cassandra.cluster import Cluster
from cassandra.io.asyncio import AsyncioConnection
from kafka import KafkaConsumer, KafkaProducer
from redis import Redis

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "odd_submissions"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_HIGH_PRIORITY_LIST = "high_priority_odds"
CASSANDRA_CONTACT_POINTS = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
CASSANDRA_KEYSPACE = "cap_odd_system"
CASSANDRA_TABLE = "odds"
# Please update this path according to your system.
ODD_FILES_PATH = r"c:\Users\kaan_\Desktop\waymo\cap\generated_synthetic_odd\*.yaml" # Using the path from the original file

# --- YAML Security Warning ---
# YAML files contain tags like `!!python/object/apply`.
# This can cause PyYAML to execute arbitrary Python code.
# Only use this script with YAML files from TRUSTED sources.
warnings.warn(
    "Using yaml.unsafe_load() which is insecure. "
    "Only use this with trusted YAML files."
)


def convert_numpy_types(data):
    """Converts numpy data types loaded from YAML to standard Python types."""
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_numpy_types(i) for i in data]
    if hasattr(data, "item"):  # numpy skaler tiplerini (str_, float_, int_, vb.) kontrol eder
        return data.item()
    return data


def setup_cassandra_session():
    """Connects to Cassandra and returns a session object."""
    try:
        # Explicitly set connection_class for Python 3.12+ compatibility
        # as the default 'asyncore' has been removed.
        cluster = Cluster(CASSANDRA_CONTACT_POINTS,
                          connection_class=AsyncioConnection)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        print("Successfully connected to Cassandra.")
        return session
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        return None


def read_and_produce_odds():
    """Reads YAML files and sends their content to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    yaml_files = glob.glob(ODD_FILES_PATH)
    if not yaml_files:
        print(f"No YAML files found at the specified path: {ODD_FILES_PATH}")
        return

    print(f"Found {len(yaml_files)} ODD files. Sending to Kafka...")

    for file_path in yaml_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # SECURITY RISK: Use only with trusted sources.
                data = yaml.unsafe_load(f)

            # Convert Numpy types to standard types
            clean_data = convert_numpy_types(data)

            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=clean_data)
            print(f"-> {clean_data.get('ODD_ID')} sent to Kafka.")

        except Exception as e:
            print(f"Error: Could not process {file_path}. - {e}")

    producer.flush()
    producer.close()
    print("All files have been sent to Kafka.")


def consume_and_prioritize_odds():
    """Reads ODD data from Kafka, prioritizes it, and writes to Redis/Cassandra."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",  # Read all messages from the beginning
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,  # Stop if no message arrives after 10 seconds
    )

    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    cassandra_session = setup_cassandra_session()

    if not cassandra_session:
        print("Cannot start consumer because Cassandra connection could not be established.")
        return

    print("\nWaiting for messages from Kafka...")

    # Prepared statement for Cassandra
    insert_query = cassandra_session.prepare(
        f"INSERT INTO {CASSANDRA_TABLE} (odd_id, category, raw_data) VALUES (?, ?, ?)"
    )

    message_count = 0
    for message in consumer:
        message_count += 1
        odd_data = message.value
        odd_id = odd_data.get("ODD_ID")
        category = odd_data.get("Category")

        print(f"\nProcessing: {odd_id}")

        # --- Prioritization Logic ---
        if category == "Corner Case":
            # 1. Add important ODD to Redis
            redis_client.lpush(REDIS_HIGH_PRIORITY_LIST, json.dumps(odd_data))
            print(f"  -> [IMPORTANT] {odd_id} added to Redis list.")

        # 2. Save all ODDs to Cassandra
        try:
            cassandra_session.execute(
                insert_query, [odd_id, category, json.dumps(odd_data)]
            )
            print(f"  -> {odd_id} saved to Cassandra.")
        except Exception as e:
            print(f"  -> Error: Could not save {odd_id} to Cassandra. - {e}")

    if message_count == 0:
        print("No new messages found in the Kafka topic.")
    else:
        print(f"\nProcessed a total of {message_count} messages.")

    consumer.close()
    cassandra_session.shutdown()


if __name__ == "__main__":
    print("--- Starting ODD Processing Pipeline ---")

    # Step 1: Run the producer to send YAML files to Kafka.
    # In a real system, this would be a continuously running separate service.
    read_and_produce_odds()

    # A short delay to allow Kafka to process messages
    time.sleep(5)

    # Step 2: Run the consumer to process data from Kafka.
    # This would also be a continuously running separate service.
    consume_and_prioritize_odds()

    print("\n--- ODD Processing Pipeline Finished ---")