from kafka import KafkaConsumer
import psycopg2
import json
import os

# PostgreSQL Configuration
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "adtrack"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC", "processed-events"),
    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='adtrack-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# For debugging
print(f"Subscribed to topics: {consumer.subscription()}")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database!")

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ad_events (
            id SERIAL PRIMARY KEY,
            ad_id VARCHAR(255),
            action VARCHAR(255),
            timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP
        )
    """)
    conn.commit()
except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

# Process and store events
def process_event(event):
    try:
        ad_id = event.get('ad_id')
        action = event.get('action')
        timestamp = event.get('timestamp')
        processed_timestamp = event.get('processed_timestamp')
        if not ad_id or not action:
            print("Invalid event:", event)
            return
        
        cursor.execute(
            "INSERT INTO ad_events (ad_id, action, timestamp, processed_timestamp) VALUES (%s, %s, %s, %s)",
            (ad_id, action, timestamp, processed_timestamp)
        )
        conn.commit()
        print(f"Stored event: {event}")
    except Exception as e:
        print(f"Failed to process event {event}: {e}")

# Consume messages from Kafka
try:
    print("Starting Kafka consumer...")
    for message in consumer:
        event = message.value
        print(f"Consumed event: {event}")
        process_event(event)
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
    cursor.close()
    conn.close()