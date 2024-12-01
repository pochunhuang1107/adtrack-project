from kafka import KafkaConsumer
import psycopg2
import json

# PostgreSQL Configuration
DB_CONFIG = {
    "dbname": "adtrack",  # Update with your DB details
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'ad-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='adtrack-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

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
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        if not ad_id or not action:
            print("Invalid event:", event)
            return
        
        cursor.execute(
            "INSERT INTO ad_events (ad_id, action) VALUES (%s, %s)",
            (ad_id, action)
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