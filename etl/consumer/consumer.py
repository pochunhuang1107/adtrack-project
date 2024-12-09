import os
import json
import logging
from kafka import KafkaConsumer
import psycopg2
import redis
from psycopg2 import sql

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Validate Environment Variables
required_env_vars = [
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "REDIS_HOST",
    "REDIS_PORT",
    "KAFKA_TOPIC",
    "KAFKA_BROKER"
]

missing_vars = [var for var in required_env_vars if var not in os.environ]
if missing_vars:
    logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
    exit(1)

# PostgreSQL Configuration
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
}

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

# Kafka Consumer Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='adtrack-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
)

# Connect to Redis with Connection Pooling
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    logger.info("Connected to Redis.")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    exit(1)

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    logger.info("Connected to PostgreSQL database!")

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ad_events (
            id SERIAL PRIMARY KEY,
            ad_id VARCHAR(255),
            campaign_id VARCHAR(255),
            creative_id VARCHAR(255),
            action VARCHAR(255),
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            device_type VARCHAR(255),
            browser VARCHAR(255),
            operating_system VARCHAR(255),
            ip_address VARCHAR(255),
            ad_placement VARCHAR(255),
            referrer_url TEXT,
            destination_url TEXT,
            cpc FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            timestamp TIMESTAMP,
            processed_timestamp TIMESTAMP,
            total_views INT,
            total_clicks INT,
            ctr FLOAT,
            cumulative_cost FLOAT
        )
    """)
    conn.commit()
    logger.info("Ensured that the ad_events table exists.")
except Exception as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

# Process Kafka messages and update Redis and PostgreSQL
def process_event(event):
    try:
        # Extract fields from the Kafka message
        ad_id = event.get('ad_id')
        campaign_id = event.get('campaign_id')
        creative_id = event.get('creative_id')
        action = event.get('action')
        user_id = event.get('user_id')
        session_id = event.get('session_id')
        device_type = event.get('device_type')
        browser = event.get('browser')
        operating_system = event.get('operating_system')
        ip_address = event.get('ip_address')
        ad_placement = event.get('ad_placement')
        referrer_url = event.get('referrer_url')
        destination_url = event.get('destination_url')
        cpc = event.get('cpc', 0.0)
        latitude = event.get('latitude', 0.0)
        longitude = event.get('longitude', 0.0)
        timestamp = event.get('timestamp')
        processed_timestamp = event.get('processed_timestamp')
        total_views = event.get('total_views', 0)
        total_clicks = event.get('total_clicks', 0)
        ctr = event.get('ctr', 0.0)
        cumulative_cost = event.get('cumulative_cost', 0.0)

        # Insert into PostgreSQL
        insert_query = sql.SQL("""
            INSERT INTO ad_events (
                ad_id, campaign_id, creative_id, action, user_id, session_id, device_type,
                browser, operating_system, ip_address, ad_placement, referrer_url,
                destination_url, cpc, latitude, longitude, timestamp, processed_timestamp,
                total_views, total_clicks, ctr, cumulative_cost
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)

        cursor.execute(insert_query, (
            ad_id, campaign_id, creative_id, action, user_id, session_id, device_type,
            browser, operating_system, ip_address, ad_placement, referrer_url,
            destination_url, cpc, latitude, longitude, timestamp, processed_timestamp,
            total_views, total_clicks, ctr, cumulative_cost
        ))
        conn.commit()
        logger.debug(f"Inserted event into PostgreSQL for ad_id: {ad_id}")

        # Update Redis
        r.hset(f"ad_counts:{ad_id}", mapping={
            "total_views": total_views,
            "total_clicks": total_clicks,
            "ctr": ctr,
            "cumulative_cost": cumulative_cost
        })
        logger.debug(f"Updated Redis for ad_id: {ad_id}")

        # Publish to Redis channel for real-time updates
        publish_message = {
            'ad_id': ad_id,
            'action': action,
            'total_views': total_views,
            'total_clicks': total_clicks,
            'ctr': ctr,
            'cumulative_cost': cumulative_cost
        }
        r.publish('realtime-updates', json.dumps(publish_message))
        logger.debug(f"Published real-time update for ad_id: {ad_id}")

        logger.info(f"Processed and stored event for ad_id: {ad_id}")
    except Exception as e:
        logger.error(f"Failed to process event {event}: {e}")

# Consume messages from Kafka
try:
    logger.info(f"Subscribed to Kafka topic: {KAFKA_TOPIC}")
    logger.info("Starting Kafka consumer...")
    while True:
        message_batch = consumer.poll(timeout_ms=1000)
        if not message_batch:
            continue

        for tp, messages in message_batch.items():
            for message in messages:
                event = message.value
                logger.debug(f"Consumed event: {event}")
                process_event(event)

except KeyboardInterrupt:
    logger.info("Shutting down consumer due to KeyboardInterrupt...")
except Exception as e:
    logger.error(f"Unexpected error in consumer: {e}")
finally:
    consumer.close()
    cursor.close()
    conn.close()
    logger.info("Closed Kafka consumer and PostgreSQL connection.")
