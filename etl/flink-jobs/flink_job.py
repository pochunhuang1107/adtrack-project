from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common.time import Time
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from datetime import datetime, timezone
import os
import json
import psycopg2
import redis
import logging

# Configure Logging
logging.basicConfig(
    level=logging.WARNING,
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

# Kafka Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
# Enable Checkpointing for Fault Tolerance
env.enable_checkpointing(5000)  # Checkpoint every 5 seconds

# Kafka Consumer Configuration
deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(Types.ROW_NAMED(
        [
            'ad_id', 'campaign_id', 'creative_id', 'action', 'user_id', 'session_id',
            'device_type', 'browser', 'operating_system', 'ip_address', 'ad_placement',
            'referrer_url', 'destination_url', 'cpc', 'latitude', 'longitude', 'timestamp'
        ],
        [
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.STRING()
        ]
    )).build()

kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPIC,
    deserialization_schema=deserialization_schema,
    properties={
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'flink-group'
    }
)

# Kafka Producer Configuration
serialization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(Types.ROW_NAMED(
        [
            'ad_id', 'campaign_id', 'creative_id', 'action', 'user_id', 'session_id',
            'device_type', 'browser', 'operating_system', 'ip_address', 'ad_placement',
            'referrer_url', 'destination_url', 'cpc', 'latitude', 'longitude', 'timestamp',
            'processed_timestamp', 'total_views', 'total_clicks', 'ctr', 'cumulative_cost'
        ],
        [
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()
        ]
    )).build()

kafka_producer = FlinkKafkaProducer(
    topic='processed-events',
    serialization_schema=serialization_schema,
    producer_config={
        'bootstrap.servers': KAFKA_BROKER,
    }
)

ttl_config = StateTtlConfig \
  .new_builder(Time.days(1)) \
  .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
  .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
  .build()

ds = env.add_source(kafka_consumer)

class MetricsAggregator(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        # Initialize state descriptors
        view_count = ValueStateDescriptor("current_view_count", Types.INT())
        click_count = ValueStateDescriptor("current_click_count", Types.INT())
        total_cost = ValueStateDescriptor("total_cost", Types.FLOAT())
        view_count.enable_time_to_live(ttl_config)
        click_count.enable_time_to_live(ttl_config)
        total_cost.enable_time_to_live(ttl_config)
        self.view_count_state = runtime_context.get_state(view_count)
        self.click_count_state = runtime_context.get_state(click_count)
        self.total_cost_state = runtime_context.get_state(total_cost)

        # Initialize PostgreSQL connection once per subtask
        try:
            self.db_connection = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_connection.cursor()
            logger.info("Connected to PostgreSQL database!")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise e

        # Initialize Redis connection once per subtask
        try:
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.redis_client.ping()
            logger.info("Connected to Redis.")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise e

    def map(self, value: Row) -> Row:
        ad_id = value.ad_id

        current_views = self.view_count_state.value()
        current_clicks = self.click_count_state.value()
        total_cost = self.total_cost_state.value()

        # If this is the first time we see this ad_id state, initialize it from historical data
        if current_views is None and current_clicks is None and total_cost is None:
            current_views, current_clicks, total_cost = self.initialize_from_historical(ad_id)

        # Update state based on the current action
        action = value.action
        if action == "view":
            current_views += 1
        elif action == "click":
            current_clicks += 1
            if value.cpc is not None:
                total_cost += float(value.cpc)

        # Update state
        self.view_count_state.update(current_views)
        self.click_count_state.update(current_clicks)
        self.total_cost_state.update(total_cost)

        # Calculate CTR
        ctr = (current_clicks / current_views) * 100 if current_views > 0 else 0.0

        # Update Redis and publish real-time updates
        self.update_redis_and_publish(ad_id, action, current_views, current_clicks, ctr, total_cost)

        # Emit the processed record
        processed_row = Row(
            ad_id, value.campaign_id, value.creative_id, action, value.user_id,
            value.session_id, value.device_type, value.browser, value.operating_system,
            value.ip_address, value.ad_placement, value.referrer_url, value.destination_url,
            value.cpc, value.latitude, value.longitude, value.timestamp,
            datetime.now(timezone.utc).isoformat(),  # processed_timestamp
            current_views, current_clicks, ctr, total_cost
        )
        return processed_row

    def initialize_from_historical(self, ad_id: str):
        # Attempt to load from Redis first
        try:
            cached_data = self.redis_client.hmget(f"ad_counts:{ad_id}", "total_views", "total_clicks", "cumulative_cost")
            if cached_data[0] is not None and cached_data[1] is not None:
                historical_views = int(cached_data[0])
                historical_clicks = int(cached_data[1])
                historical_cost = float(cached_data[2]) if cached_data[2] is not None else 0.0
                logger.debug(f"Fetched historical data from Redis for ad_id {ad_id}: Views={historical_views}, Clicks={historical_clicks}, Cost={historical_cost}")
            else:
                # If not in Redis, fetch from PostgreSQL (if you have a table 'ad_metrics')
                # Adjust the query/table/columns as per your schema.
                self.db_cursor.execute("SELECT total_views, total_clicks, cumulative_cost FROM ad_events WHERE ad_id = %s order by processed_timestamp desc limit 1", (ad_id,))
                result = self.db_cursor.fetchone()
                if result:
                    historical_views, historical_clicks, historical_cost = result
                    logger.debug(f"Fetched historical data from PostgreSQL for ad_id {ad_id}: Views={historical_views}, Clicks={historical_clicks}, Cost={historical_cost}")
                else:
                    historical_views, historical_clicks, historical_cost = 0, 0, 0.0
                    logger.debug(f"No historical data found for ad_id {ad_id}, defaulting to Views=0, Clicks=0, Cost=0.0")
        except Exception as e:
            logger.error(f"Error fetching historical data for ad_id {ad_id}: {e}")
            # Default to zeros if error occurs
            historical_views, historical_clicks, historical_cost = 0, 0, 0.0

        return historical_views, historical_clicks, historical_cost

    def update_redis_and_publish(self, ad_id, action, current_views, current_clicks, ctr, total_cost):
        # Update Redis
        try:
            self.redis_client.hset(f"ad_counts:{ad_id}", mapping={
                "total_views": current_views,
                "total_clicks": current_clicks,
                "ctr": ctr,
                "cumulative_cost": total_cost
            })
            logger.debug(f"Updated Redis for ad_id {ad_id}: Views={current_views}, Clicks={current_clicks}, CTR={ctr}, Cost={total_cost}")
        except Exception as e:
            logger.error(f"Error updating Redis for ad_id {ad_id}: {e}")

        # Publish real-time updates to Redis channel
        try:
            publish_message = {
                'ad_id': ad_id,
                'action': action,
                'total_views': current_views,
                'total_clicks': current_clicks,
                'ctr': ctr,
                'cumulative_cost': total_cost
            }
            self.redis_client.publish('realtime-updates', json.dumps(publish_message))
            logger.debug(f"Published real-time update for ad_id {ad_id}")
        except Exception as e:
            logger.error(f"Error publishing real-time update for ad_id {ad_id}: {e}")

    def close(self):
        # Close PostgreSQL connection
        if hasattr(self, 'db_cursor') and self.db_cursor:
            self.db_cursor.close()
        if hasattr(self, 'db_connection') and self.db_connection:
            self.db_connection.close()
        logger.info("Closed PostgreSQL connection.")

        # redis.Redis client does not strictly require a close call. Just log it.
        logger.info("Freed Redis resources.")

output_type = Types.ROW_NAMED(
    [
        'ad_id', 'campaign_id', 'creative_id', 'action', 'user_id', 'session_id',
        'device_type', 'browser', 'operating_system', 'ip_address', 'ad_placement',
        'referrer_url', 'destination_url', 'cpc', 'latitude', 'longitude', 'timestamp',
        'processed_timestamp', 'total_views', 'total_clicks', 'ctr', 'cumulative_cost'
    ],
    [
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.STRING(),
        Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()
    ]
)

# Key the stream by 'ad_id' to ensure per-ad aggregation
keyed_ds = ds.key_by(lambda record: record.ad_id)

# Apply the MetricsAggregator using MapFunction
processed_ds = keyed_ds.map(
    MetricsAggregator(),
    output_type=output_type
)

# Send processed data to Kafka
processed_ds.add_sink(kafka_producer)

# Execute the Flink job
env.execute("Flink Ad Metrics Aggregation with Historical Views, Clicks, and Cost")