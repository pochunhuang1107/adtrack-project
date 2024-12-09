from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from datetime import datetime, timezone
import os

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()

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
    topics='ad-events',
    deserialization_schema=deserialization_schema,
    properties={
        'bootstrap.servers': os.getenv("KAFKA_BROKER", "localhost:9092"),
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
        'bootstrap.servers': os.getenv("KAFKA_BROKER", "localhost:9092"),
    }
)

# Processing the data stream
ds = env.add_source(kafka_consumer)

# Stateful Map Function to Compute Metrics
class MetricsAggregator:
    def __init__(self):
        self.view_count = 0
        self.click_count = 0
        self.total_cost = 0.0

    def process(self, record):
        action = record.action
        if action == "view":
            self.view_count += 1
        elif action == "click":
            self.click_count += 1
            self.total_cost += float(record.cpc)

        ctr = (self.click_count / self.view_count) * 100 if self.view_count > 0 else 0.0

        return Row(
            record.ad_id, record.campaign_id, record.creative_id, record.action, record.user_id,
            record.session_id, record.device_type, record.browser, record.operating_system,
            record.ip_address, record.ad_placement, record.referrer_url, record.destination_url,
            record.cpc, record.latitude, record.longitude, record.timestamp,
            datetime.now(timezone.utc).isoformat(),  # processed_timestamp
            self.view_count, self.click_count, ctr, self.total_cost
        )

# Global for development purpose
aggregator = MetricsAggregator()

processed_ds = ds.map(
    lambda record: aggregator.process(record),
    output_type=Types.ROW_NAMED(
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
)

# Print to console for debugging (optional)
processed_ds.print()

# Send processed data to Kafka
processed_ds.add_sink(kafka_producer)

# Execute the Flink job
env.execute("Flink Ad Metrics Aggregation")
