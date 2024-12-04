from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from datetime import datetime, timezone

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Kafka Consumer Configuration
deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(Types.ROW_NAMED(
        ['ad_id', 'action', 'timestamp'],
        [Types.STRING(), Types.STRING(), Types.STRING()]
    )).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='ad-events',
    deserialization_schema=deserialization_schema,
    properties={
        'bootstrap.servers': 'localhost:9093',
        'group.id': 'flink-test-group'
    }
)

# Kafka Producer Configuration
serialization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(Types.ROW_NAMED(
        ['ad_id', 'action', 'timestamp', 'processed_timestamp'],
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]  # Corresponding data types
    )).build()

kafka_producer = FlinkKafkaProducer(
    topic='processed-events',
    serialization_schema=serialization_schema,
    producer_config={
        'bootstrap.servers': 'localhost:9093',
    }
)

# Processing the data stream
ds = env.add_source(kafka_consumer)

# Add a transformation to append a processed timestamp
def process_record(record):
    return Row(
        record.ad_id,
        record.action,
        record.timestamp,
        datetime.now(timezone.utc).isoformat()
    )

processed_ds = ds.map(lambda x: process_record(x), output_type=Types.ROW_NAMED(
    ['ad_id', 'action', 'timestamp', 'processed_timestamp'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
))

# Print to console (optional for debugging)
processed_ds.print()

# Send to Kafka producer
processed_ds.add_sink(kafka_producer)

# Execute the Flink job
env.execute("Kafka Consumer and Producer")
