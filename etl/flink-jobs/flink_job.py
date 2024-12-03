from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json

# Initialize environment
env = StreamExecutionEnvironment.get_execution_environment()

# Configure Kafka Consumer
kafka_consumer = FlinkKafkaConsumer(
    topics='ad-events',
    properties={
        'bootstrap.servers': 'localhost:9093',
        'group.id': 'flink-group'
    }
)

def process_event(event):
    # Example: Add additional information
    event["processed"] = True
    return json.dumps(event)

# Read and process Kafka messages
stream = env.add_source(kafka_consumer).map(
    lambda message: process_event(json.loads(message))
)

# Configure Kafka Producer for results
kafka_producer = FlinkKafkaProducer(
    topic='processed-events',
    properties={'bootstrap.servers': 'localhost:9093'}
)
stream.add_sink(kafka_producer)

# Execute the Flink job
env.execute("Kafka Flink Processing Job")
