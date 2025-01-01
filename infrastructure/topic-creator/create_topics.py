from confluent_kafka.admin import AdminClient, NewTopic

def ensure_topics(broker, topics):
    admin_client = AdminClient({"bootstrap.servers": broker})
    existing_topics = admin_client.list_topics(timeout=10).topics

    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1)
        for topic in topics if topic not in existing_topics
    ]

    if new_topics:
        futures = admin_client.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Created topic: {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print("All topics already exist.")

if __name__ == "__main__":
    ensure_topics("kafka:9092", ["ad-events", "processed-events"])