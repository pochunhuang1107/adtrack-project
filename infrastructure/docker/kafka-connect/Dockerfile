FROM confluentinc/cp-kafka-connect:7.3.0

# Install S3 Sink Connector into a clean directory
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest --component-dir /usr/share/confluent-hub-components

# Set the plugin path explicitly
ENV CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components"

EXPOSE 8083
