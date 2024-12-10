FROM flink:1.20.0

# install python3 and pip3
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk-headless && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# install PyFlink
RUN pip3 install apache-flink==1.20.0 psycopg2-binary redis --no-cache-dir

RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Copy the Flink job into the container
COPY flink_job.py /opt/flink_jobs/flink_job.py

# Start the cluster and submit the job
CMD ["bash", "-c", "start-cluster.sh && flink run -py /opt/flink_jobs/flink_job.py && tail -f /dev/null"]