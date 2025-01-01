FROM python:3.8
WORKDIR /app
COPY create_topics.py /app/
RUN pip install confluent-kafka
CMD ["python", "/app/create_topics.py"]