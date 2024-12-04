from flask import Flask, request, jsonify
from prometheus_flask_exporter import PrometheusMetrics
from kafka import KafkaProducer
from datetime import datetime, timezone
import json

app = Flask(__name__)
metrics = PrometheusMetrics(app)

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/events', methods=['POST'])
def events():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid input"}), 400
    
    data['timestamp'] = datetime.now(timezone.utc).isoformat()

    producer.send('ad-events', value=data)
    producer.flush()

    return jsonify({"message": "Event received"}), 200

@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Ad Tracking API!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)