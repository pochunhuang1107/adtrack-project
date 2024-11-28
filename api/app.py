from flask import Flask, request, jsonify
from prometheus_flask_exporter import PrometheusMetrics

app = Flask(__name__)
metrics = PrometheusMetrics(app)

@app.route('/events', methods=['POST'])
def log_event():
    data = request.get_json()
    return jsonify(data), 200

@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Ad Tracking API!", 200

if __name__ == "__main__":
    app.run(debug=True)