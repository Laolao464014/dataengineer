#!/usr/bin/env python3
"""
REST API for IoT Sensor Data Platform.
Exposes sensor data from the data lake and Kafka via REST endpoints.
"""
import os
import json
import logging
from datetime import datetime, timedelta

from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.errors import KafkaError

from kafka_utils import publish_to_kafka, get_topic_metadata
from lake_utils import query_curated_data, query_consumption_data, get_latest_reading

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9091,localhost:9092,localhost:9093"
).split(",")
DATA_LAKE_PATH = os.environ.get("DATA_LAKE_PATH", "/tmp/datalake")
TOPIC_NAME = "sensor-events"
VALID_SENSORS = ["temperature", "humidity", "pressure"]

# Flask app
app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Lazy-initialized Kafka producer
_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks="all",
            retries=3,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
    return _producer


# --------------- error handlers ---------------

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "not_found",
        "message": "The requested resource was not found",
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "error": "method_not_allowed",
        "message": "The HTTP method is not allowed for this endpoint",
    }), 405


@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f"Internal server error: {error}")
    return jsonify({
        "error": "internal_server_error",
        "message": "An internal server error occurred",
    }), 500


# --------------- helpers ---------------

def validate_sensor_type(sensor_type):
    if sensor_type not in VALID_SENSORS:
        return False, f"Invalid sensor type. Must be one of: {VALID_SENSORS}"
    return True, None


def validate_days(days):
    try:
        days_int = int(days)
        if 1 <= days_int <= 90:
            return True, days_int, None
        return False, None, "days must be between 1 and 90"
    except (ValueError, TypeError):
        return False, None, "days must be an integer"


# --------------- endpoints ---------------

@app.route("/api/v1/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "kafka": "available",
            "data_lake": os.path.exists(DATA_LAKE_PATH),
        },
    }), 200


@app.route("/api/v1/sensors", methods=["GET"])
def list_sensors():
    return jsonify({
        "sensors": VALID_SENSORS,
        "count": len(VALID_SENSORS),
    }), 200


@app.route("/api/v1/sensors/<sensor_type>/latest", methods=["GET"])
def latest_reading(sensor_type):
    is_valid, error = validate_sensor_type(sensor_type)
    if not is_valid:
        return jsonify({"error": "invalid_sensor", "message": error}), 404

    reading = get_latest_reading(DATA_LAKE_PATH, sensor_type)

    if reading is None:
        return jsonify({
            "error": "not_found",
            "message": f"No readings found for sensor {sensor_type}",
        }), 404

    return jsonify({"sensor": sensor_type, "reading": reading}), 200


@app.route("/api/v1/sensors/<sensor_type>/stats", methods=["GET"])
def sensor_stats(sensor_type):
    is_valid, error = validate_sensor_type(sensor_type)
    if not is_valid:
        return jsonify({"error": "invalid_sensor", "message": error}), 404

    days_param = request.args.get("days", "7")
    is_valid_days, days, error = validate_days(days_param)
    if not is_valid_days:
        return jsonify({"error": "invalid_parameter", "message": error}), 400

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    stats = query_consumption_data(DATA_LAKE_PATH, sensor_type, start_date, end_date)

    return jsonify({
        "sensor": sensor_type,
        "days": days,
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
        "statistics": stats if stats else [],
    }), 200


@app.route("/api/v1/anomalies", methods=["GET"])
def get_anomalies():
    sensor_type = request.args.get("sensor")
    if sensor_type:
        is_valid, error = validate_sensor_type(sensor_type)
        if not is_valid:
            return jsonify({"error": "invalid_parameter", "message": error}), 400

    limit = request.args.get("limit", "50")
    try:
        limit_int = int(limit)
        if not (1 <= limit_int <= 1000):
            raise ValueError()
    except (ValueError, TypeError):
        return jsonify({
            "error": "invalid_parameter",
            "message": "limit must be an integer between 1 and 1000",
        }), 400

    anomalies = query_curated_data(
        DATA_LAKE_PATH,
        sensor_type,
        anomaly_only=True,
        limit=limit_int,
        order_by="event_time",
        order_desc=True,
    )

    return jsonify({
        "anomalies": anomalies if anomalies else [],
        "count": len(anomalies) if anomalies else 0,
        "filters": {
            "sensor": sensor_type if sensor_type else "all",
            "limit": limit_int,
        },
    }), 200


@app.route("/api/v1/readings", methods=["POST"])
def publish_reading():
    data = request.get_json()

    required_fields = ["sensor", "value", "unit", "source"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({
            "error": "validation_error",
            "message": f"Missing required fields: {missing}",
        }), 422

    is_valid, error = validate_sensor_type(data["sensor"])
    if not is_valid:
        return jsonify({"error": "validation_error", "message": error}), 422

    value = data["value"]
    sensor = data["sensor"]

    # Range validation
    range_checks = {
        "temperature": (15, 45),
        "humidity": (30, 95),
        "pressure": (980, 1040),
    }
    lo, hi = range_checks[sensor]
    if not (lo <= value <= hi):
        return jsonify({
            "error": "validation_error",
            "message": f"{sensor.capitalize()} must be between {lo} and {hi}",
        }), 422

    if "timestamp" not in data:
        data["timestamp"] = datetime.now().isoformat()

    # Compute anomaly server-side
    anomaly_rules = {
        "temperature": value > 35,
        "humidity": value > 90,
        "pressure": value < 990 or value > 1030,
    }
    data["anomaly"] = anomaly_rules[sensor]

    try:
        kafka_producer = get_producer()
        future = kafka_producer.send(TOPIC_NAME, key=sensor, value=data)
        metadata = future.get(timeout=10)

        app.logger.info(
            f"Published to Kafka: partition={metadata.partition}, offset={metadata.offset}"
        )

        return jsonify({
            "message": "Reading published successfully",
            "reading": data,
            "kafka": {
                "topic": TOPIC_NAME,
                "partition": metadata.partition,
                "offset": metadata.offset,
            },
        }), 201

    except KafkaError as e:
        app.logger.error(f"Kafka publish failed: {e}")
        return jsonify({
            "error": "kafka_error",
            "message": "Failed to publish reading to Kafka",
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
