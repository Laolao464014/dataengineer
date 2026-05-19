#!/usr/bin/env python3
"""
REST API for IoT Sensor Data Platform
Exposes Kafka and data lake data via REST endpoints
"""

import os


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092,localhost:9093').split(',')
DATA_LAKE_PATH = os.environ.get('DATA_LAKE_PATH', '/datalake')
TOPIC_NAME = 'sensor-events'
VALID_SENSORS = ['temperature', 'humidity', 'pressure']
import json
import logging
import os
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import utilities
from kafka_utils import publish_to_kafka, get_topic_metadata
from lake_utils import query_curated_data, query_consumption_data

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9091', 'localhost:9092', 'localhost:9093']
TOPIC_NAME = 'sensor-events'
DATA_LAKE_PATH = '/tmp/datalake'

# Valid sensor types
VALID_SENSORS = ['temperature', 'humidity', 'pressure']

# Create Flask app
app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# Kafka producer (lazy initialization)
producer = None


def get_producer():
    """Lazy initialization of Kafka producer"""
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks='all',
            retries=3,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
    return producer


# ============ Error Handlers ============

@app.errorhandler(404)
def not_found(error):
    """Handle 404 Not Found"""
    return jsonify({
        'error': 'not_found',
        'message': 'The requested resource was not found',
        'status_code': 404
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 Method Not Allowed"""
    return jsonify({
        'error': 'method_not_allowed',
        'message': 'The HTTP method is not allowed for this endpoint',
        'status_code': 405
    }), 405


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 Internal Server Error"""
    app.logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'internal_server_error',
        'message': 'An internal server error occurred',
        'status_code': 500
    }), 500


# ============ Helper Functions ============

def validate_sensor_type(sensor_type):
    """Validate sensor type"""
    if sensor_type not in VALID_SENSORS:
        return False, f"Invalid sensor type. Must be one of: {VALID_SENSORS}"
    return True, None


def validate_days(days):
    """Validate days parameter (1-90)"""
    try:
        days_int = int(days)
        if 1 <= days_int <= 90:
            return True, days_int, None
        return False, None, "days must be between 1 and 90"
    except ValueError:
        return False, None, "days must be an integer"


# ============ API Endpoints ============

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'services': {
            'kafka': 'available',
            'data_lake': os.path.exists(DATA_LAKE_PATH)
        }
    }), 200


@app.route('/api/v1/sensors', methods=['GET'])
def list_sensors():
    """List all sensor types"""
    return jsonify({
        'sensors': VALID_SENSORS,
        'count': len(VALID_SENSORS)
    }), 200


@app.route('/api/v1/sensors/<sensor_type>/latest', methods=['GET'])
def get_latest_reading(sensor_type):
    """Get the latest reading for a specific sensor type"""
    # Validate sensor type
    is_valid, error = validate_sensor_type(sensor_type)
    if not is_valid:
        return jsonify({'error': 'invalid_sensor', 'message': error}), 404

    # Query the curated zone for latest reading
    latest = query_curated_data(
        DATA_LAKE_PATH,
        sensor_type,
        limit=1,
        order_by='event_time',
        order_desc=True
    )

    if latest is None or len(latest) == 0:
        return jsonify({'error': 'not_found', 'message': f'No readings found for sensor {sensor_type}'}), 404

    return jsonify({
        'sensor': sensor_type,
        'reading': latest[0]
    }), 200


@app.route('/api/v1/sensors/<sensor_type>/stats', methods=['GET'])
def get_sensor_stats(sensor_type):
    """Get daily statistics for a sensor type"""
    # Validate sensor type
    is_valid, error = validate_sensor_type(sensor_type)
    if not is_valid:
        return jsonify({'error': 'invalid_sensor', 'message': error}), 404

    # Validate days parameter
    days_param = request.args.get('days', '7')
    is_valid_days, days, error = validate_days(days_param)
    if not is_valid_days:
        return jsonify({'error': 'invalid_parameter', 'message': error}), 400

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Query consumption zone for aggregates
    stats = query_consumption_data(
        DATA_LAKE_PATH,
        sensor_type,
        start_date,
        end_date
    )

    if stats is None or len(stats) == 0:
        return jsonify({
            'sensor': sensor_type,
            'days': days,
            'message': 'No data available for the specified period',
            'statistics': []
        }), 200

    return jsonify({
        'sensor': sensor_type,
        'days': days,
        'period': {
            'start': start_date.isoformat(),
            'end': end_date.isoformat()
        },
        'statistics': stats
    }), 200


@app.route('/api/v1/anomalies', methods=['GET'])
def get_anomalies():
    """Get list of recent anomalies"""
    # Validate sensor parameter (optional)
    sensor_type = request.args.get('sensor')
    if sensor_type:
        is_valid, error = validate_sensor_type(sensor_type)
        if not is_valid:
            return jsonify({'error': 'invalid_parameter', 'message': error}), 400

    # Validate limit parameter
    limit = request.args.get('limit', '50')
    try:
        limit_int = int(limit)
        if not (1 <= limit_int <= 1000):
            raise ValueError()
    except ValueError:
        return jsonify({'error': 'invalid_parameter', 'message': 'limit must be an integer between 1 and 1000'}), 400

    # Query anomalies from curated zone
    anomalies = query_curated_data(
        DATA_LAKE_PATH,
        sensor_type,
        anomaly_only=True,
        limit=limit_int,
        order_by='event_time',
        order_desc=True
    )

    return jsonify({
        'anomalies': anomalies or [],
        'count': len(anomalies) if anomalies else 0,
        'filters': {
            'sensor': sensor_type if sensor_type else 'all',
            'limit': limit_int
        }
    }), 200


@app.route('/api/v1/readings', methods=['POST'])
def publish_reading():
    """Publish a reading to Kafka"""
    data = request.get_json()

    # Validate required fields
    required_fields = ['sensor', 'value', 'unit', 'source']
    missing_fields = [f for f in required_fields if f not in data]
    if missing_fields:
        return jsonify({
            'error': 'validation_error',
            'message': f'Missing required fields: {missing_fields}',
            'status_code': 422
        }), 422

    # Validate sensor type
    is_valid, error = validate_sensor_type(data['sensor'])
    if not is_valid:
        return jsonify({'error': 'validation_error', 'message': error}), 422

    # Validate value range
    value = data['value']
    if data['sensor'] == 'temperature' and not (15 <= value <= 45):
        return jsonify({'error': 'validation_error', 'message': 'Temperature must be between 15 and 45'}), 422
    elif data['sensor'] == 'humidity' and not (30 <= value <= 95):
        return jsonify({'error': 'validation_error', 'message': 'Humidity must be between 30 and 95'}), 422
    elif data['sensor'] == 'pressure' and not (980 <= value <= 1040):
        return jsonify({'error': 'validation_error', 'message': 'Pressure must be between 980 and 1040'}), 422

    # Add timestamp if not provided
    if 'timestamp' not in data:
        data['timestamp'] = datetime.now().isoformat()

    # Add anomaly flag (computed server-side)
    is_anomaly = False
    if data['sensor'] == 'temperature' and value > 35:
        is_anomaly = True
    elif data['sensor'] == 'humidity' and value > 90:
        is_anomaly = True
    elif data['sensor'] == 'pressure' and (value < 990 or value > 1030):
        is_anomaly = True
    data['anomaly'] = is_anomaly

    # Publish to Kafka
    try:
        kafka_producer = get_producer()
        future = kafka_producer.send(TOPIC_NAME, key=data['sensor'], value=data)
        record_metadata = future.get(timeout=10)

        app.logger.info(
            f"Published reading to Kafka: partition={record_metadata.partition}, offset={record_metadata.offset}")

        return jsonify({
            'message': 'Reading published successfully',
            'reading': data,
            'kafka': {
                'topic': TOPIC_NAME,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
        }), 201

    except KafkaError as e:
        app.logger.error(f"Failed to publish to Kafka: {e}")
        return jsonify({
            'error': 'kafka_error',
            'message': 'Failed to publish reading to Kafka',
            'status_code': 500
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)