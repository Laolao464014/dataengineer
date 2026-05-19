#!/usr/bin/env python3
"""
IoT Sensor Data Producer for Kafka
Generates temperature, humidity, and pressure readings
"""
import os
import argparse
import json
import random
import time
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29091,kafka2:29092,kafka3:29093').split(',')
TOPIC_NAME = 'sensor-events'

# Sensor configurations
SENSOR_TYPES = {
    'temperature': {'unit': 'C', 'min': 15.0, 'max': 45.0, 'anomaly_threshold': 35.0},
    'humidity': {'unit': '%', 'min': 30.0, 'max': 95.0, 'anomaly_threshold': 90.0},
    'pressure': {'unit': 'hPa', 'min': 980.0, 'max': 1040.0, 'anomaly_threshold_low': 990.0,
                 'anomaly_threshold_high': 1030.0}
}

# Anomaly rate (10% as required)
ANOMALY_RATE = 0.10


class SensorProducer:
    def __init__(self, source, rate):
        self.source = source
        self.rate = rate
        self.running = True
        self.producer = None
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nShutting down producer...")
        self.running = False

    def connect(self):
        """Initialize Kafka producer with reliability settings"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                acks='all',  # Wait for all in-sync replicas
                retries=5,
                max_in_flight_requests_per_connection=1,  # Preserve ordering
                linger_ms=5,  # Small delay for batching
                batch_size=16384,  # 16KB batch
                compression_type='gzip',
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except KafkaError as e:
            print(f"Failed to connect to Kafka: {e}")
            return False

    def generate_value(self, sensor_type, force_anomaly=False):
        """Generate a sensor value, optionally forcing an anomaly"""
        config = SENSOR_TYPES[sensor_type]

        if force_anomaly:
            # Generate anomalous value
            if sensor_type == 'temperature':
                return random.uniform(config['anomaly_threshold'] + 0.1, config['max'] + 2)
            elif sensor_type == 'humidity':
                return random.uniform(config['anomaly_threshold'] + 0.1, config['max'] + 2)
            elif sensor_type == 'pressure':
                # Either too low or too high
                if random.choice([True, False]):
                    return random.uniform(config['min'] - 10, config['anomaly_threshold_low'] - 0.1)
                else:
                    return random.uniform(config['anomaly_threshold_high'] + 0.1, config['max'] + 10)
        else:
            # Generate normal value within range
            return random.uniform(config['min'], config['max'])

    def is_anomaly(self, sensor_type, value):
        """Determine if a value is anomalous"""
        config = SENSOR_TYPES[sensor_type]
        if sensor_type == 'temperature':
            return value > config['anomaly_threshold']
        elif sensor_type == 'humidity':
            return value > config['anomaly_threshold']
        elif sensor_type == 'pressure':
            return value < config['anomaly_threshold_low'] or value > config['anomaly_threshold_high']
        return False

    def create_message(self, sensor_type):
        """Create a single sensor reading message"""
        # Determine if this should be an anomaly
        should_be_anomaly = random.random() < ANOMALY_RATE

        value = self.generate_value(sensor_type, force_anomaly=should_be_anomaly)
        is_anomaly = self.is_anomaly(sensor_type, value)

        message = {
            'sensor': sensor_type,
            'value': round(value, 2),
            'unit': SENSOR_TYPES[sensor_type]['unit'],
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': self.source,
            'anomaly': is_anomaly  # Producer's anomaly flag
        }
        return message

    def send_message(self, sensor_type, message):
        """Send a message to Kafka with the sensor type as key"""
        try:
            future = self.producer.send(TOPIC_NAME, key=sensor_type, value=message)
            # Wait for acknowledgement
            record_metadata = future.get(timeout=5)
            return True, record_metadata.partition
        except KafkaError as e:
            print(f"Failed to send message: {e}")
            return False, None

    def run(self, count):
        """Main producer loop"""
        if not self.connect():
            return

        print(f"Starting producer for source '{self.source}' at {self.rate} msgs/sec")
        print(f"Generating {count} messages...")

        messages_sent = 0
        anomalies_sent = 0

        start_time = time.time()
        delay = 1.0 / self.rate if self.rate > 0 else 0

        while self.running and messages_sent < count:
            # Cycle through sensor types
            for sensor_type in SENSOR_TYPES.keys():
                if messages_sent >= count:
                    break

                message = self.create_message(sensor_type)
                success, partition = self.send_message(sensor_type, message)

                if success:
                    messages_sent += 1
                    if message['anomaly']:
                        anomalies_sent += 1
                    print(f"[{messages_sent}/{count}] Sent {message['sensor']}={message['value']}{message['unit']} "
                          f"(anomaly={message['anomaly']}) to partition {partition}")
                else:
                    print(f"Failed to send message {messages_sent + 1}")

                # Respect the rate limit
                if delay > 0 and messages_sent < count:
                    time.sleep(delay)

        # Flush any pending messages
        self.producer.flush()

        elapsed = time.time() - start_time
        print(f"\n=== Summary ===")
        print(f"Messages sent: {messages_sent}")
        print(f"Anomalies sent: {anomalies_sent} ({anomalies_sent / messages_sent * 100:.1f}%)")
        print(f"Elapsed time: {elapsed:.2f} seconds")
        print(f"Actual rate: {messages_sent / elapsed:.2f} msg/sec")

        self.producer.close()


def main():
    parser = argparse.ArgumentParser(description='IoT Sensor Data Producer')
    parser.add_argument('--count', '-c', type=int, default=100, help='Number of events to produce')
    parser.add_argument('--rate', '-r', type=int, default=10, help='Events per second')
    parser.add_argument('--source', '-s', type=str, default='site-A-rack-01', help='Site identifier')

    args = parser.parse_args()

    producer = SensorProducer(source=args.source, rate=args.rate)
    producer.run(args.count)


if __name__ == '__main__':
    main()