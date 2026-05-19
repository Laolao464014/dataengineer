#!/bin/bash

# Wait for Kafka to be ready
sleep 10

# Create topic
kafka-topics --bootstrap-server kafka1:29091 --create --topic sensor-events --partitions 3 --replication-factor 3 || true

echo "Kafka topic 'sensor-events' created"