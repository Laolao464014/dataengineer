#!/bin/bash
set -e


sleep 20


kafka-topics --bootstrap-server kafka1:29091 --create --topic sensor-events --partitions 3 --replication-factor 3 2>/dev/null || true

echo "Kafka topic 'sensor-events' is ready."