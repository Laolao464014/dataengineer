# IoT Sensor Data Engineering Platform

## Overview

This project implements an end-to-end data engineering platform for IoT sensor data, covering the complete data lifecycle:

**Generation → Ingestion (Kafka) → Processing (Spark) → Storage (Data Lake) → Exposure (REST API) → Consumption**

### Technologies Used

- **Kafka 7.5** (3-broker cluster) - Message bus for real-time ingestion
- **Spark 3.5.3** (Structured Streaming) - Stream processing and analytics
- **Flask 3.0** - REST API
- **Docker / Docker Compose** - Container orchestration
- **Parquet + Snappy** - Columnar storage format

### Sensor Types

| Sensor | Unit | Normal Range | Anomaly Threshold |
|--------|------|--------------|-------------------|
| Temperature | °C | 15 - 45 | > 35°C |
| Humidity | % | 30 - 95 | > 90% |
| Pressure | hPa | 980 - 1040 | < 990 or > 1030 |

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────────┐
│  Producer   │────▶│    Kafka    │────▶│ Spark Structured Streaming      │
│  (Python)   │     │  3-broker   │     │ - Parse JSON                    │
└─────────────┘     └─────────────┘     │ - Validate values               │
                                        │ - Detect anomalies            │
                                        │ - 5-min window aggregates       │
                                        │ - Watermark (2 min)             │
                                        └───────────────┬───────────────┘
                                                        │
                                                        ▼
                                        ┌──────────────────────────────────────────┐
                                        │       Data Lake (3 Zones)                │
                                        ├──────────────────────────────────────────┤
                                        │ /raw/ - Raw JSON                         │
                                        │ /curated/ - Cleaned Parquet              │
                                        │ /consumption/ - Aggregated Parquet       │
                                        └─────────────────────┬────────────────────┘
                                                              │
                                                              ▼
                                        ┌─────────────┐     ┌─────────────┐
                                        │   Client    │◀────│  REST API   │
                                        │             │     │  (Flask)    │
                                        └─────────────┘     └─────────────┘
```

## Prerequisites

- Docker & Docker Compose (20.10+)
- Python 3.9+
- Java 11 (for Spark)
- 8GB+ RAM recommended

## Installation & Execution

### 1. Clone and Setup

```bash
unzip LASTNAME_FIRSTNAME_exam.zip
cd LASTNAME_FIRSTNAME_exam
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Kafka Cluster

```bash
docker compose up -d
```

### 4. Create Kafka Topic

```bash
# Create sensor-events topic with 3 partitions, RF=3
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091   --create --topic sensor-events   --partitions 3 --replication-factor 3
```

Verify topic creation:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091   --describe --topic sensor-events
```

### 5. Run the Producer

```bash
# Generate 500 events at 10 msg/sec
python src/producer.py --count 500 --rate 10 --source site-A-rack-01

# Generate 1000 events with anomalies
python src/producer.py --count 1000 --rate 20 --source factory-02
```

### 6. Run the Spark Pipeline

```bash
# In a separate terminal
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3   src/spark_pipeline.py
```

### 7. Run Analytics Queries

```bash
# After data has accumulated (wait ~2 minutes)
python src/analytics.py
```

### 8. Start the REST API

```bash
# In a separate terminal
python api/app.py
```

### 9. Test the API

```bash
# Run the test script
bash tests/test_curl_commands.sh

# Or manually test health endpoint
curl http://localhost:5000/api/v1/health | python3 -m json.tool
```

## Data Lake Structure

```
/tmp/datalake/
├── raw/source=kafka/topic=sensor-events/
│   └── year=YYYY/month=MM/day=DD/hour=HH/  (JSON)
├── curated/domain=iot/
│   └── sensor_type=X/year=YYYY/month=MM/day=DD/  (Parquet)
└── consumption/use_case=sensor_averages/
    └── sensor_type=X/year=YYYY/month=MM/  (Parquet)
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/v1/health | Health check |
| GET | /api/v1/sensors | List sensor types |
| GET | /api/v1/sensors/<type>/latest | Latest reading |
| GET | /api/v1/sensors/<type>/stats?days=N | Daily stats (1-90 days) |
| GET | /api/v1/anomalies?sensor=<type>&limit=N | Recent anomalies |
| POST | /api/v1/readings | Publish reading to Kafka |

## Fault Tolerance Demonstration

The Kafka cluster is configured with:

- Replication Factor: 3
- min.insync.replicas: 2

To test fault tolerance:

```bash
# Check leader distribution
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091   --describe --topic sensor-events

# Stop a broker
docker stop kafka2

# Observe leader re-election
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091   --describe --topic sensor-events

# Restart the broker
docker start kafka2
```

## Monitoring

- Kafka UI: http://localhost:8080
- API: http://localhost:5000

## Sample API Responses

### Health Check

```json
{
  "status": "healthy",
  "timestamp": "2024-05-19T10:00:00",
  "services": {
    "kafka": "available",
    "data_lake": true
  }
}
```

### Sensor Statistics

```json
{
  "sensor": "temperature",
  "days": 7,
  "statistics": [
    {
      "window_start": "2024-05-19T00:00:00",
      "avg_value": 28.5,
      "min_value": 18.2,
      "max_value": 38.9,
      "observation_count": 288,
      "anomaly_count": 12
    }
  ]
}
```

## Limitations and Improvements

### Current Limitations

- Single-node Spark (can't scale horizontally)
- Local file system for data lake (no distributed storage)
- No authentication/authorization on API
- Exactly-once semantics not fully guaranteed

### Future Improvements

- Scalability: Deploy Spark on YARN/Kubernetes
- Storage: Migrate to S3/HDFS for the data lake
- Monitoring: Add Prometheus + Grafana for metrics
- Exactly-once: Implement idempotent producers and transactions
- Schema Registry: Use Confluent Schema Registry for evolution
- CI/CD: Add GitHub Actions for automated testing

## Troubleshooting

### Kafka connection issues

```bash
# Verify Kafka is running
docker ps | grep kafka

# Check broker logs
docker logs kafka1
```

### Spark cannot find Kafka package

```bash
# Download the package explicitly
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3   --repositories https://repos.spark-packages.org/   src/spark_pipeline.py
```

### Data lake directory permissions

```bash
# Create directory with proper permissions
sudo mkdir -p /tmp/datalake
sudo chmod 777 /tmp/datalake
```
