# IoT Sensor Data Engineering Platform

## Overview

This project implements an end-to-end data engineering platform for IoT sensor data, covering the complete data lifecycle:

**Generation → Ingestion (Kafka) → Processing (Spark) → Storage (Data Lake) → Exposure (REST API) → Consumption**

### Technologies Used

- **Kafka 7.5** (3-broker cluster) — Message bus for real-time ingestion
- **Spark 3.5.3** (Structured Streaming) — Stream processing and analytics
- **Flask 3.0** — REST API
- **Docker / Docker Compose** — Container orchestration
- **Parquet + Snappy** — Columnar storage format

### Sensor Types

| Sensor | Unit | Normal Range | Anomaly Threshold |
|--------|------|--------------|-------------------|
| Temperature | °C | 15 - 45 | > 35°C |
| Humidity | % | 30 - 95 | > 90% |
| Pressure | hPa | 980 - 1040 | < 990 or > 1030 |

## Architecture

```
┌─────────────┐     ┌───────────────┐     ┌─────────────────────────────────┐
│  Producer   │────▶│ Kafka Cluster │────▶│ Spark Structured Streaming      │
│  (Python)   │     │  (3 brokers)  │     │ - Parse JSON (explicit schema)  │
└─────────────┘     └───────────────┘     │ - Validate physical ranges      │
                                          │ - Detect anomalies              │
                                          │ - 5-min windowed aggregation    │
                                          │ - Watermark (2 min)             │
                                          │ - Triple write to data lake     │
                                          └───────────────┬─────────────────┘
                                                          │
                                                          ▼
                                          ┌─────────────────────────────────┐
                                          │     Data Lake (3 Zones)         │
                                          │  /raw/         — JSON (as-is)   │
                                          │  /curated/     — Parquet+Snappy │
                                          │  /consumption/ — Aggregates     │
                                          └─────────────┬───────────────────┘
                                                        │
                                                        ▼
                                          ┌─────────────┐     ┌────────────┐
                                          │   Client    │◀────│  REST API  │
                                          │             │     │  (Flask)   │
                                          └─────────────┘     └────────────┘
```

### Data Lake Structure

```
/tmp/datalake/
├── raw/source=kafka/topic=sensor-events/
│   └── year=YYYY/month=MM/day=DD/hour=HH/       (JSON)
├── curated/domain=iot/
│   └── sensor=X/year=YYYY/month=MM/day=DD/      (Parquet, Snappy)
└── consumption/use_case=sensor_averages/
    └── sensor=X/year=YYYY/month=MM/             (Parquet, Snappy)
```

## Prerequisites

- Docker & Docker Compose (20.10+)
- Python 3.9+
- Java 11 (for Spark)
- 8GB+ RAM recommended

## Installation & Execution

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka Cluster

```bash
docker compose up -d
```

### 3. Create Kafka Topic

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --create --topic sensor-events --partitions 3 --replication-factor 3
```

Verify:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --describe --topic sensor-events
```

### 4. Run the Producer

```bash
# Local execution (use PLAINTEXT_HOST ports)
KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:29094,localhost:29096 \
  python src/producer.py --count 500 --rate 10 --source site-A-rack-01

# Or via Docker
docker run --rm --network <kafka-network> \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9094,kafka3:9096 \
  iot-producer --count 500 --rate 10 --source site-A-rack-01
```

### 5. Run the Spark Pipeline

```bash
# Local execution
DATA_LAKE_PATH=/tmp/datalake CHECKPOINT_PATH=/tmp/checkpoints \
KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:29094,localhost:29096 \
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/spark_pipeline.py

# Or via Docker
docker run -d --name spark-pipeline --network <kafka-network> \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9094,kafka3:9096 \
  -e DATA_LAKE_PATH=/datalake -e CHECKPOINT_PATH=/checkpoints \
  -v iot_datalake:/datalake -v iot_checkpoints:/checkpoints \
  iot-spark
```

### 6. Run Analytics Queries

```bash
# After data has accumulated (~5 minutes)
python src/analytics.py
```

Results are printed to stdout and written to `outputs/analytics/`.

### 7. Start the REST API

```bash
# Local execution
DATA_LAKE_PATH=/tmp/datalake \
KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:29094,localhost:29096 \
  python api/app.py

# Or via Docker
docker run -d --name rest-api --network <kafka-network> -p 5000:5000 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9094,kafka3:9096 \
  -e DATA_LAKE_PATH=/datalake -v iot_datalake:/datalake \
  iot-api
```

### 8. Test the API

```bash
bash tests/test_curl_commands.sh
```

## API Endpoints

| Method | Endpoint | Description | Status Codes |
|--------|----------|-------------|--------------|
| GET | `/api/v1/health` | Health check | 200 |
| GET | `/api/v1/sensors` | List sensor types | 200 |
| GET | `/api/v1/sensors/<type>/latest` | Latest reading | 200 / 404 |
| GET | `/api/v1/sensors/<type>/stats?days=N` | Daily stats (1-90 days) | 200 / 400 / 404 |
| GET | `/api/v1/anomalies?sensor=<type>&limit=N` | Recent anomalies | 200 / 400 |
| POST | `/api/v1/readings` | Publish reading to Kafka | 201 / 400 / 422 |

## Technical Choices

### 1. Partitioning Strategy for the Curated Zone

The curated zone is partitioned by `sensor/year/month/day` based on `event_time`. This Hive-style layout aligns with the dominant query patterns: filtering by sensor type (the API's primary access path) and by date range (for statistics over N days). Spark's partition pruning skips irrelevant directories at the filesystem level, dramatically reducing I/O for time-bounded queries.

**Alternatives considered:** Partitioning by `source` would have been useful if cross-site queries were dominant, but the API is sensor-centric. Flat (unpartitioned) Parquet was rejected because it forces full-table scans for every query.

### 2. Spark Structured Streaming Output Mode

All three sinks use `outputMode("append")`. The raw and curated zones only ever add new rows — they never update previously written records — so append is the natural and most efficient choice. For the consumption zone, the 5-minute tumbling window with a 2-minute watermark ensures that once a window result is emitted (after the watermark passes the window end), it is final and never updated again. Append mode is therefore correct and avoids the overhead of update mode's state management.

**Alternatives considered:** `update` mode would be needed if we wanted to continuously revise window results as late data arrived, but our watermark design makes that unnecessary. `complete` mode was rejected because it requires retaining and re-emitting the entire result table on every trigger, which is untenable for an unbounded stream.

### 3. Replication Factor and min.insync.replicas

The Kafka topic uses `replication factor = 3` and `min.insync.replicas = 2`. With three brokers, each partition is replicated on all three nodes. The producer's `acks='all'` setting only acknowledges a write after at least two replicas confirm it (the ISR minimum). This tolerates a single broker failure — writes continue to succeed because two replicas remain, and no committed data is lost because at least one surviving node holds a complete copy.

**Alternatives considered:** RF=1 would lose data on any broker failure. RF=5 would increase durability but also increase inter-broker replication traffic and disk usage with no benefit for a 3-node cluster. min.insync.replicas=1 with RF=3 would allow writes during failures but risk losing acknowledged data if the leader failed before replication completed.

### 4. event_time vs ingestion_time Across Zones

The **curated zone** is partitioned by `event_time` (the timestamp embedded in the sensor reading), preserving the data's semantic time. This means queries about "temperature yesterday" return correct results regardless of when the data arrived. The **raw zone** is partitioned by `ingest_time` (when the system received the message), capturing the operational timeline. This dual approach supports both business queries and operational debugging ("what arrived during the network outage").

**Alternatives considered:** Using `ingest_time` everywhere would simplify the pipeline but would make historical queries unreliable if data arrived late. Using `event_time` for the raw zone would risk data loss if the timestamp was malformed, since the raw zone's purpose is to preserve data before any validation.

### 5. End-to-End Delivery Semantics

The platform achieves **at-least-once** semantics. The producer uses `acks='all'` and `retries=5`, but without idempotence (`enable.idempotence` is not enabled), a retried message that was already written (but whose acknowledgement was lost) produces a duplicate. On the consumer side, Spark Structured Streaming with checkpointing resumes from the last committed Kafka offset after a failure, but a partially written batch to the data lake may be re-processed, creating duplicate files.

**Alternatives considered:** Exactly-once would require Kafka idempotent producers and transactional writes, plus Spark's `availableNow` trigger with idempotent sinks. This adds latency and complexity. At-most-once (disabling retries) would lose data on transient failures, which is unacceptable for a sensor monitoring platform.

## Results

### Verified Data Lake Layout

After running the producer (200 events) and Spark pipeline:

```
/datalake/
├── raw/source=kafka/topic=sensor-events/
│   └── year=2026/month=5/day=19/hour=9/
│       └── *.json                       (raw JSON files ~8-17 KB each)
├── curated/domain=iot/
│   ├── sensor=temperature/year=2026/month=5/day=19/
│   │   └── *.snappy.parquet             (curated Parquet files ~2.6 KB)
│   ├── sensor=humidity/year=2026/month=5/day=19/
│   │   └── *.snappy.parquet
│   └── sensor=pressure/year=2026/month=5/day=19/
│       └── *.snappy.parquet
└── consumption/use_case=sensor_averages/
    └── (populated after 5-min watermark closes)
```

### Sample API Responses

**Health Check** — `GET /api/v1/health`
```json
{
    "status": "healthy",
    "timestamp": "2026-05-19T09:16:51.579272",
    "services": {
        "kafka": "available",
        "data_lake": true
    }
}
```

**Latest Reading** — `GET /api/v1/sensors/temperature/latest`
```json
{
    "sensor": "temperature",
    "reading": {
        "value": 17.57,
        "unit": "C",
        "event_time": "2026-05-19T09:11:36",
        "source": "docker-stream-test",
        "is_anomaly": false
    }
}
```

**Anomalies** — `GET /api/v1/anomalies?limit=3`
```json
{
    "anomalies": [
        {"sensor": "pressure", "value": 981.93, "unit": "hPa", "is_anomaly": true},
        {"sensor": "humidity",  "value": 94.95, "unit": "%",   "is_anomaly": true},
        {"sensor": "temperature","value": 40.48,"unit": "C",   "is_anomaly": true}
    ],
    "count": 3,
    "filters": {"sensor": "all", "limit": 3}
}
```

**Publish Reading** — `POST /api/v1/readings`
```json
{
    "message": "Reading published successfully",
    "reading": {"sensor": "temperature", "value": 25.5, "unit": "C", "anomaly": false},
    "kafka": {"topic": "sensor-events", "partition": 2, "offset": 1006}
}
```

### Error Handling Examples

```json
// 404 — invalid sensor type
{"error": "invalid_sensor", "message": "Invalid sensor type. Must be one of: [...]"}

// 400 — invalid parameter
{"error": "invalid_parameter", "message": "days must be between 1 and 90"}

// 422 — missing required fields
{"error": "validation_error", "message": "Missing required fields: ['unit', 'source']"}
```

## Fault Tolerance

The Kafka cluster is configured with:

- **Replication Factor**: 3 (each partition on all three brokers)
- **min.insync.replicas**: 2 (writes acknowledged by at least 2 replicas)

To test fault tolerance:

```bash
# Check leader distribution
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --describe --topic sensor-events

# Stop a broker
docker stop kafka2

# Observe leader re-election (leader for affected partitions moves)
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29091 \
  --describe --topic sensor-events

# Restart the broker
docker start kafka2
```

Detailed before/after traces are in [docs/fault_tolerance.md](docs/fault_tolerance.md).

## Monitoring

- Kafka UI: http://localhost:8080
- API: http://localhost:5000

## Documentation

Detailed technical documentation is in the `docs/` directory:

- [Architecture & Technical Choices](docs/architecture.md) — Extended technical justifications, design rationale
- [Fault Tolerance](docs/fault_tolerance.md) — Leader election test with before/after evidence
- [Analytics Queries](docs/analytics.md) — Query descriptions, expected outputs, partition pruning demo
- [Reflection Questions](docs/reflection.md) — Crash recovery, scaling analysis, Kafka vs Data Lake, aberrant data handling, adding new sensors

## Limitations and Improvements

### Current Limitations

- Single-node Spark (cannot scale horizontally beyond one machine)
- Local file system for data lake (no distributed storage like HDFS/S3)
- No authentication/authorization on the REST API
- At-least-once semantics (duplicates possible on producer retries)
- No schema registry for message evolution

### Future Improvements

- **Scalability**: Deploy Spark on YARN or Kubernetes for horizontal scaling
- **Storage**: Migrate data lake to S3/MinIO for distributed, durable storage
- **Exactly-once**: Enable idempotent producers and Kafka transactions
- **Monitoring**: Add Prometheus + Grafana dashboards for pipeline metrics
- **Schema Registry**: Use Confluent Schema Registry for schema evolution and compatibility checks
- **CI/CD**: Add GitHub Actions for automated testing and validation

## Troubleshooting

### Kafka connection issues

```bash
docker ps | grep kafka
docker logs kafka1
```

### MSYS2 / Git Bash path conversion

When running Docker commands in Git Bash on Windows, disable path conversion:

```bash
MSYS_NO_PATHCONV=1 docker run ...
# Or
export MSYS_NO_PATHCONV=1
```

### Spark cannot find Kafka package

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/spark_pipeline.py
```

### Data lake directory permissions

```bash
mkdir -p /tmp/datalake /tmp/checkpoints
chmod 777 /tmp/datalake /tmp/checkpoints
```
