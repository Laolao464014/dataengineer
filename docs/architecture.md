# Architecture

## Pipeline Overview

The platform implements an end-to-end IoT data engineering pipeline:

```
┌─────────────┐     ┌───────────────┐     ┌─────────────────────────────────┐
│  Producer   │────▶│ Kafka Cluster │────▶│ Spark Structured Streaming      │
│  (Python)   │     │  (3 brokers)  │     │                                 │
└─────────────┘     └───────────────┘     │ Raw zone: JSON (ingestion time) │
                                          │ Curated zone: Parquet + Snappy  │
                                          │ Consumption: 5-min aggregates   │
                                          └───────────────┬─────────────────┘
                                                          │
                                                          ▼
                                          ┌─────────────────────────────┐
                                          │     Data Lake (3 zones)     │
                                          │  /raw/  /curated/  /consumption/ │
                                          └─────────────┬───────────────┘
                                                        │
                                                        ▼
                                          ┌─────────────┐     ┌──────────────┐
                                          │   Client    │◀────│  REST API    │
                                          │             │     │  (Flask)     │
                                          └─────────────┘     └──────────────┘
```

## Component Description

### 1. Data Generator (producer.py)
A Python script that generates realistic IoT sensor readings for temperature, humidity, and pressure. Configured with `acks='all'`, `retries=5`, and `max_in_flight_requests_per_connection=1` for reliable delivery. Uses the sensor type as the Kafka message key to guarantee per-type ordering.

### 2. Kafka Cluster
A 3-broker Kafka cluster with:
- Topic `sensor-events`: 3 partitions, replication factor 3
- `min.insync.replicas=2` for fault tolerance
- Kafka UI accessible at `http://localhost:8080`

### 3. Spark Processing Pipeline (spark_pipeline.py)
A Structured Streaming job that:
- Parses JSON messages with an explicit schema
- Validates values against physical plausibility ranges
- Detects anomalies independently of the producer's flag
- Computes 5-minute windowed aggregates with a 2-minute watermark
- Writes to three data lake zones

### 4. Data Lake
Three-zone architecture under `/tmp/datalake/`:
- **Raw zone**: JSON as ingested, partitioned by `year/month/day/hour`
- **Curated zone**: Cleaned Parquet (Snappy), partitioned by `sensor_type/year/month/day`
- **Consumption zone**: Windowed aggregates (Parquet), partitioned by `sensor_type/year/month`

### 5. REST API (app.py)
A Flask API exposing 6 endpoints: health check, sensor listing, latest reading, daily stats, anomaly list, and reading publication.

## Technical Choices

### Partitioning strategy for the curated zone
The curated zone is partitioned by `sensor_type/year/month/day`. This aligns with the most common query patterns: filtering by sensor type (the API's primary access pattern) and by date range (for statistics over N days). Hive-style partitioning enables Spark's partition pruning to skip irrelevant directories, dramatically reducing scan volume for time-bounded queries.

### Spark Structured Streaming outputMode
All three sinks use `outputMode("append")`. The raw and curated zones only add new rows without updating previous ones, so append is the natural choice. For the consumption zone, the 5-minute window with watermark ensures late data is handled (up to 2 minutes late), and once a window result is emitted, it is not updated — so append is correct here too.

### Replication factor and min.insync.replicas
RF=3 means each partition is replicated across all three brokers. With `min.insync.replicas=2`, a producer with `acks='all'` only acknowledges a write after at least two replicas confirm it. This tolerates one broker failure without data loss and without blocking writes.

### event_time vs ingestion_time
The curated zone is partitioned by `event_time` (the timestamp the sensor reported), which preserves the data's semantic time. The raw zone is partitioned by `ingest_time` (when the system received it), which captures the operational timeline. This dual approach supports both business queries ("what was the temperature yesterday?") and operational debugging ("what arrived during the outage window?").

### Delivery semantics
The platform achieves **at-least-once** semantics end-to-end:
- The producer uses `acks='all'` + `retries=5` but without idempotence (`enable.idempotence`), so retries may produce duplicates.
- Spark reads from Kafka with checkpointing, so it resumes from the last committed offset after failure, but may re-process some records if a batch was partially written.
- For exactly-once, idempotent producers and Kafka transactions would be needed.
