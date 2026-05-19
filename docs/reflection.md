# Reflection Questions

## 1. Pipeline crash after raw zone write, before curated zone

**Impact**: The raw zone contains the ingested JSON records, but the curated zone is missing the corresponding cleaned Parquet records. This creates a gap: the data exists in raw form but is not queryable through the API's curated endpoints. The consumption zone also misses the windowed aggregates for the lost batch.

**Checkpoint strategy**: Spark Structured Streaming checkpoints track the Kafka offset up to which data has been consumed and processed. Since each sink (raw, curated, consumption) has its own checkpoint directory, on restart the pipeline resumes from the last committed offset for each sink independently. If the curated write failed, its checkpoint was not advanced, so Spark re-processes the batch from Kafka — the raw zone may get duplicates, but no data is permanently lost.

## 2. Scaling to 50,000 msg/sec — bottlenecks and fixes

The first bottleneck would be the **single-node Spark executor** (`local[*]`). At 50K msg/s, a single JVM cannot keep up with parsing, validation, anomaly detection, and triple writes.

The second bottleneck is the **local filesystem** for the data lake. Writing three zones (raw JSON, curated Parquet, consumption Parquet) at high throughput generates substantial disk I/O on a single machine.

Fixes:
- Deploy Spark on a cluster (YARN/Kubernetes) for horizontal scaling
- Replace local filesystem with distributed storage (S3/MinIO/HDFS)
- Increase Kafka partitions beyond 3 to parallelize consumers
- Use Spark's `trigger` processing time tuning to batch writes more efficiently

## 3. Kafka vs Parquet data lake as source of truth

**Kafka as source of truth**: Excellent for real-time access and replayability. However, Kafka's retention is typically time-bound (days/weeks). Storing all historical data in Kafka indefinitely is cost-prohibitive due to broker disk requirements. Random-access queries are inefficient.

**Parquet data lake as source of truth**: Ideal for long-term historical storage. Columnar format enables efficient analytical queries with predicate pushdown and partition pruning. Significantly cheaper per GB than Kafka storage.

**Recommendation**: Kafka is the source of truth for the _streaming_ window (hours to days), while the Parquet data lake is the source of truth for _historical_ analysis (days to years). This is the pattern our platform follows.

## 4. Aberrant sensor values for 2 hours — detection and isolation

**Detection**: The Spark pipeline's validation layer filters values outside physical plausibility ranges (temperature 15-45C, humidity 30-95%, pressure 980-1040 hPa). Values outside these ranges are dropped before reaching the curated zone. Additionally, the anomaly detection rules flag borderline-abnormal values (temperature > 35C, etc.), which appear in the `is_anomaly` column but are still stored.

**Isolation without deletion**: Write the out-of-range records to a dedicated **quarantine zone** (`/tmp/datalake/quarantine/`) instead of dropping them silently. The curated zone would only contain valid records, while the quarantine zone preserves the aberrant data for forensic analysis. The API's anomaly endpoint can also be extended to filter or flag these records.

## 5. Adding a new CO2 sensor type — modifications required

Files to modify:

1. **src/producer.py** — Add `'co2'` to `SENSOR_TYPES` with unit `'ppm'`, range 400-2000, anomaly threshold > 1000
2. **src/spark_pipeline.py** — Add CO2 to `VALID_RANGES` dict and the anomaly detection `when()` chain, and the validation filter
3. **api/app.py** — Add `'co2'` to `VALID_SENSORS` list, add range checks in the POST endpoint
4. **src/analytics.py** — No changes needed (queries are parameterized by sensor type)
5. **api/lake_utils.py** — No changes needed (reads any sensor partition)
6. **docs/architecture.md** — Update sensor table

The pipeline is designed so that adding a sensor type requires changes only in the configuration dictionaries, not in the processing logic itself.
