# Analytical Queries

## Prerequisites

Ensure the Spark pipeline has been running and the data lake contains data:

```bash
ls /tmp/datalake/curated/domain=iot/
```

## Running the Analytics

```bash
spark-submit src/analytics.py
```

Results are printed to stdout and written as CSV to `outputs/analytics/`.

## Query 1: Top 5 Hours with Most Anomalies

Identifies the hours with the highest concentration of anomalies across all sensor types. Useful for pinpointing operational incidents.

**Expected output**: A table with year, month, day, hour, and anomaly_count columns, ordered by count descending.

## Query 2: Sensor Type Statistics

Computes per-sensor global statistics: mean, min, max, standard deviation, and anomaly rate percentage. This provides a health overview of each sensor type.

**Expected output**: A table with sensor, mean_value, min_value, max_value, stddev_value, and anomaly_rate_pct columns.

Typical values:
- Temperature: mean ~30C, anomaly rate ~10%
- Humidity: mean ~62%, anomaly rate ~10%
- Pressure: mean ~1010 hPa, anomaly rate ~10%

## Query 3: Temperature Daily Evolution

Shows the daily trend of mean temperature and anomaly count for the temperature sensor. Useful for detecting environmental drift over time.

**Expected output**: A time series of daily_mean_temp, daily_anomaly_count, and total_readings for the last 30 days.

## Query 4: Partition Pruning Demonstration

Compares execution time of two queries:

1. **Without pruning**: `SELECT sensor, COUNT(*) FROM sensor_data WHERE value > 30 GROUP BY sensor`
   - Scans all partitions regardless of sensor or date.

2. **With pruning**: `SELECT sensor, COUNT(*) FROM sensor_data WHERE sensor = 'temperature' AND year = 2024 AND month = 1 GROUP BY sensor`
   - Only reads the temperature sensor partition for January 2024.

The speedup factor demonstrates the effectiveness of Hive-style partitioning. With sufficient data volume, partition pruning typically yields a 3-10x improvement in query time by skipping irrelevant directories at the filesystem level.

Results are also written to `outputs/analytics/partition_pruning_results.txt`.
