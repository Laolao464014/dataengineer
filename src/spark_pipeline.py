#!/usr/bin/env python3
"""
Spark Structured Streaming pipeline for IoT sensor data.
Consumes from Kafka, processes events, and feeds the three-zone data lake:
  raw/      - Raw JSON, partitioned by ingestion year/month/day/hour
  curated/  - Cleaned Parquet (Snappy), partitioned by sensor_type/year/month/day
  consumption/ - Windowed aggregates (5-min), partitioned by sensor_type/year/month
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
DATA_LAKE_PATH = os.environ.get('DATA_LAKE_PATH', '/tmp/datalake')
CHECKPOINT_PATH = os.environ.get('CHECKPOINT_PATH', '/tmp/checkpoints')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092,localhost:9093'
)
TOPIC_NAME = "sensor-events"

# Explicit JSON schema matching the agreed message format
SENSOR_SCHEMA = StructType([
    StructField("sensor", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True)
])

# Physical plausibility ranges for outlier filtering
VALID_RANGES = {
    "temperature": (15.0, 45.0),
    "humidity": (30.0, 95.0),
    "pressure": (980.0, 1040.0),
}


def create_spark_session():
    return SparkSession.builder \
        .appName("IoT_Sensor_Pipeline") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_PATH}/main") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def write_raw(df, epoch_id):
    """Raw zone: JSON as-is, partitioned by ingestion time (year/month/day/hour)."""
    df_with_parts = df \
        .withColumn("year", year(col("ingest_time"))) \
        .withColumn("month", month(col("ingest_time"))) \
        .withColumn("day", dayofmonth(col("ingest_time"))) \
        .withColumn("hour", hour(col("ingest_time")))

    output_path = f"{DATA_LAKE_PATH}/raw/source=kafka/topic={TOPIC_NAME}"
    df_with_parts.write \
        .mode("append") \
        .format("json") \
        .partitionBy("year", "month", "day", "hour") \
        .save(output_path)


def write_curated(df, epoch_id):
    """Curated zone: cleaned Parquet with Snappy, partitioned by sensor + event time."""
    df_with_parts = df \
        .withColumn("year", year(col("event_time"))) \
        .withColumn("month", month(col("event_time"))) \
        .withColumn("day", dayofmonth(col("event_time")))

    output_path = f"{DATA_LAKE_PATH}/curated/domain=iot"
    df_with_parts.write \
        .mode("append") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("sensor", "year", "month", "day") \
        .save(output_path)


def write_consumption(df, epoch_id):
    """Consumption zone: 5-minute windowed aggregates, partitioned by sensor + time."""
    df_with_parts = df \
        .withColumn("year", year(col("window_start"))) \
        .withColumn("month", month(col("window_start")))

    output_path = f"{DATA_LAKE_PATH}/consumption/use_case=sensor_averages"
    df_with_parts.write \
        .mode("append") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("sensor", "year", "month") \
        .save(output_path)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"IoT Sensor Pipeline starting...")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Data Lake: {DATA_LAKE_PATH}")
    print(f"  Checkpoints: {CHECKPOINT_PATH}")

    # 1. Read from Kafka in streaming mode
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Parse JSON with explicit schema — reject records missing key fields
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data")
    ).select("data.*") \
      .filter(col("sensor").isNotNull() & col("value").isNotNull())

    # Convert ISO-8601 timestamp string to Spark timestamp, record ingestion time
    parsed = parsed \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .filter(col("event_time").isNotNull()) \
        .drop("timestamp") \
        .withColumn("ingest_time", current_timestamp())

    # 3. Validation: filter values outside physical plausibility ranges
    validated = parsed.filter(
        ((col("sensor") == "temperature") & (col("value") >= 15.0) & (col("value") <= 45.0)) |
        ((col("sensor") == "humidity") & (col("value") >= 30.0) & (col("value") <= 95.0)) |
        ((col("sensor") == "pressure") & (col("value") >= 980.0) & (col("value") <= 1040.0))
    )

    # 4. Anomaly detection — computed independently of the producer's self-declared flag
    processed = validated.withColumn("is_anomaly",
        when(col("sensor") == "temperature", col("value") > 35.0)
        .when(col("sensor") == "humidity", col("value") > 90.0)
        .when(col("sensor") == "pressure", (col("value") < 990.0) | (col("value") > 1030.0))
        .otherwise(lit(False))
    )

    # === Write raw zone (the original parsed records before validation) ===
    raw_query = parsed.writeStream \
        .foreachBatch(write_raw) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/raw") \
        .trigger(processingTime="10 seconds") \
        .start()

    # === Write curated zone (validated + anomaly-flagged) ===
    curated_query = processed.writeStream \
        .foreachBatch(write_curated) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curated") \
        .trigger(processingTime="10 seconds") \
        .start()

    # 5. Windowed aggregation: 5-min tumbling window with 2-min watermark
    windowed = processed \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor")
        ) \
        .agg(
            round(avg("value"), 2).alias("avg_value"),
            round(min("value"), 2).alias("min_value"),
            round(max("value"), 2).alias("max_value"),
            count("*").alias("observation_count"),
            sum(col("is_anomaly").cast("int")).alias("anomaly_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor"),
            col("avg_value"),
            col("min_value"),
            col("max_value"),
            col("observation_count"),
            col("anomaly_count")
        )

    # === Write consumption zone (windowed aggregates) ===
    consumption_query = windowed.writeStream \
        .foreachBatch(write_consumption) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/consumption") \
        .trigger(processingTime="30 seconds") \
        .start()

    print("IoT Sensor Pipeline started. All three zones active.")
    print("Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
