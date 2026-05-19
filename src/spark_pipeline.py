#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


DATA_LAKE_PATH = "/datalake"
CHECKPOINT_PATH = "/checkpoints"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29091,kafka2:29092,kafka3:29093')
TOPIC_NAME = "sensor-events"

# JSON schema
SENSOR_SCHEMA = StructType([
    StructField("sensor", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("anomaly", BooleanType(), True)
])


def create_spark_session():
    return SparkSession.builder \
        .appName("IoT_Sensor_Pipeline") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting IoT Sensor Pipeline...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Data Lake: {DATA_LAKE_PATH}")


    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = kafka_df.select(
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data")
    ).select(
        col("data.sensor"),
        col("data.value"),
        col("data.unit"),
        to_timestamp(col("data.timestamp")).alias("event_time"),
        col("data.source"),
        col("data.anomaly").alias("producer_anomaly")
    ).filter(col("event_time").isNotNull())


    processed = parsed.withColumn("is_anomaly",
                                  when(col("sensor") == "temperature", col("value") > 35)
                                  .when(col("sensor") == "humidity", col("value") > 90)
                                  .when(col("sensor") == "pressure", (col("value") < 990) | (col("value") > 1030))
                                  .otherwise(lit(False))
                                  )


    def write_curated(df, epoch_id):
        df_with_parts = df.withColumn("year", year("event_time")) \
            .withColumn("month", month("event_time")) \
            .withColumn("day", dayofmonth("event_time"))

        output_path = f"{DATA_LAKE_PATH}/curated/domain=iot"
        df_with_parts.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("sensor", "year", "month", "day") \
            .save(output_path)

    query = processed.writeStream \
        .foreachBatch(write_curated) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/curated") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("Streaming started. Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()