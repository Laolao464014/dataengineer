#!/usr/bin/env python3
"""
Analytics queries on the data lake using Spark SQL
Demonstrates partition pruning and analytical insights
"""

import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, stddev, min, max, sum, when, hour

# Configuration
DATA_LAKE_PATH = "/tmp/datalake"
OUTPUT_PATH = "outputs/analytics"


def create_spark_session():
    """Create Spark session with optimizations"""
    return SparkSession.builder \
        .appName("SensorAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def query1_top_anomaly_hours(spark):
    """
    Query 1: Top 5 hours with highest number of anomalies, all sensors combined
    """
    print("\n" + "=" * 60)
    print("QUERY 1: Top 5 Hours with Most Anomalies")
    print("=" * 60)

    curated_path = f"{DATA_LAKE_PATH}/curated/domain=iot"

    try:
        df = spark.read.parquet(curated_path)
        df.createOrReplaceTempView("sensor_data")

        result = spark.sql("""
            SELECT 
                year, month, day, hour(event_time) as hour_of_day,
                COUNT(*) as anomaly_count
            FROM sensor_data
            WHERE is_anomaly = true
            GROUP BY year, month, day, hour(event_time)
            ORDER BY anomaly_count DESC
            LIMIT 5
        """)

        result.show(truncate=False)

        # Write to CSV
        result.write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_PATH}/query1_top_anomaly_hours")

        return result
    except Exception as e:
        print(f"Error in Query 1: {e}")
        return None


def query2_sensor_statistics(spark):
    """
    Query 2: For each sensor type: mean, min, max, stddev, anomaly rate
    """
    print("\n" + "=" * 60)
    print("QUERY 2: Sensor Type Statistics")
    print("=" * 60)

    curated_path = f"{DATA_LAKE_PATH}/curated/domain=iot"

    try:
        df = spark.read.parquet(curated_path)
        df.createOrReplaceTempView("sensor_data")

        result = spark.sql("""
            SELECT 
                sensor,
                ROUND(AVG(value), 2) as mean_value,
                ROUND(MIN(value), 2) as min_value,
                ROUND(MAX(value), 2) as max_value,
                ROUND(STDDEV(value), 2) as stddev_value,
                ROUND(SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as anomaly_rate_pct
            FROM sensor_data
            GROUP BY sensor
            ORDER BY sensor
        """)

        result.show(truncate=False)

        # Write to CSV
        result.write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_PATH}/query2_sensor_statistics")

        return result
    except Exception as e:
        print(f"Error in Query 2: {e}")
        return None


def query3_temperature_daily_evolution(spark):
    """
    Query 3: Daily evolution of mean and anomaly count for temperature sensor
    """
    print("\n" + "=" * 60)
    print("QUERY 3: Temperature Daily Evolution")
    print("=" * 60)

    curated_path = f"{DATA_LAKE_PATH}/curated/domain=iot"

    try:
        df = spark.read.parquet(curated_path)
        df.createOrReplaceTempView("sensor_data")

        result = spark.sql("""
            SELECT 
                year, month, day,
                ROUND(AVG(value), 2) as daily_mean_temp,
                SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) as daily_anomaly_count,
                COUNT(*) as total_readings
            FROM sensor_data
            WHERE sensor = 'temperature'
            GROUP BY year, month, day
            ORDER BY year DESC, month DESC, day DESC
            LIMIT 30
        """)

        result.show(truncate=False)

        # Write to CSV
        result.write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_PATH}/query3_temperature_daily_evolution")

        return result
    except Exception as e:
        print(f"Error in Query 3: {e}")
        return None


def query4_partition_pruning_demo(spark):
    """
    Query 4: Demonstrate partition pruning with execution time comparison
    """
    print("\n" + "=" * 60)
    print("QUERY 4: Partition Pruning Demonstration")
    print("=" * 60)

    curated_path = f"{DATA_LAKE_PATH}/curated/domain=iot"

    try:
        df = spark.read.parquet(curated_path)
        df.createOrReplaceTempView("sensor_data")

        # Query WITHOUT partition pruning (no filter on partition columns)
        print("\n--- Query WITHOUT partition pruning ---")
        start_time = time.time()

        no_pruning_result = spark.sql("""
            SELECT sensor, COUNT(*) as count
            FROM sensor_data
            WHERE value > 30
            GROUP BY sensor
        """)
        no_pruning_result.collect()  # Force execution
        no_pruning_time = time.time() - start_time
        print(f"Execution time: {no_pruning_time:.4f} seconds")

        # Query WITH partition pruning (with filter on partition columns)
        print("\n--- Query WITH partition pruning ---")
        start_time = time.time()

        with_pruning_result = spark.sql("""
            SELECT sensor, COUNT(*) as count
            FROM sensor_data
            WHERE sensor = 'temperature'
              AND year = 2024
              AND month = 1
            GROUP BY sensor
        """)
        with_pruning_result.collect()  # Force execution
        with_pruning_time = time.time() - start_time
        print(f"Execution time: {with_pruning_time:.4f} seconds")

        # Calculate speedup
        if with_pruning_time > 0:
            speedup = no_pruning_time / with_pruning_time
        else:
            speedup = float('inf')

        print("\n--- Performance Comparison ---")
        print(f"Without partition pruning: {no_pruning_time:.4f} seconds")
        print(f"With partition pruning: {with_pruning_time:.4f} seconds")
        print(f"Speedup factor: {speedup:.2f}x")

        # Write results to file
        with open(f"{OUTPUT_PATH}/partition_pruning_results.txt", "w") as f:
            f.write("Partition Pruning Demonstration\n")
            f.write("=" * 40 + "\n")
            f.write(f"Query without partition pruning: {no_pruning_time:.4f} seconds\n")
            f.write(f"Query with partition pruning: {with_pruning_time:.4f} seconds\n")
            f.write(f"Speedup factor: {speedup:.2f}x\n")

        return speedup
    except Exception as e:
        print(f"Error in Query 4: {e}")
        return None


def main():
    # Create output directory
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("IoT Sensor Data Lake Analytics")
    print(f"Data Lake Path: {DATA_LAKE_PATH}")
    print("=" * 60)

    # Run all queries
    query1_top_anomaly_hours(spark)
    query2_sensor_statistics(spark)
    query3_temperature_daily_evolution(spark)
    query4_partition_pruning_demo(spark)

    print("\n" + "=" * 60)
    print(f"All results written to: {OUTPUT_PATH}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()