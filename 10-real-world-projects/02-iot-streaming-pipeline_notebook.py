# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Project 02: IoT Streaming Pipeline
# MAGIC > Module 10 -- Capstone Project | Level: Advanced | Time: 2-3 hours
# MAGIC
# MAGIC ## What You Will Build
# MAGIC
# MAGIC A real-time IoT sensor data pipeline for manufacturing equipment:
# MAGIC - **Ingest**: Simulated streaming from 50 devices across 5 factory floors
# MAGIC - **Bronze**: Raw sensor data in Delta (append-only)
# MAGIC - **Silver**: 5-minute windowed aggregations, anomaly detection, device metadata enrichment
# MAGIC - **Gold**: Device health scores, anomaly summary, factory floor comparison
# MAGIC
# MAGIC This notebook uses **batch simulation** by default (streaming with
# MAGIC `trigger(availableNow=True)`) for compatibility with all Databricks tiers.
# MAGIC Optional live streaming cells are included for full-tier workspaces.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

# Configuration
DATABASE = "module10_iot"
CHECKPOINT_BASE = "/tmp/module10_iot_checkpoints"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# Clean up prior runs
existing_tables = [row.tableName for row in spark.sql("SHOW TABLES").collect()]
for t in existing_tables:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")

# Clean up checkpoint directories
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)

print(f"Database '{DATABASE}' ready.")
print(f"Checkpoint base: {CHECKPOINT_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Data Generation
# MAGIC
# MAGIC We generate 24 hours of sensor data for 50 devices across 5 factory floors.
# MAGIC Data includes intentional anomalies (~5% of readings outside normal range).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Device Metadata (Static Reference Table)

# COMMAND ----------

random.seed(42)

device_types = ["CNC Machine", "Conveyor Belt", "Robotic Arm", "Press Machine", "Assembly Unit"]
floors = ["FLOOR-A", "FLOOR-B", "FLOOR-C", "FLOOR-D", "FLOOR-E"]

device_metadata = []
for i in range(1, 51):
    floor_idx = (i - 1) // 10  # 10 devices per floor
    install_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1200))
    last_maintenance = install_date + timedelta(days=random.randint(60, 400))
    device_metadata.append({
        "device_id": f"DEV-{i:03d}",
        "floor_id": floors[floor_idx],
        "device_type": random.choice(device_types),
        "install_date": install_date.strftime("%Y-%m-%d"),
        "last_maintenance_date": last_maintenance.strftime("%Y-%m-%d"),
        "firmware_version": f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
    })

metadata_df = spark.createDataFrame(device_metadata)
metadata_df.write.format("delta").mode("overwrite").saveAsTable("device_metadata")
print(f"Device metadata: {metadata_df.count()} devices across {len(floors)} floors")
metadata_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Generate Sensor Readings (24 Hours)
# MAGIC
# MAGIC Normal ranges:
# MAGIC - Temperature: 60-80 C
# MAGIC - Pressure: 28-32 PSI
# MAGIC - Vibration: 0.5-2.0 mm/s
# MAGIC - Humidity: 35-55 %
# MAGIC
# MAGIC ~5% of readings are intentionally anomalous.

# COMMAND ----------

random.seed(42)

# Normal ranges for each sensor type: (mean, std_dev)
sensor_profiles = {
    "temperature": (70.0, 4.0),     # Normal: 60-80 C
    "pressure": (30.0, 1.0),        # Normal: 28-32 PSI
    "vibration": (1.25, 0.3),       # Normal: 0.5-2.0 mm/s
    "humidity": (45.0, 4.0),        # Normal: 35-55 %
}

# Generate 24 hours of data at 1-minute intervals per device
# For 50 devices x 4 sensor types x 1440 minutes = 288,000 readings
# We sample to keep it manageable: every 5 minutes = ~57,600 readings
start_time = datetime(2025, 1, 15, 0, 0, 0)
readings = []

for device in device_metadata:
    device_id = device["device_id"]
    floor_id = device["floor_id"]

    for minute in range(0, 1440, 5):  # Every 5 minutes
        event_time = start_time + timedelta(minutes=minute)

        for sensor_type, (mean, std) in sensor_profiles.items():
            # Normal reading
            value = round(random.gauss(mean, std), 2)

            # Inject anomaly ~5% of the time
            if random.random() < 0.05:
                # Push value outside 2 standard deviations
                direction = random.choice([-1, 1])
                value = round(mean + direction * (2.5 + random.random()) * std, 2)

            readings.append((
                device_id, floor_id, sensor_type,
                value, event_time.strftime("%Y-%m-%d %H:%M:%S")
            ))

readings_schema = StructType([
    StructField("device_id", StringType()),
    StructField("floor_id", StringType()),
    StructField("reading_type", StringType()),
    StructField("value", DoubleType()),
    StructField("event_time", StringType()),
])

sensor_df = spark.createDataFrame(readings, schema=readings_schema)
sensor_df = sensor_df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

total_readings = sensor_df.count()
print(f"Total sensor readings generated: {total_readings}")
print(f"Devices: 50 | Floors: 5 | Sensor types: 4 | Time span: 24 hours")
sensor_df.show(10, truncate=False)

# COMMAND ----------

# Write sensor data to a staging location for streaming ingestion
STAGING_PATH = "/tmp/module10_iot_staging"
dbutils.fs.rm(STAGING_PATH, recurse=True)

sensor_df.write.format("delta").mode("overwrite").save(STAGING_PATH)
print(f"Sensor data staged at: {STAGING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: BRONZE LAYER -- Raw Sensor Ingestion
# MAGIC
# MAGIC We use Structured Streaming with `trigger(availableNow=True)` to simulate
# MAGIC a streaming ingestion pattern. In production, this would be a continuous
# MAGIC stream from Kafka or Event Hubs.

# COMMAND ----------

# Create the Bronze Delta table schema first
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_sensor_readings (
        device_id STRING,
        floor_id STRING,
        reading_type STRING,
        value DOUBLE,
        event_time TIMESTAMP,
        _ingest_timestamp TIMESTAMP,
        _source STRING
    )
    USING DELTA
""")

# Stream from staging to Bronze
bronze_stream = (spark.readStream
    .format("delta")
    .load(STAGING_PATH)
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("iot_sensor_gateway"))
)

bronze_query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .trigger(availableNow=True)
    .toTable("bronze_sensor_readings")
)

bronze_query.awaitTermination()
bronze_count = spark.table("bronze_sensor_readings").count()
print(f"Bronze sensor readings: {bronze_count} rows ingested")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, floor_id, reading_type, value, event_time, _ingest_timestamp
# MAGIC FROM bronze_sensor_readings
# MAGIC ORDER BY event_time
# MAGIC LIMIT 10

# COMMAND ----------

# Distribution check
print("=== BRONZE LAYER DISTRIBUTION ===")
spark.sql("""
    SELECT reading_type,
           COUNT(*) AS count,
           ROUND(MIN(value), 2) AS min_val,
           ROUND(AVG(value), 2) AS avg_val,
           ROUND(MAX(value), 2) AS max_val,
           ROUND(STDDEV(value), 2) AS std_val
    FROM bronze_sensor_readings
    GROUP BY reading_type
    ORDER BY reading_type
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: SILVER LAYER -- Windowed Aggregations and Anomaly Detection
# MAGIC
# MAGIC **Transformations**:
# MAGIC 1. 5-minute tumbling window aggregations per device and reading type
# MAGIC 2. Anomaly detection using mean +/- 2 standard deviations
# MAGIC 3. Enrichment with device metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Five-Minute Windowed Aggregations (Streaming)

# COMMAND ----------

# Read Bronze as a stream with watermark for late data handling
bronze_stream_for_silver = (spark.readStream
    .format("delta")
    .table("bronze_sensor_readings")
    .withWatermark("event_time", "10 minutes")
)

# 5-minute tumbling window aggregation
windowed_agg = (bronze_stream_for_silver
    .groupBy(
        F.window("event_time", "5 minutes").alias("time_window"),
        "device_id",
        "floor_id",
        "reading_type"
    )
    .agg(
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.round(F.min("value"), 2).alias("min_value"),
        F.round(F.max("value"), 2).alias("max_value"),
        F.round(F.stddev("value"), 4).alias("stddev_value"),
        F.count("*").alias("reading_count"),
    )
    .withColumn("window_start", F.col("time_window.start"))
    .withColumn("window_end", F.col("time_window.end"))
    .drop("time_window")
)

# Write windowed aggregations
agg_query = (windowed_agg.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver_agg")
    .trigger(availableNow=True)
    .toTable("silver_sensor_5min_agg")
)

agg_query.awaitTermination()
agg_count = spark.table("silver_sensor_5min_agg").count()
print(f"Silver 5-min aggregations: {agg_count} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview windowed aggregations
# MAGIC SELECT device_id, reading_type, window_start, window_end,
# MAGIC        avg_value, min_value, max_value, stddev_value, reading_count
# MAGIC FROM silver_sensor_5min_agg
# MAGIC WHERE device_id = 'DEV-001'
# MAGIC ORDER BY window_start
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Anomaly Detection
# MAGIC
# MAGIC Flag individual readings that fall outside the per-device, per-sensor-type
# MAGIC baseline of mean +/- 2 standard deviations.

# COMMAND ----------

# Step 1: Compute per-device, per-reading-type baselines from Bronze
bronze_all = spark.table("bronze_sensor_readings")

baselines = (bronze_all
    .groupBy("device_id", "reading_type")
    .agg(
        F.avg("value").alias("baseline_mean"),
        F.stddev("value").alias("baseline_std"),
        F.count("*").alias("total_readings"),
    )
)

# Step 2: Join baselines with raw readings and flag anomalies
anomaly_df = (bronze_all
    .join(baselines, on=["device_id", "reading_type"], how="inner")
    .withColumn("upper_threshold",
        F.col("baseline_mean") + 2 * F.col("baseline_std"))
    .withColumn("lower_threshold",
        F.col("baseline_mean") - 2 * F.col("baseline_std"))
    .withColumn("is_anomaly",
        (F.col("value") > F.col("upper_threshold")) |
        (F.col("value") < F.col("lower_threshold")))
    .withColumn("anomaly_severity",
        F.when(~F.col("is_anomaly"), "normal")
         .when(
            (F.col("value") > F.col("baseline_mean") + 3 * F.col("baseline_std")) |
            (F.col("value") < F.col("baseline_mean") - 3 * F.col("baseline_std")),
            "critical")
         .otherwise("warning"))
)

# Step 3: Save all anomalous readings
anomalies_only = anomaly_df.filter("is_anomaly = true")
anomalies_only.write.format("delta").mode("overwrite").saveAsTable("silver_anomalies")

anomaly_count = spark.table("silver_anomalies").count()
total_count = bronze_all.count()
anomaly_rate = anomaly_count / total_count * 100
print(f"Total readings: {total_count}")
print(f"Anomalies detected: {anomaly_count} ({anomaly_rate:.1f}%)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly distribution by severity and reading type
# MAGIC SELECT reading_type, anomaly_severity, COUNT(*) AS count
# MAGIC FROM silver_anomalies
# MAGIC GROUP BY reading_type, anomaly_severity
# MAGIC ORDER BY reading_type, anomaly_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Silver Layer Data Quality Report

# COMMAND ----------

print("=" * 65)
print("SILVER LAYER DATA QUALITY REPORT")
print("=" * 65)

silver_tables = {
    "silver_sensor_5min_agg": spark.table("silver_sensor_5min_agg").count(),
    "silver_anomalies": spark.table("silver_anomalies").count(),
    "device_metadata": spark.table("device_metadata").count(),
}

for table, count in silver_tables.items():
    print(f"  {table:<30} rows={count}")

# Per-floor anomaly distribution
print("\nAnomalies by floor:")
spark.sql("""
    SELECT floor_id, COUNT(*) AS anomaly_count,
           ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_anomalies), 1) AS pct
    FROM silver_anomalies
    GROUP BY floor_id
    ORDER BY floor_id
""").show(truncate=False)
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: GOLD LAYER -- Device Health, Anomaly Dashboard, Floor Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Device Health Scores
# MAGIC
# MAGIC Composite score (0-100) based on:
# MAGIC - Normal reading percentage (40% weight)
# MAGIC - Anomaly penalty (30% weight)
# MAGIC - Reading stability (30% weight)

# COMMAND ----------

# Step 1: Calculate component metrics per device
bronze_with_anomaly = (spark.table("bronze_sensor_readings").alias("b")
    .join(
        spark.table("silver_anomalies")
            .select("device_id", "reading_type", "event_time", "is_anomaly")
            .alias("a"),
        on=["device_id", "reading_type", "event_time"],
        how="left"
    )
    .withColumn("is_anomaly", F.coalesce(F.col("a.is_anomaly"), F.lit(False)))
)

device_metrics = (bronze_with_anomaly
    .groupBy("b.device_id", "b.floor_id")
    .agg(
        F.count("*").alias("total_readings"),
        F.sum(F.when(~F.col("is_anomaly"), 1).otherwise(0)).alias("normal_readings"),
        F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_readings"),
        F.round(F.avg("b.value"), 2).alias("avg_value"),
        F.round(F.stddev("b.value"), 4).alias("overall_stddev"),
    )
)

# Step 2: Compute health score components
# Normalize stddev: lower is better. Use max stddev across all devices as reference.
max_std = device_metrics.agg(F.max("overall_stddev")).collect()[0][0] or 1.0

health_scores = (device_metrics
    .withColumn("normal_pct",
        F.round(F.col("normal_readings") / F.col("total_readings") * 100, 1))
    .withColumn("anomaly_rate",
        F.round(F.col("anomaly_readings") / F.col("total_readings") * 100, 1))
    .withColumn("stability_score",
        F.round((1 - F.col("overall_stddev") / F.lit(max_std)) * 100, 1))
    # Composite health score
    .withColumn("health_score", F.round(
        F.col("normal_pct") * 0.40 +
        F.greatest(F.lit(0), F.lit(100) - F.col("anomaly_rate") * 10) * 0.30 +
        F.col("stability_score") * 0.30,
        1
    ))
    # Health category
    .withColumn("health_category",
        F.when(F.col("health_score") >= 90, "Excellent")
         .when(F.col("health_score") >= 70, "Good")
         .when(F.col("health_score") >= 50, "Warning")
         .otherwise("Critical"))
    .select(
        F.col("b.device_id").alias("device_id"),
        F.col("b.floor_id").alias("floor_id"),
        "total_readings", "normal_readings", "anomaly_readings",
        "normal_pct", "anomaly_rate", "stability_score",
        "health_score", "health_category"
    )
)

# Step 3: Enrich with device metadata
device_meta = spark.table("device_metadata").select(
    "device_id", "device_type", "install_date", "last_maintenance_date"
)

health_enriched = (health_scores
    .join(device_meta, on="device_id", how="left")
    .orderBy(F.asc("health_score"))
)

health_enriched.write.format("delta").mode("overwrite").saveAsTable("gold_device_health_scores")
print(f"Gold device health scores: {spark.table('gold_device_health_scores').count()} rows")

# COMMAND ----------

# Show devices with lowest health scores (need attention)
print("=== DEVICES NEEDING ATTENTION (Bottom 10 by Health Score) ===")
spark.sql("""
    SELECT device_id, floor_id, device_type, health_score, health_category,
           anomaly_rate, stability_score, last_maintenance_date
    FROM gold_device_health_scores
    ORDER BY health_score ASC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Health category distribution
print("=== HEALTH CATEGORY DISTRIBUTION ===")
spark.sql("""
    SELECT health_category,
           COUNT(*) AS device_count,
           ROUND(AVG(health_score), 1) AS avg_score,
           ROUND(AVG(anomaly_rate), 1) AS avg_anomaly_rate
    FROM gold_device_health_scores
    GROUP BY health_category
    ORDER BY avg_score DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Anomaly Summary Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_anomaly_summary AS
# MAGIC WITH hourly_anomalies AS (
# MAGIC   SELECT
# MAGIC     device_id,
# MAGIC     floor_id,
# MAGIC     reading_type,
# MAGIC     DATE_TRUNC('hour', event_time) AS anomaly_hour,
# MAGIC     COUNT(*) AS anomaly_count,
# MAGIC     ROUND(AVG(value), 2) AS avg_anomaly_value,
# MAGIC     anomaly_severity
# MAGIC   FROM silver_anomalies
# MAGIC   GROUP BY device_id, floor_id, reading_type,
# MAGIC            DATE_TRUNC('hour', event_time), anomaly_severity
# MAGIC ),
# MAGIC trend_calc AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     LAG(anomaly_count) OVER (
# MAGIC       PARTITION BY device_id, reading_type
# MAGIC       ORDER BY anomaly_hour
# MAGIC     ) AS prev_hour_count
# MAGIC   FROM hourly_anomalies
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN prev_hour_count IS NULL THEN 'baseline'
# MAGIC     WHEN anomaly_count > prev_hour_count THEN 'increasing'
# MAGIC     WHEN anomaly_count < prev_hour_count THEN 'decreasing'
# MAGIC     ELSE 'stable'
# MAGIC   END AS trend
# MAGIC FROM trend_calc
# MAGIC ORDER BY anomaly_hour DESC, anomaly_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top anomaly hours
# MAGIC SELECT anomaly_hour,
# MAGIC        SUM(anomaly_count) AS total_anomalies,
# MAGIC        COUNT(DISTINCT device_id) AS affected_devices,
# MAGIC        COUNT(DISTINCT floor_id) AS affected_floors
# MAGIC FROM gold_anomaly_summary
# MAGIC GROUP BY anomaly_hour
# MAGIC ORDER BY total_anomalies DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Factory Floor Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_floor_comparison AS
# MAGIC WITH floor_health AS (
# MAGIC   SELECT
# MAGIC     floor_id,
# MAGIC     COUNT(*) AS device_count,
# MAGIC     ROUND(AVG(health_score), 1) AS avg_health_score,
# MAGIC     ROUND(MIN(health_score), 1) AS min_health_score,
# MAGIC     ROUND(MAX(health_score), 1) AS max_health_score,
# MAGIC     ROUND(AVG(anomaly_rate), 2) AS avg_anomaly_rate,
# MAGIC     SUM(anomaly_readings) AS total_anomalies,
# MAGIC     SUM(total_readings) AS total_readings,
# MAGIC     SUM(CASE WHEN health_category = 'Critical' THEN 1 ELSE 0 END) AS critical_devices,
# MAGIC     SUM(CASE WHEN health_category = 'Warning' THEN 1 ELSE 0 END) AS warning_devices,
# MAGIC     SUM(CASE WHEN health_category IN ('Good', 'Excellent') THEN 1 ELSE 0 END) AS healthy_devices
# MAGIC   FROM gold_device_health_scores
# MAGIC   GROUP BY floor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   RANK() OVER (ORDER BY avg_health_score DESC) AS health_rank,
# MAGIC   ROUND(total_anomalies * 100.0 / total_readings, 2) AS overall_anomaly_pct
# MAGIC FROM floor_health
# MAGIC ORDER BY health_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Factory floor dashboard
# MAGIC SELECT floor_id, device_count, avg_health_score, health_rank,
# MAGIC        overall_anomaly_pct, critical_devices, warning_devices, healthy_devices
# MAGIC FROM gold_floor_comparison
# MAGIC ORDER BY health_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Optional -- Live Streaming Demonstration
# MAGIC
# MAGIC This section demonstrates a continuous streaming pipeline using the Spark
# MAGIC `rate` source. It generates events in real-time and processes them through
# MAGIC the pipeline.
# MAGIC
# MAGIC **Note**: Uncomment and run these cells only on workspaces that support
# MAGIC long-running streaming queries. On Community Edition, the batch simulation
# MAGIC above covers the same concepts.

# COMMAND ----------

# # --- OPTIONAL: Live Streaming ---
# # Uncomment this cell to run a live streaming demo
#
# # Use rate source to generate events
# rate_stream = (spark.readStream
#     .format("rate")
#     .option("rowsPerSecond", 10)
#     .load()
# )
#
# # Transform rate output to sensor format
# sensor_types = ["temperature", "pressure", "vibration", "humidity"]
# means = [70.0, 30.0, 1.25, 45.0]
# stds = [4.0, 1.0, 0.3, 4.0]
#
# live_sensors = (rate_stream
#     .withColumn("device_idx", (F.col("value") % 50) + 1)
#     .withColumn("device_id", F.format_string("DEV-%03d", F.col("device_idx")))
#     .withColumn("floor_id", F.format_string("FLOOR-%s",
#         F.element_at(F.array(*[F.lit(c) for c in "ABCDE"]),
#                      ((F.col("device_idx") - 1) / 10).cast("int") + 1)))
#     .withColumn("sensor_idx", (F.col("value") % 4).cast("int"))
#     .withColumn("reading_type",
#         F.element_at(F.array(*[F.lit(s) for s in sensor_types]), F.col("sensor_idx") + 1))
#     .withColumn("base_mean",
#         F.element_at(F.array(*[F.lit(m) for m in means]), F.col("sensor_idx") + 1))
#     .withColumn("base_std",
#         F.element_at(F.array(*[F.lit(s) for s in stds]), F.col("sensor_idx") + 1))
#     .withColumn("value", F.col("base_mean") + F.randn() * F.col("base_std"))
#     .withColumn("event_time", F.col("timestamp"))
#     .select("device_id", "floor_id", "reading_type", "value", "event_time")
# )
#
# # Write to a live Bronze table
# # live_query = (live_sensors.writeStream
# #     .format("delta")
# #     .outputMode("append")
# #     .option("checkpointLocation", f"{CHECKPOINT_BASE}/live_bronze")
# #     .trigger(processingTime="10 seconds")
# #     .toTable("bronze_live_sensors")
# # )
# #
# # # Let it run for 60 seconds then stop
# # import time
# # time.sleep(60)
# # live_query.stop()
# # print(f"Live Bronze rows: {spark.table('bronze_live_sensors').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Cross-Layer Data Quality Validation

# COMMAND ----------

print("=" * 70)
print("IOT PIPELINE -- CROSS-LAYER DATA QUALITY REPORT")
print("=" * 70)

bronze_count = spark.table("bronze_sensor_readings").count()
silver_agg = spark.table("silver_sensor_5min_agg").count()
silver_anomalies = spark.table("silver_anomalies").count()
gold_health = spark.table("gold_device_health_scores").count()
gold_anomaly = spark.table("gold_anomaly_summary").count()
gold_floor = spark.table("gold_floor_comparison").count()
meta_count = spark.table("device_metadata").count()

print(f"\n{'Layer':<12} {'Table':<35} {'Rows':>8}")
print("-" * 60)
print(f"{'REF':<12} {'device_metadata':<35} {meta_count:>8}")
print(f"{'BRONZE':<12} {'bronze_sensor_readings':<35} {bronze_count:>8}")
print(f"{'SILVER':<12} {'silver_sensor_5min_agg':<35} {silver_agg:>8}")
print(f"{'SILVER':<12} {'silver_anomalies':<35} {silver_anomalies:>8}")
print(f"{'GOLD':<12} {'gold_device_health_scores':<35} {gold_health:>8}")
print(f"{'GOLD':<12} {'gold_anomaly_summary':<35} {gold_anomaly:>8}")
print(f"{'GOLD':<12} {'gold_floor_comparison':<35} {gold_floor:>8}")
print("=" * 70)

anomaly_pct = silver_anomalies / bronze_count * 100
print(f"\nAnomaly rate: {silver_anomalies}/{bronze_count} = {anomaly_pct:.1f}%")
print(f"Devices tracked: {gold_health}")
print(f"Factory floors: {gold_floor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Business Insights Summary

# COMMAND ----------

# Insight 1: Overall factory health
print("=== FACTORY HEALTH OVERVIEW ===")
spark.sql("""
    SELECT
        COUNT(*) AS total_devices,
        ROUND(AVG(health_score), 1) AS avg_health_score,
        SUM(CASE WHEN health_category = 'Excellent' THEN 1 ELSE 0 END) AS excellent,
        SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) AS good,
        SUM(CASE WHEN health_category = 'Warning' THEN 1 ELSE 0 END) AS warning,
        SUM(CASE WHEN health_category = 'Critical' THEN 1 ELSE 0 END) AS critical
    FROM gold_device_health_scores
""").show(truncate=False)

# COMMAND ----------

# Insight 2: Most problematic sensor types
print("=== ANOMALY RATE BY SENSOR TYPE ===")
spark.sql("""
    SELECT reading_type,
           COUNT(*) AS total_anomalies,
           COUNT(DISTINCT device_id) AS affected_devices,
           ROUND(COUNT(*) * 100.0 / (
               SELECT COUNT(*) FROM bronze_sensor_readings b
               WHERE b.reading_type = silver_anomalies.reading_type
           ), 1) AS anomaly_pct
    FROM silver_anomalies
    GROUP BY reading_type
    ORDER BY total_anomalies DESC
""").show(truncate=False)

# COMMAND ----------

# Insight 3: Floor ranking with details
print("=== FACTORY FLOOR RANKING ===")
spark.sql("""
    SELECT floor_id, health_rank, avg_health_score, overall_anomaly_pct,
           critical_devices, warning_devices, healthy_devices
    FROM gold_floor_comparison
    ORDER BY health_rank
""").show(truncate=False)

# COMMAND ----------

# Insight 4: Devices requiring immediate maintenance
print("=== MAINTENANCE PRIORITY LIST ===")
spark.sql("""
    SELECT h.device_id, h.floor_id, h.device_type, h.health_score,
           h.health_category, h.anomaly_rate,
           h.last_maintenance_date,
           DATEDIFF(CURRENT_DATE(), h.last_maintenance_date) AS days_since_maintenance
    FROM gold_device_health_scores h
    WHERE h.health_category IN ('Critical', 'Warning')
    ORDER BY h.health_score ASC
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Cleanup

# COMMAND ----------

# Drop all tables
tables_to_drop = [row.tableName for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()]
for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")
    print(f"  Dropped: {DATABASE}.{t}")

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE}")

# Clean up temporary paths
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
dbutils.fs.rm(STAGING_PATH, recurse=True)

print(f"\nDatabase '{DATABASE}' dropped.")
print("Checkpoint and staging directories removed.")
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC In this project you built a complete IoT Streaming Pipeline:
# MAGIC
# MAGIC | Layer | Tables | Purpose |
# MAGIC |-------|--------|---------|
# MAGIC | **Reference** | 1 table | Device metadata (type, install/maintenance dates) |
# MAGIC | **Bronze** | 1 table | Raw sensor readings (append-only, streaming ingestion) |
# MAGIC | **Silver** | 2 tables | 5-minute windowed aggregations, anomaly detection |
# MAGIC | **Gold** | 3 tables | Device health scores, anomaly dashboard, floor comparison |
# MAGIC
# MAGIC **Key techniques practiced**:
# MAGIC - Structured Streaming with `trigger(availableNow=True)` for batch simulation
# MAGIC - Watermarks for late data handling (`withWatermark`)
# MAGIC - Tumbling window aggregations (`window("event_time", "5 minutes")`)
# MAGIC - Statistical anomaly detection (mean +/- 2 standard deviations)
# MAGIC - Composite health scoring with weighted components
# MAGIC - Delta Lake as streaming sink with checkpointing
# MAGIC - Cross-layer data quality validation
