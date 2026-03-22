# standard_connector_kafka.py
# Kafka streaming ingestion script -- Standard Connector pattern
#
# This script reads from a Kafka topic, parses JSON messages, and writes
# to a Unity Catalog streaming table.
#
# Prerequisites:
#   - Kafka cluster accessible from Databricks
#   - Databricks secret scope 'kafka-secrets' configured (if using SASL)
#
# Usage: Run in a Databricks notebook or as a job task.

from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

# ── Configuration ──────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "kafka-broker.example.com:9092"
KAFKA_TOPIC = "ecommerce.order_events"
CHECKPOINT_LOCATION = "s3://ecommerce-lakehouse/checkpoints/kafka-orders"
TARGET_TABLE = "ecommerce.bronze.order_events"

# ── Define the expected schema for Kafka message values ───────────────────────

order_event_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("order_status", StringType(), False),
    StructField("event_time", TimestampType(), False),
])

# ── Read from Kafka ──────────────────────────────────────────────────────────

kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    # Uncomment for SASL/SSL authentication:
    # .option("kafka.security.protocol", "SASL_SSL")
    # .option("kafka.sasl.mechanism", "PLAIN")
    # .option("kafka.sasl.jaas.config",
    #     f"org.apache.kafka.common.security.plain.PlainLoginModule required "
    #     f"username='{dbutils.secrets.get('kafka-secrets', 'username')}' "
    #     f"password='{dbutils.secrets.get('kafka-secrets', 'password')}';")
    .load()
)

# ── Parse JSON messages ──────────────────────────────────────────────────────

parsed_stream = (
    kafka_raw
    .select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), order_event_schema).alias("data"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "kafka_key",
        "data.*",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_ingestion_method", lit("kafka_streaming"))
)

# ── Write to Unity Catalog ──────────────────────────────────────────────────

query = (
    parsed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("append")
    .toTable(TARGET_TABLE)
)

# In production, this runs continuously.
# For testing, you can use: .trigger(availableNow=True)
query.awaitTermination()
