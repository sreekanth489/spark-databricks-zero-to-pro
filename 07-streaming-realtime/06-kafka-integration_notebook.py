# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Kafka Integration -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 06 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Kafka source/sink configuration (reference templates)
# MAGIC 2. Simulated Kafka data using rate source with Kafka-like schema
# MAGIC 3. JSON deserialization patterns (from_json with schema)
# MAGIC 4. Dead-letter queue pattern for unparseable messages
# MAGIC 5. End-to-end simulated Kafka-to-Delta pipeline
# MAGIC
# MAGIC **Note**: Kafka requires an external cluster. This notebook provides
# MAGIC simulated data that mirrors Kafka's schema so you can practice
# MAGIC deserialization and processing patterns without a live Kafka cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BinaryType
)
import json
import time

BASE_PATH = "/tmp/module07_topic06"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"
BRONZE_PATH = f"{BASE_PATH}/bronze"
DLQ_PATH = f"{BASE_PATH}/dlq"

dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_PATH)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Kafka Source Configuration [REFERENCE]
# MAGIC
# MAGIC These are production-ready configuration templates for connecting
# MAGIC Spark Structured Streaming to a Kafka cluster. They require a live
# MAGIC Kafka cluster and will not run without one.

# COMMAND ----------

# -- REFERENCE CODE: Reading from Kafka --
# Uncomment and configure when you have a Kafka cluster available.

# === Basic Kafka Source ===
# kafka_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092")
#     .option("subscribe", "orders")                # single topic
#     # .option("subscribe", "orders,clicks,events") # multiple topics
#     # .option("subscribePattern", "events-.*")     # topic pattern (regex)
#     .option("startingOffsets", "earliest")         # earliest | latest | specific offsets
#     .option("maxOffsetsPerTrigger", 100000)        # rate limit
#     .option("kafka.group.id", "spark-consumer-group-01")
#     .load()
# )

# === Kafka with SASL/SSL Authentication ===
# kafka_df_secure = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "broker:9093")
#     .option("subscribe", "orders")
#     .option("kafka.security.protocol", "SASL_SSL")
#     .option("kafka.sasl.mechanism", "PLAIN")
#     .option("kafka.sasl.jaas.config",
#         'org.apache.kafka.common.security.plain.PlainLoginModule required '
#         f'username="{dbutils.secrets.get("kafka", "api-key")}" '
#         f'password="{dbutils.secrets.get("kafka", "api-secret")}";')
#     .option("kafka.ssl.endpoint.identification.algorithm", "https")
#     .load()
# )

# === Azure Event Hubs (Kafka protocol) ===
# eh_connection = dbutils.secrets.get("eventhubs", "connection-string")
# kafka_df_eh = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "<namespace>.servicebus.windows.net:9093")
#     .option("subscribe", "orders")
#     .option("kafka.security.protocol", "SASL_SSL")
#     .option("kafka.sasl.mechanism", "PLAIN")
#     .option("kafka.sasl.jaas.config",
#         'org.apache.kafka.common.security.plain.PlainLoginModule required '
#         f'username="$ConnectionString" password="{eh_connection}";')
#     .load()
# )

print("Cell 1: Kafka connection templates (reference code).")
print("Configure bootstrap.servers and authentication for your environment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Simulate Kafka-Like Data
# MAGIC
# MAGIC We create a stream that mirrors Kafka's schema: binary key, binary value,
# MAGIC topic name, partition, offset, and timestamp. This lets us practice
# MAGIC deserialization without a live Kafka cluster.

# COMMAND ----------

# Simulate Kafka messages using rate source + transformation
simulated_kafka = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    # Transform to match Kafka's schema
    .withColumn("order_data", F.struct(
        F.concat(F.lit("ORD-"), F.col("value").cast("string")).alias("order_id"),
        F.concat(F.lit("CUST-"), (F.col("value") % 100).cast("string")).alias("customer_id"),
        F.round(F.rand() * 500 + 10, 2).alias("amount"),
        F.array(
            F.when(F.col("value") % 3 == 0, "laptop")
             .when(F.col("value") % 3 == 1, "phone")
             .otherwise("tablet")
        ).alias("items"),
        F.col("timestamp").cast("string").alias("order_time")
    ))
    # Serialize to JSON and then to binary (mimicking Kafka's wire format)
    .withColumn("key", F.col("value").cast("string").cast("binary"))
    .withColumn("value", F.to_json(F.col("order_data")).cast("binary"))
    .withColumn("topic", F.lit("orders"))
    .withColumn("partition", (F.col("value").cast("int") % 3).cast("int"))
    .withColumn("offset", F.col("value").cast("long"))
    .withColumn("timestamp", F.col("timestamp"))
    .withColumn("timestampType", F.lit(0).cast("int"))
    .select("key", "value", "topic", "partition", "offset", "timestamp", "timestampType")
)

print("Simulated Kafka schema:")
simulated_kafka.printSchema()

# Write to memory for inspection
query_kafka_sim = (
    simulated_kafka.writeStream
    .format("memory")
    .queryName("simulated_kafka")
    .outputMode("append")
    .start()
)

time.sleep(5)

print("\nRaw Kafka-like messages (key and value are binary):")
spark.sql("""
    SELECT
        CAST(key AS STRING) AS key_str,
        CAST(value AS STRING) AS value_str,
        topic, partition, offset, timestamp
    FROM simulated_kafka
    ORDER BY timestamp DESC LIMIT 5
""").show(truncate=False)

query_kafka_sim.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: JSON Deserialization -- The Core Pattern
# MAGIC
# MAGIC Kafka value is always binary. The standard pattern is:
# MAGIC 1. Cast binary to string
# MAGIC 2. Parse JSON string with `from_json()` and a schema
# MAGIC 3. Flatten the parsed struct into columns

# COMMAND ----------

# Define the expected JSON schema for order messages
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("items", StringType(), True),  # simplified for demo
    StructField("order_time", StringType(), True),
])

# Recreate simulated Kafka stream
kafka_raw = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("order_json", F.to_json(F.struct(
        F.concat(F.lit("ORD-"), F.col("value").cast("string")).alias("order_id"),
        F.concat(F.lit("CUST-"), (F.col("value") % 100).cast("string")).alias("customer_id"),
        F.round(F.rand() * 500 + 10, 2).alias("amount"),
        F.when(F.col("value") % 3 == 0, "laptop")
         .when(F.col("value") % 3 == 1, "phone")
         .otherwise("tablet").alias("items"),
        F.col("timestamp").cast("string").alias("order_time")
    )))
    # Simulate Kafka binary value
    .withColumn("kafka_value", F.col("order_json").cast("binary"))
    .withColumn("kafka_key", F.col("value").cast("string").cast("binary"))
    .withColumn("kafka_topic", F.lit("orders"))
    .withColumn("kafka_offset", F.col("value").cast("long"))
    .withColumn("kafka_timestamp", F.col("timestamp"))
    .select("kafka_key", "kafka_value", "kafka_topic", "kafka_offset", "kafka_timestamp")
)

# === THE DESERIALIZATION PATTERN ===
parsed_orders = (
    kafka_raw
    # Step 1: Cast binary value to string
    .withColumn("value_string", F.col("kafka_value").cast("string"))
    # Step 2: Parse JSON string with schema
    .withColumn("parsed", F.from_json(F.col("value_string"), order_schema))
    # Step 3: Flatten the struct into individual columns
    .select(
        F.col("kafka_key").cast("string").alias("key"),
        F.col("parsed.order_id"),
        F.col("parsed.customer_id"),
        F.col("parsed.amount"),
        F.col("parsed.items"),
        F.col("parsed.order_time"),
        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("source_timestamp"),
    )
)

query_parsed = (
    parsed_orders.writeStream
    .format("memory")
    .queryName("parsed_orders")
    .outputMode("append")
    .start()
)

time.sleep(8)

print("=== Deserialized Kafka Messages ===")
spark.sql("""
    SELECT order_id, customer_id, amount, items, order_time,
           source_topic, source_offset
    FROM parsed_orders
    ORDER BY source_offset DESC
    LIMIT 10
""").show(truncate=False)

query_parsed.stop()
print("Parsing query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Writing to Kafka [REFERENCE]
# MAGIC
# MAGIC To produce messages back to Kafka, create a DataFrame with `key` (optional)
# MAGIC and `value` columns, both as strings or binary.

# COMMAND ----------

# -- REFERENCE CODE: Writing to Kafka --
# This requires a live Kafka cluster.

# === Write to a single topic ===
# (
#     processed_df
#     .select(
#         F.col("customer_id").cast("string").alias("key"),
#         F.to_json(F.struct(
#             "order_id", "customer_id", "amount",
#             "processed_at", "status"
#         )).alias("value")
#     )
#     .writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "broker1:9092")
#     .option("topic", "processed-orders")
#     .option("checkpointLocation", "/mnt/checkpoints/kafka-sink/")
#     .start()
# )

# === Write to dynamic topics (topic column in DataFrame) ===
# (
#     routed_df
#     .select(
#         F.col("key").cast("string"),
#         F.col("value").cast("string"),
#         F.col("target_topic").alias("topic")  # each row specifies its topic
#     )
#     .writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "broker1:9092")
#     .option("checkpointLocation", "/mnt/checkpoints/kafka-router/")
#     .start()
# )

print("Cell 4: Kafka sink templates (reference code).")
print("Key requirement: DataFrame must have 'value' column (string or binary).")
print("Optional: 'key' column for partitioning, 'topic' column for routing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Dead-Letter Queue Pattern
# MAGIC
# MAGIC In production, some Kafka messages may be unparseable (corrupt JSON,
# MAGIC unexpected schema). Route these to a dead-letter queue instead of
# MAGIC failing the entire pipeline.

# COMMAND ----------

# Create a stream with both valid and invalid JSON messages
mixed_messages = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("kafka_value",
        F.when(F.col("value") % 7 == 0,
               # Every 7th message is malformed JSON
               F.lit('{"order_id": "BAD", "amount": "not_a_number"'))
        .when(F.col("value") % 13 == 0,
               # Every 13th message is completely corrupt
               F.lit("this is not json at all!!!"))
        .otherwise(
               # Normal valid JSON
               F.to_json(F.struct(
                   F.concat(F.lit("ORD-"), F.col("value").cast("string")).alias("order_id"),
                   F.concat(F.lit("CUST-"), (F.col("value") % 50).cast("string")).alias("customer_id"),
                   F.round(F.rand() * 500, 2).alias("amount"),
                   F.col("timestamp").cast("string").alias("order_time")
               ))
        )
    )
    .withColumn("kafka_offset", F.col("value").cast("long"))
    .select("kafka_value", "kafka_offset", "timestamp")
)

# Parse with error handling using from_json (returns null on parse failure)
dlq_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_time", StringType(), True),
])

parsed_with_dlq = (
    mixed_messages
    .withColumn("parsed", F.from_json(F.col("kafka_value"), dlq_schema))
    # Mark rows where parsing failed (parsed struct is all-null or key fields are null)
    .withColumn("is_valid",
        F.col("parsed.order_id").isNotNull() & F.col("parsed.amount").isNotNull())
)

# Route valid and invalid messages to separate sinks using foreachBatch
def process_with_dlq(batch_df, batch_id):
    """Split batch into valid records and dead-letter records."""
    # Valid records -> Bronze table
    valid = (
        batch_df.filter(F.col("is_valid"))
        .select(
            F.col("parsed.order_id"),
            F.col("parsed.customer_id"),
            F.col("parsed.amount"),
            F.col("parsed.order_time"),
            F.col("kafka_offset"),
            F.current_timestamp().alias("_ingestion_time")
        )
    )
    if valid.count() > 0:
        valid.write.format("delta").mode("append").save(BRONZE_PATH)

    # Invalid records -> Dead-letter queue
    invalid = (
        batch_df.filter(~F.col("is_valid"))
        .select(
            F.col("kafka_value").alias("raw_message"),
            F.col("kafka_offset"),
            F.col("timestamp").alias("received_at"),
            F.lit("parse_failure").alias("error_reason"),
            F.current_timestamp().alias("_dlq_time")
        )
    )
    if invalid.count() > 0:
        invalid.write.format("delta").mode("append").save(DLQ_PATH)

query_dlq = (
    parsed_with_dlq.writeStream
    .foreachBatch(process_with_dlq)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/dlq_pipeline")
    .trigger(processingTime="5 seconds")
    .start()
)

time.sleep(20)
query_dlq.stop()

# Inspect results
print("=== Valid Records (Bronze) ===")
try:
    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Valid records: {df_bronze.count()}")
    df_bronze.show(5, truncate=False)
except Exception:
    print("No valid records written (short run).")

print("\n=== Dead-Letter Queue ===")
try:
    df_dlq = spark.read.format("delta").load(DLQ_PATH)
    print(f"Failed records: {df_dlq.count()}")
    df_dlq.show(5, truncate=False)
except Exception:
    print("No DLQ records written (short run).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Offset Management Concepts

# COMMAND ----------

print("=== Kafka Offset Management ===\n")

offset_configs = [
    ("startingOffsets", "earliest",
     "Read from the beginning of the topic (first run only)"),
    ("startingOffsets", "latest",
     "Read only new messages arriving after query starts (default)"),
    ("startingOffsets", '{"topic": {"0": 100}}',
     "Start from specific offset per partition"),
    ("maxOffsetsPerTrigger", "100000",
     "Max messages per micro-batch (rate limiting)"),
    ("minOffsetsPerTrigger", "1000",
     "Min messages to wait for before processing"),
    ("failOnDataLoss", "true",
     "Fail if offsets are out of range (data deleted from Kafka)"),
]

for option, value, description in offset_configs:
    print(f"  .option(\"{option}\", \"{value}\")")
    print(f"    --> {description}\n")

print("IMPORTANT: startingOffsets only applies to the FIRST run.")
print("After that, the checkpoint tracks offsets across restarts.")
print("Deleting the checkpoint resets the consumer position.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: End-to-End Simulated Pipeline -- Kafka to Delta

# COMMAND ----------

E2E_BRONZE = f"{BASE_PATH}/e2e_bronze"
E2E_SILVER = f"{BASE_PATH}/e2e_silver"

# Simulate Kafka source
e2e_source = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("kafka_value", F.to_json(F.struct(
        F.concat(F.lit("TXN-"), F.col("value").cast("string")).alias("txn_id"),
        F.concat(F.lit("ACCT-"), (F.col("value") % 50).cast("string")).alias("account_id"),
        F.round(F.rand() * 1000, 2).alias("amount"),
        F.when(F.col("value") % 4 == 0, "transfer")
         .when(F.col("value") % 4 == 1, "payment")
         .when(F.col("value") % 4 == 2, "deposit")
         .otherwise("withdrawal").alias("txn_type"),
        F.col("timestamp").cast("string").alias("txn_time")
    )))
)

# Bronze layer: raw parsed data
txn_schema = StructType([
    StructField("txn_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("txn_type", StringType()),
    StructField("txn_time", StringType()),
])

bronze_stream = (
    e2e_source
    .withColumn("parsed", F.from_json(F.col("kafka_value"), txn_schema))
    .select(
        "parsed.*",
        F.current_timestamp().alias("_bronze_time"),
        F.col("kafka_value").alias("_raw_json")
    )
)

query_e2e_bronze = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/e2e_bronze")
    .start(E2E_BRONZE)
)

time.sleep(15)
query_e2e_bronze.stop()

# Read Bronze and write Silver (clean, enriched)
df_bronze = spark.read.format("delta").load(E2E_BRONZE)
print(f"=== Bronze Layer: {df_bronze.count()} raw records ===")
df_bronze.show(5, truncate=False)

# Silver: clean, validate, and enrich
df_silver = (
    df_bronze
    .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    .withColumn("txn_time_parsed", F.to_timestamp("txn_time"))
    .withColumn("amount_category",
        F.when(F.col("amount") < 100, "small")
         .when(F.col("amount") < 500, "medium")
         .otherwise("large"))
    .select("txn_id", "account_id", "amount", "txn_type",
            "txn_time_parsed", "amount_category", "_bronze_time")
)

df_silver.write.format("delta").mode("overwrite").save(E2E_SILVER)
print(f"\n=== Silver Layer: {df_silver.count()} clean records ===")
df_silver.show(5, truncate=False)

# Analytics
print("=== Transaction Summary ===")
df_silver.groupBy("txn_type", "amount_category").agg(
    F.count("*").alias("count"),
    F.round(F.sum("amount"), 2).alias("total"),
    F.round(F.avg("amount"), 2).alias("avg")
).orderBy("txn_type", "amount_category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Kafka Configuration Cheat Sheet

# COMMAND ----------

configs = [
    ("kafka.bootstrap.servers", "broker:9092", "Kafka broker addresses"),
    ("subscribe", "topic1,topic2", "Topics to read from"),
    ("subscribePattern", "events-.*", "Topic regex pattern"),
    ("assign", '{"topic":[0,1]}', "Specific topic-partitions"),
    ("startingOffsets", "earliest", "Where to start reading (first run)"),
    ("maxOffsetsPerTrigger", "100000", "Rate limit per micro-batch"),
    ("kafka.group.id", "my-group", "Consumer group ID"),
    ("kafka.security.protocol", "SASL_SSL", "Authentication protocol"),
    ("kafka.sasl.mechanism", "PLAIN", "SASL mechanism"),
    ("failOnDataLoss", "true", "Fail on out-of-range offsets"),
    ("includeHeaders", "true", "Include Kafka message headers"),
]

df_configs = spark.createDataFrame(
    configs, schema=["option", "example_value", "description"]
)
print("=== Kafka Source Configuration Reference ===")
df_configs.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Exactly-Once Semantics Summary

# COMMAND ----------

print("=== Exactly-Once Processing Chain ===\n")

print("1. KAFKA SOURCE")
print("   - Offsets tracked in Spark checkpoint (not Kafka consumer group)")
print("   - On restart, Spark replays from the last committed offset")
print()

print("2. SPARK PROCESSING")
print("   - Deterministic processing: same input -> same output")
print("   - Checkpoint stores processing state (aggregations, joins)")
print()

print("3. DELTA SINK")
print("   - Idempotent writes: replaying a batch produces the same result")
print("   - Transaction log prevents duplicate data")
print("   - Combined with checkpoint, guarantees exactly-once end-to-end")
print()

print("4. FAILURE RECOVERY")
print("   - Driver failure: New driver reads checkpoint, resumes from last offset")
print("   - Executor failure: Task retried; idempotent write prevents dupes")
print("   - Kafka broker failure: Spark retries reads from replicas")
print()

print("IMPORTANT: Exactly-once requires:")
print("  - Checkpoint on durable storage (not local disk)")
print("  - Deterministic processing logic")
print("  - Idempotent sink (Delta Lake, or foreachBatch with merge)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
    print(f"Stopped: {q.name}")

dbutils.fs.rm(BASE_PATH, recurse=True)
print("All temporary data cleaned up.")
print("Notebook complete.")
