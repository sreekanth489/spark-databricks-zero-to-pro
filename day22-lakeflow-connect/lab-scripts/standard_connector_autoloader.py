# standard_connector_autoloader.py
# Auto Loader ingestion script -- Standard Connector pattern for cloud files
#
# This script reads CSV files from S3 using the cloudFiles format (Auto Loader),
# infers schema with rescue mode, and writes to a Unity Catalog managed table.
#
# Usage: Run in a Databricks notebook or as a job task.

from pyspark.sql.functions import col, current_timestamp, lit

# ── Configuration ──────────────────────────────────────────────────────────────

SOURCE_PATH = "s3://ecommerce-lakehouse/raw/products/"
SCHEMA_LOCATION = "s3://ecommerce-lakehouse/schemas/products"
CHECKPOINT_LOCATION = "s3://ecommerce-lakehouse/checkpoints/products"
TARGET_TABLE = "ecommerce.bronze.products"

# ── Auto Loader Stream ────────────────────────────────────────────────────────

stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Rescue column captures data that does not match the inferred schema
    .option("rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingestion_method", lit("autoloader"))
)

# ── Write to Unity Catalog ────────────────────────────────────────────────────

query = (
    stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # Process all available files, then stop
    .toTable(TARGET_TABLE)
)

query.awaitTermination()
print(f"Auto Loader ingestion complete -> {TARGET_TABLE}")
