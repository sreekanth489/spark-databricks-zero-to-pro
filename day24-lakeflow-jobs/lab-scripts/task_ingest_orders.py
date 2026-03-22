"""
task_ingest_orders.py -- Ingest raw order data from S3 into the Bronze layer.

This script is designed to run as a task within a Lakeflow Job. It reads
JSON files from an S3 landing zone and writes them to a Bronze Delta table
with minimal transformation (add ingestion metadata only).

Parameters (passed via job configuration):
  - catalog:      Unity Catalog name (default: ecommerce)
  - source_path:  S3 path to raw order files (default: s3://ecommerce-lakehouse/raw/orders/)
  - target_table: Fully qualified table name (default: ecommerce.bronze.orders)

Usage in a Lakeflow Job:
  Task type: Python script
  Parameters: catalog=ecommerce, source_path=s3://..., target_table=ecommerce.bronze.orders
"""

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit


def ingest_orders(
    spark: SparkSession,
    source_path: str,
    target_table: str,
    catalog: str,
) -> int:
    """Read raw order files from S3 and write to a Bronze Delta table.

    Args:
        spark: Active SparkSession.
        source_path: S3 path containing raw JSON order files.
        target_table: Fully qualified Delta table name (catalog.schema.table).
        catalog: Unity Catalog name to use.

    Returns:
        Number of records ingested.
    """
    # Set the catalog
    spark.sql(f"USE CATALOG {catalog}")

    # Read raw JSON files from S3
    raw_df = (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .load(source_path)
    )

    # Add ingestion metadata columns
    enriched_df = (
        raw_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_run_id", lit(datetime.utcnow().strftime("%Y%m%d_%H%M%S")))
    )

    record_count = enriched_df.count()

    # Write to Bronze table (append mode for incremental ingestion)
    (
        enriched_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    return record_count


def main():
    """Entry point for the ingestion task."""
    spark = SparkSession.builder.getOrCreate()

    # Parse parameters (from job configuration or defaults)
    catalog = spark.conf.get("spark.databricks.task.catalog", "ecommerce")
    source_path = spark.conf.get(
        "spark.databricks.task.source_path",
        "s3://ecommerce-lakehouse/raw/orders/",
    )
    target_table = spark.conf.get(
        "spark.databricks.task.target_table",
        "ecommerce.bronze.orders",
    )

    print(f"Starting order ingestion")
    print(f"  Source:  {source_path}")
    print(f"  Target:  {target_table}")
    print(f"  Catalog: {catalog}")

    record_count = ingest_orders(
        spark=spark,
        source_path=source_path,
        target_table=target_table,
        catalog=catalog,
    )

    print(f"Ingestion complete: {record_count} records written to {target_table}")


if __name__ == "__main__":
    main()
