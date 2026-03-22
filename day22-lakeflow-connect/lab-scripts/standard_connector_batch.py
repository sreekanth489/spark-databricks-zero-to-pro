# standard_connector_batch.py
# Batch ingestion script -- Standard Connector pattern for JDBC sources
#
# This script reads from a PostgreSQL database using JDBC (full load pattern)
# and writes the result to a Unity Catalog managed table.
#
# Prerequisites:
#   - PostgreSQL JDBC driver available on the cluster
#   - Databricks secret scope 'jdbc-secrets' configured with username and password
#
# Usage: Run in a Databricks notebook or as a job task.

from pyspark.sql.functions import current_timestamp, lit

# ── Configuration ──────────────────────────────────────────────────────────────

JDBC_URL = "jdbc:postgresql://ecommerce-db.example.com:5432/ecommerce"
SECRET_SCOPE = "jdbc-secrets"
SOURCE_TABLE = "public.products"
TARGET_TABLE = "ecommerce.bronze.products_jdbc"

# ── Read from PostgreSQL ──────────────────────────────────────────────────────

jdbc_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", SOURCE_TABLE)
    .option("user", dbutils.secrets.get(scope=SECRET_SCOPE, key="pg-username"))
    .option("password", dbutils.secrets.get(scope=SECRET_SCOPE, key="pg-password"))
    .option("driver", "org.postgresql.Driver")
    # Performance: partition the read for large tables
    .option("numPartitions", "4")
    .option("fetchsize", "10000")
    .load()
)

# ── Add ingestion metadata ────────────────────────────────────────────────────

enriched_df = (
    jdbc_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_ingestion_method", lit("jdbc_batch"))
    .withColumn("_source_system", lit("postgresql"))
)

# ── Write to Unity Catalog (full overwrite) ───────────────────────────────────

enriched_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"Batch ingestion complete: {SOURCE_TABLE} -> {TARGET_TABLE}")
print(f"Records loaded: {enriched_df.count()}")
