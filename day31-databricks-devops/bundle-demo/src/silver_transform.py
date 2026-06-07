# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Orders Transformation
# MAGIC
# MAGIC **Task**: `transform_silver` in the `ecommerce_medallion_pipeline` job
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads `bronze_orders` written by the upstream `ingest_bronze` task
# MAGIC 2. Applies data quality filters (drops nulls and bad prices)
# MAGIC 3. Computes derived columns (`total_price`, `revenue_after_discount`)
# MAGIC 4. Writes `{catalog}.{schema}.silver_orders`

# COMMAND ----------

dbutils.widgets.text("catalog",     "dev_catalog",  "Catalog")
dbutils.widgets.text("schema",      "ecommerce",    "Schema")
dbutils.widgets.text("environment", "dev",          "Environment")

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
ENVIRONMENT = dbutils.widgets.get("environment")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"

print(f"Environment  : {ENVIRONMENT}")
print(f"Source table : {BRONZE_TABLE}")
print(f"Target table : {SILVER_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
total_bronze = bronze_df.count()
print(f"Bronze rows: {total_bronze:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Data Quality Rules
# MAGIC
# MAGIC Silver layer enforces the quality contract that downstream gold consumers depend on.

# COMMAND ----------

silver_df = (
    bronze_df
    # Rule 1: drop rows with no customer identifier
    .filter(F.col("customer_id").isNotNull())
    # Rule 2: drop rows with invalid prices (negative or zero)
    .filter(F.col("unit_price") > 0)
    # Rule 3: drop rows with invalid quantities
    .filter(F.col("quantity") > 0)
    # Derive: total price before discount
    .withColumn("gross_revenue",
                F.round(F.col("unit_price") * F.col("quantity"), 2))
    # Derive: actual revenue after discount
    .withColumn("net_revenue",
                F.round(F.col("unit_price") * F.col("quantity")
                        * (1 - F.col("discount_pct")), 2))
    # Derive: order date (date part of timestamp) for partitioning
    .withColumn("order_date", F.to_date("order_ts"))
    # Standardise status to uppercase
    .withColumn("status", F.upper("status"))
    # Drop bronze metadata columns — silver has its own lineage columns
    .drop("_ingested_at", "_source", "_pipeline_run")
    # Add silver-layer lineage
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_silver_environment",  F.lit(ENVIRONMENT))
)

total_silver = silver_df.count()
dropped      = total_bronze - total_silver
print(f"Rows after quality filters: {total_silver:,}  (dropped {dropped:,} = {dropped/total_bronze*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Table
# MAGIC
# MAGIC Partitioned by `order_date` for efficient time-based queries.

# COMMAND ----------

(silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")
    .saveAsTable(SILVER_TABLE))

print(f"Wrote {spark.table(SILVER_TABLE).count():,} rows to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Quality Report

# COMMAND ----------

spark.sql(f"""
    SELECT
        status,
        COUNT(*)                         AS order_count,
        ROUND(SUM(gross_revenue), 2)     AS total_gross_revenue,
        ROUND(SUM(net_revenue), 2)       AS total_net_revenue,
        ROUND(AVG(discount_pct) * 100, 1) AS avg_discount_pct
    FROM {SILVER_TABLE}
    GROUP BY status
    ORDER BY order_count DESC
""").show(truncate=False)

print(f"\nSilver table {SILVER_TABLE} ready for gold aggregation.")
