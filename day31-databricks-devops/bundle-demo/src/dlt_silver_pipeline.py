# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # DLT Silver Pipeline: Orders
# MAGIC
# MAGIC **Resource**: `orders_dlt_pipeline` in resources/pipelines.yml
# MAGIC
# MAGIC This notebook defines a DLT (Lakeflow Declarative Pipelines) silver layer.
# MAGIC It is the streaming/DLT equivalent of src/silver_transform.py.
# MAGIC
# MAGIC Configuration is injected via the pipeline's `configuration:` block in
# MAGIC pipelines.yml and accessed with `spark.conf.get("pipelines.<key>")`.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Read pipeline configuration injected by the bundle
CATALOG     = spark.conf.get("pipelines.catalog",     "dev_catalog")
SCHEMA      = spark.conf.get("pipelines.schema",      "ecommerce")
ENVIRONMENT = spark.conf.get("pipelines.environment", "dev")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"

print(f"DLT pipeline running in environment: {ENVIRONMENT}")
print(f"Reading from: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Table: silver_orders_validated
# MAGIC
# MAGIC Applies data quality expectations. In production DLT mode (when
# MAGIC `development: false` in pipelines.yml), rows that violate expectations
# MAGIC are quarantined rather than silently dropped.

# COMMAND ----------

@dlt.table(
    name="silver_orders_validated",
    comment="Bronze orders with quality checks applied. Source of truth for gold aggregations.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "unit_price > 0")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect("known_status",
             "status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED','REFUNDED')")
def silver_orders_validated():
    return (
        dlt.read(BRONZE_TABLE)
        .withColumn("gross_revenue",
                    F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("net_revenue",
                    F.round(F.col("unit_price") * F.col("quantity")
                            * (1 - F.col("discount_pct")), 2))
        .withColumn("order_date",  F.to_date("order_ts"))
        .withColumn("status",      F.upper("status"))
        .withColumn("_dlt_env",    F.lit(ENVIRONMENT))
        .drop("_ingested_at", "_source", "_pipeline_run")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Table: silver_orders_delivered
# MAGIC
# MAGIC Subset containing only revenue-generating orders (delivered + shipped).
# MAGIC Used by the gold layer for revenue metrics.

# COMMAND ----------

@dlt.table(
    name="silver_orders_delivered",
    comment="Delivered and shipped orders only — revenue-generating subset for gold aggregation.",
    table_properties={"quality": "silver"},
)
def silver_orders_delivered():
    return (
        dlt.read("silver_orders_validated")
        .filter(F.col("status").isin("DELIVERED", "SHIPPED", "CONFIRMED"))
    )
