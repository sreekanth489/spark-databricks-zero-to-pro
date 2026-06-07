# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Daily Revenue Aggregates
# MAGIC
# MAGIC **Task**: `compute_gold` in the `ecommerce_medallion_pipeline` job
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads `silver_orders` written by the upstream `transform_silver` task
# MAGIC 2. Aggregates to daily, regional, category-level revenue metrics
# MAGIC 3. Writes `{catalog}.{schema}.gold_daily_revenue` — the BI-ready table

# COMMAND ----------

dbutils.widgets.text("catalog",     "dev_catalog",  "Catalog")
dbutils.widgets.text("schema",      "ecommerce",    "Schema")
dbutils.widgets.text("environment", "dev",          "Environment")

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
ENVIRONMENT = dbutils.widgets.get("environment")

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_TABLE   = f"{CATALOG}.{SCHEMA}.gold_daily_revenue"

print(f"Environment  : {ENVIRONMENT}")
print(f"Source table : {SILVER_TABLE}")
print(f"Target table : {GOLD_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Aggregates
# MAGIC
# MAGIC Gold tables are pre-aggregated for BI tools and dashboards.
# MAGIC Grain: one row per (order_date, region, category, channel).

# COMMAND ----------

gold_df = (
    spark.table(SILVER_TABLE)
    .filter(F.col("status").isin("DELIVERED", "SHIPPED", "CONFIRMED"))  # revenue-generating statuses
    .groupBy("order_date", "region", "category", "channel")
    .agg(
        F.count("order_id")                    .alias("order_count"),
        F.countDistinct("customer_id")         .alias("unique_customers"),
        F.sum("quantity")                      .alias("total_units_sold"),
        F.round(F.sum("gross_revenue"), 2)     .alias("gross_revenue"),
        F.round(F.sum("net_revenue"), 2)       .alias("net_revenue"),
        F.round(F.avg("net_revenue"), 2)       .alias("avg_order_value"),
        F.round(F.avg("discount_pct") * 100, 2).alias("avg_discount_pct"),
        F.round(
            (F.sum("gross_revenue") - F.sum("net_revenue"))
            / F.sum("gross_revenue") * 100, 2
        ).alias("discount_impact_pct"),
    )
    .withColumn("_gold_computed_at",   F.current_timestamp())
    .withColumn("_gold_environment",   F.lit(ENVIRONMENT))
    .orderBy("order_date", "region", "category")
)

print(f"Gold rows: {gold_df.count():,}")
gold_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Table

# COMMAND ----------

(gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE))

print(f"Wrote {spark.table(GOLD_TABLE).count():,} rows to {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top-Line Revenue Summary

# COMMAND ----------

spark.sql(f"""
    SELECT
        region,
        ROUND(SUM(gross_revenue), 2)  AS total_gross_revenue,
        ROUND(SUM(net_revenue), 2)    AS total_net_revenue,
        SUM(order_count)              AS total_orders,
        SUM(unique_customers)         AS total_customers,
        ROUND(AVG(avg_order_value),2) AS avg_order_value
    FROM {GOLD_TABLE}
    GROUP BY region
    ORDER BY total_net_revenue DESC
""").show(truncate=False)

print(f"\nGold table {GOLD_TABLE} ready for BI consumption.")
print(f"Pipeline complete for environment: {ENVIRONMENT}")
