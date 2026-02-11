# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Sharing
# MAGIC > Module 03 -- Topic 08 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a Delta table suitable for sharing
# MAGIC 2. Learn the SQL commands for creating shares and recipients
# MAGIC 3. Understand the sharing workflow end-to-end
# MAGIC 4. Explore recipient-side access patterns
# MAGIC
# MAGIC **Note**: Full Delta Sharing requires a Unity Catalog-enabled workspace.
# MAGIC This notebook demonstrates the syntax and concepts. Commands that require
# MAGIC UC admin privileges are shown as reference with explanatory comments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Sample Data for Sharing
# MAGIC
# MAGIC We create a Gold-layer table that a provider might want to share
# MAGIC with an external partner.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType
)
import random

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.gold_regional_sales")
spark.sql("DROP TABLE IF EXISTS module03.gold_product_catalog")

# Generate regional sales summary (Gold-layer quality)
random.seed(42)
regions = ["US-West", "US-East", "EU-West", "EU-East", "APAC"]
products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor"]

sales_data = []
for day in range(1, 29):
    for region in regions:
        for product in products:
            sales_data.append((
                f"2025-01-{day:02d}",
                region,
                product,
                random.randint(10, 500),
                round(random.uniform(5000, 250000), 2),
            ))

sales_schema = StructType([
    StructField("sale_date", StringType()),
    StructField("region", StringType()),
    StructField("product", StringType()),
    StructField("units_sold", IntegerType()),
    StructField("revenue", DoubleType()),
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df = sales_df.withColumn("sale_date", F.to_date("sale_date"))

sales_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("module03.gold_regional_sales")

# Product catalog reference table
catalog_data = [
    (1, "Laptop", "Electronics", 999.99, True),
    (2, "Phone", "Electronics", 699.99, True),
    (3, "Tablet", "Electronics", 449.99, True),
    (4, "Headphones", "Audio", 149.99, True),
    (5, "Monitor", "Electronics", 349.99, True),
]

catalog_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("list_price", DoubleType()),
    StructField("is_active", StringType()),
])

spark.createDataFrame(catalog_data, catalog_schema).write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("module03.gold_product_catalog")

print("Gold tables created:")
print(f"  gold_regional_sales:  {spark.table('module03.gold_regional_sales').count()} rows")
print(f"  gold_product_catalog: {spark.table('module03.gold_product_catalog').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Understanding the Sharing Workflow
# MAGIC
# MAGIC ```
# MAGIC Provider Side:                    Recipient Side:
# MAGIC =============                     ==============
# MAGIC 1. CREATE SHARE                   4. Receive activation link
# MAGIC 2. ADD TABLE(s) to share          5. Activate --> get credential file
# MAGIC 3. CREATE RECIPIENT               6. Use credential to query data
# MAGIC    --> generates activation link
# MAGIC    --> GRANT share to recipient
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Creating a Share (Provider Side)
# MAGIC
# MAGIC The following SQL commands show how to create and configure a share.
# MAGIC These require Unity Catalog admin privileges.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- REFERENCE: Share creation commands (requires UC admin)
# MAGIC -- Uncomment and run in a UC-enabled workspace
# MAGIC
# MAGIC -- Step 1: Create the share
# MAGIC CREATE SHARE IF NOT EXISTS regional_analytics_share
# MAGIC COMMENT 'Regional sales data shared with analytics partners';
# MAGIC
# MAGIC -- Step 2: Add tables to the share
# MAGIC ALTER SHARE regional_analytics_share
# MAGIC ADD TABLE module03.gold_regional_sales;
# MAGIC
# MAGIC ALTER SHARE regional_analytics_share
# MAGIC ADD TABLE module03.gold_product_catalog;
# MAGIC
# MAGIC -- Step 3: Add a table with partition filter
# MAGIC -- (recipient only sees US-West data)
# MAGIC ALTER SHARE regional_analytics_share
# MAGIC ADD TABLE module03.gold_regional_sales
# MAGIC   PARTITION (region = 'US-West') AS us_west_sales;
# MAGIC
# MAGIC -- View the share contents
# MAGIC SHOW ALL IN SHARE regional_analytics_share;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Managing Recipients
# MAGIC
# MAGIC Recipients represent external users or organizations that will
# MAGIC consume shared data.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- REFERENCE: Recipient management (requires UC admin)
# MAGIC
# MAGIC -- Create a recipient for open sharing (generates activation link)
# MAGIC CREATE RECIPIENT analytics_partner
# MAGIC COMMENT 'Partner org for Q1 analytics project';
# MAGIC
# MAGIC -- Grant the share to the recipient
# MAGIC GRANT SELECT ON SHARE regional_analytics_share
# MAGIC TO RECIPIENT analytics_partner;
# MAGIC
# MAGIC -- View all recipients
# MAGIC SHOW RECIPIENTS;
# MAGIC
# MAGIC -- See what a recipient has access to
# MAGIC SHOW GRANTS TO RECIPIENT analytics_partner;
# MAGIC
# MAGIC -- Revoke access (immediate)
# MAGIC REVOKE SELECT ON SHARE regional_analytics_share
# MAGIC FROM RECIPIENT analytics_partner;
# MAGIC
# MAGIC -- Remove a recipient entirely
# MAGIC DROP RECIPIENT IF EXISTS analytics_partner;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Recipient Side Access
# MAGIC
# MAGIC ### Option A: Databricks-to-Databricks
# MAGIC
# MAGIC ```sql
# MAGIC -- On the recipient's Databricks workspace:
# MAGIC CREATE CATALOG partner_analytics
# MAGIC USING SHARE provider_workspace.regional_analytics_share;
# MAGIC
# MAGIC -- Query shared data directly
# MAGIC SELECT * FROM partner_analytics.module03.gold_regional_sales
# MAGIC WHERE region = 'US-West';
# MAGIC ```
# MAGIC
# MAGIC ### Option B: Open Sharing (Python, Pandas, any platform)
# MAGIC
# MAGIC ```python
# MAGIC import delta_sharing
# MAGIC
# MAGIC # Use the credential file received during activation
# MAGIC profile = "/path/to/credential.share"
# MAGIC
# MAGIC # List available shares
# MAGIC client = delta_sharing.SharingClient(profile)
# MAGIC print(client.list_shares())
# MAGIC print(client.list_all_tables())
# MAGIC
# MAGIC # Load into Pandas
# MAGIC table_url = f"{profile}#regional_analytics_share.module03.gold_regional_sales"
# MAGIC pdf = delta_sharing.load_as_pandas(table_url)
# MAGIC
# MAGIC # Load into Spark
# MAGIC sdf = delta_sharing.load_as_spark(table_url)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Simulated Sharing Demo
# MAGIC
# MAGIC Since we may not have UC admin access, let's demonstrate the data
# MAGIC that would be shared and the kind of queries recipients would run.

# COMMAND ----------

# What the recipient would see: the full regional sales table
print("=== Shared Table: gold_regional_sales ===")
spark.table("module03.gold_regional_sales").show(10)

# COMMAND ----------

# What a partition-filtered recipient would see (US-West only)
print("=== Partition-Filtered Share: US-West Only ===")
us_west = spark.table("module03.gold_regional_sales").filter("region = 'US-West'")
us_west.show(10)
print(f"Rows visible to US-West recipient: {us_west.count()}")
print(f"Rows in full table: {spark.table('module03.gold_regional_sales').count()}")

# COMMAND ----------

# Typical recipient query: summarize shared data
print("=== Recipient Analysis: Monthly Summary by Product ===")
(spark.table("module03.gold_regional_sales")
    .filter("region = 'US-West'")
    .groupBy("product")
    .agg(
        F.sum("units_sold").alias("total_units"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_daily_revenue"),
    )
    .orderBy(F.desc("total_revenue"))
    .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- Security and Audit Overview
# MAGIC
# MAGIC Delta Sharing provides comprehensive security:
# MAGIC
# MAGIC | Layer | Control |
# MAGIC |-------|---------|
# MAGIC | Share | Which tables are included |
# MAGIC | Partition | What subset of data is visible |
# MAGIC | Recipient | Who can access the share |
# MAGIC | Token | Time-limited bearer tokens |
# MAGIC | Audit | Every read logged in UC audit trail |
# MAGIC | Network | Optional IP allowlisting |
# MAGIC | Revocation | Instant via DROP RECIPIENT or REVOKE |

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- REFERENCE: Audit query (requires system.access catalog)
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   event_type,
# MAGIC   request_params.recipient_name,
# MAGIC   request_params.share,
# MAGIC   request_params.schema,
# MAGIC   request_params.name AS table_name
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND action_name LIKE '%deltaShar%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. Delta Sharing enables live data sharing **without copying**
# MAGIC 2. Two modes: Databricks-to-Databricks (automatic UC) and Open Sharing (any platform)
# MAGIC 3. Providers retain full control: revoke access, filter partitions, audit reads
# MAGIC 4. Recipients get read-only access to always-current data
# MAGIC 5. Unity Catalog is required for share management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.gold_regional_sales")
spark.sql("DROP TABLE IF EXISTS module03.gold_product_catalog")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
