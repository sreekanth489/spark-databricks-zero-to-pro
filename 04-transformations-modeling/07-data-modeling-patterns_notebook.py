# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 -- Data Modeling Patterns
# MAGIC
# MAGIC **Module 04 | Topic 07 | Level: Intermediate | Time: 55 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Create managed and external tables and compare their behavior
# MAGIC - Inspect tables with DESCRIBE EXTENDED
# MAGIC - Build a star schema with fact and dimension tables
# MAGIC - Implement SCD Type 1 (overwrite) and SCD Type 2 (versioned history)
# MAGIC - Build a Bronze -> Silver -> Gold medallion pipeline
# MAGIC - Apply partitioning strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Create a Database for This Demo

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, current_timestamp, to_date, date_format,
    year, quarter, month, dayofweek, dayofmonth, when, row_number,
    count, sum as _sum, avg, round as _round, max as _max, min as _min,
    trim, lower, upper, regexp_replace, coalesce, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, BooleanType, TimestampType, LongType
)
from datetime import date, datetime

spark = SparkSession.builder.getOrCreate()

# Create a demo database (schema) for our tables
spark.sql("CREATE DATABASE IF NOT EXISTS demo_modeling")
spark.sql("USE demo_modeling")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- Managed vs External Tables
# MAGIC
# MAGIC Every table in Databricks has 2 things: the **data** and the **metadata**.
# MAGIC
# MAGIC - **Managed tables**: Databricks manages BOTH the data and metadata.
# MAGIC   Data governance is easy because Unity Catalog controls everything.
# MAGIC - **External tables**: Data is stored in S3/ADLS (you control it),
# MAGIC   but metadata lives in Databricks.
# MAGIC
# MAGIC If you delete an external table, it deletes the metadata but NOT the
# MAGIC actual content which might be stored in AWS S3.

# COMMAND ----------

# Create a MANAGED table
spark.sql("""
    CREATE OR REPLACE TABLE managed_demo (
        id INT,
        name STRING,
        category STRING
    )
    USING DELTA
""")

spark.sql("""
    INSERT INTO managed_demo VALUES
    (1, 'Laptop', 'Electronics'),
    (2, 'Book', 'Education'),
    (3, 'Jacket', 'Clothing')
""")

print("Managed table created and populated:")
spark.sql("SELECT * FROM managed_demo").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- DESCRIBE EXTENDED: Inspect Table Metadata
# MAGIC
# MAGIC Use this to see table type, location, provider, and properties.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows column info, table type, location, provider, etc.
# MAGIC DESCRIBE EXTENDED managed_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- Managed Table DROP Behavior
# MAGIC
# MAGIC When you DROP a managed table, Databricks deletes BOTH the metadata
# MAGIC AND the data files. The data is gone.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the location before dropping
# MAGIC DESCRIBE DETAIL managed_demo;

# COMMAND ----------

# Note: We will NOT actually drop it here so we can continue using it.
# But the behavior is:
# spark.sql("DROP TABLE managed_demo")
# --> Deletes BOTH metadata AND data files

# For external tables:
# spark.sql("DROP TABLE external_demo")
# --> Deletes ONLY metadata; data files in S3/ADLS remain untouched

print("""
MANAGED TABLE DROP behavior:
  - Deletes metadata from catalog
  - Deletes data files from managed storage
  - Data is GONE

EXTERNAL TABLE DROP behavior:
  - Deletes metadata from catalog
  - Data files in S3/ADLS are NOT deleted
  - You can re-register the table later

When to use:
  - Quick analysis, fully governed data --> MANAGED tables
  - Data already in S3/ADLS for years   --> EXTERNAL tables
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- Star Schema: Dimension Tables
# MAGIC
# MAGIC Build dimension tables for customers, products, stores, and dates.

# COMMAND ----------

# dim_customer
customer_data = [
    (1, "Alice Johnson", "alice@email.com", "Premium", "Seattle", "WA", date(2020, 3, 15)),
    (2, "Bob Smith", "bob@email.com", "Standard", "Portland", "OR", date(2019, 7, 20)),
    (3, "Carol Lee", "carol@email.com", "Premium", "San Francisco", "CA", date(2021, 1, 10)),
    (4, "David Kim", "david@email.com", "Standard", "Austin", "TX", date(2022, 5, 1)),
    (5, "Eva Garcia", "eva@email.com", "Premium", "Denver", "CO", date(2020, 11, 30)),
]
dim_customer = spark.createDataFrame(
    data=customer_data,
    schema=["customer_key", "name", "email", "segment", "city", "state", "join_date"]
)

spark.sql("DROP TABLE IF EXISTS dim_customer")
dim_customer.write.format("delta").saveAsTable("dim_customer")

# dim_product
product_data = [
    (101, "Laptop Pro", "Electronics", "TechCorp", 1299.99),
    (102, "Wireless Mouse", "Electronics", "TechCorp", 29.99),
    (103, "Python Guide", "Books", "DataPress", 49.99),
    (104, "Running Shoes", "Clothing", "SportFit", 89.99),
    (105, "Coffee Maker", "Appliances", "HomeBrew", 149.99),
    (106, "Data Engineering", "Books", "DataPress", 59.99),
    (107, "Mechanical Keyboard", "Electronics", "TechCorp", 129.99),
    (108, "Winter Jacket", "Clothing", "OutdoorCo", 199.99),
]
dim_product = spark.createDataFrame(
    data=product_data,
    schema=["product_key", "product_name", "category", "brand", "list_price"]
)

spark.sql("DROP TABLE IF EXISTS dim_product")
dim_product.write.format("delta").saveAsTable("dim_product")

# dim_store
store_data = [
    (1001, "Downtown Seattle", "West", "WA"),
    (1002, "Portland Mall", "West", "OR"),
    (1003, "Austin Central", "South", "TX"),
    (1004, "Denver Tech Center", "West", "CO"),
]
dim_store = spark.createDataFrame(
    data=store_data,
    schema=["store_key", "store_name", "region", "state"]
)

spark.sql("DROP TABLE IF EXISTS dim_store")
dim_store.write.format("delta").saveAsTable("dim_store")

# dim_date (a sample date dimension)
from pyspark.sql.functions import sequence, explode as _explode

date_range = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date_key
""")
dim_date = date_range.select(
    col("date_key"),
    year("date_key").alias("year"),
    quarter("date_key").alias("quarter"),
    month("date_key").alias("month"),
    dayofmonth("date_key").alias("day"),
    dayofweek("date_key").alias("day_of_week"),
    date_format("date_key", "EEEE").alias("day_name"),
)

spark.sql("DROP TABLE IF EXISTS dim_date")
dim_date.write.format("delta").saveAsTable("dim_date")

print("Dimension tables created:")
for t in ["dim_customer", "dim_product", "dim_store", "dim_date"]:
    print(f"  {t}: {spark.table(t).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Star Schema: Fact Table

# COMMAND ----------

fact_data = [
    (1, 1, 101, 1001, date(2024, 1, 15), 1, 1299.99, 1299.99),
    (2, 2, 102, 1002, date(2024, 1, 15), 2, 29.99, 59.98),
    (3, 1, 103, 1001, date(2024, 2, 10), 1, 49.99, 49.99),
    (4, 3, 104, 1003, date(2024, 2, 20), 1, 89.99, 89.99),
    (5, 4, 105, 1003, date(2024, 3, 5), 1, 149.99, 149.99),
    (6, 1, 107, 1001, date(2024, 3, 15), 1, 129.99, 129.99),
    (7, 5, 106, 1004, date(2024, 4, 1), 3, 59.99, 179.97),
    (8, 2, 108, 1002, date(2024, 4, 10), 1, 199.99, 199.99),
    (9, 3, 101, 1003, date(2024, 5, 5), 1, 1299.99, 1299.99),
    (10, 4, 102, 1003, date(2024, 5, 20), 3, 29.99, 89.97),
    (11, 5, 104, 1004, date(2024, 6, 1), 2, 89.99, 179.98),
    (12, 1, 105, 1001, date(2024, 6, 15), 1, 149.99, 149.99),
]

fact_sales = spark.createDataFrame(
    data=fact_data,
    schema=["sale_id", "customer_key", "product_key", "store_key",
            "sale_date", "quantity", "unit_price", "total_amount"]
)

spark.sql("DROP TABLE IF EXISTS fact_sales")
fact_sales.write.format("delta").saveAsTable("fact_sales")

print(f"fact_sales: {spark.table('fact_sales').count()} rows")
spark.table("fact_sales").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Star Schema Query: Join Fact + Dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Typical star schema query: revenue by category and region
# MAGIC SELECT
# MAGIC     p.category,
# MAGIC     s.region,
# MAGIC     COUNT(*) AS num_sales,
# MAGIC     ROUND(SUM(f.total_amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(f.total_amount), 2) AS avg_sale
# MAGIC FROM fact_sales f
# MAGIC JOIN dim_product p ON f.product_key = p.product_key
# MAGIC JOIN dim_store s ON f.store_key = s.store_key
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- SCD Type 1: Overwrite (No History)
# MAGIC
# MAGIC Simply update the record in place. Previous values are lost.

# COMMAND ----------

# Simulate a customer moving from Seattle to San Francisco
spark.sql("""
    MERGE INTO dim_customer AS target
    USING (
        SELECT 1 AS customer_key, 'San Francisco' AS new_city, 'CA' AS new_state
    ) AS source
    ON target.customer_key = source.customer_key
    WHEN MATCHED THEN
        UPDATE SET target.city = source.new_city, target.state = source.new_state
""")

print("After SCD Type 1 -- Alice's city is now overwritten:")
spark.sql("SELECT * FROM dim_customer WHERE customer_key = 1").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- SCD Type 2: Versioned History
# MAGIC
# MAGIC Create a new version of the dimension table that tracks history
# MAGIC with valid_from, valid_to, and is_current columns.

# COMMAND ----------

# Create SCD Type 2 version of dim_customer
scd2_data = [
    (1, "Alice Johnson", "alice@email.com", "Premium", "Seattle", "WA",
     date(2020, 3, 15), date(2024, 6, 14), False),
    (1, "Alice Johnson", "alice@email.com", "Premium", "San Francisco", "CA",
     date(2024, 6, 15), date(9999, 12, 31), True),
    (2, "Bob Smith", "bob@email.com", "Standard", "Portland", "OR",
     date(2019, 7, 20), date(9999, 12, 31), True),
    (3, "Carol Lee", "carol@email.com", "Premium", "San Francisco", "CA",
     date(2021, 1, 10), date(9999, 12, 31), True),
]

scd2_schema = StructType([
    StructField("customer_key", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("segment", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("valid_from", DateType()),
    StructField("valid_to", DateType()),
    StructField("is_current", BooleanType()),
])

spark.sql("DROP TABLE IF EXISTS dim_customer_scd2")
spark.createDataFrame(data=scd2_data, schema=scd2_schema) \
    .write.format("delta").saveAsTable("dim_customer_scd2")

print("SCD Type 2 -- customer history with versioning:")
spark.sql("SELECT * FROM dim_customer_scd2 ORDER BY customer_key, valid_from").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- SCD Type 2: MERGE for New Changes
# MAGIC
# MAGIC Bob moves from Portland to Austin. We expire his current record
# MAGIC and insert a new one.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Expire the current record for Bob
# MAGIC MERGE INTO dim_customer_scd2 AS target
# MAGIC USING (
# MAGIC     SELECT 2 AS customer_key, 'Austin' AS new_city, 'TX' AS new_state
# MAGIC ) AS source
# MAGIC ON target.customer_key = source.customer_key AND target.is_current = true
# MAGIC WHEN MATCHED AND target.city <> source.new_city THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.valid_to = current_date();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Insert the new current record for Bob
# MAGIC INSERT INTO dim_customer_scd2
# MAGIC VALUES (2, 'Bob Smith', 'bob@email.com', 'Standard', 'Austin', 'TX',
# MAGIC         current_date(), DATE '9999-12-31', true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: Bob now has 2 records (history preserved)
# MAGIC SELECT * FROM dim_customer_scd2
# MAGIC WHERE customer_key = 2
# MAGIC ORDER BY valid_from;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Medallion Architecture: Bronze -> Silver -> Gold
# MAGIC
# MAGIC The medallion architecture is a data design pattern used to logically
# MAGIC organize data in a lakehouse. Transform incrementally and progressively
# MAGIC to improve the structure and quality of data.

# COMMAND ----------

# ----- BRONZE: Raw ingestion (append-only, no transformations) -----
bronze_data = [
    (1, "alice johnson", "ALICE@EMAIL.COM", 1299.99, "2024-01-15", "electronics", "system_a", datetime(2024, 1, 15, 10, 30, 0)),
    (2, " Bob Smith ", "bob@email.com", 29.99, "2024-01-15", "electronics", "system_a", datetime(2024, 1, 15, 10, 31, 0)),
    (3, "carol lee", "carol@email.com", 49.99, "2024-02-10", "books", "system_b", datetime(2024, 2, 10, 14, 0, 0)),
    (1, "alice johnson", "ALICE@EMAIL.COM", 1299.99, "2024-01-15", "electronics", "system_a", datetime(2024, 1, 15, 12, 0, 0)),  # DUPLICATE
    (4, "david kim", "david@email.com", -50.00, "2024-03-01", "clothing", "system_b", datetime(2024, 3, 1, 9, 0, 0)),  # NEGATIVE amount (bad data)
    (5, "eva garcia", "eva@email.com", 89.99, "2024-03-05", "clothing", "system_a", datetime(2024, 3, 5, 16, 0, 0)),
]

bronze_schema = StructType([
    StructField("sale_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("email", StringType()),
    StructField("amount", DoubleType()),
    StructField("sale_date", StringType()),
    StructField("category", StringType()),
    StructField("source_system", StringType()),
    StructField("ingested_at", TimestampType()),
])

spark.sql("DROP TABLE IF EXISTS bronze_sales")
spark.createDataFrame(data=bronze_data, schema=bronze_schema) \
    .write.format("delta").saveAsTable("bronze_sales")

print("BRONZE layer -- raw data (notice duplicates, inconsistent casing, bad data):")
spark.table("bronze_sales").show(truncate=False)

# COMMAND ----------

# ----- SILVER: Cleaned, deduplicated, validated -----
bronze_df = spark.table("bronze_sales")

# Deduplicate: keep latest ingestion per sale_id
w_dedup = Window.partitionBy("sale_id").orderBy(col("ingested_at").desc())
deduped = bronze_df.withColumn("rn", row_number().over(w_dedup)).filter(col("rn") == 1).drop("rn")

# Clean and standardize
silver_df = deduped.select(
    col("sale_id"),
    trim(col("customer_name")).alias("customer_name"),   # trim whitespace
    lower(trim(col("email"))).alias("email"),            # lowercase email
    col("amount"),
    to_date(col("sale_date"), "yyyy-MM-dd").alias("sale_date"),
    lower(trim(col("category"))).alias("category"),
    col("source_system"),
    col("ingested_at"),
    current_timestamp().alias("processed_at"),
).filter(
    col("amount") > 0   # remove invalid negative amounts
)

spark.sql("DROP TABLE IF EXISTS silver_sales")
silver_df.write.format("delta").saveAsTable("silver_sales")

print("SILVER layer -- cleaned, deduped, validated:")
spark.table("silver_sales").show(truncate=False)

# COMMAND ----------

# ----- GOLD: Business-ready aggregations -----

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_daily_revenue;
# MAGIC
# MAGIC CREATE TABLE gold_daily_revenue AS
# MAGIC SELECT
# MAGIC     sale_date,
# MAGIC     category,
# MAGIC     COUNT(*) AS num_transactions,
# MAGIC     ROUND(SUM(amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(amount), 2) AS avg_transaction,
# MAGIC     ROUND(MIN(amount), 2) AS min_transaction,
# MAGIC     ROUND(MAX(amount), 2) AS max_transaction
# MAGIC FROM silver_sales
# MAGIC GROUP BY sale_date, category
# MAGIC ORDER BY sale_date, category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GOLD layer -- business-ready aggregated metrics
# MAGIC SELECT * FROM gold_daily_revenue;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 -- Table Partitioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a partitioned table
# MAGIC DROP TABLE IF EXISTS fact_sales_partitioned;
# MAGIC
# MAGIC CREATE TABLE fact_sales_partitioned (
# MAGIC     sale_id INT,
# MAGIC     customer_key INT,
# MAGIC     product_key INT,
# MAGIC     store_key INT,
# MAGIC     sale_date DATE,
# MAGIC     quantity INT,
# MAGIC     total_amount DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (sale_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into the partitioned table
# MAGIC INSERT INTO fact_sales_partitioned
# MAGIC SELECT sale_id, customer_key, product_key, store_key,
# MAGIC        sale_date, quantity, total_amount
# MAGIC FROM fact_sales;
# MAGIC
# MAGIC -- Inspect partitioning
# MAGIC DESCRIBE EXTENDED fact_sales_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 -- Temporary Views for Ad-Hoc Analysis
# MAGIC
# MAGIC Run SQL queries on top of a DataFrame using `createOrReplaceTempView`.

# COMMAND ----------

spark.table("fact_sales").createOrReplaceTempView("v_fact_sales")
spark.table("dim_product").createOrReplaceTempView("v_dim_product")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ad-hoc query using temporary views
# MAGIC SELECT
# MAGIC     p.brand,
# MAGIC     COUNT(*) AS total_sales,
# MAGIC     ROUND(SUM(f.total_amount), 2) AS brand_revenue
# MAGIC FROM v_fact_sales f
# MAGIC JOIN v_dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.brand
# MAGIC ORDER BY brand_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 -- Cleanup
# MAGIC
# MAGIC Drop all tables and the demo database.

# COMMAND ----------

tables_to_drop = [
    "managed_demo", "dim_customer", "dim_product", "dim_store",
    "dim_date", "fact_sales", "dim_customer_scd2", "bronze_sales",
    "silver_sales", "gold_daily_revenue", "fact_sales_partitioned"
]

for table in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS demo_modeling.{table}")

spark.sql("DROP VIEW IF EXISTS v_fact_sales")
spark.sql("DROP VIEW IF EXISTS v_dim_product")
spark.sql("DROP DATABASE IF EXISTS demo_modeling")

print("All tables, views, and database cleaned up.")
