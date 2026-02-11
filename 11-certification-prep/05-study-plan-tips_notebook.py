# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quick-Reference Code Snippets -- Exam Study Cheat Sheet
# MAGIC
# MAGIC This notebook contains **25 essential code patterns** that cover the most common
# MAGIC operations tested on the Databricks Data Engineer Associate and Professional exams.
# MAGIC
# MAGIC **How to use this notebook:**
# MAGIC - Run each cell to see the pattern in action
# MAGIC - Bookmark this notebook for quick reference during your study sessions
# MAGIC - Every snippet is self-contained (generates its own data)
# MAGIC - Focus on understanding the pattern, not memorizing syntax
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 13.x+ recommended.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Create a DataFrame from Python Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, ArrayType, TimestampType
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, upper, lower, trim,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lag, lead,
    current_timestamp, current_date, date_format, datediff, to_date, to_timestamp,
    explode, split, collect_list, collect_set, array, struct,
    round as spark_round, expr, broadcast, sha2,
    window as time_window, approx_count_distinct,
    transform, filter as array_filter, aggregate as array_aggregate
)
from pyspark.sql.window import Window
from datetime import date, datetime

# Method 1: From a list of tuples with column names
df = spark.createDataFrame(
    [(1, "Alice", 85000.0), (2, "Bob", 72000.0), (3, "Charlie", 95000.0)],
    ["id", "name", "salary"]
)

# Method 2: With explicit schema (preferred for production)
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("salary", DoubleType(), nullable=True),
])
df_typed = spark.createDataFrame(
    [(1, "Alice", 85000.0), (2, "Bob", 72000.0)],
    schema=schema
)

df.show()
df_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Read CSV / JSON / Parquet Files

# COMMAND ----------

# CSV with options
# df_csv = (
#     spark.read
#     .format("csv")
#     .option("header", "true")
#     .option("inferSchema", "true")
#     .option("delimiter", ",")
#     .option("nullValue", "NA")
#     .load("/path/to/data.csv")
# )

# JSON
# df_json = spark.read.format("json").load("/path/to/data.json")

# Parquet (no schema inference needed -- schema embedded in files)
# df_parquet = spark.read.format("parquet").load("/path/to/data.parquet")

# Read with explicit schema (faster, safer -- no inference)
# df_explicit = (
#     spark.read
#     .format("csv")
#     .schema(schema)
#     .option("header", "true")
#     .load("/path/to/data.csv")
# )

# Demonstration with in-memory data
sample_data = [(1, "2024-01-15", "Electronics", 1200.0, 2),
               (2, "2024-01-16", "Books", 39.99, 5),
               (3, "2024-01-17", "Clothing", 79.99, 3)]
df_demo = spark.createDataFrame(
    sample_data,
    ["txn_id", "txn_date", "category", "price", "quantity"]
)
df_demo.show()
print("Read operations are commented out -- uncomment and replace paths for your environment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Write to Delta Lake (Overwrite, Append)

# COMMAND ----------

# Create sample data
df_write = spark.createDataFrame([
    (1, "Widget", 10.0, date(2024, 1, 15)),
    (2, "Gadget", 25.0, date(2024, 1, 16)),
    (3, "Gizmo", 15.0, date(2024, 1, 17)),
], ["id", "product", "price", "sale_date"])

# Overwrite mode: replaces all existing data
df_write.write.format("delta").mode("overwrite").saveAsTable("cheatsheet_products")

# Append mode: adds rows to existing table
df_append = spark.createDataFrame([
    (4, "Doohickey", 8.0, date(2024, 1, 18)),
], ["id", "product", "price", "sale_date"])
df_append.write.format("delta").mode("append").saveAsTable("cheatsheet_products")

print(f"Total rows after append: {spark.table('cheatsheet_products').count()}")
spark.table("cheatsheet_products").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. MERGE INTO (Upsert)

# COMMAND ----------

# Source: updated and new records
source = spark.createDataFrame([
    (2, "Gadget Pro", 30.0, date(2024, 1, 20)),  # Update: price change
    (5, "Thingamajig", 12.0, date(2024, 1, 20)), # New record
], ["id", "product", "price", "sale_date"])
source.createOrReplaceTempView("cheatsheet_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO cheatsheet_products AS target
# MAGIC USING cheatsheet_source AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.product = source.product,
# MAGIC     target.price = source.price,
# MAGIC     target.sale_date = source.sale_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, product, price, sale_date)
# MAGIC   VALUES (source.id, source.product, source.price, source.sale_date)

# COMMAND ----------

print("After MERGE:")
spark.table("cheatsheet_products").orderBy("id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Delta Time Travel and RESTORE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY cheatsheet_products

# COMMAND ----------

# Query a previous version
print("=== Version 0 (original overwrite) ===")
spark.sql("SELECT * FROM cheatsheet_products VERSION AS OF 0").show()

print("=== Current version ===")
spark.table("cheatsheet_products").show()

# Restore to a previous version (uncomment to execute):
# spark.sql("RESTORE TABLE cheatsheet_products TO VERSION AS OF 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. SELECT, FILTER, and Column Operations

# COMMAND ----------

df_ops = spark.createDataFrame([
    (1, "Alice", "Engineering", 95000.0, True),
    (2, "Bob", "Marketing", 72000.0, True),
    (3, "Charlie", "Engineering", 110000.0, False),
    (4, "Diana", "Sales", 68000.0, True),
    (5, "Eve", "Marketing", 78000.0, True),
], ["emp_id", "name", "department", "salary", "is_active"])

# Select specific columns
df_ops.select("name", "department", "salary").show()

# Filter rows
df_ops.filter(col("salary") > 80000).show()
df_ops.filter((col("department") == "Engineering") & (col("is_active") == True)).show()

# Add/rename columns
df_enriched = (
    df_ops
    .withColumn("annual_bonus", spark_round(col("salary") * 0.1, 2))
    .withColumn("name_upper", upper(col("name")))
    .withColumnRenamed("emp_id", "employee_id")
)
df_enriched.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Aggregations (GROUP BY)

# COMMAND ----------

df_agg = spark.createDataFrame([
    ("Electronics", "US", 1200.0), ("Electronics", "US", 500.0),
    ("Electronics", "EU", 350.0),  ("Books", "US", 40.0),
    ("Books", "EU", 55.0),         ("Books", "EU", 35.0),
    ("Clothing", "US", 80.0),      ("Clothing", "EU", 65.0),
], ["category", "region", "revenue"])

result = (
    df_agg
    .groupBy("category")
    .agg(
        count("*").alias("num_sales"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue"),
        spark_max("revenue").alias("max_revenue"),
    )
    .orderBy(col("total_revenue").desc())
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Joins (Inner, Left, Anti, Broadcast)

# COMMAND ----------

df_left = spark.createDataFrame([
    (1, "Alice", "D1"), (2, "Bob", "D2"), (3, "Charlie", "D3"), (4, "Diana", "D4")
], ["id", "name", "dept_id"])

df_right = spark.createDataFrame([
    ("D1", "Engineering"), ("D2", "Marketing"), ("D5", "Finance")
], ["dept_id", "dept_name"])

# Inner join
print("=== Inner Join ===")
df_left.join(df_right, on="dept_id", how="inner").show()

# Left join (keep all left rows)
print("=== Left Join ===")
df_left.join(df_right, on="dept_id", how="left").show()

# Anti join (left rows with NO match)
print("=== Anti Join ===")
df_left.join(df_right, on="dept_id", how="anti").show()

# Broadcast join (explicitly broadcast small table)
print("=== Broadcast Join ===")
df_left.join(broadcast(df_right), on="dept_id", how="inner").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Window Functions

# COMMAND ----------

df_win = spark.createDataFrame([
    ("Sales", "Alice", 85000), ("Sales", "Bob", 72000), ("Sales", "Charlie", 91000),
    ("Engineering", "Diana", 110000), ("Engineering", "Eve", 105000), ("Engineering", "Frank", 95000),
], ["department", "name", "salary"])

w = Window.partitionBy("department").orderBy(col("salary").desc())

result_win = (
    df_win
    .withColumn("rank", rank().over(w))
    .withColumn("dense_rank", dense_rank().over(w))
    .withColumn("row_num", row_number().over(w))
    .withColumn("prev_salary", lag("salary", 1).over(w))
    .withColumn("next_salary", lead("salary", 1).over(w))
)
result_win.show()

# Running total
w_running = Window.partitionBy("department").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_win.withColumn("running_total", spark_sum("salary").over(w_running)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Conditional Logic (WHEN / OTHERWISE)

# COMMAND ----------

df_cond = spark.createDataFrame([
    (1, 1200.0), (2, 75.0), (3, 350.0), (4, 25.0), (5, 500.0)
], ["id", "revenue"])

df_tiered = df_cond.withColumn(
    "tier",
    when(col("revenue") >= 1000, "Premium")
    .when(col("revenue") >= 300, "High")
    .when(col("revenue") >= 50, "Medium")
    .otherwise("Low")
)
df_tiered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Null Handling

# COMMAND ----------

df_nulls = spark.createDataFrame([
    (1, "Alice", "alice@example.com"),
    (2, "Bob", None),
    (3, None, "charlie@example.com"),
    (4, None, None),
], ["id", "name", "email"])

# Filter nulls
print("=== Rows with null email ===")
df_nulls.filter(col("email").isNull()).show()

# Replace nulls with coalesce
print("=== Coalesce ===")
df_nulls.withColumn("display_name", coalesce(col("name"), col("email"), lit("Unknown"))).show()

# fillna
print("=== fillna ===")
df_nulls.fillna({"name": "N/A", "email": "no-email@example.com"}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 12. Date Functions

# COMMAND ----------

df_dates = spark.createDataFrame([
    (1, "2024-01-15"), (2, "2024-03-22"), (3, "2024-12-01"),
], ["id", "date_str"])

df_date_ops = (
    df_dates
    .withColumn("parsed_date", to_date(col("date_str"), "yyyy-MM-dd"))
    .withColumn("day_of_week", date_format(col("parsed_date"), "EEEE"))
    .withColumn("year_month", date_format(col("parsed_date"), "yyyy-MM"))
    .withColumn("days_from_today", datediff(current_date(), col("parsed_date")))
)
df_date_ops.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 13. Explode Arrays and Collect

# COMMAND ----------

df_arrays = spark.createDataFrame([
    (1, "python,spark,delta"),
    (2, "sql,databricks"),
    (3, "streaming,kafka,spark"),
], ["id", "tags_csv"])

# Split string into array, then explode to individual rows
df_exploded = (
    df_arrays
    .withColumn("tags_array", split(col("tags_csv"), ","))
    .withColumn("tag", explode(col("tags_array")))
    .select("id", "tag")
)
print("=== Exploded ===")
df_exploded.show()

# Collect back into arrays
print("=== Collected ===")
df_exploded.groupBy("tag").agg(collect_list("id").alias("ids_with_tag")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 14. SQL CTE and Subquery

# COMMAND ----------

df_cte = spark.createDataFrame([
    (1, "Electronics", 1200.0), (2, "Electronics", 500.0),
    (3, "Books", 40.0), (4, "Books", 55.0),
    (5, "Clothing", 200.0), (6, "Clothing", 80.0),
], ["id", "category", "revenue"])
df_cte.createOrReplaceTempView("cheatsheet_sales_cte")

result_cte = spark.sql("""
    WITH ranked AS (
        SELECT
            id, category, revenue,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
        FROM cheatsheet_sales_cte
    )
    SELECT category, revenue AS top_revenue
    FROM ranked
    WHERE rn = 1
    ORDER BY top_revenue DESC
""")
result_cte.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 15. Structured Streaming Basics

# COMMAND ----------

import time

# Read from rate source (generates rows with timestamp and value)
stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("is_even", col("value") % 2 == 0)
)

# Write to memory sink
query = (
    stream_df
    .writeStream
    .format("memory")
    .queryName("cheatsheet_stream")
    .outputMode("append")
    .start()
)

time.sleep(5)
spark.sql("SELECT * FROM cheatsheet_stream ORDER BY timestamp DESC LIMIT 10").show()
query.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 16. Streaming with Watermark and Window

# COMMAND ----------

stream_wm = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withWatermark("timestamp", "10 seconds")
    .groupBy(time_window(col("timestamp"), "5 seconds"))
    .agg(count("*").alias("event_count"))
)

query_wm = (
    stream_wm
    .writeStream
    .format("memory")
    .queryName("cheatsheet_windowed")
    .outputMode("update")
    .start()
)

time.sleep(12)
spark.sql("""
    SELECT window.start, window.end, event_count
    FROM cheatsheet_windowed
    ORDER BY window.start DESC LIMIT 5
""").show(truncate=False)
query_wm.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 17. Auto Loader Pattern (Reference)

# COMMAND ----------

# Auto Loader reads new files incrementally as they arrive
# This is a reference pattern -- uncomment and adjust paths for your environment

# stream_autoloader = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", "/path/to/schema")
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#     .load("/path/to/landing/")
# )
#
# query_al = (
#     stream_autoloader
#     .writeStream
#     .format("delta")
#     .option("checkpointLocation", "/path/to/checkpoint")
#     .option("mergeSchema", "true")
#     .trigger(availableNow=True)
#     .toTable("bronze_table")
# )

print("Auto Loader pattern (reference only -- paths must be configured):")
print('  spark.readStream.format("cloudFiles")')
print('    .option("cloudFiles.format", "json")')
print('    .option("cloudFiles.schemaLocation", "/path/to/schema")')
print('    .load("/path/to/landing/")')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 18. OPTIMIZE, Z-ORDER, and VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE compacts small files into larger ones
# MAGIC OPTIMIZE cheatsheet_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER co-locates data by specified columns for faster filtering
# MAGIC OPTIMIZE cheatsheet_products
# MAGIC ZORDER BY (category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM removes old files beyond the retention period (default 7 days)
# MAGIC -- DRY RUN first to see what would be deleted:
# MAGIC -- VACUUM cheatsheet_products DRY RUN;
# MAGIC
# MAGIC -- Actual vacuum:
# MAGIC VACUUM cheatsheet_products RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 19. SCD Type 2 Pattern

# COMMAND ----------

# Create dimension table
dim_data = [
    (1, "Alice", "Gold", date(2023, 1, 1), None, True),
    (2, "Bob", "Silver", date(2023, 3, 15), None, True),
]
dim_schema = StructType([
    StructField("cust_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("tier", StringType(), False),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_current", BooleanType(), False),
])
spark.createDataFrame(dim_data, dim_schema).write.format("delta").mode("overwrite").saveAsTable("cheatsheet_dim")

# Source changes
changes = spark.createDataFrame([
    (2, "Bob", "Gold"),   # Tier change
    (3, "Charlie", "Bronze"),  # New customer
], ["cust_id", "name", "tier"])
changes.createOrReplaceTempView("cheatsheet_changes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Close existing records that changed
# MAGIC MERGE INTO cheatsheet_dim AS t
# MAGIC USING cheatsheet_changes AS s
# MAGIC ON t.cust_id = s.cust_id AND t.is_current = true
# MAGIC WHEN MATCHED AND t.tier != s.tier THEN
# MAGIC   UPDATE SET t.is_current = false, t.end_date = current_date()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (cust_id, name, tier, effective_date, end_date, is_current)
# MAGIC   VALUES (s.cust_id, s.name, s.tier, current_date(), NULL, true)

# COMMAND ----------

# Step 2: Insert new current records for changed customers
spark.sql("""
    INSERT INTO cheatsheet_dim
    SELECT s.cust_id, s.name, s.tier, current_date(), NULL, true
    FROM cheatsheet_changes s
    WHERE s.cust_id IN (
        SELECT cust_id FROM cheatsheet_dim
        WHERE is_current = false AND end_date = current_date()
    )
""")

print("=== SCD Type 2 Result (history preserved) ===")
spark.table("cheatsheet_dim").orderBy("cust_id", "effective_date").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 20. Higher-Order Functions (transform, filter, aggregate)

# COMMAND ----------

df_hof = spark.createDataFrame([
    (1, [10, 20, 30, 40, 50]),
    (2, [5, 15, 25]),
    (3, [100, 200, 300, 400]),
], ["id", "values"])

result_hof = (
    df_hof
    # transform: apply a function to each element
    .withColumn("doubled", transform(col("values"), lambda x: x * 2))
    # filter: keep elements matching a condition
    .withColumn("above_20", array_filter(col("values"), lambda x: x > 20))
    # aggregate: reduce array to a single value
    .withColumn("total", array_aggregate(col("values"), lit(0), lambda acc, x: acc + x))
)
result_hof.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 21. Execution Plan Analysis

# COMMAND ----------

df_plan = spark.createDataFrame([
    (1, "A", 100.0), (2, "B", 200.0), (3, "A", 150.0), (4, "B", 300.0)
], ["id", "group", "value"])

query_plan = (
    df_plan
    .filter(col("group") == "A")
    .groupBy("group")
    .agg(spark_sum("value").alias("total"))
)

# Physical plan only
print("=== Physical Plan ===")
query_plan.explain()

# Full plan: parsed -> analyzed -> optimized -> physical
print("\n=== Extended Plan ===")
query_plan.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 22. PII Hashing with SHA-256

# COMMAND ----------

df_pii = spark.createDataFrame([
    (1, "Alice Johnson", "alice@example.com"),
    (2, "Bob Smith", "bob@example.com"),
], ["id", "name", "email"])

df_hashed = (
    df_pii
    .withColumn("email_hash", sha2(col("email"), 256))
    .withColumn("name_masked", concat(
        upper(expr("substring(name, 1, 1)")),
        lit(". "),
        expr("substring_index(name, ' ', -1)")
    ))
    .select("id", "name_masked", "email_hash")
)

print("=== PII Pseudonymized ===")
df_hashed.show(truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 23. Unity Catalog Grant Patterns (Reference SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Step 1: USAGE on catalog (required first)
# MAGIC GRANT USAGE ON CATALOG my_catalog TO `analysts`;
# MAGIC
# MAGIC -- Step 2: USAGE on schema
# MAGIC GRANT USAGE ON SCHEMA my_catalog.my_schema TO `analysts`;
# MAGIC
# MAGIC -- Step 3: Table-level privileges
# MAGIC GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `analysts`;
# MAGIC
# MAGIC -- Grant all privileges (admin)
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA my_catalog.my_schema TO `data_engineers`;
# MAGIC
# MAGIC -- Revoke access
# MAGIC REVOKE SELECT ON TABLE my_catalog.my_schema.my_table FROM `analysts`;
# MAGIC
# MAGIC -- Show grants
# MAGIC SHOW GRANTS ON TABLE my_catalog.my_schema.my_table;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 24. Pivot and Unpivot

# COMMAND ----------

df_pivot = spark.createDataFrame([
    ("Electronics", "Q1", 1000), ("Electronics", "Q2", 1500),
    ("Books", "Q1", 400), ("Books", "Q2", 600),
    ("Clothing", "Q1", 800), ("Clothing", "Q2", 900),
], ["category", "quarter", "revenue"])

# Pivot: rows -> columns
pivoted = df_pivot.groupBy("category").pivot("quarter").agg(spark_sum("revenue"))
print("=== Pivoted ===")
pivoted.show()

# Unpivot: columns -> rows (using stack)
unpivoted = pivoted.select(
    "category",
    expr("stack(2, 'Q1', Q1, 'Q2', Q2) AS (quarter, revenue)")
).filter(col("revenue").isNotNull())

print("=== Unpivoted ===")
unpivoted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 25. Data Quality Checks (DLT-Style Expectations)

# COMMAND ----------

df_quality = spark.createDataFrame([
    (1, "Widget", 10.0, 5, "US"),
    (2, "Gadget", -5.0, 3, "EU"),   # negative price
    (3, "Gizmo", 15.0, 0, None),    # zero qty, null region
    (4, "Thing", 25.0, 2, "US"),
    (5, "Item", 0.0, 1, "APAC"),    # zero price
], ["id", "product", "price", "quantity", "region"])

# Define expectations (similar to DLT)
total = df_quality.count()
checks = {
    "price_positive": df_quality.filter(col("price") > 0).count(),
    "quantity_positive": df_quality.filter(col("quantity") > 0).count(),
    "region_not_null": df_quality.filter(col("region").isNotNull()).count(),
}

print("=== Data Quality Report ===")
for rule, passing in checks.items():
    pct = passing / total * 100
    status = "PASS" if pct == 100 else "WARN" if pct >= 80 else "FAIL"
    print(f"  {rule}: {passing}/{total} ({pct:.0f}%) [{status}]")

# Filter to clean rows only (EXPECT OR DROP equivalent)
df_clean = (
    df_quality
    .filter(col("price") > 0)
    .filter(col("quantity") > 0)
    .filter(col("region").isNotNull())
)
print(f"\nClean rows: {df_clean.count()} / {total}")
df_clean.show()

# DLT equivalent (reference):
# @dlt.expect_or_drop("valid_price", "price > 0")
# @dlt.expect_or_drop("valid_qty", "quantity > 0")
# @dlt.expect_or_drop("valid_region", "region IS NOT NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Drop all tables and views created in this notebook
tables = [
    "cheatsheet_products",
    "cheatsheet_dim",
]
views = [
    "cheatsheet_source",
    "cheatsheet_changes",
    "cheatsheet_sales_cte",
]

for t in tables:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for v in views:
    spark.sql(f"DROP VIEW IF EXISTS {v}")

print("Cleanup complete. All cheat sheet tables and views removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **This notebook covers the 25 most important code patterns for the Databricks
# MAGIC Data Engineer certification exams.**
# MAGIC
# MAGIC **Quick index:**
# MAGIC 1. Create DataFrame | 2. Read files | 3. Write Delta | 4. MERGE | 5. Time travel
# MAGIC 6. Select/Filter | 7. Aggregations | 8. Joins | 9. Windows | 10. Conditional logic
# MAGIC 11. Null handling | 12. Dates | 13. Explode/Collect | 14. CTE/Subquery | 15. Streaming
# MAGIC 16. Watermarks | 17. Auto Loader | 18. OPTIMIZE/VACUUM | 19. SCD Type 2 | 20. HOFs
# MAGIC 21. Explain plans | 22. PII hashing | 23. Unity Catalog grants | 24. Pivot | 25. DQ checks
# MAGIC
# MAGIC **Bookmark this notebook and review it regularly during your study plan.**
