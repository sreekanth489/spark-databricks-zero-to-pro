# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Associate Data Engineer -- Interactive Practice Exercises
# MAGIC
# MAGIC This notebook contains **20 code exercises** covering all five Associate exam domains.
# MAGIC Each exercise presents a scenario, asks you to write code, and then reveals the solution.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Read the scenario in each markdown cell.
# MAGIC 2. Write your solution in the empty code cell.
# MAGIC 3. Run the solution cell to check your answer.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 13.x+ recommended.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Sample Data
# MAGIC
# MAGIC Run this cell first to create the datasets used throughout the exercises.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, BooleanType, LongType
)
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, upper, lower, trim,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lag, lead,
    current_timestamp, current_date, date_format, datediff, to_date,
    explode, split, round as spark_round, expr
)
from pyspark.sql.window import Window
from datetime import date, datetime

# --- Sales transactions ---
sales_data = [
    (1, "2024-01-15", "Electronics", "Laptop", 1200.00, 2, "US"),
    (2, "2024-01-15", "Electronics", "Mouse", 25.00, 5, "US"),
    (3, "2024-01-16", "Clothing", "T-Shirt", 19.99, 10, "UK"),
    (4, "2024-01-16", "Electronics", "Keyboard", 75.00, 3, "US"),
    (5, "2024-01-17", "Clothing", "Jeans", 49.99, 4, "UK"),
    (6, "2024-01-17", "Books", "Python Guide", 39.99, 7, "CA"),
    (7, "2024-01-18", "Electronics", "Monitor", 350.00, 1, "US"),
    (8, "2024-01-18", "Books", "Spark Guide", 44.99, 5, "CA"),
    (9, "2024-01-19", "Clothing", "Jacket", 89.99, 2, "UK"),
    (10, "2024-01-19", "Electronics", "Headphones", 150.00, 6, "US"),
    (11, "2024-01-20", "Books", "Data Engineering", 54.99, 3, "US"),
    (12, "2024-01-20", "Electronics", "Tablet", 499.99, 2, "CA"),
    (13, "2024-01-21", "Clothing", "Shoes", 79.99, 3, None),
    (14, "2024-01-21", "Electronics", "Charger", 29.99, 8, "US"),
    (15, "2024-01-22", "Books", "SQL Cookbook", 34.99, 4, None),
]

sales_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("txn_date", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("country", StringType(), True),
])

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales = df_sales.withColumn("txn_date", to_date(col("txn_date")))
df_sales.createOrReplaceTempView("sales")

# --- Customer data ---
customers_data = [
    (101, "Alice Johnson", "alice@example.com", "2023-01-10", True),
    (102, "Bob Smith", "bob@example.com", "2023-03-15", True),
    (103, "Charlie Brown", None, "2023-06-20", False),
    (104, "Diana Prince", "diana@example.com", "2023-09-01", True),
    (105, "Eve Davis", "eve@example.com", "2024-01-05", True),
    (106, "Frank Miller", None, "2024-01-12", False),
]

df_customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "name", "email", "signup_date", "is_active"]
)
df_customers = df_customers.withColumn("signup_date", to_date(col("signup_date")))
df_customers.createOrReplaceTempView("customers")

# --- Employee updates (for MERGE exercises) ---
employee_base = [
    (1, "Alice", "Engineering", 95000.0, True),
    (2, "Bob", "Marketing", 72000.0, True),
    (3, "Charlie", "Sales", 68000.0, True),
    (4, "Diana", "Engineering", 110000.0, True),
    (5, "Eve", "Marketing", 78000.0, False),
]
df_emp_base = spark.createDataFrame(
    employee_base,
    ["emp_id", "name", "department", "salary", "is_active"]
)
df_emp_base.write.format("delta").mode("overwrite").saveAsTable("practice_employees")

print("Setup complete. Tables and views created:")
print("  - sales (temp view + 15 rows)")
print("  - customers (temp view + 6 rows)")
print("  - practice_employees (Delta table + 5 rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Read and Inspect Data (Domain 2)
# MAGIC
# MAGIC **Scenario:** You have a CSV file with sales data. Read it into a DataFrame and inspect it.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Display the schema of `df_sales`
# MAGIC 2. Show the first 5 rows
# MAGIC 3. Count the total number of rows

# COMMAND ----------

# YOUR CODE HERE
# Hint: use printSchema(), show(5), and count()


# COMMAND ----------

# SOLUTION
print("=== Schema ===")
df_sales.printSchema()

print(f"\n=== First 5 Rows ===")
df_sales.show(5)

print(f"\n=== Total Rows: {df_sales.count()} ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Column Operations (Domain 2)
# MAGIC
# MAGIC **Scenario:** Calculate the total revenue for each transaction.
# MAGIC
# MAGIC **Task:** Add a column called `total_revenue` that equals `price * quantity`.
# MAGIC Then select only `txn_id`, `product`, `price`, `quantity`, and `total_revenue`.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex2 = (
    df_sales
    .withColumn("total_revenue", spark_round(col("price") * col("quantity"), 2))
    .select("txn_id", "product", "price", "quantity", "total_revenue")
)
result_ex2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Filtering and Null Handling (Domain 2)
# MAGIC
# MAGIC **Scenario:** Find all transactions where the country is unknown (null).
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Filter `df_sales` to rows where `country` is null
# MAGIC 2. Then replace nulls with "Unknown" using `coalesce` or `fillna`

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Part 1: Find nulls
print("=== Transactions with null country ===")
df_sales.filter(col("country").isNull()).show()

# Part 2: Replace nulls
print("=== After replacing nulls ===")
df_filled = df_sales.withColumn(
    "country", coalesce(col("country"), lit("Unknown"))
)
df_filled.filter(col("txn_id").isin(13, 15)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Aggregations (Domain 2)
# MAGIC
# MAGIC **Scenario:** Management needs a summary of sales by category.
# MAGIC
# MAGIC **Task:** Group by `category` and calculate:
# MAGIC - Total number of transactions
# MAGIC - Total revenue (sum of price * quantity)
# MAGIC - Average price
# MAGIC
# MAGIC Sort by total revenue descending.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex4 = (
    df_sales
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy("category")
    .agg(
        count("*").alias("num_transactions"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("price"), 2).alias("avg_price"),
    )
    .orderBy(col("total_revenue").desc())
)
result_ex4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Window Functions (Domain 2)
# MAGIC
# MAGIC **Scenario:** Rank products within each category by price (highest first).
# MAGIC
# MAGIC **Task:** Use a window function to add a `price_rank` column that ranks
# MAGIC products within their category by price (descending).

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
window_spec = Window.partitionBy("category").orderBy(col("price").desc())

result_ex5 = (
    df_sales
    .withColumn("price_rank", rank().over(window_spec))
    .select("category", "product", "price", "price_rank")
    .orderBy("category", "price_rank")
)
result_ex5.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: SQL Query with CTE (Domain 2)
# MAGIC
# MAGIC **Scenario:** Find the top-selling product (by total revenue) in each category using SQL.
# MAGIC
# MAGIC **Task:** Write a SQL query using a CTE and window function.

# COMMAND ----------

# YOUR CODE HERE (use spark.sql() or %sql magic)


# COMMAND ----------

# SOLUTION
result_ex6 = spark.sql("""
    WITH revenue_ranked AS (
        SELECT
            category,
            product,
            ROUND(price * quantity, 2) AS revenue,
            ROW_NUMBER() OVER (
                PARTITION BY category ORDER BY price * quantity DESC
            ) AS rn
        FROM sales
    )
    SELECT category, product, revenue
    FROM revenue_ranked
    WHERE rn = 1
    ORDER BY revenue DESC
""")
result_ex6.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 7: Joins (Domain 2)
# MAGIC
# MAGIC **Scenario:** You need to find customers who have NOT provided an email.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Filter customers where `email` is null
# MAGIC 2. Then, separately, use a left anti join between all customers and
# MAGIC    a DataFrame of customers WITH emails to achieve the same result.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Method 1: Direct filter
print("=== Method 1: Filter for null emails ===")
df_customers.filter(col("email").isNull()).show()

# Method 2: Left anti join
print("=== Method 2: Left anti join ===")
df_with_email = df_customers.filter(col("email").isNotNull())
df_customers.join(
    df_with_email,
    on="customer_id",
    how="anti"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 8: Write Modes (Domain 2)
# MAGIC
# MAGIC **Scenario:** Save the sales DataFrame as a Delta table. Then append new data to it.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write `df_sales` as a Delta table called `practice_sales` (overwrite mode)
# MAGIC 2. Create a small DataFrame with 2 new rows and append it
# MAGIC 3. Verify the total count increased

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1: Initial write (overwrite)
df_sales.write.format("delta").mode("overwrite").saveAsTable("practice_sales")
initial_count = spark.table("practice_sales").count()
print(f"Initial count: {initial_count}")

# Step 2: Append new data
new_sales = spark.createDataFrame(
    [
        (16, date(2024, 1, 23), "Electronics", "USB Cable", 9.99, 15, "US"),
        (17, date(2024, 1, 23), "Books", "ML Handbook", 59.99, 2, "CA"),
    ],
    ["txn_id", "txn_date", "category", "product", "price", "quantity", "country"]
)
new_sales.write.format("delta").mode("append").saveAsTable("practice_sales")
final_count = spark.table("practice_sales").count()
print(f"Final count after append: {final_count}")
print(f"New rows added: {final_count - initial_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 9: MERGE INTO (Domain 2 & 3)
# MAGIC
# MAGIC **Scenario:** You receive updated employee data. Some employees have salary changes,
# MAGIC and there is one new employee.
# MAGIC
# MAGIC **Task:** Write a MERGE statement that:
# MAGIC - Updates salary for existing employees (match on emp_id)
# MAGIC - Inserts new employees

# COMMAND ----------

# Create the source data for the merge
employee_updates = [
    (2, "Bob", "Marketing", 80000.0, True),      # Existing: salary change
    (4, "Diana", "Engineering", 120000.0, True),  # Existing: salary change
    (6, "Frank", "Sales", 70000.0, True),         # New employee
]
df_emp_updates = spark.createDataFrame(
    employee_updates,
    ["emp_id", "name", "department", "salary", "is_active"]
)
df_emp_updates.createOrReplaceTempView("emp_updates")

print("Updates to apply:")
df_emp_updates.show()

print("Current employees:")
spark.table("practice_employees").show()

# COMMAND ----------

# YOUR CODE HERE (write a MERGE using spark.sql() or %sql)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION
# MAGIC MERGE INTO practice_employees AS target
# MAGIC USING emp_updates AS source
# MAGIC ON target.emp_id = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.salary = source.salary,
# MAGIC     target.department = source.department,
# MAGIC     target.is_active = source.is_active
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (emp_id, name, department, salary, is_active)
# MAGIC   VALUES (source.emp_id, source.name, source.department, source.salary, source.is_active)

# COMMAND ----------

# Verify the merge
print("After MERGE:")
spark.table("practice_employees").orderBy("emp_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 10: Time Travel (Domain 1)
# MAGIC
# MAGIC **Scenario:** After the MERGE, you want to see what the employees table
# MAGIC looked like BEFORE the merge.
# MAGIC
# MAGIC **Task:** Use Delta time travel to query version 0 of `practice_employees`.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
print("=== Current version ===")
spark.table("practice_employees").orderBy("emp_id").show()

print("=== Version 0 (original) ===")
spark.sql("SELECT * FROM practice_employees VERSION AS OF 0 ORDER BY emp_id").show()

print("=== Table History ===")
spark.sql("DESCRIBE HISTORY practice_employees").select(
    "version", "operation", "timestamp"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 11: Conditional Logic with CASE WHEN (Domain 2)
# MAGIC
# MAGIC **Scenario:** Categorize sales transactions into "High", "Medium", or "Low" value
# MAGIC based on total revenue (price * quantity).
# MAGIC
# MAGIC **Task:** Add a `value_tier` column:
# MAGIC - "High" if revenue >= 500
# MAGIC - "Medium" if revenue >= 100
# MAGIC - "Low" otherwise

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex11 = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn(
        "value_tier",
        when(col("revenue") >= 500, lit("High"))
        .when(col("revenue") >= 100, lit("Medium"))
        .otherwise(lit("Low"))
    )
    .select("txn_id", "product", "revenue", "value_tier")
    .orderBy(col("revenue").desc())
)
result_ex11.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 12: Date Functions (Domain 2)
# MAGIC
# MAGIC **Scenario:** Analyze sales by day of the week.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Add a column `day_of_week` using `date_format(txn_date, 'EEEE')`
# MAGIC 2. Count transactions per day of the week

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex12 = (
    df_sales
    .withColumn("day_of_week", date_format(col("txn_date"), "EEEE"))
    .groupBy("day_of_week")
    .agg(count("*").alias("num_transactions"))
    .orderBy(col("num_transactions").desc())
)
result_ex12.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 13: Schema Definition (Domain 2)
# MAGIC
# MAGIC **Scenario:** Create a DataFrame with an explicit schema (not inferred).
# MAGIC
# MAGIC **Task:** Define a schema for sensor data with columns:
# MAGIC - sensor_id (string, not nullable)
# MAGIC - temperature (double, nullable)
# MAGIC - humidity (double, nullable)
# MAGIC - timestamp (string, not nullable)
# MAGIC
# MAGIC Create a DataFrame with 3 rows using this schema.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
sensor_schema = StructType([
    StructField("sensor_id", StringType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
])

sensor_data = [
    ("sensor_001", 22.5, 45.0, "2024-01-20T10:00:00"),
    ("sensor_002", 18.3, None, "2024-01-20T10:05:00"),
    ("sensor_003", None, 62.1, "2024-01-20T10:10:00"),
]

df_sensors = spark.createDataFrame(sensor_data, schema=sensor_schema)
df_sensors.printSchema()
df_sensors.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 14: Explode Arrays (Domain 2)
# MAGIC
# MAGIC **Scenario:** A column contains comma-separated tags. Flatten them into individual rows.
# MAGIC
# MAGIC **Task:** Use `split` and `explode` to create one row per tag.

# COMMAND ----------

tags_data = [
    (1, "python,spark,delta"),
    (2, "sql,databricks"),
    (3, "streaming,kafka,spark"),
]
df_tags = spark.createDataFrame(tags_data, ["id", "tags_csv"])
df_tags.show(truncate=False)

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex14 = (
    df_tags
    .withColumn("tag", explode(split(col("tags_csv"), ",")))
    .select("id", "tag")
)
result_ex14.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 15: Structured Streaming Basics (Domain 3)
# MAGIC
# MAGIC **Scenario:** Set up a simple streaming query using the rate source.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Create a streaming DataFrame from the `rate` source (2 rows per second)
# MAGIC 2. Add a column `is_even` that is True when value is even
# MAGIC 3. Write to an in-memory table called `practice_stream`
# MAGIC 4. Query and display results after 10 seconds

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
import time

stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()
    .withColumn("is_even", col("value") % 2 == 0)
)

query = (
    stream_df
    .writeStream
    .format("memory")
    .queryName("practice_stream")
    .outputMode("append")
    .start()
)

time.sleep(10)
spark.sql("SELECT * FROM practice_stream ORDER BY timestamp DESC LIMIT 10").show()
query.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 16: PIVOT Table (Domain 2)
# MAGIC
# MAGIC **Scenario:** Create a pivot table showing total revenue by category and country.
# MAGIC
# MAGIC **Task:** Pivot the sales data so that countries become columns and cells contain
# MAGIC the total revenue (price * quantity) for each category-country combination.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex16 = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .groupBy("category")
    .pivot("country")
    .agg(spark_round(spark_sum("revenue"), 2))
)
result_ex16.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 17: SQL -- Subquery and EXISTS (Domain 2)
# MAGIC
# MAGIC **Scenario:** Find all active customers who signed up after the average signup date.
# MAGIC
# MAGIC **Task:** Write a SQL query using a subquery.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ex17 = spark.sql("""
    SELECT name, signup_date, is_active
    FROM customers
    WHERE is_active = true
      AND signup_date > (SELECT AVG(signup_date) FROM customers)
    ORDER BY signup_date
""")
result_ex17.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 18: Delta Table Operations (Domain 1 & 2)
# MAGIC
# MAGIC **Scenario:** Delete inactive employees and update Engineering salaries.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. DELETE all rows from `practice_employees` where `is_active = false`
# MAGIC 2. UPDATE all Engineering employees with a 5% raise
# MAGIC 3. Show the final state

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION
# MAGIC -- Step 1: Delete inactive employees
# MAGIC DELETE FROM practice_employees WHERE is_active = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Update Engineering salaries
# MAGIC UPDATE practice_employees
# MAGIC SET salary = ROUND(salary * 1.05, 2)
# MAGIC WHERE department = 'Engineering';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Verify
# MAGIC SELECT * FROM practice_employees ORDER BY emp_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 19: Describe and Inspect Tables (Domain 1)
# MAGIC
# MAGIC **Scenario:** Use SQL commands to inspect the `practice_employees` table metadata.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Use `DESCRIBE TABLE` to see columns
# MAGIC 2. Use `DESCRIBE DETAIL` to see table properties (format, location, size)
# MAGIC 3. Use `DESCRIBE HISTORY` to see the transaction log

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION
# MAGIC -- Step 1: Column details
# MAGIC DESCRIBE TABLE practice_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Table details (format, location, partitioning)
# MAGIC DESCRIBE DETAIL practice_employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Transaction history
# MAGIC DESCRIBE HISTORY practice_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 20: Data Quality Check (Domain 3 & 5)
# MAGIC
# MAGIC **Scenario:** Implement a simple data quality check on the sales data.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Count rows where `country` is null (data quality issue)
# MAGIC 2. Count rows where `price <= 0` (invalid data)
# MAGIC 3. Calculate the percentage of "clean" rows (no nulls, positive price)
# MAGIC
# MAGIC This mimics what DLT expectations do declaratively.

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
total_rows = df_sales.count()

null_country = df_sales.filter(col("country").isNull()).count()
invalid_price = df_sales.filter(col("price") <= 0).count()
clean_rows = df_sales.filter(
    col("country").isNotNull() & (col("price") > 0)
).count()

print(f"Total rows: {total_rows}")
print(f"Rows with null country: {null_country}")
print(f"Rows with invalid price: {invalid_price}")
print(f"Clean rows: {clean_rows}")
print(f"Data quality score: {clean_rows / total_rows * 100:.1f}%")

# DLT equivalent (syntax reference only):
# @dlt.expect_or_drop("valid_country", "country IS NOT NULL")
# @dlt.expect_or_drop("valid_price", "price > 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up all tables and views created in this notebook
spark.sql("DROP TABLE IF EXISTS practice_sales")
spark.sql("DROP TABLE IF EXISTS practice_employees")
spark.sql("DROP VIEW IF EXISTS sales")
spark.sql("DROP VIEW IF EXISTS customers")
spark.sql("DROP VIEW IF EXISTS emp_updates")
print("Cleanup complete. All practice tables and views removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Congratulations!** You have completed 20 hands-on exercises covering the Associate exam domains.
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Review any exercises where you struggled
# MAGIC - Revisit the corresponding course modules for deeper understanding
# MAGIC - Move on to Topic 03 (Professional Exam Guide) if pursuing that certification
