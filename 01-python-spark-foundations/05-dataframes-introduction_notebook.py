# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrames Introduction -- Hands-On
# MAGIC
# MAGIC Practice creating DataFrames, defining schemas, performing column operations,
# MAGIC and converting between Spark and Pandas.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating DataFrames from Python Lists

# COMMAND ----------

# Method 1: List of tuples with column names
data = [
    ("Alice", 34, "Engineering", 95000.0),
    ("Bob", 28, "Marketing", 72000.0),
    ("Charlie", 45, "Engineering", 120000.0),
    ("Diana", 31, "Marketing", 68000.0),
    ("Eve", 26, "Data Science", 85000.0),
    ("Frank", 39, "Data Science", 110000.0),
]

df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating DataFrames from Row Objects

# COMMAND ----------

from pyspark.sql import Row

rows = [
    Row(product="Laptop", category="Electronics", price=999.99, in_stock=True),
    Row(product="Shirt", category="Clothing", price=29.99, in_stock=True),
    Row(product="Headphones", category="Electronics", price=149.99, in_stock=False),
    Row(product="Jeans", category="Clothing", price=59.99, in_stock=True),
    Row(product="Tablet", category="Electronics", price=499.99, in_stock=True),
]

products_df = spark.createDataFrame(rows)
products_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating DataFrames from Dictionaries

# COMMAND ----------

dict_data = [
    {"city": "New York", "state": "NY", "population": 8336817},
    {"city": "Los Angeles", "state": "CA", "population": 3979576},
    {"city": "Chicago", "state": "IL", "population": 2693976},
    {"city": "Houston", "state": "TX", "population": 2320268},
    {"city": "Phoenix", "state": "AZ", "population": 1680992},
]

cities_df = spark.createDataFrame(dict_data)
cities_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Defining Schemas with StructType

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

# Explicit schema
employee_schema = StructType([
    StructField("emp_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("is_manager", BooleanType(), nullable=True),
])

emp_data = [
    (1, "Alice", "Engineering", 95000.0, False),
    (2, "Bob", "Marketing", 72000.0, False),
    (3, "Charlie", "Engineering", 120000.0, True),
    (4, "Diana", "Marketing", 68000.0, True),
]

emp_df = spark.createDataFrame(emp_data, schema=employee_schema)
emp_df.printSchema()
emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DDL-Style Schema String

# COMMAND ----------

ddl_schema = "order_id INT, customer STRING, amount DOUBLE, order_date DATE"

order_data = [
    (1, "Alice", 250.50, "2024-01-15"),
    (2, "Bob", 175.25, "2024-01-16"),
    (3, "Alice", 320.00, "2024-01-17"),
]

# Note: date strings need to be parsed; use StringType then cast, or use createDataFrame with Rows
from datetime import date
order_data_typed = [
    (1, "Alice", 250.50, date(2024, 1, 15)),
    (2, "Bob", 175.25, date(2024, 1, 16)),
    (3, "Alice", 320.00, date(2024, 1, 17)),
]

orders_df = spark.createDataFrame(order_data_typed, schema=ddl_schema)
orders_df.printSchema()
orders_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Selecting Columns

# COMMAND ----------

from pyspark.sql.functions import col

# Four ways to select columns
print("=== By string name ===")
df.select("name", "salary").show()

print("=== By col() function ===")
df.select(col("name"), col("salary")).show()

print("=== By DataFrame attribute ===")
df.select(df.name, df.salary).show()

print("=== By bracket notation ===")
df.select(df["name"], df["salary"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Adding and Modifying Columns with withColumn

# COMMAND ----------

from pyspark.sql.functions import upper, round as spark_round, lit, when

# Add a new column
df_enhanced = (
    df
    .withColumn("salary_monthly", spark_round(col("salary") / 12, 2))
    .withColumn("name_upper", upper(col("name")))
    .withColumn("country", lit("USA"))
    .withColumn("seniority", when(col("age") >= 35, "Senior").otherwise("Junior"))
)

df_enhanced.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Filtering Rows

# COMMAND ----------

print("=== Age > 30 ===")
df.filter(col("age") > 30).show()

print("=== SQL expression string ===")
df.filter("salary >= 80000").show()

print("=== AND condition ===")
df.filter((col("age") > 30) & (col("department") == "Engineering")).show()

print("=== OR condition ===")
df.filter((col("department") == "Engineering") | (col("department") == "Data Science")).show()

print("=== Using isin ===")
df.filter(col("department").isin("Engineering", "Data Science")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Dropping and Renaming Columns

# COMMAND ----------

# Drop a column
df_no_dept = df.drop("department")
print("After dropping 'department':")
df_no_dept.show()

# Rename a column
df_renamed = df.withColumnRenamed("name", "full_name")
print("After renaming 'name' to 'full_name':")
df_renamed.show()

# Rename with alias inside select
df_aliased = df.select(
    col("name").alias("employee_name"),
    col("age"),
    col("salary").alias("annual_salary"),
)
print("With aliases:")
df_aliased.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Using col() vs. expr()

# COMMAND ----------

from pyspark.sql.functions import expr

# col() for simple operations
df_col = df.withColumn("tax", col("salary") * 0.08)

# expr() for complex SQL-like expressions
df_expr = df.withColumn("tax", expr("salary * 0.08"))
df_expr2 = df.withColumn(
    "salary_band",
    expr("CASE WHEN salary >= 100000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END")
)

print("Using expr() for CASE WHEN:")
df_expr2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Inspecting DataFrames

# COMMAND ----------

# Schema as a tree
print("=== printSchema() ===")
df.printSchema()

# Column names
print("=== columns ===")
print(df.columns)

# Column names and types
print("\n=== dtypes ===")
print(df.dtypes)

# Row count
print(f"\n=== count ===")
print(f"Rows: {df.count()}")

# Summary statistics
print("\n=== describe() ===")
df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Sorting and Ordering

# COMMAND ----------

from pyspark.sql.functions import desc, asc

# Sort ascending (default)
print("=== Sort by age ascending ===")
df.orderBy("age").show()

# Sort descending
print("=== Sort by salary descending ===")
df.orderBy(desc("salary")).show()

# Multi-column sort
print("=== Sort by department asc, salary desc ===")
df.orderBy(asc("department"), desc("salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Handling Duplicates and Nulls

# COMMAND ----------

# Create data with duplicates and nulls
dup_data = [
    ("Alice", 34, 95000.0),
    ("Bob", 28, 72000.0),
    ("Alice", 34, 95000.0),  # duplicate
    ("Charlie", None, 120000.0),  # null age
    ("Diana", 31, None),  # null salary
]

dup_df = spark.createDataFrame(dup_data, ["name", "age", "salary"])
print("=== Original with duplicates and nulls ===")
dup_df.show()

# Remove exact duplicates
print("=== After dropDuplicates() ===")
dup_df.dropDuplicates().show()

# Drop rows with any null
print("=== After dropna() ===")
dup_df.dropna().show()

# Fill nulls
print("=== After fillna ===")
dup_df.fillna({"age": 0, "salary": 0.0}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Converting Between Spark and Pandas

# COMMAND ----------

import pandas as pd

# Spark DataFrame to Pandas
pdf = df.toPandas()
print(f"Pandas DataFrame type: {type(pdf)}")
print(f"Shape: {pdf.shape}")
print(pdf.head())

# COMMAND ----------

# Pandas to Spark DataFrame
pandas_df = pd.DataFrame({
    "fruit": ["apple", "banana", "cherry", "date"],
    "quantity": [10, 25, 5, 15],
    "price": [1.50, 0.75, 3.00, 5.00],
})

spark_df = spark.createDataFrame(pandas_df)
spark_df.show()
spark_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Chaining It All Together
# MAGIC
# MAGIC A realistic pipeline combining multiple operations.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, round as spark_round

# Generate a larger dataset
from pyspark.sql.functions import rand, floor

sales_df = (
    spark.range(0, 10000)
    .withColumn("store_id", (col("id") % 5 + 1).cast("int"))
    .withColumn("product",
        when(col("id") % 4 == 0, "Widget")
        .when(col("id") % 4 == 1, "Gadget")
        .when(col("id") % 4 == 2, "Doohickey")
        .otherwise("Thingamajig")
    )
    .withColumn("amount", spark_round(rand(seed=42) * 200 + 10, 2))
    .drop("id")
)

# Full pipeline
summary = (
    sales_df
    .filter(col("amount") > 50)
    .groupBy("store_id", "product")
    .agg(
        count("*").alias("num_sales"),
        spark_round(avg("amount"), 2).alias("avg_amount"),
        spark_round(_sum("amount"), 2).alias("total_amount"),
    )
    .orderBy("store_id", desc("total_amount"))
)

print("=== Sales Summary ===")
summary.show(25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC No temp views or tables were created. All data was generated inline.

# COMMAND ----------

print("Notebook complete. All DataFrame exercises finished.")
