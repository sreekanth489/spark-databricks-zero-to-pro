# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Evolution
# MAGIC > Module 03 -- Topic 04 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. See schema enforcement reject a mismatched write
# MAGIC 2. Enable mergeSchema and add new columns
# MAGIC 3. Demonstrate nested struct schema evolution
# MAGIC 4. Use column mapping for DROP COLUMN and RENAME COLUMN
# MAGIC 5. Compare mergeSchema vs overwriteSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Base Table

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.schema_evo")

schema_v1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
])

data_v1 = [
    (1, "Alice", "Engineering", 95000.0),
    (2, "Bob", "Marketing", 72000.0),
    (3, "Carol", "Engineering", 105000.0),
]

df_v1 = spark.createDataFrame(data_v1, schema=schema_v1)
df_v1.write.format("delta").mode("overwrite").saveAsTable("module03.schema_evo")

print("Base table schema:")
spark.table("module03.schema_evo").printSchema()
spark.table("module03.schema_evo").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Schema Enforcement (Default)
# MAGIC
# MAGIC By default, Delta rejects writes with columns not in the table schema.

# COMMAND ----------

# This DataFrame has an extra column 'location' not in the table
schema_v2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("location", StringType(), True),  # NEW column
])

new_data = [(4, "David", "Sales", 68000.0, "New York")]
df_extra_col = spark.createDataFrame(new_data, schema=schema_v2)

try:
    # This WILL FAIL because 'location' is not in the target schema
    df_extra_col.write.format("delta").mode("append").saveAsTable("module03.schema_evo")
except Exception as e:
    print("SCHEMA ENFORCEMENT ERROR (expected):")
    print(str(e)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Enable mergeSchema
# MAGIC
# MAGIC Adding `mergeSchema=true` tells Delta to add new columns automatically.

# COMMAND ----------

# Same write, but now with mergeSchema enabled
df_extra_col.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("module03.schema_evo")

print("After mergeSchema -- notice the new 'location' column:")
spark.table("module03.schema_evo").printSchema()
spark.table("module03.schema_evo").show(truncate=False)

# Note: existing rows have location = null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Adding Multiple New Columns
# MAGIC
# MAGIC Schema evolution can add several columns in a single write.

# COMMAND ----------

from pyspark.sql.types import BooleanType

schema_v3 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("is_manager", BooleanType(), True),   # NEW
    StructField("hire_year", IntegerType(), True),     # NEW
])

more_data = [
    (5, "Eve", "Marketing", 78000.0, "Chicago", True, 2022),
    (6, "Frank", "Engineering", 112000.0, "San Jose", False, 2024),
]

df_v3 = spark.createDataFrame(more_data, schema=schema_v3)
df_v3.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("module03.schema_evo")

print("After adding is_manager and hire_year:")
spark.table("module03.schema_evo").printSchema()
spark.table("module03.schema_evo").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Nested Struct Schema Evolution
# MAGIC
# MAGIC Delta supports adding fields inside nested structs.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.nested_evo")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create table with a nested address struct
nested_schema_v1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
    ]))
])

nested_data_v1 = [
    (1, "Alice", {"street": "123 Main St", "city": "Seattle"}),
    (2, "Bob", {"street": "456 Oak Ave", "city": "Portland"}),
]

df_nested_v1 = spark.createDataFrame(nested_data_v1, schema=nested_schema_v1)
df_nested_v1.write.format("delta").mode("overwrite").saveAsTable("module03.nested_evo")

print("Nested schema v1:")
spark.table("module03.nested_evo").printSchema()

# COMMAND ----------

# Now add zip_code inside the address struct
nested_schema_v2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip_code", StringType(), True),  # NEW nested field
    ]))
])

nested_data_v2 = [
    (3, "Carol", {"street": "789 Pine Rd", "city": "Denver", "zip_code": "80202"}),
]

df_nested_v2 = spark.createDataFrame(nested_data_v2, schema=nested_schema_v2)
df_nested_v2.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("module03.nested_evo")

print("Nested schema v2 (zip_code added inside address):")
spark.table("module03.nested_evo").printSchema()
spark.table("module03.nested_evo").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Column Mapping: DROP and RENAME
# MAGIC
# MAGIC Column mapping mode must be enabled to support DROP/RENAME operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable column mapping on the main table
# MAGIC ALTER TABLE module03.schema_evo SET TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP COLUMN: remove is_manager
# MAGIC ALTER TABLE module03.schema_evo DROP COLUMN is_manager;
# MAGIC
# MAGIC -- Verify
# MAGIC DESCRIBE module03.schema_evo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RENAME COLUMN: hire_year -> year_joined
# MAGIC ALTER TABLE module03.schema_evo RENAME COLUMN hire_year TO year_joined;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT * FROM module03.schema_evo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- overwriteSchema (Complete Replacement)
# MAGIC
# MAGIC Unlike mergeSchema, overwriteSchema replaces the entire table schema.
# MAGIC This requires `mode("overwrite")`.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.overwrite_demo")

# Create initial table
init_data = [(1, "Alice", 100), (2, "Bob", 200)]
init_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", IntegerType()),
])
spark.createDataFrame(init_data, init_schema).write.format("delta") \
    .mode("overwrite").saveAsTable("module03.overwrite_demo")

print("Before overwriteSchema:")
spark.table("module03.overwrite_demo").printSchema()
spark.table("module03.overwrite_demo").show()

# COMMAND ----------

# Completely different schema
new_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("price", DoubleType()),
    StructField("category", StringType()),
])
new_data = [(101, "Laptop", 999.99, "Electronics")]

spark.createDataFrame(new_data, new_schema).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("module03.overwrite_demo")

print("After overwriteSchema -- completely new schema and data:")
spark.table("module03.overwrite_demo").printSchema()
spark.table("module03.overwrite_demo").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Approach | Adds Cols | Removes Cols | Changes Types | Preserves Data |
# MAGIC |----------|-----------|-------------|---------------|----------------|
# MAGIC | Default (enforcement) | No | N/A | No | Yes |
# MAGIC | mergeSchema | Yes | No | No | Yes |
# MAGIC | overwriteSchema | Yes | Yes | Yes | No |
# MAGIC | Column mapping + ALTER | Via ALTER | Yes | No | Yes |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.schema_evo")
spark.sql("DROP TABLE IF EXISTS module03.nested_evo")
spark.sql("DROP TABLE IF EXISTS module03.overwrite_demo")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
