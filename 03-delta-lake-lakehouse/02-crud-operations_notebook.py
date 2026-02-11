# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # CRUD & MERGE Operations
# MAGIC > Module 03 -- Topic 02 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a Delta table with product inventory data
# MAGIC 2. INSERT new rows (SQL and DataFrame API)
# MAGIC 3. UPDATE rows with conditions
# MAGIC 4. DELETE rows
# MAGIC 5. Perform a full MERGE (upsert) with SCD Type 1 logic
# MAGIC 6. Inspect MERGE output metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create the Base Table

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from delta.tables import DeltaTable

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.products")

schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock", IntegerType(), True),
])

data = [
    (1, "Laptop Pro 15", "Electronics", 1299.99, 50),
    (2, "Wireless Mouse", "Accessories", 29.99, 200),
    (3, "Mechanical Keyboard", "Accessories", 89.99, 150),
    (4, "4K Monitor", "Electronics", 449.99, 30),
    (5, "USB-C Hub", "Accessories", 49.99, 100),
    (6, "Webcam HD", "Electronics", 69.99, 80),
    (7, "Desk Lamp", "Office", 34.99, 120),
    (8, "Standing Desk", "Office", 599.99, 25),
]

df = spark.createDataFrame(data, schema=schema)
df.write.format("delta").mode("overwrite").saveAsTable("module03.products")

print("Base table created:")
spark.table("module03.products").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSERT -- Adding New Rows
# MAGIC
# MAGIC ### Method 1: SQL INSERT INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO module03.products VALUES
# MAGIC   (9, 'Noise-Cancelling Headphones', 'Electronics', 249.99, 60),
# MAGIC   (10, 'Ergonomic Chair', 'Office', 399.99, 40);
# MAGIC
# MAGIC SELECT * FROM module03.products WHERE product_id >= 9;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: DataFrame Append

# COMMAND ----------

new_products = spark.createDataFrame(
    [(11, "Portable SSD", "Storage", 89.99, 200),
     (12, "Docking Station", "Accessories", 179.99, 45)],
    schema=schema
)

new_products.write.format("delta").mode("append").saveAsTable("module03.products")

print(f"Total rows after inserts: {spark.table('module03.products').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE -- Modifying Existing Rows
# MAGIC
# MAGIC ### Method 1: SQL UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 10% price increase for all Electronics
# MAGIC UPDATE module03.products
# MAGIC SET price = price * 1.10
# MAGIC WHERE category = 'Electronics';
# MAGIC
# MAGIC SELECT * FROM module03.products WHERE category = 'Electronics';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: DeltaTable Python API

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "module03.products")

# Restock all accessories to at least 250 units
delta_table.update(
    condition="category = 'Accessories' AND stock < 250",
    set={"stock": "250"}
)

spark.table("module03.products").filter("category = 'Accessories'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE -- Removing Rows
# MAGIC
# MAGIC ### Method 1: SQL DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove products with low stock that are too expensive
# MAGIC DELETE FROM module03.products
# MAGIC WHERE stock < 30 AND price > 500;
# MAGIC
# MAGIC SELECT * FROM module03.products ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: DeltaTable Python API

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "module03.products")
delta_table.delete(condition="product_id = 12")

print(f"Rows after deletes: {spark.table('module03.products').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE -- The Complete Upsert
# MAGIC
# MAGIC This is the most important DML operation for Delta Lake. We will simulate
# MAGIC an incoming batch of product updates:
# MAGIC - Some products exist (should be updated)
# MAGIC - Some are new (should be inserted)

# COMMAND ----------

# Incoming update batch
updates_data = [
    (1, "Laptop Pro 15 (2025)", "Electronics", 1399.99, 75),   # UPDATE: new model, new price
    (2, "Wireless Mouse", "Accessories", 24.99, 300),           # UPDATE: price drop, restock
    (5, "USB-C Hub", "Accessories", 44.99, 150),                # UPDATE: price drop, restock
    (13, "Smart Whiteboard", "Office", 899.99, 15),             # INSERT: new product
    (14, "Cable Management Kit", "Accessories", 19.99, 500),    # INSERT: new product
]

updates_df = spark.createDataFrame(updates_data, schema=schema)

print("Incoming updates:")
updates_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE using SQL (SCD Type 1 -- Overwrite)

# COMMAND ----------

updates_df.createOrReplaceTempView("product_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO module03.products AS target
# MAGIC USING product_updates AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.category = source.category,
# MAGIC     target.price = source.price,
# MAGIC     target.stock = source.stock
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (product_id, name, category, price, stock)
# MAGIC   VALUES (source.product_id, source.name, source.category,
# MAGIC           source.price, source.stock)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the merge results
# MAGIC SELECT * FROM module03.products ORDER BY product_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE using the DeltaTable Python API
# MAGIC
# MAGIC Let's do another merge round with the Python API.

# COMMAND ----------

# Another batch of updates
batch2_data = [
    (2, "Wireless Mouse v2", "Accessories", 34.99, 400),   # UPDATE
    (15, "Ring Light", "Electronics", 44.99, 100),          # INSERT
]
batch2_df = spark.createDataFrame(batch2_data, schema=schema)

delta_table = DeltaTable.forName(spark, "module03.products")

delta_table.alias("t").merge(
    batch2_df.alias("s"),
    "t.product_id = s.product_id"
).whenMatchedUpdate(
    set={
        "name": "s.name",
        "category": "s.category",
        "price": "s.price",
        "stock": "s.stock",
    }
).whenNotMatchedInsert(
    values={
        "product_id": "s.product_id",
        "name": "s.name",
        "category": "s.category",
        "price": "s.price",
        "stock": "s.stock",
    }
).execute()

print("After Python API merge:")
spark.table("module03.products").orderBy("product_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Table History
# MAGIC
# MAGIC Every DML operation creates a new version in the transaction log.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY module03.products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.products")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
