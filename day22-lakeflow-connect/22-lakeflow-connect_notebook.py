# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 22: Lakeflow Connect -- Data Ingestion into the Lakehouse
# MAGIC
# MAGIC **Objective**: Master the three ingestion methods in Lakeflow Connect:
# MAGIC Manual File Upload, Standard Connectors, and Managed Connectors
# MAGIC
# MAGIC **Key Concept**: Lakeflow Connect is the INGESTION layer of the Lakeflow ecosystem.
# MAGIC It brings external data INTO the Lakehouse. It does NOT transform data --
# MAGIC that is the job of Spark Declarative Pipelines (Day 24).
# MAGIC
# MAGIC ```
# MAGIC Lakeflow Ecosystem:
# MAGIC   Connect (Ingest)  -->  Spark Declarative Pipelines (Transform)  -->  Jobs (Orchestrate)
# MAGIC     Day 22                         Day 24                                  Day 25
# MAGIC ```
# MAGIC
# MAGIC **Three Ingestion Types**:
# MAGIC
# MAGIC | Type | Method | Complexity | Best For |
# MAGIC |------|--------|------------|----------|
# MAGIC | Manual Upload | UI drag-and-drop | None | Ad-hoc, small files |
# MAGIC | Standard Connectors | Code (PySpark/SQL) | Medium | Custom pipelines |
# MAGIC | Managed Connectors | No-code UI/SQL | Low | Enterprise DB & SaaS |
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lakeflow_connect_lab
# MAGIC COMMENT 'Day 22: Lakeflow Connect lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA lakeflow_connect_lab

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType
)
from pyspark.sql.functions import (
    col, current_timestamp, lit, from_json, expr,
    to_timestamp, rand, round as spark_round
)
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Generate Sample Data
# MAGIC
# MAGIC We create sample data files to simulate external sources.
# MAGIC In production, these would exist in S3, a database, or a SaaS app.

# COMMAND ----------

# -- Products CSV --
products_data = [
    ("P001", "Wireless Mouse", "Electronics", 29.99, "TechCorp"),
    ("P002", "USB-C Hub", "Electronics", 49.99, "TechCorp"),
    ("P003", "Standing Desk", "Furniture", 399.99, "OfficePro"),
    ("P004", "Ergonomic Chair", "Furniture", 549.99, "OfficePro"),
    ("P005", "Mechanical Keyboard", "Electronics", 89.99, "KeyMasters"),
    ("P006", "Monitor Arm", "Accessories", 79.99, "MountIt"),
    ("P007", "Desk Lamp", "Lighting", 34.99, "BrightLife"),
    ("P008", "Webcam HD", "Electronics", 69.99, "TechCorp"),
    ("P009", "Noise-Cancelling Headphones", "Electronics", 199.99, "AudioWave"),
    ("P010", "Laptop Stand", "Accessories", 44.99, "MountIt"),
]

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("supplier", StringType(), False),
])

products_df = spark.createDataFrame(data=products_data, schema=products_schema)

# Write to S3 as CSV (simulating an external source)
products_path = "s3://ecommerce-lakehouse/raw/lakeflow-connect/products/"
products_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(products_path)

print(f"Products written to: {products_path}")

# COMMAND ----------

# -- Customers JSON (Batch 1) --
customers_data = [
    ("C001", "Alice Johnson", "alice.johnson@example.com", "2024-01-15", "Gold"),
    ("C002", "Bob Smith", "bob.smith@example.com", "2024-02-20", "Silver"),
    ("C003", "Carol Williams", "carol.w@example.com", "2024-03-10", "Bronze"),
    ("C004", "David Brown", "david.brown@example.com", "2024-04-05", "Gold"),
    ("C005", "Eva Martinez", "eva.m@example.com", "2024-05-18", "Silver"),
    ("C006", "Frank Lee", "frank.lee@example.com", "2024-06-22", "Bronze"),
    ("C007", "Grace Kim", "grace.kim@example.com", "2024-07-30", "Gold"),
    ("C008", "Henry Davis", "henry.d@example.com", "2024-08-14", "Silver"),
]

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("signup_date", StringType(), False),
    StructField("tier", StringType(), False),
])

customers_df = spark.createDataFrame(data=customers_data, schema=customers_schema)

# Write to S3 as JSON (simulating an external source)
customers_path = "s3://ecommerce-lakehouse/raw/lakeflow-connect/customers/"
customers_df.coalesce(1).write.mode("overwrite").json(customers_path)

print(f"Customers written to: {customers_path}")

# COMMAND ----------

# -- Clickstream data (multiple small batches to simulate streaming) --
import random

def generate_clickstream_batch(batch_id, num_records=20):
    """Generate a batch of clickstream events."""
    actions = ["page_view", "add_to_cart", "purchase", "search", "wishlist"]
    pages = ["/home", "/products", "/cart", "/checkout", "/search", "/account"]
    product_ids = [f"P{str(i).zfill(3)}" for i in range(1, 11)]
    customer_ids = [f"C{str(i).zfill(3)}" for i in range(1, 9)]

    random.seed(batch_id * 42)
    records = []
    for i in range(num_records):
        records.append((
            f"EVT-{batch_id}-{str(i).zfill(4)}",
            random.choice(customer_ids),
            random.choice(product_ids),
            random.choice(actions),
            random.choice(pages),
            f"2024-09-{random.randint(1,30):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            random.choice(["mobile", "desktop", "tablet"]),
        ))
    return records

clickstream_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("page", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("device", StringType(), False),
])

# Write 3 batches to simulate files arriving over time
clickstream_path = "s3://ecommerce-lakehouse/raw/lakeflow-connect/clickstream/"

for batch_id in range(1, 4):
    batch_data = generate_clickstream_batch(batch_id=batch_id)
    batch_df = spark.createDataFrame(data=batch_data, schema=clickstream_schema)
    batch_df.coalesce(1).write.mode("append").json(
        f"{clickstream_path}"
    )
    print(f"  Clickstream batch {batch_id} written ({len(batch_data)} events)")

print(f"\nClickstream data written to: {clickstream_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Standard Connector -- Batch Ingestion (spark.read)
# MAGIC
# MAGIC The simplest standard connector pattern: read an entire file/directory and
# MAGIC write it as a managed table. This is a **full load** -- every run replaces
# MAGIC (or appends to) the target table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Batch Ingest Products CSV

# COMMAND ----------

# Read CSV from S3 -- Standard Connector (batch mode)
batch_products_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(products_path)
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingestion_method", lit("standard_connector_batch"))
)

batch_products_df.display()

# COMMAND ----------

# Write to Unity Catalog managed table (full overwrite)
batch_products_df.write \
    .mode("overwrite") \
    .saveAsTable("ecommerce.lakeflow_connect_lab.products_batch")

print("Products ingested via batch (full load) -> ecommerce.lakeflow_connect_lab.products_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce.lakeflow_connect_lab.products_batch

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Batch Ingest Customers JSON

# COMMAND ----------

# Read JSON from S3 -- Standard Connector (batch mode)
batch_customers_df = (
    spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(customers_path)
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingestion_method", lit("standard_connector_batch"))
)

batch_customers_df.write \
    .mode("overwrite") \
    .saveAsTable("ecommerce.lakeflow_connect_lab.customers_batch")

print("Customers ingested via batch (full load) -> ecommerce.lakeflow_connect_lab.customers_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce.lakeflow_connect_lab.customers_batch

# COMMAND ----------

# MAGIC %md
# MAGIC **Batch ingestion summary**:
# MAGIC - Reads ALL data every time (full load)
# MAGIC - Simple to implement: `spark.read` + `write.mode("overwrite")`
# MAGIC - Best for small dimension tables or when you need a complete snapshot
# MAGIC - Not efficient for large tables that change incrementally

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Standard Connector -- Auto Loader (Streaming Ingestion)
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) is the **recommended standard connector for cloud
# MAGIC file ingestion**. It incrementally discovers and processes new files.
# MAGIC
# MAGIC This is the same Auto Loader from Day 20, now framed as part of Lakeflow Connect.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Auto Loader with Schema Inference

# COMMAND ----------

# Auto Loader: Standard Connector for cloud files
# Uses cloudFiles format to incrementally process new files from S3
clickstream_checkpoint = "s3://ecommerce-lakehouse/checkpoints/lakeflow-connect/clickstream"
clickstream_schema_loc = "s3://ecommerce-lakehouse/schemas/lakeflow-connect/clickstream"

clickstream_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", clickstream_schema_loc)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(clickstream_path)
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_method", lit("standard_connector_autoloader"))
)

# Write as a streaming table in Unity Catalog
query = (
    clickstream_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", clickstream_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)  # Process all available files, then stop
    .toTable("ecommerce.lakeflow_connect_lab.clickstream_autoloader")
)

query.awaitTermination()
print("Clickstream ingested via Auto Loader -> ecommerce.lakeflow_connect_lab.clickstream_autoloader")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ingestion_method, source_file, COUNT(*) as record_count
# MAGIC FROM ecommerce.lakeflow_connect_lab.clickstream_autoloader
# MAGIC GROUP BY ingestion_method, source_file
# MAGIC ORDER BY source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Schema Evolution with Auto Loader
# MAGIC
# MAGIC Let's simulate a new batch of clickstream data with an additional column
# MAGIC (`session_id`). Auto Loader handles this automatically with
# MAGIC `schemaEvolutionMode = addNewColumns`.

# COMMAND ----------

# Generate a new batch with an extra column (session_id)
evolved_data = [
    ("EVT-4-0001", "C001", "P002", "page_view", "/products", "2024-10-01T10:00:00", "desktop", "SESS-1001"),
    ("EVT-4-0002", "C003", "P005", "add_to_cart", "/cart", "2024-10-01T10:05:00", "mobile", "SESS-1002"),
    ("EVT-4-0003", "C005", "P008", "purchase", "/checkout", "2024-10-01T10:10:00", "desktop", "SESS-1003"),
]

evolved_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("page", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("device", StringType(), False),
    StructField("session_id", StringType(), True),  # NEW column
])

evolved_df = spark.createDataFrame(data=evolved_data, schema=evolved_schema)
evolved_df.coalesce(1).write.mode("append").json(clickstream_path)
print("New batch with session_id column written to clickstream path")

# COMMAND ----------

# Re-run Auto Loader -- it will pick up only the new file and evolve the schema
query2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", clickstream_schema_loc)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(clickstream_path)
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_method", lit("standard_connector_autoloader"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", clickstream_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("ecommerce.lakeflow_connect_lab.clickstream_autoloader")
)

query2.awaitTermination()
print("Schema evolution complete -- session_id column added automatically")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution: session_id column should appear (NULL for older records)
# MAGIC SELECT event_id, customer_id, action, device, session_id, source_file
# MAGIC FROM ecommerce.lakeflow_connect_lab.clickstream_autoloader
# MAGIC ORDER BY event_id
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC **Auto Loader ingestion summary**:
# MAGIC - Incrementally discovers new files (no re-processing)
# MAGIC - Schema inference persisted to `schemaLocation`
# MAGIC - Schema evolution handles new columns automatically
# MAGIC - Exactly-once guarantees via checkpoint
# MAGIC - Recommended for all cloud file ingestion in production

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Standard Connector -- Simulated Kafka Streaming
# MAGIC
# MAGIC In production, you would use `spark.readStream.format("kafka")` to read from
# MAGIC a Kafka topic. Here we simulate the pattern using rate source.

# COMMAND ----------

# Simulate a Kafka-like streaming source using rate source
# In production, replace with: .format("kafka").option("subscribe", "topic")

kafka_sim_checkpoint = "s3://ecommerce-lakehouse/checkpoints/lakeflow-connect/kafka-sim"

simulated_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .option("numPartitions", 2)
    .load()
    .withColumn("customer_id", expr("concat('C', lpad(cast(abs(hash(value)) % 8 + 1 as string), 3, '0'))"))
    .withColumn("product_id", expr("concat('P', lpad(cast(abs(hash(value + 100)) % 10 + 1 as string), 3, '0'))"))
    .withColumn("action", expr("""
        CASE abs(hash(value + 200)) % 5
            WHEN 0 THEN 'page_view'
            WHEN 1 THEN 'add_to_cart'
            WHEN 2 THEN 'purchase'
            WHEN 3 THEN 'search'
            ELSE 'wishlist'
        END
    """))
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingestion_method", lit("standard_connector_kafka"))
    .select("timestamp", "customer_id", "product_id", "action", "ingested_at", "ingestion_method")
)

# Run for a short burst then stop
query3 = (
    simulated_stream.writeStream
    .format("delta")
    .option("checkpointLocation", kafka_sim_checkpoint)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .toTable("ecommerce.lakeflow_connect_lab.events_kafka_sim")
)

# Let it run for 15 seconds, then stop
time.sleep(15)
query3.stop()
print("Kafka simulation stopped -> ecommerce.lakeflow_connect_lab.events_kafka_sim")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ingestion_method, action, COUNT(*) as event_count
# MAGIC FROM ecommerce.lakeflow_connect_lab.events_kafka_sim
# MAGIC GROUP BY ingestion_method, action
# MAGIC ORDER BY event_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC **Kafka streaming ingestion summary**:
# MAGIC - In production: `spark.readStream.format("kafka").option("subscribe", "topic")`
# MAGIC - Parse JSON payloads with `from_json(col("value").cast("string"), schema)`
# MAGIC - Continuous ingestion with exactly-once via checkpoints
# MAGIC - Best for real-time event streams, IoT data, and message queues

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Managed Connectors (UI-Based Setup)
# MAGIC
# MAGIC Managed Connectors are **no-code**, **serverless** connectors configured
# MAGIC entirely through the Databricks UI or SQL. They are purpose-built for
# MAGIC databases and SaaS applications.
# MAGIC
# MAGIC ### How to Set Up a Managed Connector (UI Walkthrough)
# MAGIC
# MAGIC **Step 1**: Navigate to **Catalog** in the left sidebar
# MAGIC
# MAGIC **Step 2**: Click **Create** > **Connection**
# MAGIC
# MAGIC **Step 3**: Select your source type:
# MAGIC - Databases: PostgreSQL, MySQL, SQL Server, Oracle, Db2
# MAGIC - SaaS: Salesforce, Workday, ServiceNow, Dynamics 365
# MAGIC
# MAGIC **Step 4**: Enter connection details:
# MAGIC - Host, port, database name
# MAGIC - Authentication (username/password or secret scope)
# MAGIC
# MAGIC **Step 5**: Test the connection
# MAGIC
# MAGIC **Step 6**: Select tables to ingest
# MAGIC - Choose specific tables or entire schemas
# MAGIC - Configure incremental vs full load per table
# MAGIC
# MAGIC **Step 7**: Choose destination catalog and schema
# MAGIC
# MAGIC **Step 8**: Set schedule (continuous CDC or triggered)
# MAGIC
# MAGIC **Step 9**: Start the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Connector via SQL
# MAGIC
# MAGIC You can also create Managed Connectors using SQL statements.
# MAGIC This is useful for version-controlled, repeatable setups.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: This SQL is for reference only.
# MAGIC -- It requires an actual PostgreSQL instance to connect to.
# MAGIC -- Uncomment and modify for your environment.
# MAGIC
# MAGIC -- Step 1: Create a connection object in Unity Catalog
# MAGIC -- CREATE CONNECTION IF NOT EXISTS ecommerce_postgres
# MAGIC -- TYPE postgresql
# MAGIC -- OPTIONS (
# MAGIC --     host 'ecommerce-db.example.com',
# MAGIC --     port '5432',
# MAGIC --     user secret('jdbc-secrets', 'pg-username'),
# MAGIC --     password secret('jdbc-secrets', 'pg-password')
# MAGIC -- );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Create a streaming table that ingests via CDC
# MAGIC -- CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.pg_orders
# MAGIC -- AS SELECT * FROM STREAM read_changefeed(
# MAGIC --     'ecommerce_postgres',
# MAGIC --     'public.orders'
# MAGIC -- );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Connector Key Benefits
# MAGIC
# MAGIC | Feature | Detail |
# MAGIC |---------|--------|
# MAGIC | **No code** | Configure via UI or simple SQL |
# MAGIC | **CDC-based** | Only reads changes (inserts, updates, deletes) |
# MAGIC | **Serverless** | No clusters to provision or manage |
# MAGIC | **Auto-scaling** | Compute scales with data volume |
# MAGIC | **Schema evolution** | New source columns added automatically |
# MAGIC | **Unity Catalog** | Connection objects, tables, and lineage all governed |
# MAGIC | **Monitoring** | Built-in pipeline health dashboards |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6: Manual File Upload
# MAGIC
# MAGIC The simplest ingestion method. Upload files directly through the
# MAGIC Databricks workspace UI.
# MAGIC
# MAGIC ### How to Upload Files (UI Walkthrough)
# MAGIC
# MAGIC **Option A: Upload to a Volume**
# MAGIC 1. Navigate to **Catalog** > select your catalog/schema
# MAGIC 2. Click on a **Volume** (or create one)
# MAGIC 3. Click **Upload to this volume**
# MAGIC 4. Drag and drop your file (CSV, JSON, Parquet, etc.)
# MAGIC 5. The file is stored in the volume and accessible via path
# MAGIC
# MAGIC **Option B: Upload as a Table**
# MAGIC 1. Navigate to **Catalog** > select your catalog/schema
# MAGIC 2. Click **Create** > **Create table**
# MAGIC 3. Drop a file or browse to select one
# MAGIC 4. Databricks infers the schema and previews the data
# MAGIC 5. Confirm column names and types
# MAGIC 6. Click **Create table** -- a managed Delta table is created

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using read_files After Upload
# MAGIC
# MAGIC Once a file is uploaded to a Volume, you can query it with SQL or PySpark.

# COMMAND ----------

# Simulate reading an uploaded file from a volume path
# In production, the path would be: /Volumes/<catalog>/<schema>/<volume>/filename.csv
# Here we read from our S3 source to demonstrate the pattern

uploaded_products = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(products_path)
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingestion_method", lit("manual_upload"))
)

uploaded_products.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In production with a volume, you would use:
# MAGIC -- SELECT * FROM read_files('/Volumes/ecommerce/lakeflow_connect_lab/uploads/products.csv')
# MAGIC --
# MAGIC -- The read_files function automatically infers format and schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Comparing All Ingestion Methods

# COMMAND ----------

# Build a comparison summary from our ingested data

# Count records from each method
batch_count = spark.sql("""
    SELECT 'Batch (Products)' as source, ingestion_method, COUNT(*) as records
    FROM ecommerce.lakeflow_connect_lab.products_batch
    GROUP BY ingestion_method
""")

autoloader_count = spark.sql("""
    SELECT 'Auto Loader (Clickstream)' as source, ingestion_method, COUNT(*) as records
    FROM ecommerce.lakeflow_connect_lab.clickstream_autoloader
    GROUP BY ingestion_method
""")

kafka_count = spark.sql("""
    SELECT 'Kafka Sim (Events)' as source, ingestion_method, COUNT(*) as records
    FROM ecommerce.lakeflow_connect_lab.events_kafka_sim
    GROUP BY ingestion_method
""")

comparison = batch_count.union(autoloader_count).union(kafka_count)
comparison.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Guide: Which Ingestion Method to Use?
# MAGIC
# MAGIC ```
# MAGIC Is the source...
# MAGIC │
# MAGIC ├── A local file (one-time)?
# MAGIC │   └── Manual File Upload
# MAGIC │
# MAGIC ├── Cloud storage (S3/ADLS/GCS)?
# MAGIC │   └── Standard Connector: Auto Loader (cloudFiles)
# MAGIC │
# MAGIC ├── A message queue (Kafka/Event Hubs)?
# MAGIC │   └── Standard Connector: Kafka format
# MAGIC │
# MAGIC ├── A database (PostgreSQL/MySQL/Oracle)?
# MAGIC │   ├── Need full control? → Standard Connector: JDBC
# MAGIC │   └── Want no-code + CDC? → Managed Connector
# MAGIC │
# MAGIC └── A SaaS app (Salesforce/Workday)?
# MAGIC     └── Managed Connector
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ecommerce.lakeflow_connect_lab.products_batch;
# MAGIC DROP TABLE IF EXISTS ecommerce.lakeflow_connect_lab.customers_batch;
# MAGIC DROP TABLE IF EXISTS ecommerce.lakeflow_connect_lab.clickstream_autoloader;
# MAGIC DROP TABLE IF EXISTS ecommerce.lakeflow_connect_lab.events_kafka_sim;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ecommerce.lakeflow_connect_lab CASCADE

# COMMAND ----------

# Clean up S3 checkpoint and schema directories
dbutils.fs.rm("s3://ecommerce-lakehouse/checkpoints/lakeflow-connect/", recurse=True)
dbutils.fs.rm("s3://ecommerce-lakehouse/schemas/lakeflow-connect/", recurse=True)
dbutils.fs.rm("s3://ecommerce-lakehouse/raw/lakeflow-connect/", recurse=True)
print("Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Lakeflow Connect** is the ingestion layer -- it brings data INTO the Lakehouse
# MAGIC 2. **Manual Upload**: Drag-and-drop for small, one-time files
# MAGIC 3. **Standard Connectors**: Code-based (Auto Loader, JDBC, Kafka) for custom pipelines
# MAGIC 4. **Managed Connectors**: No-code, serverless, CDC-based for databases and SaaS apps
# MAGIC 5. **Auto Loader** is the standard connector for cloud file ingestion (Day 20)
# MAGIC 6. All methods integrate with **Unity Catalog** for governance and lineage
# MAGIC 7. Next: Lakeflow continues with **Spark Declarative Pipelines** (Day 24) for transformation
