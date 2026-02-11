# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # External Sources: JDBC, Kafka, Cloud Storage
# MAGIC
# MAGIC **Module 02 -- Topic 04 | Databricks Zero-to-Pro**
# MAGIC
# MAGIC This notebook demonstrates patterns for ingesting data from external
# MAGIC sources. Since JDBC requires a running database and Kafka requires a
# MAGIC running broker, we simulate these scenarios with inline data and
# MAGIC provide the actual connection code in commented blocks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, LongType, BinaryType
)
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp

TMP_DIR = "/tmp/m02_external_sources"
dbutils.fs.rm(TMP_DIR, recurse=True)
dbutils.fs.mkdirs(TMP_DIR)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. JDBC Read Patterns
# MAGIC
# MAGIC We simulate a "database table" by creating a DataFrame, saving it as a
# MAGIC Delta table, and then demonstrating the JDBC read patterns you would
# MAGIC use with a real database.

# COMMAND ----------

# Simulate a database table: orders
import random
from datetime import datetime, timedelta

random.seed(42)
orders_data = []
base_date = datetime(2024, 1, 1)

for i in range(1, 101):
    orders_data.append((
        i,
        f"CUST-{random.randint(1, 20):03d}",
        random.choice(["Widget A", "Widget B", "Widget C", "Gadget X", "Gadget Y"]),
        round(random.uniform(5.0, 500.0), 2),
        (base_date + timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
        random.choice(["US", "EU", "APAC"]),
    ))

orders_schema = "order_id INT, customer_id STRING, product STRING, amount DOUBLE, order_date STRING, region STRING"
df_orders = spark.createDataFrame(orders_data, schema=orders_schema)
df_orders.write.mode("overwrite").saveAsTable("m02_mock_orders")

print(f"Mock 'database table' created with {df_orders.count()} rows.")
df_orders.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern A: Basic JDBC Read
# MAGIC
# MAGIC This is the connection code you would use with a real PostgreSQL database.

# COMMAND ----------

# ============================================================
# ACTUAL JDBC READ (requires a running database)
# Uncomment and configure for your environment.
# ============================================================

# jdbc_url = "jdbc:postgresql://your-host:5432/your-database"
# jdbc_user = dbutils.secrets.get(scope="my-scope", key="jdbc-user")
# jdbc_password = dbutils.secrets.get(scope="my-scope", key="jdbc-password")
#
# df_jdbc = (
#     spark.read.format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", "public.orders")
#     .option("user", jdbc_user)
#     .option("password", jdbc_password)
#     .option("fetchsize", 10000)
#     .load()
# )
# df_jdbc.show()

# -- SIMULATED VERSION --
# We read from our mock Delta table as if it were a JDBC source.
df_jdbc_sim = spark.table("m02_mock_orders")
print("Simulated JDBC read (basic):")
df_jdbc_sim.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern B: Partitioned JDBC Read
# MAGIC
# MAGIC For large tables, partitioned reads open multiple parallel connections.

# COMMAND ----------

# ============================================================
# PARTITIONED JDBC READ (requires a running database)
# ============================================================

# df_partitioned = (
#     spark.read.format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", "public.orders")
#     .option("user", jdbc_user)
#     .option("password", jdbc_password)
#     .option("numPartitions", 8)
#     .option("partitionColumn", "order_id")
#     .option("lowerBound", 1)
#     .option("upperBound", 1000000)
#     .option("fetchsize", 10000)
#     .load()
# )

# -- SIMULATED VERSION --
# We demonstrate the concept using repartition
df_partitioned_sim = df_jdbc_sim.repartition(8, "order_id")
print(f"Simulated partitioned read: {df_partitioned_sim.rdd.getNumPartitions()} partitions")
print(f"Row count: {df_partitioned_sim.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern C: JDBC Read with Subquery (Predicate Pushdown)

# COMMAND ----------

# ============================================================
# JDBC READ WITH SUBQUERY (requires a running database)
# ============================================================

# subquery = """
#     (SELECT order_id, customer_id, product, amount, order_date
#      FROM public.orders
#      WHERE order_date >= '2024-01-01'
#        AND region = 'US') AS us_recent_orders
# """
#
# df_subquery = (
#     spark.read.format("jdbc")
#     .option("url", jdbc_url)
#     .option("dbtable", subquery)
#     .option("user", jdbc_user)
#     .option("password", jdbc_password)
#     .load()
# )

# -- SIMULATED VERSION --
df_us_orders = (
    spark.table("m02_mock_orders")
    .filter("order_date >= '2024-01-01' AND region = 'US'")
    .select("order_id", "customer_id", "product", "amount", "order_date")
)

print("Simulated JDBC subquery (US orders since 2024-01-01):")
df_us_orders.show(10, truncate=False)
print(f"Filtered row count: {df_us_orders.count()} out of 100 total")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern D: Checking Predicate Pushdown
# MAGIC
# MAGIC Use `.explain()` to verify that filters are pushed to the source.

# COMMAND ----------

# With a real JDBC source, you would see "PushedFilters" in the plan.
# Here we demonstrate the concept with a Delta table.
df_pushed = spark.table("m02_mock_orders").filter("amount > 100")
print("Physical plan (look for PushedFilters with a real JDBC source):")
df_pushed.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Kafka Integration Patterns
# MAGIC
# MAGIC Kafka requires a running broker, so this section shows the configuration
# MAGIC patterns with detailed comments. We then simulate a Kafka DataFrame to
# MAGIC demonstrate message parsing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kafka Streaming Read Configuration

# COMMAND ----------

# ============================================================
# KAFKA STREAMING READ (requires a running Kafka broker)
# ============================================================

# df_kafka = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
#     .option("subscribe", "user_events")
#     .option("startingOffsets", "earliest")
#     # For SSL/SASL authentication:
#     # .option("kafka.security.protocol", "SASL_SSL")
#     # .option("kafka.sasl.mechanism", "PLAIN")
#     # .option("kafka.sasl.jaas.config",
#     #     "org.apache.kafka.common.security.plain.PlainLoginModule required "
#     #     "username='...' password='...';")
#     .load()
# )

# -- SIMULATE A KAFKA DATAFRAME --
# Kafka always returns: key (binary), value (binary), topic, partition, offset, timestamp
kafka_sim_data = [
    (bytearray(b"user-1"), bytearray(b'{"user_id":"U001","action":"login","amount":null,"ts":"2024-01-15T08:00:00"}'),
     "user_events", 0, 100, "2024-01-15T08:00:00"),
    (bytearray(b"user-2"), bytearray(b'{"user_id":"U002","action":"purchase","amount":42.50,"ts":"2024-01-15T08:05:00"}'),
     "user_events", 0, 101, "2024-01-15T08:05:00"),
    (bytearray(b"user-1"), bytearray(b'{"user_id":"U001","action":"purchase","amount":19.99,"ts":"2024-01-15T08:10:00"}'),
     "user_events", 1, 50, "2024-01-15T08:10:00"),
    (bytearray(b"user-3"), bytearray(b'{"user_id":"U003","action":"signup","amount":null,"ts":"2024-01-15T08:15:00"}'),
     "user_events", 1, 51, "2024-01-15T08:15:00"),
]

kafka_schema = StructType([
    StructField("key", BinaryType()),
    StructField("value", BinaryType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("timestamp", StringType()),
])

df_kafka_sim = spark.createDataFrame(kafka_sim_data, schema=kafka_schema)

print("Simulated Kafka DataFrame (raw):")
df_kafka_sim.show(truncate=False)
print("\nSchema (matches real Kafka source):")
df_kafka_sim.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parsing Kafka Messages
# MAGIC
# MAGIC The `value` column is binary. You must cast it to string and parse the
# MAGIC JSON payload.

# COMMAND ----------

# Define the expected message schema
msg_schema = "user_id STRING, action STRING, amount DOUBLE, ts TIMESTAMP"

# Parse the value column
df_parsed = (
    df_kafka_sim
    .select(
        col("key").cast("string").alias("msg_key"),
        from_json(col("value").cast("string"), msg_schema).alias("data"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_ts"),
    )
    .select("msg_key", "data.*", "topic", "kafka_partition", "kafka_offset", "kafka_ts")
)

print("Parsed Kafka messages:")
df_parsed.show(truncate=False)
df_parsed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cloud Storage Access Patterns
# MAGIC
# MAGIC This section demonstrates the path formats and configuration patterns
# MAGIC for each cloud provider.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cloud Storage Path Reference
# MAGIC
# MAGIC | Cloud | Service | Path Format |
# MAGIC |-------|---------|-------------|
# MAGIC | AWS | S3 | `s3://bucket/prefix/file.parquet` |
# MAGIC | Azure | ADLS Gen2 | `abfss://container@account.dfs.core.windows.net/path/` |
# MAGIC | Azure | Blob Storage | `wasbs://container@account.blob.core.windows.net/path/` |
# MAGIC | GCP | GCS | `gs://bucket/prefix/file.parquet` |
# MAGIC | Any | Unity Catalog Volume | `/Volumes/catalog/schema/volume/path/` |
# MAGIC | Any | DBFS | `dbfs:/path/` or `/dbfs/path/` (FUSE) |

# COMMAND ----------

# Write sample data to demonstrate local/DBFS reading
sample_parquet_path = f"{TMP_DIR}/cloud_sim/events.parquet"
df_events = spark.createDataFrame([
    (1, "login",    "2024-01-15"),
    (2, "purchase", "2024-01-15"),
    (3, "logout",   "2024-01-15"),
], schema="event_id INT, action STRING, event_date STRING")

df_events.write.mode("overwrite").parquet(sample_parquet_path)
print(f"Sample data written to: {sample_parquet_path}")

# Read it back (simulates reading from cloud storage)
df_cloud = spark.read.parquet(sample_parquet_path)
print("\nData read from 'cloud storage' path:")
df_cloud.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AWS S3 Configuration Example

# COMMAND ----------

# ============================================================
# AWS S3 ACCESS (requires AWS credentials)
# ============================================================

# Option 1: Instance profile (no code needed -- configured on cluster)
# df = spark.read.parquet("s3://my-bucket/data/")

# Option 2: Access keys via secrets
# spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("aws", "access-key"))
# spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("aws", "secret-key"))
# df = spark.read.parquet("s3a://my-bucket/data/")

# Option 3: Assume role
# spark.conf.set("fs.s3a.aws.credentials.provider",
#     "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
# spark.conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789:role/my-role")
# df = spark.read.parquet("s3a://my-bucket/data/")

print("AWS S3 configuration examples shown in comments above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure ADLS Gen2 Configuration Example

# COMMAND ----------

# ============================================================
# AZURE ADLS GEN2 ACCESS (requires Azure credentials)
# ============================================================

# Option 1: Service principal with OAuth
# storage_account = "mystorageaccount"
# spark.conf.set(
#     f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
# spark.conf.set(
#     f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
#     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(
#     f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
#     dbutils.secrets.get("azure", "client-id"))
# spark.conf.set(
#     f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
#     dbutils.secrets.get("azure", "client-secret"))
# spark.conf.set(
#     f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
#     "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
#
# df = spark.read.parquet(
#     f"abfss://mycontainer@{storage_account}.dfs.core.windows.net/data/")

# Option 2: SAS token
# spark.conf.set(
#     f"fs.azure.sas.mycontainer.{storage_account}.blob.core.windows.net",
#     dbutils.secrets.get("azure", "sas-token"))

print("Azure ADLS Gen2 configuration examples shown in comments above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GCP GCS Configuration Example

# COMMAND ----------

# ============================================================
# GCP GCS ACCESS (requires GCP credentials)
# ============================================================

# Option 1: Service account (configured at cluster level)
# df = spark.read.parquet("gs://my-bucket/data/")

# Option 2: Service account key via Spark config
# spark.conf.set("google.cloud.auth.service.account.enable", "true")
# spark.conf.set("fs.gs.project.id", "my-gcp-project")
# spark.conf.set("fs.gs.auth.service.account.email",
#     "my-sa@my-gcp-project.iam.gserviceaccount.com")

print("GCP GCS configuration examples shown in comments above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Unity Catalog Volumes

# COMMAND ----------

# ============================================================
# UNITY CATALOG VOLUMES (requires UC-enabled workspace)
# ============================================================

# Managed volume -- Databricks manages the storage
# df = spark.read.csv("/Volumes/my_catalog/my_schema/my_volume/data.csv")

# External volume -- maps to your cloud storage
# df = spark.read.parquet("/Volumes/my_catalog/my_schema/ext_volume/events/")

# Listing files in a volume
# files = dbutils.fs.ls("/Volumes/my_catalog/my_schema/my_volume/")
# display(files)

print("Unity Catalog volume examples shown in comments above.")
print("\nUC Volumes are the recommended approach for file access in new projects.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary: Access Pattern Quick Reference
# MAGIC
# MAGIC | Source Type | Read Method | Key Consideration |
# MAGIC |------------|-------------|-------------------|
# MAGIC | Relational DB | `spark.read.format("jdbc")` | Use partitioned reads for large tables |
# MAGIC | Kafka | `spark.readStream.format("kafka")` | Parse binary value column |
# MAGIC | S3 | `spark.read.parquet("s3://...")` | Use instance profile or UC |
# MAGIC | ADLS Gen2 | `spark.read.parquet("abfss://...")` | Use service principal or UC |
# MAGIC | GCS | `spark.read.parquet("gs://...")` | Use service account or UC |
# MAGIC | UC Volume | `spark.read.csv("/Volumes/...")` | Simplest and most governed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS m02_mock_orders")
dbutils.fs.rm(TMP_DIR, recurse=True)
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **05 -- Multi-Hop Ingestion Patterns** to learn how to
# MAGIC build Bronze-Silver-Gold pipelines that tie all these ingestion methods
# MAGIC together.
