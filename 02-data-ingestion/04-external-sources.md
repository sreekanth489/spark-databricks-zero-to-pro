# External Sources (JDBC, Kafka, Cloud Storage)

> Module 02 -- Topic 04 | Level: Intermediate | Time: 50 min

---

## Learning Objectives

- Connect to relational databases via JDBC with predicate pushdown and partitioned reads
- Understand Kafka consumer/producer integration with Structured Streaming
- Access cloud storage directly (S3, ADLS Gen2, GCS) with multiple authentication methods
- Compare mounting vs. direct access vs. Unity Catalog external locations
- Design connection patterns that are secure, performant, and maintainable

---

## Conceptual Overview

Not all data lives in files. Production data pipelines pull from relational
databases, message brokers, APIs, and cloud storage services. This topic covers
the three most common external source categories:

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks Cluster                    │
│                                                         │
│   ┌─────────┐    ┌─────────┐    ┌──────────────────┐   │
│   │  JDBC   │    │  Kafka  │    │  Cloud Storage   │   │
│   │ Reader  │    │ Source  │    │  (S3/ADLS/GCS)   │   │
│   └────┬────┘    └────┬────┘    └────────┬─────────┘   │
│        │              │                   │             │
│        v              v                   v             │
│   ┌──────────────────────────────────────────────┐     │
│   │              Spark DataFrame                  │     │
│   │         (unified processing API)              │     │
│   └──────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────┘
         │                │                │
         v                v                v
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │ Postgres │    │  Kafka   │    │ S3/ADLS/ │
   │ MySQL    │    │  Broker  │    │   GCS    │
   │ Oracle   │    │          │    │          │
   │ SQL Svr  │    │          │    │          │
   └──────────┘    └──────────┘    └──────────┘
```

---

## JDBC Connections

### Basic JDBC Read

```python
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mydb")
    .option("dbtable", "public.customers")
    .option("user", "reader")
    .option("password", dbutils.secrets.get("scope", "pg-password"))
    .load()
)
```

### Key JDBC Options

| Option | Description |
|--------|-------------|
| `url` | JDBC connection URL |
| `dbtable` | Table name or subquery: `(SELECT * FROM t WHERE ...) AS subq` |
| `user` / `password` | Credentials (use secrets, not plaintext!) |
| `driver` | JDBC driver class (auto-detected for common databases) |
| `fetchsize` | Number of rows per network round-trip (default varies by DB) |
| `batchsize` | Rows per insert batch when writing (default 1000) |
| `numPartitions` | Number of parallel connections |
| `partitionColumn` | Column to partition on (must be numeric, date, or timestamp) |
| `lowerBound` / `upperBound` | Range for partitioning |
| `pushDownPredicate` | `true` (default) -- push WHERE clauses to the database |
| `pushDownAggregate` | Push aggregations to the database (Spark 3.3+) |
| `pushDownLimit` | Push LIMIT to the database |
| `sessionInitStatement` | SQL to run on each connection before reading |

### Predicate Pushdown

By default, Spark pushes `WHERE` clause predicates down to the JDBC source.
This means the database filters rows before sending them over the network,
which can dramatically reduce data transfer.

```python
# The filter is pushed down -- the database executes the WHERE clause
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("user", user)
    .option("password", password)
    .load()
    .filter("order_date >= '2024-01-01'")
)
```

To see what was pushed down, check the physical plan:

```python
df.explain(True)
# Look for "PushedFilters" in the Scan node
```

### Partitioned JDBC Reads

Without partitioning, Spark reads the entire table through a single JDBC
connection -- a major bottleneck. Partitioned reads open multiple parallel
connections, each reading a slice of the data:

```python
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("user", user)
    .option("password", password)
    .option("numPartitions", 8)
    .option("partitionColumn", "order_id")
    .option("lowerBound", 1)
    .option("upperBound", 1000000)
    .load()
)
```

How it works:

```
Spark generates 8 parallel queries:
  Connection 1: SELECT * FROM orders WHERE order_id >= 1      AND order_id < 125001
  Connection 2: SELECT * FROM orders WHERE order_id >= 125001 AND order_id < 250001
  ...
  Connection 8: SELECT * FROM orders WHERE order_id >= 875001 AND order_id < 1000001
```

**Important considerations:**

- `lowerBound` / `upperBound` are NOT filters -- they only control how Spark
  slices the partition ranges. Rows outside the bounds are still read.
- Choose a column with uniform distribution to avoid skewed partitions
- Too many partitions can overwhelm the source database
- Use `fetchsize` to tune the number of rows per network round-trip

### Using a Subquery as dbtable

Instead of reading an entire table, pass a SQL subquery:

```python
query = """
    (SELECT customer_id, name, email, created_at
     FROM customers
     WHERE created_at >= '2024-01-01'
     ORDER BY created_at) AS recent_customers
"""

df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", user)
    .option("password", password)
    .load()
)
```

### JDBC Write

```python
df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "target_table") \
    .option("user", user) \
    .option("password", password) \
    .option("batchsize", 5000) \
    .mode("append") \
    .save()
```

### Supported Databases

Databricks includes JDBC drivers for:

| Database | Driver Class (auto-detected) |
|----------|------------------------------|
| PostgreSQL | `org.postgresql.Driver` |
| MySQL | `com.mysql.cj.jdbc.Driver` |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| Oracle | `oracle.jdbc.OracleDriver` |
| Snowflake | `net.snowflake.client.jdbc.SnowflakeDriver` |
| Redshift | Use the Databricks Redshift connector instead |

For databases not pre-installed, upload the JDBC driver JAR to your cluster
libraries.

---

## Kafka Integration

### Kafka as a Streaming Source

Apache Kafka is the most common streaming data source. Spark's Kafka
integration uses the Structured Streaming API:

```python
df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "events_topic")
    .option("startingOffsets", "earliest")
    .load()
)
```

### Kafka DataFrame Schema

Every Kafka read returns a DataFrame with this fixed schema:

| Column | Type | Description |
|--------|------|-------------|
| `key` | binary | Message key |
| `value` | binary | Message payload |
| `topic` | string | Source topic |
| `partition` | int | Kafka partition |
| `offset` | long | Message offset |
| `timestamp` | timestamp | Message timestamp |
| `timestampType` | int | Timestamp type indicator |

### Parsing Kafka Messages

The `value` column is binary. You must cast and parse it:

```python
from pyspark.sql.functions import col, from_json

# JSON messages
schema = "user_id STRING, action STRING, amount DOUBLE, ts TIMESTAMP"

df_parsed = (
    df_kafka
    .select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_ts")
    )
    .select("key", "data.*", "kafka_ts")
)
```

### Key Kafka Options

| Option | Description |
|--------|-------------|
| `kafka.bootstrap.servers` | Comma-separated broker list |
| `subscribe` | Topic name(s) to subscribe to |
| `subscribePattern` | Regex pattern for topic names |
| `startingOffsets` | `earliest`, `latest`, or JSON offset spec |
| `endingOffsets` | Used with batch reads (`spark.read`) |
| `maxOffsetsPerTrigger` | Rate limit per micro-batch |
| `kafka.group.id` | Consumer group ID |
| `kafka.security.protocol` | `PLAINTEXT`, `SSL`, `SASL_SSL`, etc. |
| `kafka.sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `kafka.sasl.jaas.config` | JAAS configuration string |
| `failOnDataLoss` | `true` (default) -- fail if offsets are out of range |

### Kafka as a Batch Source

You can also read Kafka as a batch source (useful for backfills):

```python
df_batch = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "events_topic")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
```

### Writing to Kafka

```python
(df_output
    .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "output_topic")
    .option("checkpointLocation", "/checkpoints/kafka_output/")
    .start()
)
```

---

## Cloud Storage Access

### Access Methods Comparison

| Method | Description | Unity Catalog | Recommended |
|--------|-------------|---------------|-------------|
| **External location** (UC) | Governed access via Unity Catalog | Yes | Best practice |
| **Storage credential** (UC) | Reusable credential mapped in UC | Yes | Best practice |
| **Direct path + secrets** | Configure Spark with secrets | No | Acceptable |
| **DBFS mount** | Mount cloud path to `/mnt/` | No | Legacy (avoid in new projects) |
| **Instance profile** (AWS) | IAM role attached to cluster | No | Acceptable (AWS) |
| **Credential passthrough** | Use user's Azure AD token | No | Limited scenarios |

### AWS S3

```python
# Option 1: Unity Catalog external location (recommended)
df = spark.read.parquet("s3://my-bucket/data/events/")

# Option 2: Direct access with secrets
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("aws", "access-key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("aws", "secret-key"))
df = spark.read.parquet("s3a://my-bucket/data/events/")

# Option 3: Instance profile (configured at cluster level)
# No code needed -- the cluster's IAM role provides access
df = spark.read.parquet("s3://my-bucket/data/events/")

# Option 4: DBFS mount (legacy)
# dbutils.fs.mount("s3a://my-bucket", "/mnt/my-bucket",
#     extra_configs={"fs.s3a.access.key": "...", "fs.s3a.secret.key": "..."})
# df = spark.read.parquet("/mnt/my-bucket/data/events/")
```

### Azure ADLS Gen2

```python
# Option 1: Unity Catalog external location (recommended)
df = spark.read.parquet(
    "abfss://container@storageaccount.dfs.core.windows.net/data/events/"
)

# Option 2: Service principal with secrets
spark.conf.set(
    "fs.azure.account.auth.type.storageaccount.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.storageaccount.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.storageaccount.dfs.core.windows.net",
    dbutils.secrets.get("azure", "client-id")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.storageaccount.dfs.core.windows.net",
    dbutils.secrets.get("azure", "client-secret")
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.storageaccount.dfs.core.windows.net",
    "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
)
df = spark.read.parquet(
    "abfss://container@storageaccount.dfs.core.windows.net/data/events/"
)

# Option 3: SAS token
spark.conf.set(
    "fs.azure.sas.container.storageaccount.blob.core.windows.net",
    dbutils.secrets.get("azure", "sas-token")
)
```

### GCP Google Cloud Storage

```python
# Option 1: Unity Catalog external location (recommended)
df = spark.read.parquet("gs://my-bucket/data/events/")

# Option 2: Service account key
spark.conf.set(
    "google.cloud.auth.service.account.enable", "true"
)
spark.conf.set(
    "fs.gs.project.id", "my-project"
)
spark.conf.set(
    "fs.gs.auth.service.account.email", "sa@my-project.iam.gserviceaccount.com"
)
# Service account JSON key uploaded as a cluster init script or secret
```

### Unity Catalog Volumes

The simplest and most governed way to access files in Databricks:

```python
# Unity Catalog managed volume
df = spark.read.csv("/Volumes/catalog/schema/volume/data.csv")

# Unity Catalog external volume (maps to cloud storage)
df = spark.read.parquet("/Volumes/catalog/schema/ext_volume/events/")
```

---

## Mounting vs. Direct Access -- Decision Matrix

```
Are you using Unity Catalog?
├── YES --> Use external locations or volumes (never mount)
└── NO
    │
    Is this a new project?
    ├── YES --> Use direct paths + secrets (prepare for UC migration)
    └── NO
        │
        Do you have existing mounts?
        ├── YES --> Keep them but plan migration to UC
        └── NO  --> Use direct paths + secrets
```

**Why avoid mounts in new projects?**

1. Mounts are cluster-scoped, not governed by Unity Catalog
2. All users on the cluster get the same access (no fine-grained control)
3. Mount credentials are stored in DBFS metadata (less secure)
4. Unity Catalog external locations provide audit logging and lineage

---

## Hands-On Walkthrough

Open the companion notebook `04-external-sources_notebook.py`. The notebook:

1. Demonstrates JDBC read patterns with a mock setup (using a Spark-generated
   DataFrame as a stand-in for a database table)
2. Shows cloud storage path patterns for all three cloud providers
3. Walks through Kafka configuration (commented code since a broker is required)
4. Demonstrates Unity Catalog volume access patterns
5. Cleans up all resources

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Object storage | S3 | ADLS Gen2 / Blob Storage | GCS |
| Path scheme | `s3://` or `s3a://` | `abfss://` (ADLS), `wasbs://` (Blob) | `gs://` |
| IAM integration | Instance profile | Managed identity | Service account |
| Secret management | AWS Secrets Manager | Azure Key Vault | GCP Secret Manager |
| Databricks secret scope | Backed by AWS SM or Databricks | Backed by AKV or Databricks | Backed by GCP SM or Databricks |
| Kafka managed service | MSK | Event Hubs (Kafka protocol) | Managed Kafka (or Confluent) |

---

## Certification Tip

For the **Databricks Certified Data Engineer Associate** exam:

- Know JDBC read options: `url`, `dbtable`, `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound`
- Understand predicate pushdown and why it improves performance
- Know the fixed schema returned by the Kafka source (key, value, topic, partition, offset, timestamp)
- Know the difference between mounts and Unity Catalog external locations
- Understand that DBFS mounts are legacy and UC external locations are preferred

For the **Professional** exam:

- Partitioned JDBC reads and performance tuning
- Kafka exactly-once semantics with checkpointing
- Security configurations (SASL_SSL, credential passthrough)

---

## Key Takeaways

- JDBC reads should always be partitioned for large tables -- a single connection is a bottleneck
- Use `fetchsize` and `numPartitions` to tune JDBC performance; use subqueries to limit data at the source
- Kafka integration uses Structured Streaming; the `value` column is binary and must be parsed
- Always store credentials in Databricks secret scopes, never in plaintext
- Unity Catalog external locations and volumes are the recommended way to access cloud storage
- DBFS mounts are legacy -- avoid them in new projects

---

## Next Steps

Proceed to [05 -- Multi-Hop Ingestion Patterns](05-multi-hop-ingestion.md) to
learn how to combine these ingestion methods into Bronze-Silver-Gold pipelines.
