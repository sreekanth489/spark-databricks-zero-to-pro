# Kafka Integration
> Module 07 -- Topic 06 | Level: Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:

1. Configure Spark Structured Streaming to read from Apache Kafka
2. Parse the Kafka source schema (key, value, topic, partition, offset, timestamp)
3. Deserialize JSON and Avro payloads from Kafka messages
4. Write processed data back to Kafka topics
5. Manage offsets and understand exactly-once semantics
6. Design production Kafka-to-Delta streaming pipelines

## Conceptual Overview

### Kafka as a Streaming Source

Apache Kafka is the most widely used distributed event streaming platform. It serves as
the backbone for real-time data architectures, handling millions of events per second
across thousands of topics.

```
  Producers                Kafka Cluster                   Consumers
  +---------+         +-------------------+           +---------+
  | App A   |-------->| Topic: orders     |---------->| Spark   |
  +---------+         | Partition 0: [...] |          | Streaming|
                      | Partition 1: [...] |          +---------+
  +---------+         | Partition 2: [...] |
  | App B   |-------->|                   |---------->+---------+
  +---------+         | Topic: clicks     |          | Another |
                      | Partition 0: [...] |          | Consumer|
  +---------+         | Partition 1: [...] |          +---------+
  | IoT Hub |-------->|                   |
  +---------+         +-------------------+
```

### Kafka Source Schema

When Spark reads from Kafka, every message has a fixed schema:

```
  +--------+--------+-------+-----------+--------+-------------+
  | key    | value  | topic | partition | offset | timestamp   |
  | binary | binary | string| int       | long   | timestamp   |
  +--------+--------+-------+-----------+--------+-------------+
```

| Column | Type | Description |
|--------|------|-------------|
| `key` | binary | Message key (often used for partitioning) |
| `value` | binary | Message payload (your actual data) |
| `topic` | string | Kafka topic name |
| `partition` | int | Kafka partition number |
| `offset` | long | Message offset within the partition |
| `timestamp` | timestamp | Message timestamp (producer or broker time) |
| `timestampType` | int | 0=CreateTime, 1=LogAppendTime |

**Critical**: Both `key` and `value` are **binary**. You must cast them to string and
then parse (e.g., JSON) to access the actual data.

### Subscription Modes

```python
# Mode 1: Subscribe to specific topics
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders,clicks")
    .load()
)

# Mode 2: Subscribe by pattern (regex)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribePattern", "events-.*")
    .load()
)

# Mode 3: Assign specific partitions
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("assign", '{"orders": [0, 1, 2]}')
    .load()
)
```

### Deserializing Kafka Messages

#### JSON Deserialization

The most common pattern: cast value from binary to string, then parse JSON.

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("order_time", StringType()),
])

parsed = (
    kafka_df
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(
            F.col("value").cast("string"),
            order_schema
        ).alias("data"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp")
    )
    .select("kafka_key", "data.*", "topic", "partition", "offset", "kafka_timestamp")
)
```

#### Avro Deserialization (with Schema Registry)

For Avro-encoded messages with Confluent Schema Registry:

```python
from pyspark.sql.avro.functions import from_avro

# Using Schema Registry
parsed = (
    kafka_df
    .select(
        from_avro(
            F.col("value"),
            subject="orders-value",
            options={"schema.registry.url": "http://schema-registry:8081"}
        ).alias("data")
    )
    .select("data.*")
)
```

### Writing to Kafka

To write back to Kafka, your DataFrame must have a `value` column (and optionally `key`
and `topic`):

```python
(
    processed_df
    .select(
        F.col("customer_id").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("topic", "processed-orders")
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

### Offset Management

Kafka offsets determine where Spark starts reading:

```python
# Start from earliest available offset
.option("startingOffsets", "earliest")

# Start from latest offset (default)
.option("startingOffsets", "latest")

# Start from specific offsets
.option("startingOffsets",
    '{"orders": {"0": 100, "1": 200, "2": 150}}')

# End at specific offsets (batch only)
.option("endingOffsets", "latest")
```

```
  Kafka Partition 0:
  [offset 0] [offset 1] ... [offset 99] [offset 100] ... [offset 200]
                                             ^
                                         startingOffsets = 100
                                         (skip first 100 messages)
```

**Important**: After the first run, offsets are tracked in the **checkpoint**. The
`startingOffsets` option only applies to the very first run.

### Exactly-Once Semantics

The combination of Kafka + Structured Streaming + Delta provides end-to-end exactly-once:

```
  Kafka                    Spark                    Delta Lake
  +--------+              +----------+              +----------+
  | Offsets|   committed  | Checkpoint|  idempotent | Transaction|
  | tracked|<------------>| tracks   |  writes     | log ensures|
  | in     |              | offsets  |------------>| no dupes   |
  | checkpoint             +----------+             +----------+
  +--------+

  1. Spark reads batch of messages from Kafka
  2. Processes the data
  3. Writes to Delta (idempotent -- same batch = same result)
  4. Commits offsets to checkpoint
  5. If failure at any step, replay from checkpoint (no data loss, no dupes)
```

### Production Architecture

```
  +----------+    +-------------+    +----------+    +---------+
  | Kafka    |--->| Spark       |--->| Bronze   |--->| Silver  |
  | Topic    |    | Streaming   |    | Delta    |    | Delta   |
  | (raw)    |    | (parse +    |    | (raw +   |    | (clean) |
  |          |    |  validate)  |    |  metadata)|    |         |
  +----------+    +-------------+    +----------+    +---------+
                        |
                        | Dead-letter handling
                        v
                  +----------+
                  | DLQ Delta|
                  | (bad     |
                  |  records)|
                  +----------+
```

Key production patterns:
1. **Dead-letter queue (DLQ)**: Route unparseable messages to a separate table
2. **Idempotent writes**: Use `foreachBatch` with Delta merge for deduplication
3. **Schema Registry**: Validate message schemas before processing
4. **Consumer groups**: Spark creates its own consumer group; do not share with others
5. **Rate limiting**: Use `maxOffsetsPerTrigger` to control throughput

### Security Configuration

```python
# SASL/SSL authentication (common in production)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9093")
    .option("subscribe", "orders")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="api-key" password="api-secret";')
    .load()
)
```

**Best practice**: Store credentials in Databricks Secrets, not in code:

```python
api_key = dbutils.secrets.get(scope="kafka", key="api-key")
api_secret = dbutils.secrets.get(scope="kafka", key="api-secret")
```

## Hands-On Walkthrough

Open `06-kafka-integration_notebook.py` and work through:

1. **Kafka source configuration**: See the complete connection setup (reference code)
2. **Simulated Kafka data**: Rate source transformed to match Kafka schema
3. **JSON deserialization**: Parse binary value column into structured data
4. **Write to Kafka**: Configuration templates for producing back to Kafka
5. **Dead-letter pattern**: Handle unparseable messages gracefully
6. **End-to-end pipeline**: Simulated Kafka-to-Delta pipeline

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Managed Kafka | Amazon MSK | Azure Event Hubs (Kafka protocol) | Confluent on GCP |
| Confluent Cloud | Supported | Supported | Supported |
| Authentication | IAM, SASL/SCRAM | SAS, SASL/PLAIN | SASL/PLAIN |
| Network | VPC peering, PrivateLink | VNet injection, Private Endpoints | VPC peering |
| Event Hubs compatibility | N/A | Kafka protocol support built-in | N/A |

Azure Event Hubs supports the Kafka wire protocol, allowing you to use the same Spark
Kafka connector with Event Hubs endpoints. Replace the bootstrap servers with your
Event Hubs namespace endpoint.

## Certification Tip

Kafka integration appears on the Professional exam:
- Know the Kafka source schema columns (key, value, topic, partition, offset, timestamp)
- Know that key and value are binary and must be cast before use
- Understand the three subscription modes (subscribe, subscribePattern, assign)
- Know that `from_json()` with a schema is used for JSON deserialization
- Understand that `startingOffsets` only applies to the first run (checkpoint takes over)
- Know the exactly-once guarantee chain: Kafka offsets + checkpoint + Delta idempotent writes

## Key Takeaways

1. **Kafka source** returns a fixed schema with binary key/value that must be cast and parsed
2. Three subscription modes: **subscribe** (topics), **subscribePattern** (regex), **assign** (partitions)
3. **JSON deserialization**: `F.from_json(F.col("value").cast("string"), schema)` is the standard pattern
4. **Exactly-once semantics** come from Kafka offsets tracked in the Spark checkpoint + Delta idempotent writes
5. **Production patterns**: Dead-letter queues, Schema Registry validation, rate limiting, secure authentication
6. **Offset management**: `startingOffsets` sets the initial position; checkpoint handles subsequent runs

## Next Steps

You have completed Module 07: Streaming & Real-Time. Proceed to **Module 08 --
Governance & Security** to learn about Unity Catalog, data access control, lineage,
and PII protection strategies.
