# Lakeflow Connect: Getting Data Into the Lakehouse Without Writing a Single Line of Code

sreekanth keerthipati

---

In the [previous articles](https://medium.com/@sreekanth489), we covered a lot of ground.

We built a Medallion Architecture with Bronze, Silver, and Gold layers. We learned how Structured Streaming processes data continuously. We used Auto Loader to ingest files from S3 into Bronze.

But here's what we actually did in our community sessions:

We wrote a Bronze script. Ran it manually. Then wrote a Silver script. Ran it manually. Then Gold. Ran that manually too.

Three scripts. Three manual executions. Three checkpoints to manage.

That was fine for learning. But in production?

You need **orchestration**. You need **managed ingestion**. You need **scheduling**.

You need **Lakeflow**.

---

## What You'll Learn

- What Lakeflow is and its three components
- How Lakeflow Connect provides no-code data ingestion
- The three types of connectors: Manual Upload, Standard, Managed
- How to set up an external location on AWS S3
- When to use which connector type
- How Connect feeds into the rest of the Lakeflow platform

---

## What Is Lakeflow?

Lakeflow is Databricks' unified platform for building, running, and monitoring data pipelines end to end.

It's not one tool. It's three components working together:

![Lakeflow Components: Connect, Spark Declarative Pipelines, Jobs](images/lakeflow-connect.png)
<p align="center"><em>Image credit: <a href="https://www.databricks.com/product/lakeflow">Databricks</a></em></p>

**1. Lakeflow Connect** — Get data into the lakehouse. Managed connectors, file uploads, and standard ingestion sources.

**2. Spark Declarative Pipelines (SDP)** — Transform data through Bronze, Silver, and Gold layers using declarative code. Formerly known as Delta Live Tables (DLT).

**3. Lakeflow Jobs** — Orchestrate and schedule everything. Multi-task workflows, retries, monitoring.

Think of it as a production assembly line:

- **Connect** brings raw materials into the factory
- **SDP** processes and refines them through quality stages
- **Jobs** runs the entire factory on schedule

This article focuses on the first component: **Lakeflow Connect**.

---

## The Ingestion Problem

Before Lakeflow Connect, getting data into Databricks meant writing code.

Needed data from Salesforce? Write an API client. Parse the response. Handle pagination. Write to S3. Set up Auto Loader.

Needed data from PostgreSQL? Set up JDBC connections. Handle credentials. Manage change detection. Build incremental logic.

Needed data from a partner's CSV files? Upload manually. Parse schemas. Handle format variations.

Every source needed custom code. Custom error handling. Custom monitoring.

And if you were a data engineer at a company with 50 data sources? That's 50 custom ingestion pipelines to build and maintain.

Lakeflow Connect changes this.

---

## Three Types of Connectors

Lakeflow Connect provides three ways to get data into your lakehouse:

### 1. Manual File Upload

The simplest option. Upload files directly through the Databricks UI.

You drag and drop CSV, JSON, or Parquet files into a Unity Catalog volume or table.

**When to use it**: one-time loads, small reference data, quick prototyping.

**When NOT to use it**: anything recurring. If you're uploading the same file type every week, you need a connector.

### 2. Standard Connectors

These are the connectors you already know.

- **Auto Loader** — File ingestion from cloud storage (we covered this in the [previous article](https://medium.com/@sreekanth489))
- **JDBC batch ingestion** — Read from databases using JDBC drivers
- **Kafka streaming** — Consume from Apache Kafka topics

Standard connectors require code. You write PySpark or SQL to configure the source, define the schema, and set up the pipeline.

But you get full control. Custom transformations at read time. Complex filter logic. Schema manipulation.

**When to use them**: when you need fine-grained control, when your source is cloud storage or Kafka, or when you need custom logic during ingestion.

### 3. Managed Connectors

This is the new piece. And it's a game-changer for many teams.

Managed connectors are **no-code, UI-driven** ingestion pipelines that Databricks fully manages.

You don't write a single line of code.

You configure the source in a wizard. Databricks handles the rest: connection management, change detection, incremental ingestion, schema mapping, error handling, and monitoring.

---

## Managed Connectors Deep Dive

Let's break down what makes managed connectors special.

### Supported Sources

Managed connectors support a growing list of enterprise data sources:

**Databases:**
- PostgreSQL
- MySQL
- SQL Server
- Oracle

**SaaS Applications:**
- Salesforce
- Workday
- ServiceNow
- SharePoint
- Google Analytics
- NetSuite

**And through Fivetran integration, hundreds more.**

Databricks partnered with Fivetran to power many of these connectors. If you've used Fivetran before, you know the breadth of their connector ecosystem. Now that ecosystem is available natively inside Databricks.

### How It Works

Setting up a managed connector takes minutes:

1. **Select your source** from the connector catalog in the Databricks UI
2. **Configure credentials** — enter your Salesforce login, your database connection string, etc.
3. **Select tables or objects** — pick which tables or API objects you want to ingest
4. **Choose a destination** — specify the Unity Catalog schema where data should land
5. **Set a schedule** — how often should data sync?
6. **Start the pipeline** — click run

That's it.

No Python. No Spark. No checkpoint management. No schema inference code.

### CDC-Based Incremental Ingestion

Managed connectors don't do full reloads every time.

For database sources, they use **Change Data Capture (CDC)** to detect only the rows that changed since the last sync.

For SaaS sources, they use the source's native change tracking APIs.

This means:

- **First sync**: Full load of the selected tables
- **Subsequent syncs**: Only changed rows are transferred

Efficient. Fast. Cost-effective.

### Unity Catalog Governance

Every table created by a managed connector lands in **Unity Catalog**.

That means:

- **Access control** — who can read this data?
- **Lineage tracking** — where did this data come from?
- **Audit logging** — who accessed this data and when?
- **Data discovery** — search and find datasets across your organization

Your ingested data is governed from the moment it lands in the lakehouse.

### Serverless Compute

Managed connectors run on **serverless compute**.

You don't provision clusters. You don't manage infrastructure. Databricks allocates the compute needed for each sync and releases it when done.

No idle clusters burning money. No capacity planning.

---

## Standard Connectors in Detail

Since managed connectors handle the no-code use cases, let's revisit the standard connectors you'll still use regularly.

### Auto Loader (Already in Your Toolkit)

If you read the [previous article on Structured Streaming and Auto Loader](https://medium.com/@sreekanth489), you already know this one.

Auto Loader uses `cloudFiles` format to ingest files from S3, ADLS, or GCS:

```python
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .load("s3://my-bucket/raw/events/")
)
```

It handles schema inference, schema evolution, and incremental file detection.

Auto Loader is still the **recommended approach for Bronze layer ingestion from cloud object storage**. Managed connectors don't replace it — they complement it for non-file sources.

### JDBC Batch Ingestion

For databases where you need custom query logic:

```python
df = (
    spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://host:5432/mydb")
        .option("dbtable", "public.customers")
        .option("user", "reader")
        .option("password", dbutils.secrets.get("scope", "pg_password"))
        .load()
)
```

JDBC gives you full SQL control. You can push down filters, specify custom queries, and partition reads for parallelism.

But you manage everything: connection pooling, error handling, incremental logic, scheduling.

### Kafka Streaming

For real-time event streams:

```python
df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "order_events")
        .option("startingOffsets", "earliest")
        .load()
)
```

Kafka streaming is a standard connector because it requires code to deserialize messages, handle schemas (often Avro or Protobuf), and manage offsets.

---

## Setting Up an External Location on AWS S3

In our community session, we walked through connecting Databricks to an S3 bucket using Unity Catalog's external locations.

This is a foundational setup. Whether you're using Auto Loader, managed connectors, or any other ingestion method, you need Databricks to access your cloud storage.

Here's the step-by-step process on AWS:

### Step 1: Create an S3 Bucket

Go to the AWS Console. Create a new S3 bucket (or use an existing one).

```
Bucket name: my-company-lakehouse-raw
Region: us-east-1
```

Keep the defaults for versioning and encryption. Block public access (always).

### Step 2: Navigate to External Locations in Unity Catalog

In the Databricks workspace:

1. Go to **Catalog** in the left sidebar
2. Click the **+** icon or go to **External Locations**
3. Click **Create External Location**

### Step 3: Generate a PAT Token

If prompted, generate a **Personal Access Token (PAT)** from your Databricks workspace settings.

Go to **Settings > Developer > Access Tokens > Generate New Token**.

This token is used during the CloudFormation setup to authenticate the connection between AWS and Databricks.

### Step 4: Run the CloudFormation Stack

Databricks provides a **CloudFormation template** that automatically creates the required IAM roles and policies.

When you create the external location, Databricks gives you a link to launch the CloudFormation stack in AWS.

The stack creates:

- An **IAM role** that Databricks can assume
- A **trust policy** allowing the Databricks control plane to assume the role
- An **inline policy** granting read/write access to your S3 bucket

You don't need to manually create IAM roles or policies. The CloudFormation template handles it all.

### Step 5: Complete the Connection

Once the CloudFormation stack finishes:

1. Go back to the Databricks external location creation wizard
2. Enter the S3 URL: `s3://my-company-lakehouse-raw/`
3. Select the storage credential created by CloudFormation
4. Click **Create**

### Step 6: Test the Connection

Databricks lets you **test the connection** right in the UI.

It verifies:
- Can Databricks assume the IAM role?
- Can it list objects in the bucket?
- Can it read from the bucket?
- Can it write to the bucket?

If all four checks pass, your external location is ready.

### Step 7: Enable File Events (Optional)

If you plan to use **Managed File Events** with Auto Loader (the recommended production setup we discussed in the previous article), toggle on **File Events** for this external location.

This tells Databricks to set up S3 event notifications so Auto Loader can detect new files instantly instead of scanning the directory.

---

## When to Use What

Here's the decision matrix:

| Scenario | Connector Type | Why |
|----------|---------------|-----|
| CSV files landing in S3 | **Standard (Auto Loader)** | Best for cloud storage file ingestion |
| Salesforce data sync | **Managed** | No-code, CDC-based, Fivetran-powered |
| PostgreSQL to lakehouse | **Managed** or **Standard (JDBC)** | Managed for simple sync; JDBC for custom queries |
| Kafka event stream | **Standard (Kafka)** | Requires code for deserialization |
| One-time reference data | **Manual Upload** | Drag-and-drop in UI |
| Oracle database tables | **Managed** | No-code, CDC-based incremental sync |
| Custom API source | **Standard (custom code)** | Write your own ingestion logic |
| Partner file drops on S3 | **Standard (Auto Loader)** | Schema evolution handles format changes |

### The General Rule

If a **managed connector** exists for your source, start there. It's less code to maintain, less infrastructure to manage, and less surface area for bugs.

If you need **custom logic** during ingestion (complex filters, custom transformations, non-standard formats), use a standard connector.

If it's a **one-time thing**, just upload manually.

---

## How Connect Feeds Into the Rest of Lakeflow

Lakeflow Connect is the entry point. But it's not the whole story.

Here's the typical flow:

```
External Sources
    |
    v
Lakeflow Connect                    ← Ingest
(Managed Connectors / Auto Loader)
    |
    v
Unity Catalog (Raw Tables)
    |
    v
Spark Declarative Pipelines         ← Transform
(Bronze → Silver → Gold)
    |
    v
Lakeflow Jobs                       ← Orchestrate
(Schedule, Monitor, Retry)
    |
    v
Dashboards / ML Models / Reports    ← Consume
```

Connect gets data **in**. SDP transforms it **through** the medallion layers. Jobs runs it all **on schedule**.

In our community session, we started here — understanding how data gets into the lakehouse — before moving on to how it's transformed. That order matters. You can't build a pipeline if you don't have data flowing in.

---

## A Quick Note on Fivetran Integration

If your organization already uses Fivetran, there's good news.

Databricks has a **native Fivetran integration** within Lakeflow Connect. You can use your existing Fivetran account and connectors, but have the data land directly in Unity Catalog.

This gives you:

- Fivetran's connector breadth (500+ sources)
- Unity Catalog's governance and lineage
- A single pane of glass in the Databricks UI

If you're starting fresh, Databricks' native managed connectors may be sufficient. If you're migrating from an existing Fivetran setup, the integration path is smoother than rebuilding everything.

---

## Key Takeaways

1. **Lakeflow has three components**: Connect (ingest), Spark Declarative Pipelines (transform), and Jobs (orchestrate).

2. **Lakeflow Connect provides three connector types**: Manual Upload for one-time loads, Standard Connectors (Auto Loader, JDBC, Kafka) for code-based ingestion, and Managed Connectors for no-code ingestion.

3. **Managed connectors are fully managed by Databricks** — no clusters, no code, no checkpoint management. They use CDC for incremental ingestion and land data directly in Unity Catalog.

4. **Auto Loader remains the recommended approach** for ingesting files from cloud object storage. Managed connectors complement it for database and SaaS sources.

5. **External locations in Unity Catalog** connect Databricks to your cloud storage. On AWS, a CloudFormation template handles the IAM setup.

6. **Start with managed connectors when available.** Fall back to standard connectors when you need custom logic.

---

## What's Next?

Data is flowing into the lakehouse. But it's raw. Unstructured. Ungoverned.

In the next article, we'll tackle the transformation layer: **Spark Declarative Pipelines**.

We'll see how to replace manual Bronze, Silver, and Gold scripts with declarative code that handles orchestration, data quality, retries, and change data capture — automatically.

No more running three scripts manually. No more managing checkpoints by hand. No more waking up at 3 AM because a network glitch crashed your pipeline.

---

All the lab notebooks are available on GitHub:

- [Day 22: Lakeflow Connect](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day22-lakeflow-connect)
- [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day00-environment-setup)

---

*Previously in this series:*

- [Structured Streaming & Auto Loader: Moving Data in Real Time Through the Medallion Architecture](https://medium.com/@sreekanth489) *(previous article)*
- [Medallion Architecture: Building Production Data Pipelines with Bronze, Silver, and Gold Layers](https://medium.com/@sreekanth489)
- [Inside the Delta Log — The Complete Series](https://medium.com/@sreekanth489/inside-the-delta-log-the-complete-series-acid-internals-performance-concurrency-a5db53b2fb6f)
- [From Data Lakes to Delta Lake: A Practical Guide](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30)
- [Why Hadoop, Spark, and Databricks Exist](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148)
