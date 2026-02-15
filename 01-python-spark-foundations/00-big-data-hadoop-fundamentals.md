# Big Data & Hadoop Fundamentals
> Module 01 — Topic 00 | Level: Beginner | Time: 90 min

## Learning Objectives

- Define Big Data and explain the 5 V's that characterize it
- Identify why traditional single-machine databases fail at scale
- Explain vertical scaling vs. horizontal scaling and why distributed systems win
- Describe the Hadoop ecosystem (HDFS, MapReduce, YARN, Hive)
- Explain how HDFS stores, replicates, and serves data across a cluster
- Understand MapReduce's disk-based processing model and its limitations
- Explain how Hive brought SQL to Hadoop
- Articulate why Apache Spark replaced MapReduce
- Understand Spark's Master-Worker-Driver-Executor architecture at a high level
- Explain how data is broken into partitions for parallel processing
- Describe the DAG (Directed Acyclic Graph), lazy evaluation, and the Catalyst Optimizer
- Compare distributed file systems: HDFS, Amazon S3, Azure ADLS, Google GCS
- Connect these foundational concepts to the Databricks Lakehouse Platform

---

## Conceptual Overview

### What Is "Big Data"?

Big Data is **not** simply "large data." It is data that:

- **Cannot be processed efficiently** using traditional databases (MySQL, PostgreSQL, Oracle)
- **Exceeds the storage or compute capacity** of a single machine
- **Requires distributed systems** to store, process, and analyze

The term became mainstream around 2005-2010 when companies like Google, Yahoo, and Facebook
began generating data volumes that broke every traditional tool.

#### The 5 V's of Big Data

The industry uses the **5 V's** framework to characterize Big Data challenges:

```
                            ┌──────────────────────────────────┐
                            │          THE 5 V's               │
                            │         OF BIG DATA              │
                            └──────────────┬───────────────────┘
                                           │
          ┌────────────┬───────────┬───────┴───────┬────────────┐
          ▼            ▼           ▼               ▼            ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
    │  VOLUME  │ │ VELOCITY │ │ VARIETY  │ │ VERACITY │ │  VALUE   │
    │          │ │          │ │          │ │          │ │          │
    │ Terabytes│ │ Real-time│ │ Structured│ │ Trust & │ │ Business │
    │ to       │ │ streams  │ │ Semi-str │ │ accuracy│ │ insights │
    │ Petabytes│ │ of data  │ │ Unstruc. │ │ of data │ │ from data│
    └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

| V | Description | Real-World Example |
|---|-------------|-------------------|
| **Volume** | Sheer amount of data generated | Netflix ingests **~15 PB** of data daily across logs, clickstream, and video telemetry |
| **Velocity** | Speed at which data arrives and must be processed | Twitter processes **~500 million tweets/day**; stock exchanges process millions of trades per second |
| **Variety** | Different formats and structures of data | A hospital system deals with structured lab results, semi-structured JSON from IoT monitors, unstructured doctor's notes, and DICOM medical images |
| **Veracity** | Trustworthiness, accuracy, and quality of data | Sensor data from IoT devices may have gaps, duplicates, or drift; social media data contains bots and spam |
| **Value** | Ability to turn data into actionable business insights | Amazon's recommendation engine (driven by Big Data) generates **~35% of its revenue** |

#### Real-World Big Data Numbers

To understand why traditional systems fail, consider these real data volumes:

```
  Company/Industry     Daily Data Volume    Use Case
  ─────────────────    ─────────────────    ────────────────────────────────
  Facebook/Meta        ~600 TB/day          User interactions, ad clicks, logs
  Netflix              ~15 PB/day           Video streaming telemetry
  Walmart              ~2.5 PB/day          Transaction, inventory, supply chain
  CERN (LHC)           ~1 PB/day            Particle collision experiment data
  Uber                 ~200 TB/day          Trip data, pricing, maps, driver GPS
  A mid-size e-comm    ~5 TB/day            Clickstream, orders, product catalog
```

> **Perspective**: 1 Petabyte = 1,000 Terabytes = 1,000,000 Gigabytes. That is roughly
> 500 billion pages of text, or enough to fill 1.5 million CD-ROMs.

---

### The Core Big Data Problem

Imagine this scenario:

> Your company is a mid-size e-commerce platform. You generate **5 TB of clickstream data
> daily**. You store it in **MySQL on a single server**. Queries take hours. The server
> crashes under load during Black Friday.

This is the **classic Big Data problem**.

#### Why Traditional Databases Fail

```
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    TRADITIONAL SYSTEM (Single Server)                 │
  │                                                                       │
  │    ┌──────────────────────────────────────────────────────────────┐   │
  │    │                    MySQL / PostgreSQL                         │   │
  │    │                                                              │   │
  │    │    CPU: 64 cores    RAM: 512 GB    Disk: 10 TB SSD          │   │
  │    │                                                              │   │
  │    │    ┌──────────────────────────────────────────────────┐      │   │
  │    │    │  SELECT customer_id, COUNT(*)                     │      │   │
  │    │    │  FROM clickstream                                 │      │   │
  │    │    │  WHERE event_date = '2024-11-29'   -- Black Friday│      │   │
  │    │    │  GROUP BY customer_id;                             │      │   │
  │    │    │                                                    │      │   │
  │    │    │  5 TB table... scanning... 4 hours later...       │      │   │
  │    │    │  ❌ OUT OF MEMORY  /  ❌ CONNECTION TIMEOUT        │      │   │
  │    │    └──────────────────────────────────────────────────┘      │   │
  │    └──────────────────────────────────────────────────────────────┘   │
  └───────────────────────────────────────────────────────────────────────┘
```

**Problems with single-machine databases at Big Data scale:**

| Problem | Description | Impact |
|---------|-------------|--------|
| **Vertical scaling limits** | You can only add so much RAM/CPU to one machine. A 4 TB RAM server costs $500K+ and still has a ceiling. | Hardware becomes exponentially expensive |
| **Single point of failure** | If the server crashes, everything is down — no redundancy | One hardware failure = total outage |
| **Limited concurrency** | One server can handle a limited number of simultaneous queries | Users experience timeouts during peak load |
| **I/O bottleneck** | All reads/writes go through one disk controller | Throughput is capped by hardware limits |
| **No horizontal distribution** | Data cannot be split across machines for parallel processing | You cannot throw more machines at the problem |
| **Backup/recovery is slow** | Backing up 50 TB from a single server takes hours to days | Recovery time after failure is unacceptable |

---

### Vertical Scaling vs. Horizontal Scaling

The fundamental question is: **when one machine is not enough, what do you do?**

There are two approaches:

```
  VERTICAL SCALING (Scale Up)              HORIZONTAL SCALING (Scale Out)
  ═══════════════════════════              ════════════════════════════════

  Add more power to ONE machine            Add MORE machines to the cluster

     ┌─────────────┐                       ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
     │             │                       │        │ │        │ │        │ │        │
     │   128 GB    │                       │  32 GB │ │  32 GB │ │  32 GB │ │  32 GB │
     │   64 cores  │                       │ 8 cores│ │ 8 cores│ │ 8 cores│ │ 8 cores│
     │   $$$$$     │                       │   $    │ │   $    │ │   $    │ │   $    │
     │             │                       │        │ │        │ │        │ │        │
     └─────────────┘                       └────────┘ └────────┘ └────────┘ └────────┘
           ▲                                    │         │         │         │
           │                                    └─────────┴─────────┴─────────┘
     Bigger & Bigger                              Commodity hardware cluster
     (has a ceiling!)                             (scales linearly!)

     ┌─────────────┐
     │             │
     │   256 GB    │
     │  128 cores  │
     │  $$$$$$$$   │    ← Costs grow exponentially
     │             │       Still has a ceiling!
     └─────────────┘
```

| Aspect | Vertical Scaling (Scale Up) | Horizontal Scaling (Scale Out) |
|--------|---------------------------|-------------------------------|
| **Approach** | Bigger, more powerful single machine | Many smaller machines working together |
| **Cost curve** | Exponential (doubling power > doubles cost) | Linear (doubling machines ≈ doubles cost) |
| **Ceiling** | Physical limits of single machine hardware | Practically unlimited (add more nodes) |
| **Fault tolerance** | Single point of failure | Nodes can fail; system continues |
| **Complexity** | Simple — one machine to manage | Complex — distributed coordination needed |
| **Examples** | Upgrade MySQL server RAM from 64 GB to 256 GB | Hadoop/Spark cluster with 100 commodity nodes |

> **Key Insight**: Big Data systems chose **horizontal scaling**. Instead of buying one
> supercomputer, you buy hundreds of cheap commodity servers and distribute the work.
> This is the foundational principle behind Hadoop and Spark.

---

### Solutions for Big Data

When one machine is not enough, you **distribute the data and computation across multiple
machines**. Two major solutions emerged:

```
  THE BIG DATA TIMELINE
  ═════════════════════

  2003-2004          2006              2009              2014            2020+
  ────────┬──────────┬─────────────────┬─────────────────┬───────────────┬──────
          │          │                 │                 │               │
   Google publishes  Apache Hadoop    Spark created    Spark 1.0      Databricks
   GFS & MapReduce   released         at UC Berkeley   released       Lakehouse
   papers            (open source)    (AMPLab)         (Apache)       Platform

          │          │                 │                 │               │
          ▼          ▼                 ▼                 ▼               ▼
    Research      HADOOP ERA        TRANSITION        SPARK ERA      LAKEHOUSE ERA
    Phase         (2006-2014)       (2009-2014)       (2014-now)     (2020-now)
```

---

## Hadoop: The First Big Data Framework

### What Is Hadoop?

Apache Hadoop is an **open-source framework** for distributed storage and processing of
large datasets across clusters of commodity hardware. It was inspired by two Google papers:
- **Google File System (GFS)** — 2003 → became HDFS
- **MapReduce** — 2004 → became Hadoop MapReduce

### Hadoop Core Components

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                    HADOOP ECOSYSTEM                              │
  │                                                                  │
  │   ┌─────────────────────────────────────────────────────────┐   │
  │   │                 APPLICATION LAYER                        │   │
  │   │   ┌──────┐  ┌──────┐  ┌───────┐  ┌───────┐  ┌───────┐ │   │
  │   │   │ Hive │  │ Pig  │  │ HBase │  │Sqoop  │  │ Oozie │ │   │
  │   │   │(SQL) │  │(ETL) │  │(NoSQL)│  │(Import│  │(Sched)│ │   │
  │   │   └──────┘  └──────┘  └───────┘  └───────┘  └───────┘ │   │
  │   └─────────────────────────────────────────────────────────┘   │
  │                                                                  │
  │   ┌───────────────────────┐  ┌──────────────────────────────┐   │
  │   │    YARN               │  │     MapReduce                │   │
  │   │    Resource Manager   │  │     Processing Engine        │   │
  │   │                       │  │                              │   │
  │   │  - Allocates CPU/RAM  │  │  - Map phase (transform)    │   │
  │   │  - Manages containers │  │  - Shuffle & sort           │   │
  │   │  - Schedules jobs     │  │  - Reduce phase (aggregate) │   │
  │   └───────────────────────┘  └──────────────────────────────┘   │
  │                                                                  │
  │   ┌─────────────────────────────────────────────────────────┐   │
  │   │                      HDFS                                │   │
  │   │              Hadoop Distributed File System              │   │
  │   │                                                          │   │
  │   │  - Stores data across multiple machines                  │   │
  │   │  - Breaks files into 128 MB blocks                       │   │
  │   │  - Replicates each block 3x for fault tolerance          │   │
  │   │  - Write-once, read-many optimized                       │   │
  │   └─────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────┘
```

| Component | Purpose | Analogy |
|-----------|---------|---------|
| **HDFS** | Distributed storage | A warehouse with shelves across multiple buildings — each item is stored in 3 different buildings for safety |
| **MapReduce** | Distributed processing engine | An assembly line — workers at different stations process parts (Map) and a final station combines them (Reduce) |
| **YARN** | Resource management (CPU, RAM allocation) | A manager who assigns workers to assembly lines and ensures no line is overloaded |
| **Hive** | SQL query layer on top of Hadoop | A translator who converts your English (SQL) into factory floor instructions (MapReduce jobs) |

---

### HDFS (Hadoop Distributed File System) — Deep Dive

#### Why HDFS?

Traditional file systems store data on a single machine's disk. When you have petabytes
of data, a single disk (or even a single machine) cannot hold it all. HDFS solves this by:

1. **Breaking files into blocks** (128 MB default, configurable)
2. **Distributing blocks across multiple machines** (DataNodes)
3. **Replicating each block** (default 3 copies) for fault tolerance
4. **Tracking metadata centrally** via the NameNode

#### HDFS Architecture

```
                     ┌──────────────────────────────┐
                     │           NAMENODE            │
                     │      (Master / Metadata)      │
                     │                               │
                     │  File: /data/sales.csv         │
                     │  Size: 384 MB → 3 blocks      │
                     │                               │
                     │  Block A → DN1, DN2, DN3      │
                     │  Block B → DN2, DN3, DN4      │
                     │  Block C → DN1, DN3, DN4      │
                     └──────────────┬───────────────┘
                                    │ metadata lookups
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
    ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
    │   DATANODE 1     │  │   DATANODE 2     │  │   DATANODE 3     │
    │   (Worker)       │  │   (Worker)       │  │   (Worker)       │
    │                  │  │                  │  │                  │
    │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │
    │  │  Block A   │  │  │  │  Block A   │  │  │  │  Block A   │  │
    │  │  (128 MB)  │  │  │  │  (replica) │  │  │  │  (replica) │  │
    │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │
    │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │
    │  │  Block C   │  │  │  │  Block B   │  │  │  │  Block B   │  │
    │  │  (replica) │  │  │  │  (128 MB)  │  │  │  │  (replica) │  │
    │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │
    └──────────────────┘  └──────────────────┘  └──────────────────┘

                                    ┌──────────────────┐
                                    │   DATANODE 4     │
                                    │   (Worker)       │
                                    │                  │
                                    │  ┌────────────┐  │
                                    │  │  Block B   │  │
                                    │  │  (replica) │  │
                                    │  └────────────┘  │
                                    │  ┌────────────┐  │
                                    │  │  Block C   │  │
                                    │  │  (128 MB)  │  │
                                    │  └────────────┘  │
                                    └──────────────────┘
```

#### HDFS — Step by Step Example

Let's trace what happens when you store a 384 MB file (`sales.csv`) in HDFS:

**Step 1: Client requests to write the file**
```
  Client → NameNode: "I want to write /data/sales.csv (384 MB)"
  NameNode → Client: "Split into 3 blocks of 128 MB. Here are the DataNodes for each block."
```

**Step 2: File is split into blocks and distributed**
```
  sales.csv (384 MB)
  ┌──────────────────┬──────────────────┬──────────────────┐
  │   Block A        │   Block B        │   Block C        │
  │   (128 MB)       │   (128 MB)       │   (128 MB)       │
  │   Rows 1-1M     │   Rows 1M-2M    │   Rows 2M-3M    │
  └────────┬─────────┴────────┬─────────┴────────┬─────────┘
           │                  │                  │
           ▼                  ▼                  ▼
     Replicated 3x      Replicated 3x      Replicated 3x
     across DataNodes   across DataNodes   across DataNodes
```

**Step 3: Replication pipeline (for Block A)**
```
  Client ──writes──▶ DataNode 1 ──replicates──▶ DataNode 2 ──replicates──▶ DataNode 3
                     (primary)                   (replica 1)                (replica 2)
```

**Step 4: NameNode records metadata**
```
  NameNode's metadata table:
  ┌────────────────────────────────────────────────────┐
  │  File: /data/sales.csv                             │
  │  Total Size: 384 MB                                │
  │  Block Size: 128 MB                                │
  │  Replication Factor: 3                             │
  │                                                    │
  │  Block A  →  DataNode 1 (primary), DN2, DN3       │
  │  Block B  →  DataNode 2 (primary), DN3, DN4       │
  │  Block C  →  DataNode 1 (primary), DN3, DN4       │
  └────────────────────────────────────────────────────┘
```

#### What Happens When a DataNode Fails?

```
  BEFORE FAILURE                          AFTER DATANODE 2 FAILS
  ══════════════                          ══════════════════════

  DN1: Block A, Block C                   DN1: Block A, Block C
  DN2: Block A, Block B    ← FAILS! →    DN2: ████████████████  (DEAD)
  DN3: Block A, Block B, Block C          DN3: Block A, Block B, Block C
  DN4: Block B, Block C                   DN4: Block B, Block C

  NameNode detects DN2 is dead (no heartbeat for 10 minutes)
  NameNode instructs DN4 to replicate Block A from DN1
  → Block A is now back to 3 replicas: DN1, DN3, DN4

  Result: NO DATA LOST. System self-healed automatically.
```

#### HDFS Key Properties

| Property | Value | Why |
|----------|-------|-----|
| **Default block size** | 128 MB | Large blocks reduce NameNode metadata overhead and improve sequential read throughput |
| **Default replication** | 3 copies | Survives 2 simultaneous node failures |
| **Write pattern** | Write-once, read-many | Optimized for batch analytics, not random updates |
| **NameNode** | Single master (with standby) | Centralized metadata management; HA mode uses standby NameNode |
| **Heartbeat** | Every 3 seconds | DataNodes send heartbeats to NameNode; missed heartbeats = node presumed dead |
| **Rack awareness** | Replicas spread across racks | Survives entire rack power failures |

#### HDFS Commands (Reference)

```bash
# List files
hdfs dfs -ls /data/

# Upload a local file to HDFS
hdfs dfs -put local_file.csv /data/sales.csv

# Download from HDFS to local
hdfs dfs -get /data/sales.csv ./local_copy.csv

# Check file block locations
hdfs fsck /data/sales.csv -files -blocks -locations

# Check HDFS health
hdfs dfsadmin -report
```

---

### MapReduce — Processing Engine

MapReduce is Hadoop's original distributed processing engine. Understanding it is critical
to appreciating why Spark was created.

#### How MapReduce Works

Every MapReduce job has exactly two phases:

1. **Map Phase**: Each mapper processes one block of input data and emits key-value pairs
2. **Reduce Phase**: Reducers receive all values for a given key and produce final output

Between them is a **Shuffle & Sort** phase handled by the framework.

#### Word Count Example (The "Hello World" of Big Data)

**Input file** (stored across 2 HDFS blocks):
```
Block 1: "the cat sat on the mat"
Block 2: "the dog sat on the log"
```

**MapReduce Execution:**

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                     MapReduce: Word Count                           │
  └─────────────────────────────────────────────────────────────────────┘

  INPUT (HDFS Blocks)         MAP PHASE                SHUFFLE & SORT
  ═══════════════════         ═════════                ══════════════

  Block 1:                    Mapper 1 output:         Group by key:
  "the cat sat               (the, 1)                 ┌──────────────┐
   on the mat"               (cat, 1)                 │ cat  → [1]   │
       │                     (sat, 1)                 │ dog  → [1]   │
       └──▶ Mapper 1         (on,  1)                 │ log  → [1]   │
                             (the, 1)                 │ mat  → [1]   │
                             (mat, 1)                 │ on   → [1,1] │
                                                      │ sat  → [1,1] │
  Block 2:                    Mapper 2 output:         │ the  → [1,1, │
  "the dog sat               (the, 1)                 │        1,1]  │
   on the log"               (dog, 1)                 └──────┬───────┘
       │                     (sat, 1)                        │
       └──▶ Mapper 2         (on,  1)                        │
                             (the, 1)                        ▼
                             (log, 1)
                                                      REDUCE PHASE
                                                      ════════════
                                                      Reducer output:
                                                      (cat, 1)
                                                      (dog, 1)
                                                      (log, 1)
                                                      (mat, 1)
                                                      (on,  2)
                                                      (sat, 2)
                                                      (the, 4)
```

#### The Disk I/O Problem

Here is the critical issue with MapReduce. **Every step reads from and writes to disk:**

```
  ┌──────┐     ┌──────┐     ┌──────┐     ┌──────┐     ┌──────┐
  │ HDFS │────▶│ Map  │────▶│ Disk │────▶│Reduce│────▶│ HDFS │
  │(read)│     │      │     │(write│     │      │     │(write│
  └──────┘     └──────┘     │ read)│     └──────┘     │ back)│
                            └──────┘                   └──────┘
     DISK         RAM          DISK         RAM          DISK
```

For a **multi-step pipeline** (common in real analytics), each step is a separate
MapReduce job, and **each job reads/writes to disk**:

```
  JOB 1                    JOB 2                    JOB 3
  ┌──────┐   ┌──────┐     ┌──────┐   ┌──────┐     ┌──────┐   ┌──────┐
  │ Read │──▶│Write │────▶│ Read │──▶│Write │────▶│ Read │──▶│Write │
  │ HDFS │   │ HDFS │     │ HDFS │   │ HDFS │     │ HDFS │   │ HDFS │
  └──────┘   └──────┘     └──────┘   └──────┘     └──────┘   └──────┘
     ↑           ↑            ↑           ↑            ↑           ↑
     └───────────┴────────────┴───────────┴────────────┴───────────┘
              6 DISK OPERATIONS for a 3-step pipeline!
              Each disk read/write adds minutes of latency.
```

**This is the #1 reason Spark was created** — to keep data **in memory** between stages.

#### MapReduce in Java (Verbose!)

A simple word count in Java MapReduce requires ~50 lines of boilerplate:

```java
// Mapper class
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            word.set(token.toLowerCase());
            context.write(word, one);
        }
    }
}

// Reducer class
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

// Driver class (another 20+ lines to configure and run the job)
```

The same in **Spark (Python)** — 1 line:

```python
sc.textFile("hdfs:///data/input.txt").flatMap(lambda line: line.split()).countByValue()
```

Or with DataFrames and SQL:

```python
spark.read.text("hdfs:///data/input.txt") \
    .selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count() \
    .orderBy("count", ascending=False) \
    .show()
```

---

### YARN — Yet Another Resource Negotiator

YARN manages cluster resources and schedules jobs. Before YARN (Hadoop 1.x), MapReduce
handled both processing AND resource management — a poor design that limited the
ecosystem.

```
  HADOOP 1.x (Before YARN)              HADOOP 2.x (With YARN)
  ═════════════════════════              ═══════════════════════

  ┌──────────────────────┐              ┌──────────────────────┐
  │  JobTracker           │              │  ResourceManager     │
  │  (MapReduce ONLY)     │              │  (ANY framework)     │
  │                       │              │                       │
  │  - Schedules tasks    │              │  ┌────────┐          │
  │  - Manages resources  │              │  │  MR    │          │
  │  - Only MapReduce!    │              │  ├────────┤          │
  └───────────────────────┘              │  │ Spark  │          │
                                         │  ├────────┤          │
  Only ONE framework                     │  │ Flink  │          │
  could run on the cluster               │  ├────────┤          │
                                         │  │ Tez    │          │
                                         │  └────────┘          │
                                         └──────────────────────┘

                                         Multiple frameworks share
                                         the same cluster!
```

**YARN made it possible for Spark to run on Hadoop clusters** without replacing the
entire ecosystem. Organizations could adopt Spark incrementally.

---

### Hive — SQL on Hadoop

#### Before Hive

Before Hive, the only way to process data on Hadoop was to write **Java MapReduce jobs**.
This meant:

- Data analysts who knew SQL could **not** use Hadoop
- Simple analytics queries required hundreds of lines of Java
- The feedback loop was slow: write code → compile → package → submit → wait → debug

#### What Hive Does

Apache Hive (created by Facebook in 2009) brought **SQL to Hadoop**:

```
  ┌────────────────────────────────────────────────────────────────┐
  │                        HIVE ARCHITECTURE                       │
  │                                                                │
  │   User writes SQL:                                             │
  │   ┌──────────────────────────────────────────────────────────┐ │
  │   │ SELECT customer_id, COUNT(*) as visit_count              │ │
  │   │ FROM clickstream                                         │ │
  │   │ WHERE event_date = '2024-11-29'                          │ │
  │   │ GROUP BY customer_id                                     │ │
  │   │ HAVING COUNT(*) > 10;                                    │ │
  │   └──────────────────────────┬───────────────────────────────┘ │
  │                              │                                 │
  │                              ▼                                 │
  │                    ┌──────────────────┐                        │
  │                    │   Hive Compiler  │                        │
  │                    │   (SQL Parser)   │                        │
  │                    └────────┬─────────┘                        │
  │                             │ translates to                    │
  │                             ▼                                  │
  │                    ┌──────────────────┐                        │
  │                    │  MapReduce Jobs  │                        │
  │                    │  (or Tez / Spark)│                        │
  │                    └────────┬─────────┘                        │
  │                             │ executes on                      │
  │                             ▼                                  │
  │                    ┌──────────────────┐                        │
  │                    │   HDFS / YARN    │                        │
  │                    └──────────────────┘                        │
  └────────────────────────────────────────────────────────────────┘
```

#### Hive Use Cases

| Use Case | Example |
|----------|---------|
| **Batch analytics** | Daily revenue aggregation across all stores |
| **Data warehousing** | Star/snowflake schemas on Hadoop for BI reporting |
| **ETL pipelines** | Transform raw logs into structured tables for analysts |
| **Ad-hoc queries** | Analysts explore large datasets using familiar SQL syntax |

#### Hive Limitations

| Limitation | Description |
|-----------|-------------|
| **Batch only** | Not suitable for real-time or low-latency queries (minutes, not seconds) |
| **MapReduce dependency** | Original Hive translates to MapReduce (slow disk-based execution) |
| **No ACID transactions** | No UPDATE/DELETE support in original Hive (added later with limitations) |
| **High latency** | Even simple queries take 30-60 seconds due to MapReduce startup overhead |

> **Modern Note**: Hive has evolved significantly. Hive on Tez/Spark is faster. Hive ACID
> (v3+) supports transactions. But in the Databricks world, **Spark SQL** and **Delta Lake**
> have largely replaced Hive's role.

---

## Why Spark Replaced MapReduce

### The Problems That Drove the Change

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │          WHY SPARK? — The MapReduce Pain Points                      │
  │                                                                      │
  │  1. DISK-BASED PROCESSING                                           │
  │     Every stage reads from disk and writes back to disk.             │
  │     Multi-step analytics pipelines are painfully slow.               │
  │                                                                      │
  │  2. SLOW ITERATIVE PROCESSING                                       │
  │     ML algorithms need 100+ iterations over the same data.           │
  │     Each iteration = new MapReduce job = new disk I/O cycle.         │
  │     Training a model takes hours instead of minutes.                 │
  │                                                                      │
  │  3. COMPLEX PROGRAMMING MODEL                                       │
  │     Everything must fit into Map and Reduce phases.                  │
  │     Joins require custom partitioners and multi-stage jobs.          │
  │     50+ lines of Java for simple operations.                         │
  │                                                                      │
  │  4. NO INTERACTIVE ANALYSIS                                         │
  │     Cannot run ad-hoc queries and get quick results.                 │
  │     Job startup alone takes 30-60 seconds.                           │
  │                                                                      │
  │  5. NO NATIVE STREAMING                                             │
  │     MapReduce is batch-only by design.                               │
  │     Real-time processing requires separate tools (Storm, Samza).     │
  └──────────────────────────────────────────────────────────────────────┘
```

### What Is Apache Spark?

Apache Spark is a **unified distributed processing engine** that:

- Works **in memory** (keeps data in RAM between stages)
- Is **10-100x faster** than Hadoop MapReduce for most workloads
- Supports **batch + streaming + ML + graph** processing in one framework
- Offers APIs in **Python, Scala, Java, R, and SQL**

#### Spark Architecture Overview

```
                         ┌───────────────────────────────────────┐
                         │           YOUR APPLICATION            │
                         │  (Python / Scala / Java / R / SQL)    │
                         └───────────────────┬───────────────────┘
                                             │
                         ┌───────────────────▼───────────────────┐
                         │           SPARK CORE ENGINE           │
                         │                                       │
                         │  ┌─────────┐ ┌──────────┐ ┌────────┐│
                         │  │Spark SQL│ │Structured│ │ MLlib  ││
                         │  │         │ │Streaming │ │   (ML) ││
                         │  └─────────┘ └──────────┘ └────────┘│
                         │  ┌─────────┐ ┌──────────────────────┐│
                         │  │ GraphX  │ │  DataFrames / RDDs   ││
                         │  └─────────┘ └──────────────────────┘│
                         └───────────────────┬───────────────────┘
                                             │
                 ┌───────────────────────────┼───────────────────────────┐
                 │                           │                           │
          ┌──────▼──────┐           ┌───────▼───────┐           ┌──────▼──────┐
          │ Standalone  │           │     YARN      │           │ Kubernetes  │
          │  Cluster    │           │   (Hadoop)    │           │   Cluster   │
          └─────────────┘           └───────────────┘           └─────────────┘
                 │                           │                           │
          ┌──────▼──────┐           ┌───────▼───────┐           ┌──────▼──────┐
          │    Local     │           │     HDFS      │           │   S3/ADLS   │
          │    Disk      │           │               │           │   GCS       │
          └─────────────┘           └───────────────┘           └─────────────┘
```

#### Spark's In-Memory Advantage

This is the fundamental difference. Instead of writing intermediate results to disk,
Spark keeps them **in memory**:

```
  MapReduce (3-step pipeline):
  ┌──────┐ → ┌──────┐ → ┌──────┐ → ┌──────┐ → ┌──────┐ → ┌──────┐
  │ HDFS │   │ Map  │   │ HDFS │   │ Map  │   │ HDFS │   │ Map  │
  │ Read │   │Reduce│   │Write/│   │Reduce│   │Write/│   │Reduce│ → HDFS
  └──────┘   └──────┘   │ Read │   └──────┘   │ Read │   └──────┘
                         └──────┘              └──────┘
  Time: ████████████████████████████████████████████████████  (30 min)
        ↑ disk  ↑ disk  ↑ disk  ↑ disk  ↑ disk  ↑ disk


  Spark (same 3-step pipeline):
  ┌──────┐ → ┌──────────────────────────────────────────┐ → ┌──────┐
  │ HDFS │   │           IN-MEMORY PROCESSING            │   │ HDFS │
  │ Read │   │  Step 1 ──▶ Step 2 ──▶ Step 3            │   │Write │
  └──────┘   └──────────────────────────────────────────┘   └──────┘
  Time: ██████████  (3 min)
        ↑ disk                                      ↑ disk
        (only at start and end!)
```

### Spark vs. Hadoop MapReduce — Complete Comparison

| Feature | Hadoop MapReduce | Apache Spark |
|---------|-----------------|--------------|
| **Processing model** | Disk-based (read → process → write to disk) | In-memory (keeps data in RAM between stages) |
| **Speed** | Baseline (1x) | **10-100x faster** for iterative/multi-stage workloads |
| **Ease of use** | Java-heavy, verbose (50+ lines for word count) | Python, Scala, SQL (1-5 lines for word count) |
| **Streaming** | No native support (need separate tools) | Built-in Structured Streaming |
| **Machine Learning** | No native support (need Mahout) | Built-in MLlib library |
| **Interactive queries** | Not designed for it (high startup latency) | Spark SQL with sub-second latency |
| **Fault tolerance** | Data replication (3x storage cost) | RDD lineage (recompute lost partitions — no extra storage) |
| **Resource management** | YARN | YARN, Kubernetes, Standalone, Databricks |
| **Storage** | Tightly coupled with HDFS | Any source: HDFS, S3, ADLS, GCS, Delta Lake, JDBC |
| **Graph processing** | No native support | GraphX library |
| **API** | Map and Reduce only | Rich API: map, filter, join, groupBy, window, SQL, ML, streaming |
| **Community** | Declining | Rapidly growing; most popular Big Data framework |
| **Cost** | More nodes needed due to slow processing | Fewer nodes needed; shorter job times = lower cloud costs |

#### Speed Comparison — Real Numbers

```
  BENCHMARK: Logistic Regression (ML) — 100 iterations over 100 GB

  Hadoop MapReduce:  ████████████████████████████████████████████████  110 minutes
  Apache Spark:      ████                                               4 minutes

  BENCHMARK: Sort 100 TB (Daytona GraySort record)

  Hadoop MapReduce:  72 minutes on 2100 nodes
  Apache Spark:      23 minutes on  206 nodes  ← 10x fewer machines, 3x faster
```

---

## Spark Fundamentals — How It Actually Works

Now that you understand *why* Spark exists, let's look at *how* it works. This section
gives you the mental model you need before writing any code.

### The Word Count Story — Why Simple Problems Become Hard at Scale

Consider a simple task: count the occurrence of each word in a file. On a single machine,
it is trivial — read the file, split into words, store word and frequency in a hash map.

```python
# Simple word count on a single machine
word_counts = {}
with open("small_file.txt") as f:
    for line in f:
        for word in line.split():
            word_counts[word] = word_counts.get(word, 0) + 1
```

**Now imagine doing this for Big Data** — say you want to find what is trending on the
internet and you need to process petabytes of text. You cannot rely on a single application
running on a single computer. The program logic is simple (counting words), but doing it
at Big Data scale is an entirely different story.

```
  SIMPLE PROBLEM                           SAME PROBLEM AT SCALE
  ══════════════                           ══════════════════════

  ┌───────────────┐                        ┌───────────────────────────────────┐
  │ 1 file        │                        │ 10 billion files                  │
  │ 1 MB          │                        │ 5 PB total                        │
  │ 1 computer    │                        │ 1000 computers                    │
  │ 1 second      │                        │ ??? time                          │
  │               │                        │                                   │
  │ word_counts   │                        │ How to distribute files evenly?   │
  │ = {}          │                        │ How to coordinate 1000 machines?  │
  │ Easy!         │                        │ How to aggregate partial counts?  │
  └───────────────┘                        │ What if a machine crashes?        │
                                           │ What if network connection drops? │
                                           │ What about memory overflow?       │
                                           │ How to handle stragglers?         │
                                           └───────────────────────────────────┘
```

If you were to build this system yourself, you would need to:

1. **Ingest** very large files and figure out how to **distribute them evenly** across the cluster
2. **Create a master process** to coordinate which machine processes which part of the text
3. **Aggregate** each machine's partial counts into final totals
4. **Handle failures** — machines running out of memory, losing network connectivity, crashing
5. **Optimize performance** — data locality, load balancing, minimizing network transfer

**Here is the good news: Spark handles ALL of this complexity for you.** You write simple
code as if it were running on your local machine. Once you give that code to Spark, it
figures out how to distribute and run it across a massive cluster.

```python
# The SAME word count, but now it runs on 1000 machines via Spark
# You write this on your laptop. Spark runs it across the cluster.

word_counts = (
    spark.read.text("s3://my-bucket/internet-data/")     # reads petabytes
    .selectExpr("explode(split(value, ' ')) as word")     # splits into words
    .groupBy("word").count()                               # counts per word
    .orderBy("count", ascending=False)                     # sorts by frequency
)
word_counts.show()
```

That is the power of Spark — **the code looks like local single-machine code, but it
executes across hundreds or thousands of machines.**

### Spark's API Evolution — Simpler Over Time

Spark's API has evolved to become increasingly user-friendly:

```
  HADOOP MapReduce (Java)              SPARK RDD API (older)         SPARK DataFrame/SQL API (modern)
  ════════════════════════             ═════════════════════         ═══════════════════════════════

  public class WordCount {             sc.textFile("data.txt")      spark.read.text("data.txt")
    public static class Map              .flatMap(                     .selectExpr(
      extends Mapper<...> {                lambda l: l.split())         "explode(split(value,' '))
      public void map(...) {             .map(                           as word")
        String[] tokens =                   lambda w: (w, 1))         .groupBy("word")
          value.toString()               .reduceByKey(                .count()
            .split("\\s+");                lambda a,b: a+b)
        for (String t: tokens){          .collect()
          context.write(                                              # Or pure SQL:
            new Text(t),                                              # spark.sql("""
            new IntWritable(1));                                       #   SELECT word, COUNT(*)
        }                                                             #   FROM text_table
      }                                                               #   GROUP BY word
    }                                                                 # """)
    public static class Reduce
      extends Reducer<...> {
      public void reduce(...){
        int sum = 0;
        for (IntWritable v:vals)
          sum += v.get();
        context.write(key,
          new IntWritable(sum));
      }
    }
    // + 20 more lines of config
  }

  ~50 lines of Java                   ~5 lines of Python            ~4 lines of Python/SQL
  Must compile, package, submit       Interactive, testable         Even simpler, query-optimized
  ```

> **Key point**: The modern DataFrame/SQL API lets you focus on solving business problems.
> You don't worry about clusters, partitions, or coordination. Spark handles all of that
> behind the scenes. If you need lower-level control, you can still access RDD objects
> directly (covered in [04 - RDDs Fundamentals](04-rdds-fundamentals.md)).

---

### Master, Worker, Driver, and Executors

When you start a Spark cluster, several processes coordinate to run your code across
multiple machines. Understanding this architecture is essential.

```
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    SPARK CLUSTER ARCHITECTURE                               │
  │                                                                             │
  │   ┌──────────────────┐                                                     │
  │   │   MASTER NODE    │     Ensures workers are up and running              │
  │   │   (JVM process)  │     Receives job submissions                        │
  │   │                  │     Assigns driver to a worker node                 │
  │   └────────┬─────────┘     Can restart crashed workers                     │
  │            │                                                                │
  │     ┌──────┼───────────────────────────────┐                               │
  │     │      │                               │                               │
  │     ▼      ▼                               ▼                               │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
  │  │  WORKER NODE 1   │  │  WORKER NODE 2   │  │  WORKER NODE 3   │          │
  │  │  (JVM process)   │  │  (JVM process)   │  │  (JVM process)   │          │
  │  │                  │  │                  │  │                  │          │
  │  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │          │
  │  │  │  EXECUTOR  │  │  │  │  DRIVER ★  │  │  │  │  EXECUTOR  │  │          │
  │  │  │  (JVM)     │  │  │  │ (your code)│  │  │  │  (JVM)     │  │          │
  │  │  │            │  │  │  │            │  │  │  │            │  │          │
  │  │  │ ┌────┐┌──┐│  │  │  │ Orchestrates│  │  │  │ ┌────┐┌──┐│  │          │
  │  │  │ │Task││T ││  │  │  │ entire job  │  │  │  │ │Task││T ││  │          │
  │  │  │ │  1 ││2 ││  │  │  └────────────┘  │  │  │ │  5 ││6 ││  │          │
  │  │  │ └────┘└──┘│  │  │  ┌────────────┐  │  │  │ └────┘└──┘│  │          │
  │  │  └────────────┘  │  │  │  EXECUTOR  │  │  │  └────────────┘  │          │
  │  │                  │  │  │  (JVM)     │  │  │                  │          │
  │  │                  │  │  │ ┌────┐┌──┐│  │  │                  │          │
  │  │                  │  │  │ │Task││T ││  │  │                  │          │
  │  │                  │  │  │ │  3 ││4 ││  │  │                  │          │
  │  │                  │  │  │ └────┘└──┘│  │  │                  │          │
  │  │                  │  │  └────────────┘  │  │                  │          │
  │  └──────────────────┘  └──────────────────┘  └──────────────────┘          │
  └─────────────────────────────────────────────────────────────────────────────┘

  ★ There is exactly ONE driver process per Spark job.
    The driver is the orchestrator — it does NOT execute data processing itself.
    Executors do all the heavy lifting.
```

#### The Process Step by Step

Here is exactly what happens when you submit a Spark application:

```
  Step 1: Start the cluster
  ┌────────────────────────────────────────────────────────────────┐
  │  Master JVM starts on master node                              │
  │  Worker JVM starts on each worker node                         │
  │  Workers register with Master ("I'm alive, I have 8 cores")   │
  │  Master monitors workers via heartbeats                        │
  └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  Step 2: Submit your application
  ┌────────────────────────────────────────────────────────────────┐
  │  You submit your code to the Master node                       │
  │  (In Databricks, this happens when you click "Run" in a        │
  │   notebook — the platform handles submission for you)          │
  └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  Step 3: Master launches the Driver
  ┌────────────────────────────────────────────────────────────────┐
  │  Master picks a worker node and tells it to start the          │
  │  DRIVER process. The Driver contains your code logic:          │
  │  - Where to get the data from                                  │
  │  - What transformations to apply                               │
  │  - What to do with the results                                 │
  └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  Step 4: Driver requests Executors
  ┌────────────────────────────────────────────────────────────────┐
  │  Driver asks Master to launch EXECUTOR JVMs on worker nodes    │
  │  Master tells each Worker to start an Executor process         │
  │  Executors are the processes that actually run your code       │
  │  (Allocate enough memory to executors — they do the work!)     │
  └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  Step 5: Driver distributes tasks to Executors
  ┌────────────────────────────────────────────────────────────────┐
  │  Driver sends code to each Executor                            │
  │  Driver assigns data partitions to each Executor               │
  │  Executors spawn Task threads to process partitions            │
  │  Driver monitors progress and handles retries on failure       │
  └────────────────────────────────────────────────────────────────┘
                                │
                                ▼
  Step 6: Executors process data and return results
  ┌────────────────────────────────────────────────────────────────┐
  │  Each Task reads its partition, applies transformations         │
  │  Results flow back to the Driver for aggregation               │
  │  Driver writes final output or displays results                │
  └────────────────────────────────────────────────────────────────┘
```

#### Key Roles Summary

| Process | Role | Quantity | Where It Runs |
|---------|------|----------|---------------|
| **Master** | Ensures workers are alive; receives job submissions | 1 (can be HA with standby) | Master node |
| **Worker** | Manages executors on its node; reports to master | 1 per machine | Each worker machine |
| **Driver** | Orchestrates the entire job; contains your code logic; assigns tasks | **Exactly 1** per job | On a worker node (cluster mode) or your laptop (client mode) |
| **Executor** | Runs tasks, processes data partitions, caches data | 1+ per worker | Worker nodes (can also run on master node) |
| **Task** | Smallest unit of work; processes one partition | Many per executor | Inside executor JVMs |

#### Fault Tolerance at Every Level

```
  If an EXECUTOR crashes  →  Worker restarts it
  If a WORKER crashes     →  Master restarts it
  If the DRIVER crashes   →  Master can relaunch it (configurable)
                              but the entire job restarts from scratch
                              (there is only ONE driver per job)
```

> **Databricks Note**: In Databricks, you don't manage Master/Worker processes directly.
> When you create a cluster, Databricks provisions the nodes, starts the processes, and
> handles all the coordination. Your notebook runs as the Driver. You focus on code.

---

### Partitions — The Foundation of Parallelism

Partitions are how Spark breaks data into chunks that can be processed in parallel across
the cluster. Understanding partitions is crucial for performance.

#### What Are Partitions?

A partition is simply a **group of rows** from your dataset. When Spark reads a large file,
it automatically splits it into partitions so that multiple tasks can work simultaneously.

```
  A 1 TB CSV file split into partitions:

  ┌─────────────────────────────────────────────────────────────────────┐
  │                    Original File (1 TB)                              │
  │  Row 1, Row 2, Row 3, ... ... ... Row 1,000,000,000                │
  └──────────┬──────────┬──────────┬──────────┬──────────┬──────────────┘
             │          │          │          │          │
             ▼          ▼          ▼          ▼          ▼
        ┌─────────┐┌─────────┐┌─────────┐┌─────────┐┌─────────┐
        │ Part 0  ││ Part 1  ││ Part 2  ││  ...    ││Part 7999│   ~8000 partitions
        │ 128 MB  ││ 128 MB  ││ 128 MB  ││         ││ 128 MB  │   (1 TB / 128 MB)
        │ Rows    ││ Rows    ││ Rows    ││         ││ Rows    │
        │ 1-125K  ││125K-250K││250K-375K││         ││ ...-1B  │
        └────┬────┘└────┬────┘└────┬────┘└────┬────┘└────┬────┘
             │          │          │          │          │
             ▼          ▼          ▼          ▼          ▼
          Task 0     Task 1     Task 2     ...      Task 7999
        (thread)   (thread)   (thread)             (thread)
```

#### How Partitions Enable Parallelism

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    WORKER NODE 1                                     │
  │                                                                      │
  │   ┌────────────────────────────────────────────────────────────┐    │
  │   │  EXECUTOR (JVM)                                            │    │
  │   │  Memory: 16 GB                    Cores: 4                 │    │
  │   │                                                            │    │
  │   │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌─────────┐│    │
  │   │  │  Task 0    │ │  Task 1    │ │  Task 2    │ │ Task 3  ││    │
  │   │  │  (thread)  │ │  (thread)  │ │  (thread)  │ │(thread) ││    │
  │   │  │            │ │            │ │            │ │         ││    │
  │   │  │ Partition 0│ │ Partition 1│ │ Partition 2│ │Partn 3  ││    │
  │   │  │ loaded in  │ │ loaded in  │ │ loaded in  │ │loaded in││    │
  │   │  │ memory     │ │ memory     │ │ memory     │ │memory   ││    │
  │   │  └────────────┘ └────────────┘ └────────────┘ └─────────┘│    │
  │   └────────────────────────────────────────────────────────────┘    │
  │                                                                      │
  │  Each Task thread reads its partition, performs transformations,      │
  │  and writes results — all in parallel!                               │
  └─────────────────────────────────────────────────────────────────────┘
```

#### Partition Sizing Rules

| Rule | Guideline |
|------|-----------|
| **Default partition size** | Spark is configured to work with **128 MB per partition** by default |
| **Calculating partition count** | For a 1 TB file: 1 TB / 128 MB = **~8,000 partitions** |
| **Tasks per core** | Best practice: **2-3x** the number of available cores. If you have 4 cores, aim for 8-12 tasks. |
| **Why over-provision tasks?** | Some tasks finish faster than others. Having extra tasks keeps cores busy (no idle time). |

#### Sizing Your Cluster

```
  EXAMPLE: Process a 1 TB file

  Step 1: Calculate partitions
           1 TB / 128 MB = ~8,000 partitions = ~8,000 tasks

  Step 2: Decide on acceptable processing time
           Want it done in 30 minutes? Need enough cores to handle 8,000 tasks
           in 30 minutes.

  Step 3: Choose nodes
           Option A: 10 nodes × 8 cores = 80 cores → 8000/80 = 100 batches
           Option B: 50 nodes × 8 cores = 400 cores → 8000/400 = 20 batches

  COST TIP:
  ┌────────────────────────────────────────────────────────────────────┐
  │  Use MORE cheap nodes with good memory and cores rather than      │
  │  FEWER expensive high-end nodes. Horizontal scaling gives better  │
  │  performance per dollar in distributed systems.                   │
  └────────────────────────────────────────────────────────────────────┘
```

> **Deep Dive**: For a complete treatment of partitions, shuffles, and narrow vs. wide
> transformations, see [03 - Distributed Computing](03-distributed-computing.md).

---

### DAG, Lazy Evaluation, and the Catalyst Optimizer

These three concepts explain *how* Spark achieves its performance. They are among the most
important ideas in all of Spark.

#### The DAG (Directed Acyclic Graph)

When you write Spark code, Spark does not execute your instructions immediately. Instead,
it builds a **DAG** — a Directed Acyclic Graph — which is essentially a **recipe** of
all the steps needed to produce your result.

```
  YOUR CODE                              SPARK'S INTERNAL DAG
  ═════════                              ════════════════════

  df = spark.read.csv("data.csv")        ┌─────────────┐
                                         │  Read CSV    │
  df2 = df.filter(col("age") > 25)       └──────┬──────┘
                                                │
  df3 = df2.select("name", "city")        ┌─────▼──────┐
                                         │  Filter     │
  df4 = df3.groupBy("city").count()       │  age > 25   │
                                         └──────┬──────┘
  df4.show()  ← ACTION triggers                │
                  execution!              ┌─────▼──────┐
                                         │  Select     │
                                         │  name, city │
                                         └──────┬──────┘
                                                │
                                         ┌─────▼──────────┐
                                         │  GroupBy city   │
                                         │  + Count        │
                                         └──────┬─────────┘
                                                │
                                         ┌─────▼──────┐
                                         │   Show      │ ← Execution starts
                                         │  (ACTION)   │    HERE and flows
                                         └─────────────┘    UPWARD through DAG
```

The DAG is literally a one-way graph (directed) that cannot loop back on itself (acyclic).
Think of it as a **recipe card** — the chef (Spark) reads the recipe and figures out the
most efficient way to execute it before turning on the stove.

#### Lazy Evaluation — The "Recipe" Analogy

Spark uses **lazy evaluation**: transformations are NOT executed when you write them.
They are only executed when you call an **action**.

```
  TRANSFORMATIONS (Lazy — just build the recipe)     ACTIONS (Eager — execute the recipe)
  ═══════════════════════════════════════════════     ═══════════════════════════════════

  .filter()          — adds a step to the recipe     .show()       — displays results
  .select()          — adds a step to the recipe     .count()      — returns a number
  .groupBy()         — adds a step to the recipe     .collect()    — brings data to driver
  .join()            — adds a step to the recipe     .write.save() — writes to storage
  .withColumn()      — adds a step to the recipe     .first()      — returns first row
  .orderBy()         — adds a step to the recipe     .take(n)      — returns n rows
```

**Why is this powerful?** Because Spark can look at the **entire recipe** before executing
and optimize it. This is where the Catalyst Optimizer comes in.

#### The Catalyst Optimizer — Spark's Secret Weapon

The Catalyst Optimizer examines your entire DAG and **rewrites it** to be more efficient.
It is the reason Spark is so fast — it does not blindly execute your code step by step.

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                      CATALYST OPTIMIZER                              │
  │                                                                      │
  │  YOUR CODE (what you wrote):                                        │
  │  ──────────────────────────                                         │
  │  df = spark.read.csv("data.csv")           # read all columns      │
  │  df2 = df.filter(col("status") == "active") # filter rows           │
  │  df3 = df2.withColumn("tax", col("price") * 0.1)                   │
  │  df4 = df3.select("name", "price", "tax")   # select 3 columns     │
  │  df5 = df4.drop("tax")                      # drop tax column      │
  │  df5.show()                                                         │
  │                                                                      │
  │  CATALYST'S OPTIMIZED PLAN (what actually executes):                │
  │  ───────────────────────────────────────────────────                │
  │  1. Read ONLY "name", "price", "status" columns    ← Column pruning│
  │     (why read ALL columns if you only need 3?)        saves I/O!   │
  │  2. Filter status == "active" FIRST                 ← Predicate    │
  │     (reduces rows before expensive operations)        pushdown!    │
  │  3. Skip "tax" computation entirely                 ← Dead code    │
  │     (you created tax column then dropped it —         elimination! │
  │      Catalyst sees it's unused and skips it)                        │
  │  4. Select "name", "price"                                          │
  │                                                                      │
  │  Result: Reads less data, processes fewer rows,                     │
  │          skips unnecessary computations = MUCH FASTER               │
  └─────────────────────────────────────────────────────────────────────┘
```

**Real example of Catalyst optimization:**

```
  BEFORE CATALYST                          AFTER CATALYST
  (what you wrote)                         (what actually runs)

  ┌──────────────┐                         ┌──────────────┐
  │ Read ALL 50  │                         │ Read ONLY 3  │  ← Column pruning
  │ columns from │                         │ columns from │
  │ 1 TB file    │                         │ 1 TB file    │
  └──────┬───────┘                         └──────┬───────┘
         │                                        │
  ┌──────▼───────┐                         ┌──────▼───────┐
  │ Join with    │                         │ Filter first │  ← Predicate pushdown
  │ another      │                         │ (reduce 1B   │    (filter BEFORE join
  │ 500M row     │                         │  rows to 1M) │     to join fewer rows)
  │ table        │                         └──────┬───────┘
  └──────┬───────┘                                │
         │                                 ┌──────▼───────┐
  ┌──────▼───────┐                         │ Join with    │  ← Joins 1M rows
  │ Filter       │                         │ 500M table   │    instead of 1B!
  │ (reduce to   │                         └──────┬───────┘
  │  1M rows)    │                                │
  └──────┬───────┘                         ┌──────▼───────┐
         │                                 │ Select 3     │
  ┌──────▼───────┐                         │ columns      │
  │ Drop 2       │                         └──────────────┘
  │ columns      │
  └──────┬───────┘                         Catalyst AUTOMATICALLY:
         │                                 - Pushed filter before join
  ┌──────▼───────┐                         - Pruned unused columns
  │ Select 3     │                         - Eliminated dead code
  │ columns      │                         - Chose optimal join strategy
  └──────────────┘
```

#### Transformations vs. Actions — Summary

| Concept | Description | Example |
|---------|-------------|---------|
| **Transformation** | An instruction added to the DAG. NOT executed immediately (lazy). | `filter()`, `select()`, `groupBy()`, `join()`, `withColumn()` |
| **Action** | Triggers execution of the entire DAG. Results are computed. | `show()`, `count()`, `collect()`, `write.save()`, `first()` |
| **DAG** | Directed Acyclic Graph — the "recipe" of all transformations. Built lazily. | Spark builds this internally as you chain transformations |
| **Catalyst** | The query optimizer that rewrites the DAG for maximum efficiency. | Column pruning, predicate pushdown, join reordering, dead code elimination |

> **Deep Dive**: The Catalyst Optimizer is covered in detail in
> [07 - Catalyst Optimizer](07-catalyst-optimizer.md). The concepts of narrow/wide
> transformations and shuffles are covered in [03 - Distributed Computing](03-distributed-computing.md).

---

### Spark Libraries — A Unified Engine

Unlike Hadoop, which needed separate tools for every task, Spark provides **everything
in one unified engine**:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                    SPARK UNIFIED ENGINE                              │
  │                                                                      │
  │  ┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌─────────────┐ │
  │  │  Spark SQL  │ │  Structured  │ │   MLlib     │ │   GraphX    │ │
  │  │             │ │  Streaming   │ │             │ │             │ │
  │  │ DataFrames  │ │              │ │ ML Pipelines│ │ Graph       │ │
  │  │ SQL queries │ │ Real-time    │ │ Classification│ Processing  │ │
  │  │ Hive compat │ │ event        │ │ Regression  │ │ PageRank    │ │
  │  │ JSON/CSV/   │ │ processing   │ │ Clustering  │ │ Connected   │ │
  │  │ Parquet/XML │ │ Kafka, HDFS  │ │ Recommend.  │ │ Components  │ │
  │  └──────┬──────┘ └──────┬───────┘ └──────┬──────┘ └──────┬──────┘ │
  │         │               │                │               │         │
  │         └───────────────┴────────────────┴───────────────┘         │
  │                                  │                                  │
  │                        ┌─────────▼──────────┐                      │
  │                        │    SPARK CORE       │                      │
  │                        │  (RDDs, Scheduling, │                      │
  │                        │   Memory Mgmt,      │                      │
  │                        │   Fault Tolerance)  │                      │
  │                        └────────────────────┘                      │
  └─────────────────────────────────────────────────────────────────────┘
```

| Library | Purpose | Hadoop Equivalent |
|---------|---------|-------------------|
| **Spark SQL** | SQL queries, DataFrames, reading/writing structured data (JSON, CSV, Parquet, XML, JDBC) | Hive |
| **Structured Streaming** | Real-time ingestion from Kafka, HDFS, Flume, event hubs | Storm / Samza (separate tools) |
| **MLlib** | Machine learning — classification, regression, clustering, recommendations | Mahout (limited, separate tool) |
| **GraphX** | Graph processing — PageRank, connected components, shortest paths | Giraph (separate tool) |

> **Spark SQL** can read and write data to and from various structured formats such as JSON,
> XML, CSV, Parquet, ORC, as well as relational databases via JDBC and many other formats.
> This makes Spark a powerful general-purpose data processing engine.

---

## Distributed File Systems

Now that you understand Hadoop/HDFS and Spark, let's connect to the **modern cloud world**
where your Databricks workloads actually run.

### What Is a Distributed File System?

A **distributed file system** is a storage system where:

- Data is **spread across multiple machines** (or availability zones / regions)
- Provides **scalability** — store petabytes without worrying about single-disk limits
- Is **fault tolerant** — data survives hardware failures through replication
- Offers a **unified namespace** — users see one file system, not individual machines

### Comparing Distributed File Systems

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │                 DISTRIBUTED FILE SYSTEMS LANDSCAPE                   │
  │                                                                      │
  │    ON-PREMISES                          CLOUD-NATIVE                 │
  │    ═══════════                          ════════════                 │
  │                                                                      │
  │    ┌────────────┐      ┌────────────┐  ┌────────────┐  ┌──────────┐ │
  │    │   HDFS     │      │ Amazon S3  │  │ Azure ADLS │  │ Google   │ │
  │    │            │      │            │  │ Gen2       │  │ GCS      │ │
  │    │ Hadoop     │      │ AWS        │  │ Azure      │  │ GCP      │ │
  │    │ clusters   │      │            │  │            │  │          │ │
  │    └────────────┘      └────────────┘  └────────────┘  └──────────┘ │
  │                                                                      │
  │    Self-managed         Fully managed by cloud provider              │
  │    Hardware required    No hardware — pay per GB stored              │
  │    Fixed capacity       Virtually unlimited                          │
  └──────────────────────────────────────────────────────────────────────┘
```

| Feature | HDFS | Amazon S3 | Azure ADLS Gen2 | Google GCS |
|---------|------|-----------|-----------------|------------|
| **Type** | Distributed file system | Object storage | Object storage (with hierarchical namespace) | Object storage |
| **Deployment** | On-premises / self-managed | AWS managed service | Azure managed service | GCP managed service |
| **Capacity** | Limited by cluster hardware | Virtually unlimited | Virtually unlimited | Virtually unlimited |
| **Durability** | 3x replication (configurable) | 99.999999999% (11 9's) | 99.999999999% (11 9's) | 99.999999999% (11 9's) |
| **Cost** | High (hardware + ops team) | $0.023/GB/month (Standard) | $0.018/GB/month (Hot tier) | $0.020/GB/month (Standard) |
| **Scaling** | Add nodes manually | Automatic | Automatic | Automatic |
| **Performance** | High throughput (data locality) | High throughput (no data locality) | High throughput | High throughput |
| **Access protocol** | HDFS protocol (`hdfs://`) | S3 API (`s3://`) | ABFS (`abfss://`) | GCS API (`gs://`) |
| **Spark integration** | Native (built-in) | Via hadoop-aws connector | Via hadoop-azure connector | Via gcs-connector |
| **Databricks support** | Via DBFS mount | Native (default on AWS Databricks) | Native (default on Azure Databricks) | Native (default on GCP Databricks) |

#### HDFS (Hadoop Distributed File System)

```
  Best for: On-premises Hadoop clusters where data locality matters
  Path format: hdfs://namenode:8020/path/to/data

  ┌──────────────────────────────────────────┐
  │  HDFS Cluster                            │
  │                                          │
  │  NameNode ──────┬──── DataNode 1        │
  │  (metadata)     ├──── DataNode 2        │
  │                 ├──── DataNode 3        │
  │                 └──── DataNode N        │
  │                                          │
  │  Data locality: Spark tasks run on the   │
  │  same node where the data blocks are     │
  │  stored → minimal network transfer       │
  └──────────────────────────────────────────┘
```

**Pros**: Data locality (fast), mature ecosystem, good for sequential reads
**Cons**: Self-managed hardware, fixed capacity, expensive to scale, not cloud-native

#### Amazon S3 (Simple Storage Service)

```
  Best for: AWS-based data platforms, Databricks on AWS
  Path format: s3://bucket-name/path/to/data

  ┌──────────────────────────────────────────┐
  │  Amazon S3                               │
  │                                          │
  │  ┌──────────────┐                       │
  │  │   Bucket:     │                       │
  │  │  my-data-lake │                       │
  │  │               │                       │
  │  │  /raw/        │  ← landing zone      │
  │  │  /bronze/     │  ← raw ingested      │
  │  │  /silver/     │  ← cleaned/enriched  │
  │  │  /gold/       │  ← business-ready    │
  │  └──────────────┘                       │
  │                                          │
  │  Stored across 3+ Availability Zones    │
  │  11 nines of durability                  │
  │  Pay only for what you store             │
  └──────────────────────────────────────────┘
```

**Pros**: Virtually unlimited, extremely durable, cheap, no hardware to manage
**Cons**: Higher latency than local disk, no data locality, eventual consistency (mostly resolved)

#### Azure Data Lake Storage Gen2 (ADLS)

```
  Best for: Azure-based data platforms, Databricks on Azure
  Path format: abfss://container@account.dfs.core.windows.net/path/to/data

  ┌──────────────────────────────────────────┐
  │  Azure ADLS Gen2                         │
  │                                          │
  │  ┌──────────────┐                       │
  │  │ Storage Acct: │                       │
  │  │ mydatalake    │                       │
  │  │               │                       │
  │  │  Container:   │                       │
  │  │  analytics    │                       │
  │  │    /raw/      │                       │
  │  │    /curated/  │                       │
  │  │    /serving/  │                       │
  │  └──────────────┘                       │
  │                                          │
  │  Hierarchical namespace (true            │
  │  directories, not just key prefixes)     │
  │  Fine-grained ACLs (POSIX-like)         │
  └──────────────────────────────────────────┘
```

**Pros**: Hierarchical namespace (fast directory operations), fine-grained ACLs, Azure AD integration
**Cons**: Azure-only, more complex permission model

#### Google Cloud Storage (GCS)

```
  Best for: GCP-based data platforms, Databricks on GCP
  Path format: gs://bucket-name/path/to/data

  ┌──────────────────────────────────────────┐
  │  Google Cloud Storage                    │
  │                                          │
  │  ┌──────────────┐                       │
  │  │   Bucket:     │                       │
  │  │  my-data-lake │                       │
  │  │               │                       │
  │  │  Storage classes:                     │
  │  │  - Standard (frequently accessed)     │
  │  │  - Nearline (monthly access)          │
  │  │  - Coldline (quarterly access)        │
  │  │  - Archive (yearly access)            │
  │  └──────────────┘                       │
  │                                          │
  │  Strong consistency (since 2021)        │
  │  Autoclass for automatic tiering         │
  └──────────────────────────────────────────┘
```

**Pros**: Strong consistency, automatic storage class transitions, tight BigQuery integration
**Cons**: GCP-only, smaller ecosystem than S3

### How Spark Reads from Distributed Storage

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │              SPARK + CLOUD STORAGE = SEPARATION OF                   │
  │                    COMPUTE AND STORAGE                               │
  └─────────────────────────────────────────────────────────────────────┘

  TRADITIONAL (Hadoop):                 MODERN (Cloud + Spark):
  Compute and storage on               Compute and storage are
  the SAME machines                     SEPARATE and independent

  ┌────────────────────┐                ┌───────────────────┐
  │   Hadoop Node       │                │  Spark Cluster    │  ← Compute
  │                     │                │  (scales up/down) │
  │  CPU + RAM + DISK  │                └────────┬──────────┘
  │  (data is HERE)    │                         │  reads over network
  │                     │                         │
  │  Compute + Storage │                ┌────────▼──────────┐
  │  tightly coupled   │                │  S3 / ADLS / GCS  │  ← Storage
  └────────────────────┘                │  (always on,       │
                                        │   scales infinitely)│
  Cannot scale them                     └───────────────────┘
  independently!
                                        Scale compute and storage
                                        INDEPENDENTLY!
```

**Why separation matters:**
- **Cost**: Pay for compute only when processing; storage is cheap and always on
- **Elasticity**: Spin up a large cluster for a 2-hour job, then shut it down
- **Sharing**: Multiple Spark clusters can read the same data in S3/ADLS
- **No data migration**: Upgrade compute without moving data

---

## Real-World Architecture Example

### Netflix: From Raw Data to Recommendations

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  NETFLIX DATA ARCHITECTURE (simplified)              │
  │                                                                      │
  │  1 BILLION+ users  │  5+ PB of data  │  Millions of events/second  │
  └─────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │ User clicks  │     │ Video plays  │     │ Search logs  │
  │ "play movie" │     │ buffer events│     │ queries      │
  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Apache Kafka    │   ← Real-time event streaming
                    │  (event bus)     │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌──────────────────┐          ┌──────────────────┐
    │  Amazon S3       │          │  Spark Streaming  │   ← Real-time
    │  (raw data lake) │          │  (live dashboards)│      processing
    │                  │          └──────────────────┘
    │  /raw/clicks/    │
    │  /raw/plays/     │
    │  /raw/searches/  │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │  Apache Spark    │   ← Batch processing (ETL)
    │  (clean, enrich, │
    │   aggregate)     │
    └────────┬─────────┘
             │
    ┌────────┴──────────────────────────────────┐
    │                                            │
    ▼                                            ▼
  ┌──────────────────┐                ┌──────────────────┐
  │  S3 / Data       │                │  Spark MLlib     │
  │  Warehouse       │                │  (train models)  │
  │  (analytics)     │                │                  │
  │                  │                │  - Collaborative │
  │  Used by:        │                │    filtering     │
  │  - BI dashboards │                │  - Content-based │
  │  - SQL analysts  │                │  - Deep learning │
  │  - Executives    │                └────────┬─────────┘
  └──────────────────┘                         │
                                               ▼
                                     ┌──────────────────┐
                                     │  Model Serving   │
                                     │  (recommendations │
                                     │   to 1B users)   │
                                     └──────────────────┘
```

**Every concept we covered is used here:**

| Concept from this guide | Netflix equivalent |
|------------------------|-------------------|
| HDFS / Distributed storage | Amazon S3 (raw data lake) |
| MapReduce (batch processing) | Apache Spark (10-100x faster) |
| Hive (SQL analytics) | Spark SQL / Presto / Trino |
| YARN (resource management) | Kubernetes / cloud orchestrators |
| Streaming | Spark Structured Streaming + Kafka |
| ML/AI | Spark MLlib + custom deep learning |

---

## How This All Connects to Databricks

Databricks is a **managed Lakehouse platform** that combines the best of everything
we've discussed into a single, unified product:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                     DATABRICKS LAKEHOUSE PLATFORM                    │
  │                                                                      │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │  What you learned            Databricks equivalent            │  │
  │  │  ═══════════════════         ═══════════════════════════      │  │
  │  │                                                               │  │
  │  │  HDFS                   →    DBFS / S3 / ADLS / GCS          │  │
  │  │  (distributed storage)       (cloud object storage)           │  │
  │  │                                                               │  │
  │  │  MapReduce              →    Apache Spark                     │  │
  │  │  (batch processing)          (in-memory, 10-100x faster)     │  │
  │  │                                                               │  │
  │  │  Hive                   →    Spark SQL + Delta Lake           │  │
  │  │  (SQL analytics)             (ACID transactions, time travel) │  │
  │  │                                                               │  │
  │  │  YARN                   →    Databricks Cluster Manager       │  │
  │  │  (resource management)       (autoscaling, serverless)        │  │
  │  │                                                               │  │
  │  │  Oozie                  →    Databricks Workflows             │  │
  │  │  (job scheduling)            (orchestration + monitoring)     │  │
  │  │                                                               │  │
  │  │  Hadoop cluster setup   →    One-click cluster creation       │  │
  │  │  (weeks of work)            (30 seconds)                      │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                      │
  │  BONUS: Things Hadoop never had                                     │
  │  ───────────────────────────────                                    │
  │  - Delta Lake (ACID transactions on data lakes)                     │
  │  - Unity Catalog (governance + lineage + access control)            │
  │  - MLflow (ML experiment tracking + model registry)                 │
  │  - Photon (C++ vectorized query engine, even faster than Spark)    │
  │  - Serverless compute (no cluster management at all)                │
  └─────────────────────────────────────────────────────────────────────┘
```

### The Evolution in One Diagram

```
  2006                    2014                    2020+
  HADOOP ERA              SPARK ERA               LAKEHOUSE ERA
  ══════════              ═════════               ═════════════

  ┌────────────┐          ┌────────────┐          ┌────────────────────┐
  │ Storage:   │          │ Storage:   │          │ Storage:           │
  │ HDFS       │   ──▶    │ HDFS/S3/   │   ──▶    │ S3/ADLS/GCS       │
  │            │          │ ADLS       │          │ + Delta Lake       │
  ├────────────┤          ├────────────┤          │ (ACID + versions)  │
  │ Processing:│          │ Processing:│          ├────────────────────┤
  │ MapReduce  │          │ Spark      │          │ Processing:        │
  │ (slow,     │          │ (fast,     │          │ Spark + Photon     │
  │  disk I/O) │          │  in-memory)│          │ (fastest, C++)     │
  ├────────────┤          ├────────────┤          ├────────────────────┤
  │ SQL:       │          │ SQL:       │          │ SQL:               │
  │ Hive       │          │ Spark SQL  │          │ Spark SQL +        │
  │ (batch     │          │ (fast,     │          │ Serverless SQL     │
  │  only)     │          │  interactive│          │ Warehouse          │
  ├────────────┤          ├────────────┤          ├────────────────────┤
  │ Resources: │          │ Resources: │          │ Resources:         │
  │ YARN       │          │ YARN/K8s   │          │ Databricks         │
  │ (manual)   │          │ (better)   │          │ (autoscale,        │
  │            │          │            │          │  serverless)       │
  ├────────────┤          ├────────────┤          ├────────────────────┤
  │ ML:        │          │ ML:        │          │ ML:                │
  │ Mahout     │          │ MLlib      │          │ MLlib + MLflow +   │
  │ (limited)  │          │ (built-in) │          │ AutoML + GenAI     │
  └────────────┘          └────────────┘          └────────────────────┘

  Pain: Slow, complex,     Better: Fast, flexible,   Best: Unified, managed,
  Java-only, no streaming  multi-language, streaming  governed, AI-native
```

---

## Hands-On Walkthrough

Open the companion notebook `00-big-data-hadoop-fundamentals_notebook.py` in Databricks.
You will:

- Verify your Spark environment and see how it replaces the Hadoop stack
- Explore the SparkSession object and its connections to the concepts above
- Run a word count in Spark vs. simulated MapReduce to see the difference
- Read data from cloud storage (DBFS) and understand the distributed file system abstraction
- Inspect how Spark partitions data across the cluster

## Cloud Provider Notes

| Concept | AWS | Azure | GCP |
|---------|-----|-------|-----|
| **Object storage** | S3 (`s3://`) | ADLS Gen2 (`abfss://`) | GCS (`gs://`) |
| **Managed Spark** | EMR, Databricks | HDInsight, Databricks | Dataproc, Databricks |
| **Hive-compatible catalog** | AWS Glue Data Catalog | Azure Synapse / Unity Catalog | BigQuery Metastore / Unity Catalog |
| **Hadoop availability** | EMR (managed Hadoop) | HDInsight (managed Hadoop) | Dataproc (managed Hadoop) |
| **Default Databricks storage** | S3 (DBFS backed by S3) | ADLS Gen2 (DBFS backed by ADLS) | GCS (DBFS backed by GCS) |

## Certification Tip

The Databricks Data Engineer Associate exam expects you to:

- **Understand why Spark replaced MapReduce** — in-memory processing, rich API, streaming
  support. If asked "What is the primary advantage of Spark over MapReduce?", the answer
  is **in-memory processing** (avoids disk I/O between stages).
- **Know the role of cloud storage** — Databricks separates compute (Spark clusters) from
  storage (S3/ADLS/GCS). This enables independent scaling and cost optimization.
- **Recognize DBFS** — Databricks File System is an abstraction layer on top of cloud object
  storage. When you write to `dbfs:/`, you are actually writing to S3/ADLS/GCS.
- **Delta Lake vs. Hive** — Delta Lake provides ACID transactions, time travel, and schema
  enforcement that Hive lacked. This is a frequent exam topic.

## Key Takeaways

- **Big Data** is defined by the 5 V's: Volume, Velocity, Variety, Veracity, and Value
- **Traditional databases fail** at Big Data scale because vertical scaling has physical and cost limits
- **Horizontal scaling** (distributing across many machines) is the foundation of all Big Data systems
- **Hadoop** solved the storage problem (HDFS) and processing problem (MapReduce) but was **slow and complex**
- **HDFS** stores data in 128 MB blocks replicated 3x across DataNodes — fault tolerant but hardware-intensive
- **MapReduce** processes data through Map → Shuffle → Reduce, but every step hits disk — painfully slow for multi-step pipelines
- **Hive** brought SQL to Hadoop but was still limited by MapReduce's batch-only, slow execution
- **Spark** replaced MapReduce with **in-memory processing**, achieving 10-100x speedups while supporting batch, streaming, SQL, and ML
- **Cloud storage** (S3, ADLS, GCS) replaced HDFS for most modern workloads — infinitely scalable, durable, and cheap
- **Separation of compute and storage** is the modern architecture: Spark clusters scale independently from data in cloud storage
- **Databricks** unifies all of this: managed Spark + Delta Lake + cloud storage + governance in a single Lakehouse platform

## Next Steps

Continue to [01 - Python Essentials](01-python-essentials.md) to learn the Python
patterns that power PySpark, or jump to [02 - Spark Architecture](02-spark-architecture.md)
for a deep dive into how Spark's driver-executor model works internally.
