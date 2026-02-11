# Spark Architecture

> Module 01 -- Topic 02 | Level: Beginner | Time: 60 min

## Learning Objectives

- Explain why Spark was created and how it improves on Hadoop MapReduce
- Describe the driver-executor model and the role of each component
- Differentiate between cluster managers (Standalone, YARN, Mesos, Kubernetes)
- Explain the relationship between SparkSession and SparkContext
- Trace a Spark job through the jobs-stages-tasks hierarchy
- Understand memory management zones in Spark executors
- Compare client mode vs. cluster deploy mode

## Conceptual Overview

### Hadoop MapReduce: Where Spark Came From

Before Spark, **Apache Hadoop** was the dominant framework for distributed data processing.
Understanding Hadoop helps explain *why* Spark was designed the way it is.

**Hadoop has two core components:**

1. **HDFS (Hadoop Distributed File System)** -- splits files into blocks and replicates
   them across a cluster for fault tolerance.
2. **MapReduce** -- a programming model that processes data in two phases: **Map**
   (transform each record) and **Reduce** (aggregate results).

```
  ┌──────────────────────────────────────────────────────┐
  │                   HADOOP CLUSTER                      │
  │                                                       │
  │  ┌────────────┐        ┌────────────────────────────┐ │
  │  │  NameNode   │        │       JobTracker           │ │
  │  │  (metadata) │        │  (schedules Map & Reduce)  │ │
  │  └─────┬──────┘        └────────────┬───────────────┘ │
  │        │                            │                  │
  │   ┌────▼────┐  ┌────────┐  ┌───────▼──────┐          │
  │   │DataNode │  │DataNode│  │ TaskTracker   │          │
  │   │ Block A │  │Block A'│  │ ┌──────────┐  │          │
  │   │ Block B │  │Block C │  │ │ Map Task │  │          │
  │   └────────┘  └────────┘  │ │ Reduce   │  │          │
  │                            │ └──────────┘  │          │
  │                            └───────────────┘          │
  └──────────────────────────────────────────────────────┘
```

A typical MapReduce job flows like this:

1. **Input splits** -- HDFS blocks are assigned to Map tasks
2. **Map phase** -- each mapper processes its split and writes intermediate key-value pairs
   **to local disk**
3. **Shuffle & Sort** -- the framework redistributes data by key across the network
4. **Reduce phase** -- reducers aggregate the shuffled data and write final output **back to
   HDFS (disk)**

#### Hadoop's Strengths

- **Fault tolerance** -- HDFS replicates every block 3x by default; if a node fails, data
  is not lost
- **Horizontal scalability** -- scales to thousands of nodes and petabytes of data
- **Cost-effective storage** -- runs on commodity hardware instead of expensive specialized
  servers
- **Battle-tested** -- proven at massive scale by Yahoo, Facebook, and LinkedIn for batch
  ETL workloads

#### Hadoop's Drawbacks (Why Spark Was Created)

- **Disk I/O between every stage** -- Map output is written to disk, then Reduce reads it
  back. Multi-step pipelines chain multiple MapReduce jobs, each hitting disk.
- **Only two primitives** -- everything must be expressed as Map and Reduce. Complex
  analytics (joins, windowing) require awkward chains of jobs.
- **No support for iterative algorithms** -- ML and graph algorithms re-read the same data
  many times. Each iteration is a separate MapReduce job reading from disk.
- **High latency** -- even simple queries take minutes due to job startup overhead and disk
  I/O. Not suitable for interactive analysis or real-time processing.
- **Verbose programming model** -- a simple word count requires ~50 lines of Java boilerplate
  with custom Mapper and Reducer classes.

#### Spark vs. Hadoop MapReduce

| Aspect | Hadoop MapReduce | Apache Spark |
|--------|-----------------|--------------|
| **Processing model** | Disk-based (read/write between stages) | In-memory (keeps data in RAM across stages) |
| **Speed** | Baseline | 10–100x faster for iterative workloads |
| **Programming model** | Map and Reduce only | Rich API: map, filter, join, groupBy, window, SQL |
| **Languages** | Primarily Java | Python, Scala, Java, R, SQL |
| **Streaming** | Limited (via separate tools) | Built-in Structured Streaming |
| **Interactive queries** | Not designed for it | Spark SQL with sub-second latency |
| **Fault tolerance** | Data replication (3x storage cost) | RDD lineage (recompute lost partitions) |
| **Storage** | HDFS (tightly coupled) | Any source: HDFS, S3, ADLS, GCS, Delta Lake |
| **Ecosystem** | Hive, Pig, HBase, Oozie | Spark SQL, MLlib, GraphX, Streaming -- all unified |

> **Key Insight**: Spark is a **compute engine**, not a storage system. It *replaced
> MapReduce* as the processing layer but still commonly reads from HDFS or cloud object
> storage (S3, ADLS, GCS). When someone says "we migrated from Hadoop to Spark," they
> usually mean they replaced MapReduce with Spark while keeping the underlying storage.

Now that you understand the limitations Spark was designed to overcome, let's look at
how Spark's architecture achieves this.

### The Big Picture

Apache Spark is a distributed computing engine. When you run a Spark application, your
code does not execute on a single machine. Instead, a **driver** process coordinates work
across many **executor** processes, which may run on different physical or virtual nodes.

```
                         ┌───────────────────────────┐
                         │       CLUSTER MANAGER      │
                         │  (YARN / K8s / Standalone) │
                         └─────────┬─────────────────┘
                                   │ allocates
                    ┌──────────────┼──────────────┐
                    │              │              │
               ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
               │ EXECUTOR │   │ EXECUTOR │   │ EXECUTOR │
               │  Node 1  │   │  Node 2  │   │  Node 3  │
               │ ┌──────┐ │   │ ┌──────┐ │   │ ┌──────┐ │
               │ │Task 1│ │   │ │Task 3│ │   │ │Task 5│ │
               │ │Task 2│ │   │ │Task 4│ │   │ │Task 6│ │
               │ └──────┘ │   │ └──────┘ │   │ └──────┘ │
               └────▲─────┘   └────▲─────┘   └────▲─────┘
                    │              │              │
                    └──────────────┼──────────────┘
                                   │ schedules tasks
                         ┌─────────▼─────────┐
                         │      DRIVER       │
                         │  (SparkSession)   │
                         │  ┌─────────────┐  │
                         │  │ DAG Sched.  │  │
                         │  │ Task Sched. │  │
                         │  │ Catalyst    │  │
                         │  └─────────────┘  │
                         └───────────────────┘
```

### The Driver

The driver is the process that runs your `main()` function. It is responsible for:

1. **Creating the SparkSession** -- the single entry point to all Spark functionality
2. **Analyzing and optimizing** your code via the Catalyst optimizer
3. **Building the DAG** (Directed Acyclic Graph) of transformations
4. **Splitting the DAG into stages** at shuffle boundaries
5. **Scheduling tasks** and sending them to executors
6. **Tracking task status** and handling retries on failure
7. **Collecting results** when actions like `collect()` or `count()` are called

The driver maintains metadata about your DataFrames, RDDs, and broadcast variables. It
does **not** typically process large volumes of data itself.

### Executors

Executors are JVM processes that run on worker nodes. Each executor:

1. **Receives tasks** from the driver
2. **Executes the tasks** against its local data partitions
3. **Stores data** in memory or on disk for caching
4. **Reports results** back to the driver

An executor is long-lived -- it stays running for the entire Spark application. You
configure the number of executors and their resources at application startup.

### Cluster Manager

The cluster manager is responsible for allocating resources (CPU, memory) to Spark
applications. Spark supports several cluster managers:

| Manager | Best For | Notes |
|---------|---------|-------|
| **Standalone** | Development, simple clusters | Ships with Spark; no external dependencies |
| **YARN** | Hadoop ecosystems (AWS EMR, on-prem) | Shares resources with other Hadoop workloads |
| **Mesos** | Multi-framework environments | Deprecated in Spark 3.x; legacy use only |
| **Kubernetes** | Cloud-native deployments | Growing adoption; pods as executors |
| **Databricks** | Managed Spark | Abstracts cluster management entirely |

In Databricks, the cluster manager is handled for you. You choose a cluster size and
Databricks provisions driver and executor nodes automatically.

### SparkSession vs SparkContext

```
  ┌─────────────────────────────────────────────────────┐
  │                   SparkSession                      │
  │                                                     │
  │  ┌───────────────┐  ┌──────────┐  ┌─────────────┐  │
  │  │ SparkContext   │  │ SQLContext│  │ HiveContext  │  │
  │  │ (RDD API)     │  │ (SQL API) │  │ (Hive API)  │  │
  │  └───────────────┘  └──────────┘  └─────────────┘  │
  │                                                     │
  │  .read  .sql()  .table()  .createDataFrame()        │
  │  .catalog  .conf  .udf  .streams                    │
  └─────────────────────────────────────────────────────┘
```

- **SparkContext** (Spark 1.x): The original entry point for RDD operations. Manages the
  connection to the cluster.
- **SQLContext / HiveContext** (Spark 1.x): Added SQL and Hive support on top of
  SparkContext.
- **SparkSession** (Spark 2.0+): The unified entry point. Encapsulates SparkContext,
  SQLContext, and HiveContext in a single object. **Always use SparkSession.**

In Databricks notebooks, `spark` is a pre-initialized SparkSession available in every
cell. You can access the underlying SparkContext via `spark.sparkContext` or `sc`.

### Jobs, Stages, and Tasks

Understanding this hierarchy is critical for debugging and performance tuning.

```
  Action (e.g., df.count())
       │
       ▼
  ┌─── JOB ──────────────────────────────────────────────┐
  │                                                       │
  │  ┌─── STAGE 0 (read) ───────┐  ┌─── STAGE 1 ──────┐ │
  │  │                           │  │   (after shuffle) │ │
  │  │  Task 0 ─ Partition 0    │  │  Task 0 ─ Part 0  │ │
  │  │  Task 1 ─ Partition 1    │  │  Task 1 ─ Part 1  │ │
  │  │  Task 2 ─ Partition 2    │  │                    │ │
  │  │                           │  │                    │ │
  │  └───────────────────────────┘  └────────────────────┘ │
  └────────────────────────────────────────────────────────┘
```

- **Job**: Created by each **action** (count, collect, save, show). One action = one job.
- **Stage**: A job is broken into stages at **shuffle boundaries**. Within a stage, all
  transformations can be pipelined (executed one after another on the same partition
  without moving data).
- **Task**: The smallest unit of work. One task processes one partition in one stage. If a
  stage has 200 partitions, it has 200 tasks.

### Memory Management

Each executor's memory is divided into regions:

```
  ┌───────────────────────────────────────────┐
  │              EXECUTOR MEMORY              │
  │                                           │
  │  ┌─────────────────────────────────────┐  │
  │  │     Unified Memory (60% default)    │  │
  │  │  ┌──────────┐  ┌────────────────┐   │  │
  │  │  │Execution │  │   Storage      │   │  │
  │  │  │ (shuffle,│  │   (cached      │   │  │
  │  │  │  sort,   │  │    RDDs /      │   │  │
  │  │  │  joins)  │  │    DataFrames) │   │  │
  │  │  └──────────┘  └────────────────┘   │  │
  │  └─────────────────────────────────────┘  │
  │  ┌─────────────────────────────────────┐  │
  │  │       User Memory (40% default)     │  │
  │  │  (data structures in your UDFs)     │  │
  │  └─────────────────────────────────────┘  │
  │  ┌─────────────────────────────────────┐  │
  │  │       Reserved (300 MB)             │  │
  │  │  (Spark internal objects)           │  │
  │  └─────────────────────────────────────┘  │
  └───────────────────────────────────────────┘
```

Spark uses a **unified memory manager** (since Spark 1.6) that allows execution and
storage to borrow from each other. If there is no cached data, execution can use the
full unified pool. If execution needs more space, it can evict cached partitions.

Key configuration:
- `spark.executor.memory` -- total executor heap (e.g., `4g`)
- `spark.memory.fraction` -- fraction of heap for unified memory (default `0.6`)
- `spark.memory.storageFraction` -- initial fraction of unified memory reserved for
  storage (default `0.5`), but execution can borrow from it

### Deploy Modes: Client vs. Cluster

| Aspect | Client Mode | Cluster Mode |
|--------|------------|--------------|
| Driver location | Runs on the machine that submitted the job | Runs on a worker node inside the cluster |
| Use case | Interactive development, notebooks | Production batch jobs |
| stdout/stderr | Visible on the submitting machine | Accessible via cluster manager logs |
| Network dependency | Submitting machine must stay connected | Submitting machine can disconnect |
| Databricks | Default for interactive clusters | Default for job clusters |

```
  CLIENT MODE                        CLUSTER MODE
  ┌──────────┐                       ┌──────────────────┐
  │  Your    │ ◄── driver here       │     Cluster      │
  │  Laptop  │                       │  ┌────────────┐  │
  └────┬─────┘                       │  │   DRIVER   │  │ ◄── driver here
       │                             │  └────────────┘  │
       ▼                             │  ┌────────────┐  │
  ┌──────────────────┐               │  │  Executor  │  │
  │     Cluster      │               │  └────────────┘  │
  │  ┌────────────┐  │               │  ┌────────────┐  │
  │  │  Executor  │  │               │  │  Executor  │  │
  │  └────────────┘  │               │  └────────────┘  │
  │  ┌────────────┐  │               └──────────────────┘
  │  │  Executor  │  │
  │  └────────────┘  │
  └──────────────────┘
```

### The Spark UI

The Spark UI (accessible on port 4040 by default, or through the Databricks cluster UI)
provides real-time insight into your running application:

| Tab | Shows |
|-----|-------|
| **Jobs** | All jobs triggered by actions, their status, and duration |
| **Stages** | Stage details, task distribution, shuffle read/write |
| **Storage** | Cached RDDs/DataFrames and memory usage |
| **Environment** | Spark configuration, JVM properties, classpath |
| **Executors** | Resource usage per executor (memory, cores, tasks) |
| **SQL** | Query plans and execution metrics for DataFrame/SQL ops |

## Hands-On Walkthrough

Open the companion notebook `02-spark-architecture_notebook.py` in Databricks. You will:

- Explore the SparkSession and SparkContext objects
- Inspect cluster configuration programmatically
- Trigger a job and observe the job/stage/task breakdown
- Read the Spark UI to understand execution flow

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|-------------------|----------------|
| Cluster manager | YARN (default) | Managed by Databricks | YARN (default) |
| Driver node | Master node | Driver node in workspace | Master node |
| Spark UI access | Port 4040 via SSH tunnel | Built into notebook UI | Port 4040 via SSH tunnel |
| Autoscaling | EMR Managed Scaling | Databricks Autoscale | Dataproc Autoscaler |

## Certification Tip

The Databricks certification exams expect you to:

- Know that one **action** triggers one **job**, and jobs are split into **stages** at
  shuffle boundaries
- Understand that the driver is the coordinator, not a data processor
- Recognize that `SparkSession` is the unified entry point since Spark 2.0
- Know the difference between client and cluster deploy modes

When a question asks "how many stages will this code produce?", count the number of
shuffle operations (groupBy, join, repartition, distinct) plus one for the initial read.

## Key Takeaways

- The **driver** plans and coordinates; **executors** do the data processing
- **SparkSession** is the single entry point for all Spark functionality
- Each **action** creates a **job**; each shuffle creates a **stage boundary**; each
  partition in a stage creates a **task**
- Executor memory is shared between execution (shuffles, sorts) and storage (cache)
- **Client mode** keeps the driver on your machine; **cluster mode** puts it in the cluster
- The **Spark UI** is your primary tool for understanding and debugging execution

## Next Steps

Continue to [03 - Distributed Computing](03-distributed-computing.md) to learn how Spark
distributes data and computation across the cluster.
