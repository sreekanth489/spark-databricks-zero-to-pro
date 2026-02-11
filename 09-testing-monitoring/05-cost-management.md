# Cost Management
> Module 09 -- Topic 05 | Level: Advanced | Time: 40 min

## Learning Objectives

By the end of this topic you will be able to:
1. Understand Databricks pricing model (DBU types and workload tiers)
2. Right-size clusters using data-driven sizing strategies
3. Choose between job clusters and all-purpose clusters based on cost analysis
4. Configure autoscaling with appropriate min/max workers
5. Leverage spot instances and reserved capacity for cost savings
6. Optimize storage costs with VACUUM, OPTIMIZE, and partition strategies
7. Apply query optimization techniques that directly reduce compute cost
8. Implement tagging for cost allocation by team, project, and environment

---

## Conceptual Overview

### Databricks Pricing Model

Databricks charges based on **Databricks Units (DBUs)**, which represent a unit
of processing capability per hour. The DBU rate varies by workload type:

```
  Workload Type           Relative Cost     Best For
  ====================    ==============    ============================
  Jobs Compute            $                 Scheduled ETL, batch pipelines
  All-Purpose Compute     $$$               Interactive development, ad hoc
  SQL Compute             $$                SQL analytics, BI dashboards
  DLT Compute             $$                Delta Live Tables pipelines
  Serverless Compute      $$$$              Auto-provisioned, zero-management
  Model Serving           $$ (per token)    ML inference endpoints
```

**Key insight**: All-Purpose Compute costs roughly 2-3x more per DBU than Jobs
Compute. Moving production workloads from all-purpose to job clusters is often
the single biggest cost optimization.

### Total Cost = Cloud Infra + DBU

```
  +-----------------------------+
  |    Total Hourly Cost        |
  +-----------------------------+
  |                             |
  |  Cloud VM Cost (AWS/Azure)  |  <-- EC2, Azure VMs, GCE instances
  |  + Databricks DBU Cost      |  <-- DBU * rate per workload type
  |  + Storage Cost             |  <-- S3, ADLS, GCS
  |  + Data Transfer Cost       |  <-- Cross-region, internet egress
  |                             |
  +-----------------------------+
```

### The Five Biggest Cost Levers

```
  Impact     Lever                        Typical Savings
  ========   =========================    ================
  HIGH       1. Job clusters, not         50-70% on compute
             all-purpose for production

  HIGH       2. Spot/preemptible          60-90% on VM cost
             instances

  MEDIUM     3. Right-size clusters       20-40% on compute
             (fewer, larger nodes)

  MEDIUM     4. Optimize queries          20-50% on compute
             (reduce shuffle, pushdown)

  MEDIUM     5. Storage optimization      10-30% on storage
             (VACUUM, OPTIMIZE, partition)
```

---

## Cluster Sizing Strategies

### Right-Sizing Framework

```
  Step 1: Profile your workload
  +-----------------------------------+
  | What is the data volume?          |
  | What is the transformation type?  |
  | Narrow (map) or wide (shuffle)?   |
  | Memory-intensive or CPU-intensive?|
  +-----------------------------------+
          |
  Step 2: Start small, measure, adjust
  +-----------------------------------+
  | Begin with 2-4 workers            |
  | Monitor Spark UI for:             |
  |   - Spill (need more memory)      |
  |   - CPU wait (need more cores)    |
  |   - GC pauses (need more memory)  |
  |   - Task time variance (skew)     |
  +-----------------------------------+
          |
  Step 3: Scale by bottleneck
  +-----------------------------------+
  | Memory bottleneck: bigger VMs     |
  | CPU bottleneck: more VMs          |
  | Shuffle bottleneck: optimize query|
  | I/O bottleneck: faster disks/SSD  |
  +-----------------------------------+
```

### Instance Type Selection

| Workload Pattern | Recommended Instance Family | Why |
|------------------|-----------------------------|-----|
| General ETL | m5/m6i (AWS), Standard_D (Azure) | Balanced CPU/memory |
| Memory-intensive (large joins, caching) | r5/r6i (AWS), Standard_E (Azure) | High memory-to-CPU ratio |
| CPU-intensive (ML training, complex UDFs) | c5/c6i (AWS), Standard_F (Azure) | High CPU-to-memory ratio |
| Storage-intensive (large shuffles) | i3/d3 (AWS), Standard_L (Azure) | Fast local NVMe storage |

### Autoscaling Configuration

```
  Autoscaling Best Practices
  ==========================

  Min Workers = Expected steady-state demand
                (the load you always have)

  Max Workers = Peak capacity
                (handle spikes without queuing)

  Target Utilization = 70-80%
                       (headroom for burst)

  Example for a pipeline that processes 100 GB daily:
    Steady state: 4 workers handle it in 30 min
    Peak (month-end): 400 GB, needs 12 workers
    --> Set min=4, max=12
```

**Autoscaling pitfalls**:
- Setting min=0 causes cold start delays (cluster takes 2-5 min to spin up)
- Setting max too high risks runaway costs from data explosions
- Autoscaling reacts to task backlog, not data volume -- a single large partition
  will not trigger scale-out

---

## Job Clusters vs All-Purpose Clusters

```
  Feature             Job Cluster         All-Purpose Cluster
  ================    ================    ====================
  Lifecycle           Created per job,    Always running until
                      terminated after    manually stopped

  DBU Rate            $0.07-0.15/DBU      $0.15-0.40/DBU
                      (Jobs Compute)      (All-Purpose Compute)

  Best For            Scheduled ETL,      Interactive dev,
                      production jobs     exploration, debugging

  Startup Time        2-5 minutes         Already running

  Cost Risk           Low (auto-stop)     HIGH (forgotten clusters)

  Spot Instance       Fully supported     Supported but risky
  Support                                 (session interrupted)
```

**Rule of thumb**: If a cluster runs a scheduled job, it should be a job cluster.
If a human is interactively working on it, it is all-purpose. Never run production
pipelines on all-purpose clusters.

---

## Spot Instances

Spot instances (AWS) / Spot VMs (Azure) / Preemptible VMs (GCP) offer 60-90%
cost savings but can be reclaimed by the cloud provider at any time.

### Configuration Strategy

```
  Recommended Spot Configuration
  ================================

  Driver node:    ALWAYS on-demand
                  (losing the driver kills the job)

  Worker nodes:   Spot instances with on-demand fallback
                  (if spot is unavailable, use on-demand)

  Ratio:          80-90% spot workers, 10-20% on-demand workers
                  (ensures baseline availability)
```

### When NOT to Use Spot

- **Long-running streaming jobs** -- Spot termination restarts the entire query
- **Jobs with no checkpointing** -- Lost work must be recomputed from scratch
- **Time-critical SLA workloads** -- Spot delays may violate SLAs

---

## Storage Cost Optimization

### VACUUM: Remove Old Files

Delta tables keep old versions for time travel. VACUUM removes files older than
the retention period:

```sql
-- Remove files older than 7 days (default retention)
VACUUM my_catalog.my_schema.my_table;

-- Remove files older than 24 hours (minimum)
VACUUM my_catalog.my_schema.my_table RETAIN 24 HOURS;
```

**Impact**: A table with 1 TB of current data and 30 days of history may have
3-5 TB of actual storage. VACUUM with 7-day retention reduces this significantly.

### OPTIMIZE: Compact Small Files

Small files cause excessive overhead in file listing, metadata management, and
task scheduling:

```sql
-- Compact small files in the entire table
OPTIMIZE my_catalog.my_schema.my_table;

-- Compact small files in specific partitions
OPTIMIZE my_catalog.my_schema.my_table
WHERE date >= '2024-01-01';

-- Z-ORDER for query-aligned file layout
OPTIMIZE my_catalog.my_schema.my_table
ZORDER BY (customer_id, order_date);
```

### Partition Strategy Impact on Cost

| Strategy | Storage Cost | Query Cost | Best For |
|----------|-------------|------------|----------|
| No partitioning | Low (fewer files) | Higher (full scans) | Small tables (<1 GB) |
| Over-partitioning | High (many small files) | Variable | Avoid this |
| Right partitioning | Moderate | Lower (partition pruning) | Large tables, filtered by date |

**Rule of thumb**: Partition when each partition is at least 1 GB. Common
partition key: date or a low-cardinality business dimension.

---

## Query Optimization for Cost

Every optimization that reduces data scanned or shuffled also reduces DBU
consumption:

### Predicate Pushdown

```python
# EXPENSIVE: Reads all data, then filters
df = spark.read.table("orders")
result = df.filter(col("status") == "active")

# CHEAPER: Filter is pushed down to file scan
# (Only reads files that could contain "active" rows)
# Ensure the table is partitioned by status or Z-ORDERed by status
```

### Column Pruning

```python
# EXPENSIVE: Reads all 50 columns
df = spark.read.table("wide_table")
result = df.filter(col("status") == "active")

# CHEAPER: Only reads 3 columns from storage
result = spark.read.table("wide_table").select("id", "name", "status")
result = result.filter(col("status") == "active")
```

### Broadcast Joins for Small Tables

```python
from pyspark.sql.functions import broadcast

# EXPENSIVE: Shuffle both sides of the join (network transfer)
result = large_df.join(small_df, "key")

# CHEAPER: Broadcast the small table to all executors (no shuffle)
result = large_df.join(broadcast(small_df), "key")
```

---

## Tagging for Cost Allocation

Tags enable cost tracking by team, project, environment, and cost center:

### Cluster Tags

```json
{
  "team": "data-engineering",
  "project": "customer-360",
  "environment": "production",
  "cost_center": "CC-1234",
  "owner": "jane.smith@company.com"
}
```

### Job Tags

```python
# In a Databricks job configuration
{
  "name": "daily-orders-etl",
  "tags": {
    "team": "data-engineering",
    "project": "orders-pipeline",
    "environment": "production"
  }
}
```

### Cost Analysis with Tags

```sql
-- Query Databricks account console billing data
SELECT
  tag_team,
  tag_project,
  SUM(dbu_consumed) as total_dbus,
  SUM(dbu_consumed * dbu_rate) as estimated_cost
FROM billing.usage
WHERE usage_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY tag_team, tag_project
ORDER BY estimated_cost DESC
```

---

## Photon and Serverless: Cost Considerations

### Photon

Photon is a C++ native vectorized execution engine. It uses more DBUs per hour
but finishes work faster:

```
  Without Photon:    10 workers x 2 hours x $0.15/DBU = $3.00
  With Photon:        6 workers x 1 hour  x $0.25/DBU = $1.50  (50% savings!)

  Photon is cost-effective when:
  - Queries are scan-heavy (reading large Delta tables)
  - Workloads involve heavy aggregations and joins
  - Your bottleneck is CPU, not I/O or network
```

### Serverless

Serverless eliminates cluster management but at a premium price:

```
  Serverless is cost-effective when:
  - Workloads are bursty (run for 5 min every hour)
  - Cluster startup time is a significant fraction of total time
  - Team lacks expertise to right-size clusters
  - You value developer productivity over raw cost efficiency
```

---

## Hands-On Walkthrough

Open `05-cost-management_notebook.py` to practice:
- Calculating DBU consumption estimates for different configurations
- Demonstrating cost-efficient coding patterns (narrow vs wide transformations)
- Analyzing storage costs (file sizes, partition strategy impact)
- Building a cost estimation utility function
- Comparing broadcast join vs shuffle join costs

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| DBU pricing page | databricks.com/pricing | databricks.com/pricing | databricks.com/pricing |
| Spot instances | EC2 Spot (60-90% off) | Azure Spot VMs (60-90% off) | Preemptible VMs (60-80% off) |
| Reserved capacity | Databricks Commit | Databricks Commit | Databricks Commit |
| Cost dashboard | Account Console | Account Console | Account Console |
| Storage pricing | S3 (~$0.023/GB/month) | ADLS (~$0.02/GB/month) | GCS (~$0.02/GB/month) |
| Tagging | Cluster & job tags | Cluster & job tags | Cluster & job tags |
| Budget alerts | AWS Budgets + SNS | Azure Cost Mgmt + Action Groups | GCP Budget Alerts |

---

## Certification Tip

> **Databricks Certified Data Engineer Associate**: Cost optimization questions
> focus on practical decisions:
> - **Job clusters vs all-purpose clusters**: Job clusters are cheaper and auto-terminate
> - **Autoscaling**: Set min workers to steady-state, max to peak capacity
> - **VACUUM and OPTIMIZE**: VACUUM removes old files; OPTIMIZE compacts small files
> - **Spot instances**: Use for workers, never for driver; good for batch, risky for streaming
> - **Photon**: Uses more DBUs/hour but may reduce total cost through faster execution
>
> You will NOT be asked specific pricing numbers, but you must understand the
> relative cost of different workload types and cluster configurations.

---

## Key Takeaways

1. **All-Purpose Compute is 2-3x more expensive** than Jobs Compute. Move production
   workloads to job clusters for the single biggest cost reduction.
2. **Spot instances save 60-90%** on worker costs. Use on-demand for the driver,
   spot for workers, with on-demand fallback.
3. **Right-size by profiling**: Start small, monitor Spark UI for bottlenecks
   (spill, CPU wait, GC), then scale the specific resource that is constrained.
4. **Autoscaling**: Set min workers to expected steady-state load, max workers to
   peak capacity. Never set min=0 for latency-sensitive workloads.
5. **Storage costs compound**: VACUUM removes old Delta versions, OPTIMIZE compacts
   small files. Both reduce storage costs and improve query performance.
6. **Query optimization is cost optimization**: Predicate pushdown, column pruning,
   and broadcast joins all reduce the compute work Spark must do.
7. **Tag everything**: Clusters, jobs, and pipelines should be tagged with team,
   project, and environment for cost allocation and accountability.
8. **Photon uses more DBUs per hour but finishes faster** -- evaluate net cost, not
   just the DBU rate.

---

## Next Steps

- Apply cost optimization principles to your pipelines from Modules 04-07.
- Review Module 05 (Performance Optimization) for query-level optimizations
  that directly reduce compute cost.
- Proceed to **Module 10** to build end-to-end projects that incorporate all
  the testing, monitoring, logging, and cost practices from this module.
