# Cluster Management
> Module 00 — Topic 02 | Level: Beginner | Time: 35 min

## Learning Objectives
- Distinguish between all-purpose clusters and job clusters
- Configure a cluster with appropriate runtime, node types, and autoscaling
- Understand cluster policies, init scripts, and cluster pools
- Apply cost management best practices for development and production
- Navigate the cluster lifecycle (create, start, restart, terminate)

## Conceptual Overview

### What Is a Cluster?

A Databricks cluster is a set of cloud VMs (virtual machines) that run the
Databricks Runtime (Apache Spark + Databricks enhancements). When you execute
code in a notebook, the cluster is the compute engine that does the work.

```
  CLUSTER ARCHITECTURE
  +========================================================+
  |                    DRIVER NODE                          |
  |  +--------------------------------------------------+  |
  |  |  SparkContext / SparkSession                      |  |
  |  |  Coordinates tasks, collects results              |  |
  |  |  Runs notebook code (Python/Scala/R interpreter)  |  |
  |  +--------------------------------------------------+  |
  +========================|===============================+
                           |  Task distribution
          +----------------+------------------+
          |                |                  |
  +-------v------+  +-----v--------+  +------v-------+
  | WORKER NODE 1|  | WORKER NODE 2|  | WORKER NODE 3|
  | +----------+ |  | +----------+ |  | +----------+ |
  | | Executor | |  | | Executor | |  | | Executor | |
  | | - Tasks  | |  | | - Tasks  | |  | | - Tasks  | |
  | | - Cache  | |  | | - Cache  | |  | | - Cache  | |
  | +----------+ |  | +----------+ |  | +----------+ |
  +--------------+  +--------------+  +--------------+
```

### Cluster Types

| Type | Created Via | Lifetime | Use Case | Cost |
|------|-------------|----------|----------|------|
| **All-Purpose** | Workspace UI / API | Persistent until terminated | Interactive development, ad-hoc analysis | Higher DBU rate |
| **Job Cluster** | Workflow/Job run | Ephemeral (created per run, terminated after) | Scheduled production workloads | Lower DBU rate |

**Best practice**: Use all-purpose clusters for development and exploration. Use
job clusters for production workflows — they cost less per DBU and enforce clean
environments for each run.

### Single-Node vs. Multi-Node

```
  SINGLE-NODE CLUSTER              MULTI-NODE CLUSTER
  +------------------+             +------------------+
  |  Driver + Worker |             |     Driver       |
  |  (same VM)       |             +--------+---------+
  |  No distributed  |                      |
  |  execution        |             +--------+---------+
  +------------------+             |   Worker 1       |
                                   +--------+---------+
  Good for:                                 |
  - Small data (< 20 GB)          +--------+---------+
  - Library testing                |   Worker 2       |
  - Single-machine ML             +------------------+

                                   Good for:
                                   - Large data (TB+)
                                   - Distributed Spark jobs
                                   - Production pipelines
```

Single-node clusters run everything on one machine. They are cheaper and faster
to start but cannot distribute work. Use them for development with small data
or for single-node ML libraries (scikit-learn, XGBoost on one node).

### Cluster Configuration Deep Dive

When creating a cluster, you configure:

#### 1. Databricks Runtime Version
Choose the runtime that matches your workload:
- **Standard** — general data engineering and analytics
- **ML** — adds PyTorch, TensorFlow, scikit-learn, MLflow
- **Photon** — adds the Photon C++ vectorized query engine for faster SQL
- **GPU** — adds CUDA drivers for deep learning

Always prefer the latest **LTS** version for stability.

#### 2. Node Type (Instance Type)
The VM size determines CPU cores, memory, and optional GPU:

| Category | Example (AWS) | Example (Azure) | Use Case |
|----------|---------------|-----------------|----------|
| General purpose | m5.xlarge | Standard_DS3_v2 | Balanced workloads |
| Memory optimized | r5.xlarge | Standard_E4s_v3 | Large shuffles, caching |
| Compute optimized | c5.xlarge | Standard_F4s_v2 | CPU-heavy transforms |
| Storage optimized | i3.xlarge | Standard_L4s | Heavy spill to disk |
| GPU | p3.2xlarge | Standard_NC6s_v3 | Deep learning |

**Best practice**: Start with general purpose nodes. Switch to memory-optimized
if you see spill-to-disk in the Spark UI.

#### 3. Autoscaling
Autoscaling automatically adds or removes worker nodes based on load:

```
  Min workers: 2     Max workers: 8

  Load increases -->  Databricks adds workers (up to 8)
  Load decreases -->  Databricks removes workers (down to 2)

  Timeline:
  |  2  |  2  |  4  |  6  |  8  |  8  |  5  |  3  |  2  |
  ^idle  ^low   ^med   ^high  ^peak       ^declining  ^idle
```

- **Enable for interactive clusters** — saves cost during idle periods
- **Disable for predictable batch jobs** — fixed size avoids scaling latency
- Set the minimum workers to your baseline and the maximum to your peak need

#### 4. Auto-Termination
Clusters can automatically terminate after a period of inactivity:
- Default is **120 minutes** (2 hours)
- Set to **30-60 minutes** for development clusters to reduce waste
- Job clusters always terminate when the job finishes

#### 5. Spot Instances / Preemptible VMs
Use cheaper spot instances for worker nodes to reduce cost:

| Cloud | Name | Savings | Risk |
|-------|------|---------|------|
| AWS | Spot Instances | Up to 90% | Can be reclaimed with 2 min notice |
| Azure | Spot VMs | Up to 90% | Can be evicted at any time |
| GCP | Preemptible VMs | Up to 80% | 24-hour max lifetime, can be preempted |

**Best practice**: Use spot instances for workers but keep the driver on
on-demand to avoid losing your notebook state.

### Cluster Policies

Cluster policies are administrator-defined templates that restrict what users
can configure:

```
  ADMIN defines policy:
  +---------------------------------------+
  |  Policy: "Data Engineering Standard"  |
  |  - Runtime: 15.4 LTS (fixed)         |
  |  - Max workers: 10                    |
  |  - Node type: m5.xlarge (fixed)       |
  |  - Auto-terminate: 60 min (fixed)     |
  |  - Spot: enabled for workers          |
  +---------------------------------------+
          |
          v
  USER creates cluster from policy:
  +---------------------------------------+
  |  Can only change: cluster name,       |
  |  number of workers (up to 10)         |
  |  Everything else is locked            |
  +---------------------------------------+
```

Benefits:
- Prevent runaway costs (users cannot select expensive instance types)
- Enforce organizational standards (runtime version, tagging)
- Simplify the cluster creation experience for end users

### Init Scripts

Init scripts run shell commands on each node when a cluster starts. They are
used to install OS-level packages or configure the environment:

```
  Cluster starts
       |
       v
  [Init scripts run on EVERY node]
       |
       +---> Install system packages (apt-get, yum)
       +---> Set environment variables
       +---> Configure logging agents
       +---> Mount network drives
       |
       v
  [Databricks Runtime loads]
       |
       v
  [Cluster is ready]
```

Types:
- **Cluster-scoped** — attached to a specific cluster
- **Global** — run on every cluster in the workspace (admin only)

Store init scripts in a workspace location, DBFS, or a Unity Catalog Volume.

### Cluster Pools

Cluster pools keep a set of idle VMs pre-allocated so that clusters start faster:

```
  WITHOUT POOL                      WITH POOL
  Request --> Provision VMs         Request --> Grab idle VMs from pool
  (3-7 minutes)                     (30-90 seconds)
```

Pool configuration:
- **Min idle instances** — VMs kept warm (you pay for these even when idle)
- **Max capacity** — upper limit of VMs in the pool
- **Instance type** — all VMs in a pool share the same type

**Best practice**: Use pools for all-purpose clusters in teams where multiple
people start clusters throughout the day.

## Hands-On Walkthrough

Import the companion notebook `02-cluster-management_notebook.py` into your
workspace. The notebook will guide you through:

1. Inspecting cluster configuration via `spark.conf`
2. Checking available CPU cores and memory
3. Understanding cluster mode (single-node vs. standard)
4. Examining executor and driver resources
5. Reviewing Spark environment variables

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Instance naming | EC2 types (m5.xlarge) | VM sizes (Standard_DS3_v2) | Machine types (n1-standard-4) |
| Spot/preemptible | Spot Instances | Spot VMs | Preemptible VMs |
| Default storage | EBS (gp3) | Managed Disks (Premium SSD) | Persistent Disk (SSD) |
| GPU availability | p3, p4, g5 families | NC, ND families | A2, T4 families |
| Cluster pool support | Yes | Yes | Yes |
| Photon support | Yes | Yes | Yes |

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- Knowing the difference between all-purpose and job clusters
- Understanding when to use autoscaling
- Awareness of cluster access modes (single user, shared, no isolation shared)

The **Professional** exam goes deeper:
- Cluster pool configuration for minimizing startup time
- Init script types and execution order
- Spot instance strategies and fallback to on-demand

Remember: **Job clusters = production, lower cost. All-purpose clusters =
development, higher cost.** This is a frequently tested concept.

## Key Takeaways

- All-purpose clusters are for interactive development; job clusters are for production workflows
- Always choose the latest LTS runtime unless you need a specific newer feature
- Configure autoscaling with sensible min/max to balance cost and performance
- Set auto-termination to 30-60 minutes for development clusters
- Use spot instances for worker nodes to reduce cost by up to 90%
- Cluster policies enforce guardrails so teams cannot overspend
- Cluster pools trade idle cost for faster startup times
- The driver node should always be on-demand to protect your session state

## Next Steps

Proceed to [03 — Notebook Fundamentals](03-notebook-fundamentals.md) to learn
how to write and run multi-language notebooks effectively.
