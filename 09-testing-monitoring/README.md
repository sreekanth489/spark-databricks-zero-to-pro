# Module 09 — Testing & Monitoring

> Build production-grade testing, data quality, monitoring, logging, and cost
> management practices for Spark and Databricks workloads.

---

## Why This Module Matters

Writing Spark code that works once is easy. Building pipelines that stay correct,
observable, and cost-efficient over months and years is what separates hobby
projects from production systems. This module teaches you the engineering
disciplines that make the difference: automated testing, data quality enforcement,
real-time monitoring, structured logging, and deliberate cost management.

Every concept here maps directly to what platform teams and data engineering
organizations expect from senior practitioners -- and what the Databricks
certification exams test.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| **Modules 00-06 completed** | You need working knowledge of PySpark, Delta Lake, Spark SQL, and Databricks workflows |
| **Databricks workspace (recommended)** | Monitoring features and DLT expectations require a full workspace |
| **Basic Python testing knowledge** | Familiarity with `assert` statements and function design helps in Topic 01 |

> **Community Edition users**: Every notebook is self-contained and generates its
> own sample data. Where Databricks-specific features are used (DLT, SQL Alerts),
> the notebook includes commented examples with simulated output so you can follow
> along.

---

## Table of Contents

| # | Topic | Guide | Notebook | Time | Level |
|---|-------|-------|----------|------|-------|
| 01 | Testing Spark Code | [Guide](01-testing-spark-code.md) | [Notebook](01-testing-spark-code_notebook.py) | 50 min | Intermediate |
| 02 | Data Quality Frameworks | [Guide](02-data-quality.md) | [Notebook](02-data-quality_notebook.py) | 50 min | Intermediate |
| 03 | Monitoring & Alerting | [Guide](03-monitoring-alerting.md) | [Notebook](03-monitoring-alerting_notebook.py) | 45 min | Intermediate |
| 04 | Logging Best Practices | [Guide](04-logging-best-practices.md) | [Notebook](04-logging-best-practices_notebook.py) | 45 min | Intermediate |
| 05 | Cost Management | [Guide](05-cost-management.md) | [Notebook](05-cost-management_notebook.py) | 40 min | Advanced |

**Total estimated time: ~3.5 hours**

---

## Learning Path

```
  Module 09 Learning Flow
  ========================

  01-Testing Spark Code
    |
    |  Learn to write testable transformations, use pytest fixtures
    |  for SparkSession, compare DataFrames, and test UDFs and
    |  edge cases (nulls, empty DataFrames, schema mismatches).
    |
    v
  02-Data Quality Frameworks
    |
    |  Build custom data quality checks across six dimensions.
    |  Understand DLT expectations (EXPECT / DROP / FAIL) and
    |  quarantine patterns for bad records.
    |
    v
  03-Monitoring & Alerting
    |
    |  Read the Spark UI like a pro. Monitor streaming queries.
    |  Build custom metric dashboards and configure alerting
    |  thresholds for pipeline health.
    |
    v
  04-Logging Best Practices
    |
    |  Set up structured logging on driver and executors.
    |  Build pipeline run trackers, dead letter tables, and
    |  error handling patterns that survive production.
    |
    v
  05-Cost Management
    |
    |  Understand DBU pricing, right-size clusters, leverage
    |  spot instances, optimize storage, and implement tagging
    |  for cost allocation by team and project.
```

---

## Key Concepts at a Glance

- **Testing Spark Code** -- Treat transformations as pure functions: input DataFrame
  in, output DataFrame out. Use `chispa` or manual comparison for DataFrame equality.
  Local `SparkSession` with `master("local[*]")` for unit tests; Databricks provides
  `spark` automatically in notebooks.

- **Data Quality** -- Six dimensions: completeness, uniqueness, timeliness, validity,
  accuracy, consistency. DLT expectations are the Databricks-native enforcement
  mechanism with three severity levels (warn, drop, fail).

- **Monitoring** -- The Spark UI is the number-one debugging tool. In streaming,
  watch trigger execution time, input rate vs processing rate, and state store size.

- **Logging** -- Driver logging is straightforward; executor logging requires
  understanding that each executor is a separate JVM. Structured JSON logging
  enables search and aggregation. Pipeline metadata tables (run tracking) are
  essential for SLA monitoring.

- **Cost Management** -- The biggest levers are (1) right-size clusters, (2) use job
  clusters instead of all-purpose, (3) use spot/preemptible instances, (4) optimize
  queries to reduce shuffle, (5) compact small files with OPTIMIZE and clean up with
  VACUUM.

---

## Important Notes

1. **Self-contained notebooks** -- Every notebook generates its own sample data and
   cleans up after itself. No external datasets are required.

2. **DLT expectations** are shown as syntax examples and comments because DLT
   pipelines require a specific Databricks runtime configuration. The data quality
   notebook implements equivalent logic using standard PySpark.

3. **Cost figures** are illustrative and based on publicly available Databricks
   pricing. Actual costs vary by cloud provider, region, commitment tier, and
   negotiated discounts.

4. **Certification relevance** -- Testing, data quality (especially DLT expectations),
   Spark UI interpretation, and cost optimization are all tested on the Databricks
   Certified Data Engineer Associate and Professional exams. Look for "Certification
   Tip" callouts in each guide.

---

## Next Steps

After completing this module, proceed to:
- **Module 10** -- Real-World Projects (apply everything in end-to-end scenarios)
- **Module 11** -- Certification Prep (targeted review and practice questions)
