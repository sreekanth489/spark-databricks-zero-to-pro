# Module 10 -- Real-World Projects

> Capstone projects that integrate concepts from Modules 00-09 into complete,
> production-style data engineering pipelines.

---

## Why This Module Matters

Individual skills mean little if you cannot combine them into end-to-end
solutions. These capstone projects force you to make the same design decisions
that arise in real data engineering work: choosing the right architecture,
handling messy data, balancing performance against cost, and building pipelines
that are auditable and maintainable.

Each project is a self-contained Databricks notebook that generates its own
sample data, implements a complete pipeline, and cleans up after itself. There
are no external dependencies -- just import the notebook and run it.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| **Modules 00-04 completed** | Core PySpark, Delta Lake, transformations, and data modeling are used in every project |
| **Module 05 (Performance)** | Projects use OPTIMIZE, VACUUM, caching, and partitioning |
| **Module 07 (Streaming)** | Project 02 uses Structured Streaming, watermarks, and windowed aggregations |
| **Module 03 (Delta Lake)** | All projects use Delta Lake MERGE, time travel, and Change Data Feed |
| **Module 09 (Testing/Monitoring)** | Data quality checks are embedded in every pipeline |
| **Databricks workspace** | Community Edition works for all projects (streaming project uses batch simulation as fallback) |

---

## Projects

| # | Project | Guide | Notebook | Time | Difficulty | Key Concepts |
|---|---------|-------|----------|------|------------|--------------|
| 01 | E-Commerce Data Pipeline | [Guide](01-ecommerce-pipeline.md) | [Notebook](01-ecommerce-pipeline_notebook.py) | 3-4 hrs | Intermediate | Medallion architecture, RFM analysis, window functions, data quality |
| 02 | IoT Streaming Pipeline | [Guide](02-iot-streaming-pipeline.md) | [Notebook](02-iot-streaming-pipeline_notebook.py) | 2-3 hrs | Advanced | Structured Streaming, windowed aggregations, anomaly detection, device health |
| 03 | CDC Pipeline | [Guide](03-cdc-pipeline.md) | [Notebook](03-cdc-pipeline_notebook.py) | 3-4 hrs | Advanced | MERGE, SCD Type 1 and 2, Change Data Feed, out-of-order event handling |

**Total estimated time: 8-11 hours**

---

## Concept Coverage Map

Each project deliberately exercises concepts from prior modules. Use this map to
trace which skills are reinforced in each project.

```
Module                          Project 01    Project 02    Project 03
                                E-Commerce    IoT Stream    CDC Pipeline
=======================================================================
00 Setup & Basics                  x              x             x
01 Python & Spark Foundations      x              x             x
02 Data Ingestion                  x              x             x
03 Delta Lake & Lakehouse          x              x             x
04 Transformations & Modeling      x              x             x
05 Performance Optimization        x              .             x
06 Orchestration & CI/CD           .              .             .
07 Streaming & Real-Time           .              x             .
08 Governance & Security           .              .             .
09 Testing & Monitoring            x              x             x
```

`x` = primary use, `.` = incidental/optional use

---

## How to Use These Projects

### Recommended approach

1. **Read the project guide first** -- Understand the architecture, requirements,
   and design decisions before looking at the notebook.
2. **Try building it yourself** -- Use the guide as a specification and attempt your
   own implementation. This is the best way to learn.
3. **Compare with the reference notebook** -- After your attempt, study the provided
   notebook. Focus on differences in approach, not on matching the code exactly.
4. **Extend the project** -- Each guide includes extension ideas. Pick one and
   implement it to push your skills further.

### Quick-start approach

1. Import the notebook into your Databricks workspace (Workspace > Import > File).
2. Attach to a cluster running Databricks Runtime 13.3 LTS or later.
3. Run all cells from top to bottom.
4. Read the markdown commentary as you go.

---

## Architecture Patterns

All three projects follow the **Medallion Architecture** pattern:

```
  +-----------+      +------------+      +-----------+
  |  BRONZE   | ---> |   SILVER   | ---> |   GOLD    |
  |  (Raw)    |      |  (Cleaned) |      | (Business)|
  +-----------+      +------------+      +-----------+
   Append-only        Deduplicated        Aggregated
   Full fidelity      Type-enforced       KPI-ready
   + metadata         Quality-checked     BI-optimized
```

Beyond the shared pattern, each project introduces domain-specific techniques:

- **Project 01**: RFM customer segmentation, customer lifetime value, product
  performance analytics
- **Project 02**: Real-time anomaly detection, device health scoring, factory
  floor monitoring
- **Project 03**: Change Data Capture, SCD Type 1 and Type 2, out-of-order event
  reconciliation

---

## Important Notes

1. **Self-contained notebooks** -- Every notebook generates its own sample data
   inline. No external files, volumes, or DBFS paths are required.

2. **Cleanup included** -- Each notebook drops all tables and databases it creates
   in a final cleanup cell. Run the full notebook to leave your workspace clean.

3. **Deterministic data** -- All data generators use `random.seed(42)` for
   reproducible output. Running the notebook twice produces the same results.

4. **Community Edition compatible** -- All projects work on Databricks Community
   Edition. The IoT streaming project includes a batch simulation fallback for
   environments where long-running streams are impractical.

5. **Certification relevance** -- These projects cover material tested on both the
   Databricks Certified Data Engineer Associate and Professional exams. Medallion
   architecture, Delta Lake MERGE, streaming fundamentals, and data quality are
   high-frequency exam topics.

---

## Next Steps

After completing these projects, continue to:
- **Module 11** -- Certification Prep (targeted review and practice questions)
- **Module 20** -- ML & AI Foundations (apply your data engineering skills to
  machine learning pipelines)
