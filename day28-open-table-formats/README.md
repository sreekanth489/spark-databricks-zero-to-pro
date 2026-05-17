# Day 28: Open Table Formats

Delta Lake vs Apache Iceberg vs Apache Hudi — architecture, use cases, and interoperability.

## Topics Covered

- The world before open table formats: raw Parquet + Hive Metastore pain
- Why open table formats emerged in 2017–2019 (GDPR, streaming, cloud storage maturity)
- Delta Lake: `_delta_log/` transaction log, ACID, time travel, MERGE, CDF, UniForm
- Apache Iceberg: metadata tree, hidden partitioning, partition evolution, v2 delete files, branches
- Apache Hudi: Copy-on-Write vs Merge-on-Read, indexed upserts, incremental queries
- Head-to-head feature comparison
- Decision framework: when to use which format
- Delta UniForm: one Delta table readable as Iceberg by Snowflake/Trino/Athena

## Guide

[28-open-table-formats.md](28-open-table-formats.md) — 120 min read

## Notebook

[28-open-table-formats_notebook.py](28-open-table-formats_notebook.py) — hands-on lab

### What the notebook covers
1. Raw Parquet pain: demonstrate ACID violations and missing delete capability
2. Delta Lake: create table, MERGE, time travel, CDF, OPTIMIZE, schema evolution
3. Iceberg: create v2 table, snapshot inspection, hidden partitioning, row-level deletes
4. Hudi CoW: create table, upsert, read back
5. Hudi MoR: create table, multiple upsert batches, snapshot vs read-optimized reads, incremental queries
6. Side-by-side: same GDPR delete across all three formats
7. UniForm: enable on Delta table, read via Iceberg API
8. Format recommendation function: input your scenario, get a scored recommendation

## Prerequisites

- Databricks Runtime 13.0+ (for Iceberg v2 and UniForm support)
- Unity Catalog metastore with `CREATE TABLE` privilege
- For Hudi: confirm `org.apache.hudi:hudi-spark3.3-bundle_2.12` is on the cluster or use a cluster with Hudi pre-installed

## Time Estimate

- Guide: 120 min
- Notebook: 60 min
