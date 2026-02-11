# Project 03: CDC Pipeline (Change Data Capture)

> Module 10 -- Capstone Project | Level: Advanced | Time: 3-4 hours

## Project Overview

Implement a Change Data Capture (CDC) pipeline that processes database change
events -- inserts, updates, and deletes -- and maintains both a current-state
table (SCD Type 1) and a full historical tracking table (SCD Type 2). The
pipeline handles out-of-order events, maintains an immutable audit trail, and
produces analytics on customer change patterns.

This project integrates concepts from Modules 01-05 and 09: Delta Lake MERGE,
time travel, Change Data Feed, window functions, and data quality monitoring.

---

## Architecture

```
  SOURCE SYSTEM (Simulated)
  ==========================
  Initial load: 1000 customer records
  CDC events:   500 inserts + 300 updates + 100 deletes
  Each event:   operation type (I/U/D), before/after images, timestamp

       |
       v
  +================================================================+
  |                     BRONZE LAYER                                |
  |  Raw CDC events -- append-only, immutable audit trail           |
  |                                                                 |
  |  bronze_cdc_events                                              |
  |  + event_id, operation (I/U/D), customer_id, timestamp          |
  |  + before_image (JSON), after_image (JSON)                      |
  |  + _ingest_timestamp, _source, _batch_id                        |
  +================================================================+
       |
       |  Apply CDC operations using MERGE
       |  Handle out-of-order events via timestamps
       |  Build SCD Type 1 and SCD Type 2 tables
       |
       v
  +================================================================+
  |                     SILVER LAYER                                |
  |                                                                 |
  |  silver_customers_current                                       |
  |    SCD Type 1: Current state only (overwrite on update)         |
  |    MERGE with DELETE for 'D' operations                         |
  |                                                                 |
  |  silver_customers_history                                       |
  |    SCD Type 2: Full history with is_current, effective_date,    |
  |    end_date columns. Updates close old record + insert new.     |
  |                                                                 |
  |  silver_cdc_events_validated                                    |
  |    Validated CDC events with out-of-order detection             |
  +================================================================+
       |
       |  Analytics on change patterns
       |  Churn detection from deletes
       |  Data freshness monitoring
       |
       v
  +================================================================+
  |                     GOLD LAYER                                  |
  |                                                                 |
  |  gold_customer_change_frequency                                 |
  |    How often each customer's record changes                     |
  |                                                                 |
  |  gold_churn_analysis                                            |
  |    Deleted customers: tenure, last activity, segment            |
  |                                                                 |
  |  gold_data_freshness                                            |
  |    Pipeline health: latency, throughput, event distribution     |
  |                                                                 |
  |  gold_cdc_audit_dashboard                                       |
  |    Operations over time, out-of-order rate, error tracking      |
  +================================================================+
       |
       v
  Data Quality Reports / Operational Dashboards
```

---

## Requirements

### Data Generation

| Dataset | Count | Description |
|---------|-------|-------------|
| Initial customers | 1000 | Full customer records to seed the target table |
| Insert events (I) | 500 | New customer registrations |
| Update events (U) | 300 | Changes to existing customer attributes (name, email, tier, city) |
| Delete events (D) | 100 | Customer account closures |

Each CDC event includes:
- `event_id`: Unique identifier
- `operation`: "I" (insert), "U" (update), or "D" (delete)
- `customer_id`: The affected customer
- `event_timestamp`: When the change occurred in the source system
- `before_image`: Customer record before the change (null for inserts)
- `after_image`: Customer record after the change (null for deletes)
- `source_table`: Origin table name
- `transaction_id`: Source database transaction ID

Out-of-order events: ~5% of events have timestamps that are earlier than
previously processed events for the same customer_id. The pipeline must detect
and handle these correctly.

### Bronze Layer

1. Ingest all CDC events into an append-only Delta table.
2. Preserve full event structure including before/after images.
3. Add metadata columns: `_ingest_timestamp`, `_source`, `_batch_id`.
4. This table serves as the **immutable audit trail** -- never update or delete.

### Silver Layer

**SCD Type 1 (Current State)**:
1. Start with the initial 1000 customer records.
2. Apply CDC events in timestamp order using MERGE:
   - `WHEN MATCHED AND operation = 'D' THEN DELETE`
   - `WHEN MATCHED AND operation = 'U' THEN UPDATE SET ...`
   - `WHEN NOT MATCHED AND operation = 'I' THEN INSERT ...`
3. Handle out-of-order events: only apply an event if its timestamp is newer
   than the current record's `last_updated` timestamp.

**SCD Type 2 (Full History)**:
1. Add columns: `is_current` (boolean), `effective_date` (timestamp),
   `end_date` (timestamp), `version` (integer).
2. On insert: Create new record with `is_current=true`, `end_date=null`.
3. On update: Close the current record (set `end_date`, `is_current=false`),
   then insert a new record with the updated values and `is_current=true`.
4. On delete: Close the current record (set `end_date`, `is_current=false`).
   Do not physically delete -- the history is preserved.

**Validated CDC Events**:
1. Process all events and tag each one:
   - `is_valid`: Did the event apply successfully?
   - `is_out_of_order`: Was the event's timestamp older than expected?
   - `applied_action`: What actually happened (applied, skipped, error)?

### Gold Layer

1. **Customer change frequency**: Per-customer count of changes, average days
   between changes, most frequently changed fields.
2. **Churn analysis**: Customers who were deleted -- their tenure (days from
   first insert to delete), tier at deletion, city, and last update before
   deletion.
3. **Data freshness monitoring**: End-to-end latency (event_timestamp to
   ingest_timestamp), events per hour, cumulative event count.
4. **CDC audit dashboard**: Operations by type over time, out-of-order event
   rate, error rate, processing throughput.

### Delta Lake Features

1. Enable **Change Data Feed** on the Silver current-state table.
2. Query the CDF to see exactly what changed between versions.
3. Use **time travel** to compare table states before and after CDC processing.

---

## CDC MERGE Pattern Reference

The core MERGE statement for SCD Type 1:

```sql
MERGE INTO silver_customers_current AS target
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY event_timestamp DESC
        ) AS rn
        FROM bronze_cdc_events
    ) WHERE rn = 1
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.operation = 'D' THEN DELETE
WHEN MATCHED AND source.operation = 'U'
    AND source.event_timestamp > target.last_updated
THEN UPDATE SET
    target.name = source.after_name,
    target.email = source.after_email,
    ...
    target.last_updated = source.event_timestamp
WHEN NOT MATCHED AND source.operation = 'I' THEN INSERT (...)
```

Key points:
- **Deduplication in source**: The ROW_NUMBER window ensures only the latest
  event per customer_id is processed when multiple events exist.
- **Out-of-order guard**: The `source.event_timestamp > target.last_updated`
  condition prevents older events from overwriting newer state.
- **Delete handling**: MERGE supports DELETE as a matched action.

---

## SCD Type 2 Pattern Reference

```
  Timeline for customer C-001:

  Event 1 (Insert):
  +------+-----------+----------+------+--------+----+
  | C-001| John Smith| NY       | Gold | 2024-01| cur|
  +------+-----------+----------+------+--------+----+

  Event 2 (Update - moved to LA):
  +------+-----------+----------+------+--------+----+
  | C-001| John Smith| NY       | Gold | 2024-01| 04 |  <- closed (end_date = 2024-04)
  | C-001| John Smith| LA       | Gold | 2024-04| cur|  <- new current record
  +------+-----------+----------+------+--------+----+

  Event 3 (Delete - account closed):
  +------+-----------+----------+------+--------+----+
  | C-001| John Smith| NY       | Gold | 2024-01| 04 |  <- historical
  | C-001| John Smith| LA       | Gold | 2024-04| 07 |  <- closed (end_date = 2024-07)
  +------+-----------+----------+------+--------+----+
  No "current" record -- customer is deleted
```

Columns for SCD Type 2:
- `is_current` (boolean): True for the active version
- `effective_date` (timestamp): When this version became active
- `end_date` (timestamp): When this version was superseded (null if current)
- `version` (integer): Sequential version number per customer_id

---

## Implementation Tips

1. **Process CDC events in timestamp order.** Sort events by `event_timestamp`
   before applying them. This is critical for correct SCD Type 2 history.

2. **Use batch processing for SCD Type 2.** While MERGE handles Type 1 cleanly,
   Type 2 requires closing old records and inserting new ones -- which is easier
   to implement with a `foreachBatch`-style loop or a two-step MERGE.

3. **Test with small batches first.** Apply 10 events, verify the result, then
   apply the rest. CDC bugs are hard to diagnose on large datasets.

4. **Before/after images are valuable.** The before_image lets you verify that
   the update was applied to the expected state. Use it for validation.

5. **Change Data Feed is your friend.** Enable CDF on the target table and query
   it with `spark.read.option("readChangeFeed", "true")` to see exactly what
   MERGE changed. This is invaluable for debugging.

---

## Extension Ideas

1. **Multi-table CDC**: Extend the pipeline to handle CDC events for orders and
   products in addition to customers. Maintain referential integrity across
   tables.

2. **Conflict resolution**: Implement a conflict resolution strategy for when two
   CDC events modify the same field with different values at the same timestamp.

3. **Schema evolution**: Add a new column to the source system mid-stream and
   handle the schema change in the pipeline using Delta Lake schema evolution.

4. **Streaming CDC**: Convert the batch CDC pipeline to a streaming pipeline that
   continuously processes CDC events as they arrive.

5. **Data lineage**: Build a lineage table that tracks which Bronze events
   contributed to each Silver record's current state.

---

## Companion Notebook

The reference implementation is in
[03-cdc-pipeline_notebook.py](03-cdc-pipeline_notebook.py). Import it into
Databricks via Workspace > Import > File.

The notebook is self-contained: it generates all CDC events, builds both SCD
Type 1 and Type 2 pipelines, enables Change Data Feed, and cleans up all tables
at the end.

---

## Concepts Practiced

| Concept | Module Source | How It Is Used |
|---------|--------------|----------------|
| Delta Lake MERGE | Module 03 | SCD Type 1 current-state table |
| Time travel | Module 03 | Compare table states before/after CDC |
| Change Data Feed | Module 03 | Audit what MERGE operations changed |
| Window functions | Module 04 | Dedup CDC events, version numbering |
| SCD Type 1 | Module 04 | Overwrite-based current state |
| SCD Type 2 | Module 04 | Full history with effective/end dates |
| OPTIMIZE / ZORDER | Module 05 | Table optimization |
| Data quality | Module 09 | Out-of-order detection, validation |
