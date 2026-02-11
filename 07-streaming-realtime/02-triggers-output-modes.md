# Triggers & Output Modes
> Module 07 -- Topic 02 | Level: Intermediate | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:

1. Explain the three output modes (append, complete, update) and when to use each
2. Configure trigger types (default, fixed interval, once, availableNow)
3. Articulate the critical difference between `trigger.once` and `trigger.availableNow`
4. Select the correct output mode for a given streaming scenario
5. Design production streaming patterns using trigger combinations

## Conceptual Overview

### Output Modes: How Results Are Written

When a streaming query produces results, the output mode determines *which rows* are
written to the sink in each micro-batch.

```
  Input (micro-batch N)       Processing          Output Mode Decision
  +------------------+       +----------+         +-----------------+
  | new rows         | ──>   | transform| ──>     | APPEND:  new    |
  | (since batch N-1)|       | aggregate|         | COMPLETE: all   |
  +------------------+       +----------+         | UPDATE: changed |
                                                  +-----------------+
```

#### Append Mode (default)

Only **new rows** added to the result table since the last trigger are written to the sink.

```
  Batch 0 result:  [A, B]         --> Output: [A, B]
  Batch 1 result:  [A, B, C, D]   --> Output: [C, D]  (only new rows)
  Batch 2 result:  [A, B, C, D, E]--> Output: [E]     (only new row)
```

**Use when**: No aggregations, or aggregations with watermarks (after watermark guarantees
rows will not change). Most common mode for ETL pipelines.

**Restrictions**: Cannot use with aggregations unless a watermark is defined (because
aggregate values can change, and append cannot retract previous output).

#### Complete Mode

The **entire result table** is written to the sink every trigger.

```
  Batch 0 result:  {A:10}              --> Output: {A:10}
  Batch 1 result:  {A:15, B:5}         --> Output: {A:15, B:5}  (full table)
  Batch 2 result:  {A:20, B:8, C:3}    --> Output: {A:20, B:8, C:3}  (full table)
```

**Use when**: Aggregation queries where you need the full result every time (dashboards,
materialized views). The sink must support overwriting.

**Warning**: If the result table is large, complete mode rewrites everything on each
trigger. This is expensive for high-cardinality aggregations.

#### Update Mode

Only rows that **changed** since the last trigger are written.

```
  Batch 0 result:  {A:10}              --> Output: {A:10}
  Batch 1 result:  {A:15, B:5}         --> Output: {A:15, B:5}  (A changed, B is new)
  Batch 2 result:  {A:20, B:8, C:3}    --> Output: {A:20, B:8, C:3}  (all changed)
```

**Use when**: Aggregations where you only want to write modified rows. More efficient
than complete mode but requires the sink to support updates (e.g., `foreachBatch` with
upsert logic).

**Note**: If there are no aggregations, update mode behaves identically to append mode.

### Output Mode Selection Guide

```
  +-------------------+--------+----------+--------+
  | Scenario          | Append | Complete | Update |
  +-------------------+--------+----------+--------+
  | No aggregation    |   Y    |    N     |   Y    |
  | (map-only)        |        |          |        |
  +-------------------+--------+----------+--------+
  | Aggregation       |   N*   |    Y     |   Y    |
  | (no watermark)    |        |          |        |
  +-------------------+--------+----------+--------+
  | Aggregation       |   Y    |    Y     |   Y    |
  | (with watermark)  |        |          |        |
  +-------------------+--------+----------+--------+
  | mapGroupsWithState|   N    |    N     |   Y    |
  +-------------------+--------+----------+--------+

  * Append + aggregation without watermark throws AnalysisException
```

### Trigger Types: When Processing Happens

Triggers control the **timing** of micro-batch execution.

#### Default (Micro-Batch)

Processes the next batch as soon as the previous one completes. No idle time between
batches. This gives the lowest latency.

```python
# No trigger specification needed (this is the default)
query = df.writeStream.format("delta").start(path)
```

#### Fixed Interval

Waits a specified duration between the start of consecutive batches. If a batch takes
longer than the interval, the next batch starts immediately after.

```python
query = (
    df.writeStream
    .trigger(processingTime="30 seconds")
    .format("delta")
    .start(path)
)
```

```
  |--batch--|idle|--batch--|idle|--batch--|  (batches < interval)
  |------batch------||--batch--|idle|       (batch > interval)
```

#### Once (trigger.once) -- Deprecated Pattern

Processes **all available data in a single batch**, then stops. All data is processed in
one giant micro-batch regardless of rate limits.

```python
query = (
    df.writeStream
    .trigger(once=True)
    .format("delta")
    .start(path)
)
```

**Problem**: Ignores `maxFilesPerTrigger` and `maxBytesPerTrigger`. If 10,000 files are
waiting, it processes all of them in one batch, potentially causing OOM errors.

#### AvailableNow (trigger.availableNow) -- Recommended

Processes all available data, then stops -- but **respects rate limits**. Data is
processed across multiple micro-batches.

```python
query = (
    df.writeStream
    .trigger(availableNow=True)
    .format("delta")
    .start(path)
)
```

```
  trigger.once:          |=======ALL DATA IN ONE BATCH=======| stop

  trigger.availableNow:  |--batch1--|--batch2--|--batch3--| stop
                          (respects maxFilesPerTrigger)
```

### trigger.once vs trigger.availableNow

This is one of the most important distinctions for production pipelines:

| Feature | trigger.once | trigger.availableNow |
|---------|-------------|---------------------|
| Processes all available data | Yes | Yes |
| Stops after processing | Yes | Yes |
| Respects rate limits | **No** | **Yes** |
| Number of micro-batches | Always 1 | Multiple (based on rate limits) |
| OOM risk with large backlogs | **High** | Low |
| Recommended for production | No | **Yes** |

**Production pattern**: Use `trigger(availableNow=True)` with Databricks Workflows
to create cost-effective "streaming" pipelines that run on a schedule (e.g., every
15 minutes) using job clusters.

```
  Workflow Schedule (every 15 min):

  Run 1:  [start cluster] -> [process available data] -> [stop] -> [terminate cluster]
  Run 2:  [start cluster] -> [process available data] -> [stop] -> [terminate cluster]
  Run 3:  [start cluster] -> [process available data] -> [stop] -> [terminate cluster]

  Cost: Pay only for compute during actual processing
  vs. Always-on streaming: Pay for 24/7 cluster uptime
```

### Trigger Selection Guide

| Requirement | Recommended Trigger |
|-------------|-------------------|
| Lowest latency (sub-second) | Default (continuous micro-batch) |
| Controlled resource usage | Fixed interval (e.g., `processingTime="1 minute"`) |
| Batch-like scheduled runs | `availableNow=True` with Workflows |
| One-time backfill | `availableNow=True` |
| Legacy (avoid in new code) | `once=True` |

## Hands-On Walkthrough

Open the companion notebook `02-triggers-output-modes_notebook.py` and work through:

1. **Append mode**: Stream without aggregations to a memory sink
2. **Complete mode**: Aggregation query with full result output
3. **Update mode**: Aggregation query outputting only changed rows
4. **Fixed interval trigger**: 10-second processing time
5. **availableNow trigger**: Process-and-stop pattern
6. **Comparison**: trigger.once vs trigger.availableNow behavior

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Trigger behavior | Identical | Identical | Identical |
| Output modes | Identical | Identical | Identical |
| availableNow support | DBR 10.1+ | DBR 10.1+ | DBR 10.1+ |
| Serverless triggers | Preview | Preview | Preview |

Trigger and output mode behavior is consistent across all cloud providers. The only
variation is in underlying storage performance, which may affect micro-batch duration.

## Certification Tip

The Databricks Data Engineer Associate exam tests:
- Which output modes support aggregations (complete, update; append only with watermark)
- The difference between trigger.once and trigger.availableNow (rate limit respect)
- Default trigger behavior (next batch starts immediately after previous completes)
- When to use complete vs append vs update mode
- That append mode is the default and most common for ETL pipelines

## Key Takeaways

1. **Append mode** writes only new rows -- best for non-aggregating ETL pipelines
2. **Complete mode** rewrites the entire result -- use for dashboards and small aggregations
3. **Update mode** writes only changed rows -- efficient for aggregations with upsert sinks
4. **trigger.availableNow** is the production replacement for trigger.once -- it respects rate limits and prevents OOM
5. Combine **availableNow + Workflows** for cost-effective scheduled streaming on job clusters
6. Default trigger (no specification) gives the **lowest latency** for always-on streaming

## Next Steps

Proceed to [03 - Watermarks & Late Data](03-watermarks-late-data.md) to learn how to
handle late-arriving data and manage state in windowed aggregations.
