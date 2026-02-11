# Stream-Stream Joins
> Module 07 -- Topic 04 | Level: Advanced | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:

1. Perform inner joins between two streaming DataFrames
2. Configure outer joins with watermarks for state cleanup
3. Use time-bound conditions to limit join state
4. Implement stream-static joins for enriching streams with dimension data
5. Understand state management implications of different join types
6. Design production-ready stream-stream join pipelines

## Conceptual Overview

### Why Join Streams?

In real-world systems, related data often arrives on separate streams:

```
  Stream A: Ad Impressions           Stream B: Ad Clicks
  +------------------+              +------------------+
  | impression_id    |              | click_id         |
  | ad_id            |              | impression_id    |
  | user_id          |              | user_id          |
  | impression_time  |              | click_time       |
  +------------------+              +------------------+
          |                                  |
          +----------- JOIN ON --------------+
          |        impression_id             |
          v                                  v
  +-------------------------------------------+
  | impression_id | ad_id | impression_time   |
  |               |       | click_time | CTR  |
  +-------------------------------------------+
```

Other common examples:
- **Orders + Shipments**: Match orders with their shipment events
- **Transactions + Fraud Signals**: Correlate payments with risk scores
- **Sensor readings + Reference data**: Enrich IoT data with device metadata
- **Page views + Conversions**: Attribution analysis in marketing

### Stream-Stream Inner Join

An inner join between two streams returns matched rows from both sides. Because events
can arrive out of order, Spark buffers both streams in state until a match is found.

```
  Stream A (impressions):    Stream B (clicks):

  Time T1: [imp_1, ad_100]    Time T1: (nothing yet)
    State A: {imp_1}            State B: {}
    Output: (nothing)

  Time T2: [imp_2, ad_200]    Time T2: [clk_1, imp_1]
    State A: {imp_1, imp_2}     State B: {}          <-- imp_1 matched!
    Output: (imp_1 joined with clk_1)

  Time T3: [imp_3, ad_300]    Time T3: [clk_2, imp_3]
    State A: {imp_2, imp_3}     State B: {}          <-- imp_3 matched!
    Output: (imp_3 joined with clk_2)
```

**Problem**: Without a time bound, Spark keeps imp_2 in state forever (waiting for a
possible click that may never come).

### Time-Bound Conditions for State Cleanup

Add a time constraint to the join condition to tell Spark when it is safe to discard
unmatched events:

```python
# A click must happen within 30 minutes of the impression
joined = impressions.join(
    clicks,
    on=F.expr("""
        impressions.impression_id = clicks.impression_id
        AND clicks.click_time >= impressions.impression_time
        AND clicks.click_time <= impressions.impression_time + INTERVAL 30 MINUTES
    """),
    how="inner"
)
```

```
  With time bound (30 min):

  impression_time: 10:00
    |
    | <--- join window: 10:00 to 10:30 --->|
    |                                       |
    v                                       v
  10:00                                   10:30

  After 10:30, if no click matched, Spark drops imp from state.
```

### Stream-Stream Outer Joins (Require Watermarks)

Outer joins include unmatched rows from one or both sides. Because Spark must decide
*when* to emit an unmatched row (with nulls), **watermarks are required**.

```
  LEFT OUTER JOIN:
  All impressions + matching clicks (null if no click)

  RIGHT OUTER JOIN:
  All clicks + matching impressions (null if no impression)

  FULL OUTER JOIN:
  All rows from both sides (nulls where no match)
```

```python
# Left outer: all impressions, with click data if available
impressions_with_wm = impressions.withWatermark("impression_time", "10 minutes")
clicks_with_wm = clicks.withWatermark("click_time", "10 minutes")

joined = impressions_with_wm.join(
    clicks_with_wm,
    on=F.expr("""
        impressions.impression_id = clicks.impression_id
        AND clicks.click_time >= impressions.impression_time
        AND clicks.click_time <= impressions.impression_time + INTERVAL 30 MINUTES
    """),
    how="leftOuter"
)
```

**When does Spark emit an unmatched impression?**
When the watermark on the clicks stream advances past `impression_time + 30 minutes`,
Spark knows no click can ever arrive to match that impression, so it emits the row
with null click columns.

### Stream-Static Joins

A stream-static join enriches a streaming DataFrame with a static (batch) DataFrame.
This is simpler because only one side is streaming:

```
  Stream: Transactions              Static: Products (dimension table)
  +------------------+              +------------------+
  | txn_id           |              | product_id       |
  | product_id       |              | product_name     |
  | amount           |              | category         |
  | txn_time         |              | price            |
  +------------------+              +------------------+
           |                                 |
           +------ JOIN ON product_id -------+
           |                                 |
           v                                 v
  +-------------------------------------------+
  | txn_id | product_name | category | amount |
  +-------------------------------------------+
```

```python
# Static DataFrame (read once at query start)
products = spark.read.format("delta").load("/data/products")

# Streaming DataFrame
transactions = spark.readStream.format("delta").load("/data/transactions")

# Join: stream left-joined with static dimension
enriched = transactions.join(products, on="product_id", how="left")
```

**Key characteristics of stream-static joins**:
- No watermark required (static side has no concept of time)
- Static side is read **once** at query start (not refreshed automatically)
- To refresh the static side, restart the query or use `foreachBatch`
- Inner and left outer joins are supported (stream on the left side)

### State Management for Joins

| Join Type | Watermark Required | Time Bound Required | State Growth |
|-----------|-------------------|-------------------|--------------|
| Inner (stream-stream) | No (but recommended) | Highly recommended | Unbounded without time bound |
| Left outer (stream-stream) | **Yes** (both sides) | **Yes** | Bounded by watermark + time bound |
| Right outer (stream-stream) | **Yes** (both sides) | **Yes** | Bounded by watermark + time bound |
| Full outer (stream-stream) | **Yes** (both sides) | **Yes** | Bounded by watermark + time bound |
| Inner (stream-static) | No | No | No join state (static cached) |
| Left outer (stream-static) | No | No | No join state (static cached) |

### Join Output Modes

- **Append mode**: The only supported mode for stream-stream joins. Results are emitted
  once and cannot be retracted.
- **Complete and update modes**: Not supported for stream-stream joins.

## Hands-On Walkthrough

Open `04-stream-stream-joins_notebook.py` and work through:

1. **Create two rate streams**: Impressions and clicks with a shared key
2. **Inner join**: Match impressions with clicks
3. **Time-bound condition**: Limit state with a join time window
4. **Stream-static join**: Enrich a stream with a dimension table
5. **Monitoring**: Observe join state size in query progress

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Stream-stream join support | Full | Full | Full |
| State backend | RocksDB | RocksDB | RocksDB |
| Max state size | Instance memory dependent | Instance memory dependent | Instance memory dependent |
| Recommended instance | Memory-optimized (r5/r6g) | Memory-optimized (E-series) | Memory-optimized (n2-highmem) |

For large stream-stream joins, use memory-optimized instances. The state store holds
buffered events from both streams, which can be significant for high-throughput joins
with wide time bounds.

## Certification Tip

Stream-stream joins are tested on the Professional exam:
- Know that outer joins require watermarks on **both** streams
- Know that only append mode is supported for stream-stream joins
- Understand that time-bound conditions are essential for state cleanup
- Know that stream-static joins do not require watermarks
- Be able to identify when a stream-static join's static side becomes stale

## Key Takeaways

1. **Stream-stream inner joins** buffer both sides in state until a match is found
2. **Time-bound conditions** in the join expression are critical -- they tell Spark when to discard unmatched events
3. **Outer joins** require watermarks on both streams so Spark knows when to emit unmatched rows with nulls
4. **Stream-static joins** are simpler -- no watermark needed, but the static side is read once at query start
5. **Only append mode** is supported for stream-stream joins
6. Monitor **state size** in query progress to detect unbounded growth early

## Next Steps

Proceed to [05 - Auto Loader Streaming](05-auto-loader-streaming.md) to learn the
Databricks-specific file ingestion engine for production streaming pipelines.
