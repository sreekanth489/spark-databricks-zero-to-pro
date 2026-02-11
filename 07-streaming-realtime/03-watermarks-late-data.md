# Watermarks & Late Data
> Module 07 -- Topic 03 | Level: Intermediate-Advanced | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:

1. Distinguish between event time and processing time
2. Explain why late data is a fundamental challenge in streaming systems
3. Configure watermarks using `withWatermark()` to handle late-arriving data
4. Describe how watermarks interact with window aggregations
5. Understand state store management and why unbounded state is dangerous
6. Design watermark strategies for production streaming pipelines

## Conceptual Overview

### Event Time vs Processing Time

Every event in a streaming system has two timestamps:

```
  Event generated             Event processed
  at the source               by Spark
        |                          |
        v                          v
  +-----------+              +-----------+
  | Event     |  --network-->| Spark     |
  | time:     |   --queue--> | processes |
  | 10:00:05  |   --delay--> | at:       |
  +-----------+              | 10:00:12  |
                             +-----------+
       ^                          ^
   EVENT TIME               PROCESSING TIME
```

**Event time**: When the event actually occurred (embedded in the data).
**Processing time**: When Spark receives and processes the event.

The gap between these two can range from milliseconds (low-latency systems) to hours
(mobile devices going offline, network partitions, batch uploads).

**Why event time matters**: Business logic almost always depends on when something
*happened*, not when it was *processed*. A purchase at 11:59 PM should count in today's
sales, even if it arrives at 12:05 AM tomorrow.

### The Late Data Problem

```
  Window: 10:00 - 10:05

  Timeline of arrivals:
  ──────────────────────────────────────────────> processing time

  10:00  [event_time=10:01] arrives --> IN WINDOW (on time)
  10:02  [event_time=10:03] arrives --> IN WINDOW (on time)
  10:05  [event_time=10:04] arrives --> IN WINDOW (on time)
  10:06  Window closes... or does it?
  10:08  [event_time=10:02] arrives --> LATE DATA!  <--- problem
  10:15  [event_time=10:01] arrives --> VERY LATE!  <--- bigger problem
```

Without a mechanism to handle late data, Spark has two bad options:
1. **Keep all state forever**: Every window stays open indefinitely, waiting for late
   events. State grows unboundedly until the system runs out of memory.
2. **Drop all late data**: Close windows strictly, losing accuracy.

**Watermarks** provide the middle ground.

### What Is a Watermark?

A watermark is a **moving threshold** that tells Spark: *"I can guarantee that no event
will arrive with an event time earlier than this threshold."*

```
  Watermark = max(event_time seen so far) - watermark delay

  If watermark delay = 10 minutes:

  At processing time 10:30:
    Max event time seen = 10:28
    Watermark = 10:28 - 10 min = 10:18

  Meaning: Spark assumes no event with event_time < 10:18 will ever arrive.
  State for windows ending before 10:18 can be safely dropped.
```

### How Watermarks Work with Windows

```
  Watermark delay: 10 minutes
  Window size: 5 minutes

  Time ──────────────────────────────────────────>

  Max event time seen:  10:20    10:25    10:30    10:35
  Watermark:            10:10    10:15    10:20    10:25
                          |        |        |        |
  Window [10:00-10:05]:  OPEN     OPEN    CLOSED   DROPPED
  Window [10:05-10:10]:  OPEN     OPEN     OPEN    CLOSED
  Window [10:10-10:15]:   --      OPEN     OPEN     OPEN
  Window [10:15-10:20]:   --       --      OPEN     OPEN

  OPEN    = accepting events, state maintained
  CLOSED  = finalized, output emitted (in append mode)
  DROPPED = state removed from memory
```

When a window is **closed** (watermark passes the window's end time):
- In **append mode**: The window's aggregation result is emitted to the sink
- The state for that window is cleaned up
- Any event arriving for that window is **dropped** (too late)

### The withWatermark() API

```python
from pyspark.sql import functions as F

# Define watermark on the event_time column
windowed_counts = (
    events_stream
    .withWatermark("event_time", "10 minutes")  # tolerate 10 min late data
    .groupBy(
        F.window("event_time", "5 minutes"),    # 5-minute tumbling window
        "category"
    )
    .agg(F.count("*").alias("event_count"))
)

# With watermark, we CAN use append mode for windowed aggregations
query = (
    windowed_counts.writeStream
    .format("delta")
    .outputMode("append")      # emits finalized windows only
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)
```

### State Store: The Hidden Cost of Streaming

Every aggregation in Structured Streaming maintains a **state store**:

```
  Without watermark:                 With watermark:
  +-------------------+             +-------------------+
  | State Store       |             | State Store       |
  | window_1: 42      |             | window_4: 15      |
  | window_2: 88      |             | window_5: 23      |
  | window_3: 55      |             | window_6: 7       |
  | window_4: 15      |             +-------------------+
  | window_5: 23      |              (old windows cleaned up)
  | window_6: 7       |
  | ... growing ...   |
  +-------------------+
   State GROWS FOREVER              State BOUNDED
   (OOM eventually)                 (production-safe)
```

**Without watermarks**: State grows with every new window. Over hours or days, this
consumes all available memory and the query fails.

**With watermarks**: State for windows that are past the watermark threshold is purged.
Memory usage stays bounded and predictable.

### Choosing a Watermark Delay

The watermark delay is a trade-off between **completeness** and **latency**:

```
  Short delay (1 min)              Long delay (1 hour)
  +------------------+             +------------------+
  | Low latency      |             | High latency     |
  | Results fast     |             | Results delayed  |
  | More data dropped|             | Less data dropped|
  | Less state       |             | More state       |
  +------------------+             +------------------+
```

Guidelines for choosing:
- **IoT sensors** (reliable network): 1-5 minutes
- **Mobile apps** (intermittent connectivity): 30-60 minutes
- **Batch uploads** (daily files): 24 hours
- **Financial transactions** (strict SLAs): Depends on reconciliation window

### Watermark Guarantees

Important nuances to understand:

1. Watermarks are a **threshold**, not a guarantee. Events within the watermark delay
   *may* still be processed -- Spark makes a best-effort attempt.
2. The watermark only **advances forward** -- it never goes backward.
3. Watermarks are computed **per partition** and then the global minimum is used.
4. Watermarks only affect **stateful operations** (aggregations, joins, dedup).
   Non-stateful operations (map, filter) are not affected.

## Hands-On Walkthrough

Open `03-watermarks-late-data_notebook.py` and work through:

1. **Event data generation**: Create events with deliberate late arrivals
2. **Windowed aggregation without watermark**: Observe state growing
3. **Add watermark**: See late data handling and state cleanup
4. **Watermark progression**: Track watermark advancement over time
5. **Append mode with watermark**: Finalized windows emitted once

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| State store backend | RocksDB (default) | RocksDB (default) | RocksDB (default) |
| State checkpoint storage | S3 | ADLS Gen2 | GCS |
| State store size limit | Depends on instance memory | Depends on instance memory | Depends on instance memory |
| RocksDB changelog checkpointing | DBR 11.2+ | DBR 11.2+ | DBR 11.2+ |

RocksDB is the recommended state store backend for large state. It is the default in
Databricks Runtime. The changelog-based checkpointing (DBR 11.2+) significantly reduces
checkpoint size for large state operations.

## Certification Tip

Watermarks are a heavily tested topic on both the Associate and Professional exams:
- Know that `withWatermark()` must reference an existing timestamp column
- Understand that watermark = max(event_time) - delay
- Know that append mode + aggregation requires a watermark
- Understand that late data beyond the watermark threshold is dropped
- Know that state grows unboundedly without watermarks (key production concern)

## Key Takeaways

1. **Event time** is when an event happened; **processing time** is when Spark sees it
2. **Late data** is inevitable in distributed systems; watermarks provide a bounded tolerance
3. `withWatermark("event_time", "10 minutes")` tells Spark to tolerate up to 10 minutes of lateness
4. **Without watermarks**, state for aggregations grows forever and eventually causes OOM
5. **Watermark delay** is a trade-off: shorter = lower latency but more data loss; longer = more complete but higher memory
6. Append mode with aggregations **requires** a watermark to know when a window is finalized

## Next Steps

Proceed to [04 - Stream-Stream Joins](04-stream-stream-joins.md) to learn how to join
two streaming DataFrames and manage state for join operations.
