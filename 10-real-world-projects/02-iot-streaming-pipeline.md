# Project 02: IoT Streaming Pipeline

> Module 10 -- Capstone Project | Level: Advanced | Time: 2-3 hours

## Project Overview

Build a real-time IoT sensor data pipeline for a manufacturing company with 50
devices across 5 factory floors. The pipeline ingests continuous sensor readings
(temperature, pressure, vibration, humidity), detects anomalies using statistical
thresholds, and produces device health scores and factory floor comparison
dashboards.

This project integrates concepts from Modules 01-05, 07, and 09: PySpark
transformations, Structured Streaming, watermarks, windowed aggregations, Delta
Lake, and data quality monitoring.

---

## Architecture

```
  SENSOR DATA (Simulated)
  ========================
  50 devices x 5 factory floors
  Readings: temperature, pressure, vibration, humidity
  Frequency: continuous (simulated via rate source)

       |
       v
  +================================================================+
  |                     INGEST LAYER                                |
  |  Simulated streaming using Spark rate source                    |
  |  Transformed to match sensor event schema                      |
  |  Options: (A) actual streaming  (B) batch simulation            |
  +================================================================+
       |
       v
  +================================================================+
  |                     BRONZE LAYER                                |
  |  Raw sensor readings -- append-only Delta table                 |
  |                                                                 |
  |  bronze_sensor_readings                                         |
  |  + device_id, floor_id, reading_type, value, event_time         |
  |  + _ingest_timestamp, _source                                   |
  +================================================================+
       |
       |  5-minute tumbling window aggregations
       |  Anomaly detection (value outside mean +/- 2 std dev)
       |  Enrich with device metadata
       |
       v
  +================================================================+
  |                     SILVER LAYER                                |
  |  Cleaned, windowed, anomaly-tagged sensor data                  |
  |                                                                 |
  |  silver_sensor_5min_agg      5-min avg, min, max, stddev        |
  |  silver_anomalies            Readings flagged as anomalous      |
  |  silver_device_metadata      Device info (type, install date)   |
  +================================================================+
       |
       |  Composite health scores
       |  Anomaly trend analysis
       |  Floor-level comparisons
       |
       v
  +================================================================+
  |                     GOLD LAYER                                  |
  |  Business dashboards -- BI-ready                                |
  |                                                                 |
  |  gold_device_health_scores   Per-device composite health score  |
  |  gold_anomaly_summary        Anomaly counts and trends          |
  |  gold_floor_comparison       Floor-level KPIs and rankings      |
  +================================================================+
       |
       v
  Factory Monitoring Dashboard / Alerting System
```

---

## Requirements

### Data Generation

| Element | Details |
|---------|---------|
| Devices | 50 devices (IDs: DEV-001 through DEV-050) |
| Factory floors | 5 floors (FLOOR-A through FLOOR-E), 10 devices per floor |
| Sensor types | temperature (C), pressure (PSI), vibration (mm/s), humidity (%) |
| Normal ranges | temperature: 60-80, pressure: 28-32, vibration: 0.5-2.0, humidity: 35-55 |
| Anomaly injection | ~5% of readings intentionally outside normal range |
| Time span | 24 hours of data at 1-minute intervals per device |

The notebook provides two ingestion modes:
1. **Batch simulation** (default): Generate all data as a static DataFrame, then
   process it using the streaming API with `trigger(availableNow=True)`.
2. **Live streaming**: Use the Spark `rate` source to generate events in real-time
   and process them as a continuous stream with a `processingTime` trigger.

### Bronze Layer

1. Ingest raw sensor events into a Delta table.
2. Schema: `device_id`, `floor_id`, `reading_type`, `value`, `event_time`,
   `_ingest_timestamp`, `_source`.
3. Append-only. No transformations on the raw values.

### Silver Layer

1. **5-minute windowed aggregations**: For each device and reading type, compute
   the average, minimum, maximum, and standard deviation over 5-minute tumbling
   windows.
2. **Anomaly detection**: Flag readings where the value falls outside the rolling
   mean +/- 2 standard deviations. Use per-device, per-reading-type baselines.
3. **Device metadata enrichment**: Join sensor data with a device metadata table
   containing device type, installation date, and last maintenance date.

### Gold Layer

1. **Device health scores**: Composite score (0-100) for each device based on:
   - How many of its readings are within normal range (weight: 40%)
   - Anomaly frequency over the last hour (weight: 30%)
   - Reading stability (low standard deviation = better, weight: 30%)
2. **Anomaly summary**: Total anomalies per device, per floor, per reading type.
   Include trend (is anomaly rate increasing or decreasing over time?).
3. **Factory floor comparison**: Per-floor average health score, anomaly rate,
   and ranking.

---

## Anomaly Detection Reference

The notebook uses a simple statistical approach for anomaly detection:

```
  For each (device_id, reading_type) combination:

  1. Compute rolling statistics:
     - mean = average of recent readings
     - std  = standard deviation of recent readings

  2. Define thresholds:
     - upper_threshold = mean + (2 * std)
     - lower_threshold = mean - (2 * std)

  3. Flag anomaly:
     - is_anomaly = (value > upper_threshold) OR (value < lower_threshold)
```

This is known as the "2-sigma rule." In a normal distribution, approximately 95%
of values fall within 2 standard deviations of the mean. Values outside this
range are flagged as anomalies.

**Note**: Production systems typically use more sophisticated methods (Isolation
Forest, DBSCAN, or time-series decomposition). The 2-sigma approach is used here
for clarity and to avoid external library dependencies.

---

## Device Health Score Calculation

```
  health_score = (
      normal_reading_pct   * 0.40   # % of readings within normal range
    + anomaly_penalty       * 0.30   # 100 - (anomaly_rate * 100), floored at 0
    + stability_score       * 0.30   # 100 - (normalized_stddev * 100), floored at 0
  )

  Score interpretation:
    90-100  Excellent   Device operating normally
    70-89   Good        Minor deviations, monitor closely
    50-69   Warning     Increasing anomalies, schedule maintenance
    0-49    Critical    Frequent anomalies, immediate attention needed
```

---

## Streaming Patterns Demonstrated

| Pattern | How It Appears |
|---------|----------------|
| Rate source | Generates events at a configurable rate for testing |
| Watermarking | `withWatermark("event_time", "10 minutes")` handles late data |
| Tumbling windows | `window("event_time", "5 minutes")` for fixed-interval aggregations |
| Trigger availableNow | Processes all available data then stops (batch-style streaming) |
| Trigger processingTime | Continuous processing at fixed intervals (shown as optional) |
| Checkpointing | Required for streaming state management |
| Delta as sink | All streaming writes use Delta format for exactly-once semantics |
| foreachBatch | Used for custom processing logic within streaming micro-batches |

---

## Implementation Tips

1. **Start with batch simulation.** Get the full pipeline working with
   `trigger(availableNow=True)` before attempting live streaming. This lets you
   debug transformations without waiting for streaming events.

2. **Generate device metadata first.** The metadata table (device type,
   installation date, etc.) is a static reference table that enriches the sensor
   data. Create it before the streaming pipeline.

3. **Use `foreachBatch` for complex Silver logic.** When applying anomaly
   detection that requires group-level statistics, `foreachBatch` lets you use
   full DataFrame API on each micro-batch.

4. **Watch out for state management.** Windowed aggregations maintain state in
   the streaming engine. Without watermarks, state grows unboundedly. Always pair
   `window()` with `withWatermark()`.

5. **Test anomaly thresholds carefully.** If too tight, every reading is an
   anomaly. If too loose, real problems are missed. The 2-sigma approach with
   the given data ranges produces a reasonable 3-8% anomaly rate.

---

## Extension Ideas

1. **Sliding windows**: Replace tumbling windows with sliding windows (e.g.,
   5-minute window, 1-minute slide) for smoother aggregations.

2. **Alert system**: Build a notification pipeline that writes critical anomalies
   to a separate Delta table with severity levels and suggested actions.

3. **Predictive maintenance**: Track device health score trends over time and
   flag devices whose scores are declining consistently (simple linear regression
   on health score time series).

4. **Multi-stream join**: Create separate streams for sensor readings and
   maintenance events, then join them to correlate maintenance with health
   improvements.

5. **Kafka integration**: Replace the rate source with a Kafka consumer to
   simulate a production-grade ingestion architecture.

---

## Companion Notebook

The reference implementation is in
[02-iot-streaming-pipeline_notebook.py](02-iot-streaming-pipeline_notebook.py).
Import it into Databricks via Workspace > Import > File.

The notebook defaults to batch simulation mode for compatibility with all
Databricks tiers including Community Edition. Live streaming cells are included
as optional sections.

---

## Concepts Practiced

| Concept | Module Source | How It Is Used |
|---------|--------------|----------------|
| Structured Streaming | Module 07 | Core streaming pipeline |
| Watermarks | Module 07 | Late data handling |
| Windowed aggregations | Module 07 | 5-minute tumbling windows |
| Triggers | Module 07 | availableNow and processingTime |
| Delta Lake | Module 03 | Bronze, Silver, Gold storage |
| Window functions | Module 04 | Health scores, rankings |
| Aggregations | Module 04 | All Gold tables |
| Joins | Module 04 | Device metadata enrichment |
| Data quality | Module 09 | Anomaly detection, health monitoring |
