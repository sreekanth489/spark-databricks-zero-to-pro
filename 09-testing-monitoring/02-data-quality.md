# Data Quality Frameworks
> Module 09 -- Topic 02 | Level: Intermediate | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:
1. Define and measure the six dimensions of data quality
2. Implement custom data quality check functions in PySpark
3. Understand DLT expectations at all three severity levels (EXPECT, DROP, FAIL)
4. Build a quarantine pattern that routes bad records to a separate table
5. Create a data quality report that tracks metrics over time
6. Design a data profiling pipeline that detects anomalies automatically

---

## Conceptual Overview

### The Six Dimensions of Data Quality

Every data quality check maps to one of these six dimensions:

```
  +----------------+  +----------------+  +----------------+
  | COMPLETENESS   |  | UNIQUENESS     |  | TIMELINESS     |
  | Are all values |  | Are there      |  | Is the data    |
  | present?       |  | duplicates?    |  | fresh enough?  |
  +----------------+  +----------------+  +----------------+

  +----------------+  +----------------+  +----------------+
  | VALIDITY       |  | ACCURACY       |  | CONSISTENCY    |
  | Do values      |  | Do values      |  | Do values      |
  | match rules?   |  | reflect truth? |  | agree across   |
  | (format, range)|  | (real-world)   |  | systems?       |
  +----------------+  +----------------+  +----------------+
```

| Dimension | Example Check | Severity |
|-----------|---------------|----------|
| Completeness | `email IS NOT NULL` | High -- missing data cannot be recovered later |
| Uniqueness | `COUNT(DISTINCT order_id) = COUNT(*)` | High -- duplicates corrupt aggregations |
| Timeliness | `MAX(event_time) > NOW() - INTERVAL 1 HOUR` | Medium -- stale data misleads dashboards |
| Validity | `age BETWEEN 0 AND 150` | Medium -- invalid values produce wrong calculations |
| Accuracy | `zip_code matches external reference` | Low -- requires external validation |
| Consistency | `SUM(line_items) = order_total` | High -- inconsistency signals data corruption |

### Where Quality Checks Belong in a Pipeline

```
  Raw Data  --->  Bronze Layer  --->  Silver Layer  --->  Gold Layer
                      |                    |                   |
                  Schema checks        Business rules     Aggregate checks
                  Null detection        Range validation   Cross-table consistency
                  Duplicate detection   Pattern matching   Freshness monitoring
                      |                    |                   |
                      v                    v                   v
                  Quarantine          Quarantine          Alert/Block
                  (dead letter)       (dead letter)       (pipeline halt)
```

**Key principle**: Check early, check often. The cost of catching a bad record
increases exponentially as it moves downstream through the pipeline.

---

## Delta Live Tables (DLT) Expectations

DLT expectations are the Databricks-native mechanism for declarative data quality.
They are defined inline with table definitions and operate at three severity levels.

### Severity Levels

```
  Severity         Behavior                     Use When
  ===============  ===========================  ===========================
  EXPECT           Log metric, pass row through You want visibility but
                                                cannot afford to lose data

  EXPECT OR DROP   Silently drop failing rows   Bad rows are noise that
                                                should be filtered out

  EXPECT OR FAIL   Halt the entire pipeline     Data integrity is critical
                                                and partial loads are worse
                                                than no load
```

### DLT Expectations Syntax

```python
# In a DLT pipeline definition:

import dlt

@dlt.table
@dlt.expect("valid_email", "email IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
def cleaned_orders():
    return spark.read.table("raw_orders")
```

Multiple expectations can be combined:

```python
@dlt.table
@dlt.expect_all({
    "valid_name": "name IS NOT NULL",
    "valid_age": "age BETWEEN 0 AND 150",
    "valid_email": "email LIKE '%@%.%'"
})
def validated_customers():
    return spark.read.table("raw_customers")
```

### DLT Expectations Metrics

DLT automatically tracks:
- Total records processed
- Records passing each expectation
- Records failing each expectation
- Pass rate percentage

These metrics are visible in the DLT pipeline UI and queryable through the
event log.

```sql
-- Query DLT expectation metrics from the event log
SELECT
  details:flow_progress:data_quality:expectations
FROM event_log(TABLE(my_pipeline))
WHERE event_type = 'flow_progress'
```

---

## Custom Data Quality Framework

When DLT is not available (or when you need more control), build a custom
framework using standard PySpark.

### Architecture

```
  +--------------------+
  | Quality Check      |    A check is a function that takes a DataFrame
  | Definition         |    and returns pass/fail with metrics.
  +--------------------+
          |
          v
  +--------------------+
  | Quality Engine     |    The engine runs all checks against a DataFrame
  |                    |    and collects results into a report.
  +--------------------+
          |
          v
  +--------------------+    +--------------------+
  | Quality Report     |    | Quarantine Table   |
  | (metrics, trends)  |    | (bad records)      |
  +--------------------+    +--------------------+
```

### Check Function Pattern

```python
def check_not_null(df, column_name):
    """Completeness check: verify no nulls in a column."""
    total = df.count()
    null_count = df.filter(F.col(column_name).isNull()).count()
    pass_rate = (total - null_count) / total if total > 0 else 1.0
    return {
        "check": "not_null",
        "column": column_name,
        "total_rows": total,
        "failing_rows": null_count,
        "pass_rate": pass_rate,
        "passed": null_count == 0
    }
```

### Quarantine Pattern

The quarantine pattern separates good records from bad records instead of
failing the entire pipeline:

```python
def apply_quality_filter(df, condition, reason):
    """Split DataFrame into good and quarantined records."""
    good_records = df.filter(condition)
    bad_records = (
        df.filter(~condition)
        .withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )
    return good_records, bad_records
```

This allows the pipeline to continue processing valid data while preserving
bad records for investigation.

---

## Data Profiling

Data profiling answers the question: "What does this data actually look like?"
before you define quality rules.

### Key Profiling Metrics

| Metric | Formula | What It Reveals |
|--------|---------|-----------------|
| Null rate | `COUNT(nulls) / COUNT(*)` | Completeness issues |
| Distinct count | `COUNT(DISTINCT col)` | Cardinality, potential keys |
| Min/Max | `MIN(col), MAX(col)` | Range, potential outliers |
| Mean/Stddev | `AVG(col), STDDEV(col)` | Distribution shape |
| Top-N values | `GROUP BY col ORDER BY COUNT DESC` | Dominant categories, skew |
| Pattern match rate | `COUNT(regex matches) / COUNT(*)` | Format consistency |

### Anomaly Detection

Compare today's profile against a historical baseline:

- **Row count anomaly**: Today's count is more than 2 standard deviations from
  the 30-day rolling average.
- **Null rate spike**: A column that normally has 0.1% nulls suddenly has 15%.
- **Cardinality change**: A column that should have exactly 50 states now has 52.

These checks can be automated and wired to alerting (covered in Topic 03).

---

## Hands-On Walkthrough

Open `02-data-quality_notebook.py` to practice:
- Building a custom data quality framework with check functions
- Implementing row-level quality checks (nulls, ranges, patterns)
- Creating a quality report DataFrame with pass rates per check
- Demonstrating the quarantine pattern for bad records
- DLT expectations syntax (as reference examples)

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| DLT Expectations | Supported in DLT pipelines | Supported in DLT pipelines | Supported in DLT pipelines |
| Great Expectations | Deploy on EC2/EMR or as a step in Step Functions | Deploy on AKS or as an Azure Function | Deploy on GKE or Cloud Functions |
| Quality metrics storage | Delta table on S3 | Delta table on ADLS Gen2 | Delta table on GCS |
| Alerting on quality failures | SNS/CloudWatch | Azure Monitor/Logic Apps | Cloud Monitoring/Pub-Sub |
| DLT event log | Stored in Delta table | Stored in Delta table | Stored in Delta table |

---

## Certification Tip

> **Databricks Certified Data Engineer Associate**: DLT expectations are a
> frequently tested topic. You must know the three severity levels and their
> exact behavior:
> - `@dlt.expect` -- logs metric, passes all rows through
> - `@dlt.expect_or_drop` -- silently drops failing rows
> - `@dlt.expect_or_fail` -- halts the pipeline entirely
>
> You may also see questions about where to place quality checks in the
> medallion architecture (bronze: schema + nulls, silver: business rules,
> gold: aggregate consistency).

---

## Key Takeaways

1. **Data quality has six dimensions**: completeness, uniqueness, timeliness,
   validity, accuracy, consistency. Every check should map to one of these.
2. **DLT expectations** are the Databricks-native solution with three severity
   levels -- use EXPECT for visibility, DROP for noise removal, FAIL for
   critical integrity constraints.
3. **Quarantine bad records** instead of failing pipelines. Route them to a
   dead letter table with reason and timestamp for investigation.
4. **Check early and often**. The cost of catching bad data increases
   exponentially as it moves downstream.
5. **Profile before you define rules**. Understand what the data actually looks
   like before deciding what it should look like.
6. **Track quality metrics over time** to detect gradual degradation and
   establish baselines for anomaly detection.

---

## Next Steps

- Proceed to **Topic 03: Monitoring & Alerting** to learn how to surface quality
  metrics in dashboards and trigger alerts when thresholds are breached.
- Review DLT pipeline configuration in Module 06 (Orchestration) for hands-on
  DLT experience.
