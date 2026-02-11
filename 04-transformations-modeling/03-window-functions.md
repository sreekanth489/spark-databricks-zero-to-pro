# Window Functions

> Module 04 -- Topic 03 | Level: Intermediate | Time: 45 min

## Learning Objectives

- Understand how window functions differ from groupBy aggregations
- Use ranking functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE
- Compute offset values with LAG and LEAD
- Calculate running totals and moving averages
- Define frame specifications with ROWS BETWEEN and RANGE BETWEEN
- Apply PARTITION BY + ORDER BY for intra-group calculations

## Conceptual Overview

### Window Functions vs groupBy

A `groupBy` aggregation collapses rows: if you group 1000 rows by category,
you get one row per category. A **window function** computes a value for every
row using a "window" of related rows, without reducing the row count.

```
  groupBy("studio").agg(sum("revenue"))     Window sum over studio

  studio    total_revenue                    studio    title          revenue  running_total
  ------    -------------                    ------    -----          -------  -------------
  Marvel    3382                              Marvel    Iron Man       585      585
  Warner    2204                              Marvel    Avengers      2797     3382
                                              Warner    Inception      836      836
  2 rows (collapsed)                          Warner    Dark Knight   1005     1841
                                              Warner    Tenet          363     2204

                                              5 rows (every row preserved)
```

### The Window Specification

Every window function requires a **window spec** that defines:

1. **PARTITION BY** -- which groups of rows form each window
2. **ORDER BY** -- how rows are ordered within each partition
3. **Frame** -- which subset of the partition to include (optional)

```
  Window = PARTITION BY studio ORDER BY release_year
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

  +---------+------------------+------+---------+---------------+
  | studio  | title            | year | revenue | running_total |
  +---------+------------------+------+---------+---------------+
  | Marvel  | Iron Man         | 2008 |  585    |  585          | <-- partition 1
  | Marvel  | Avengers         | 2019 | 2797    | 3382          |
  +---------+------------------+------+---------+---------------+
  | Warner  | Dark Knight      | 2008 | 1005    | 1005          | <-- partition 2
  | Warner  | Inception        | 2010 |  836    | 1841          |
  | Warner  | Tenet            | 2020 |  363    | 2204          |
  +---------+------------------+------+---------+---------------+
```

### Ranking Functions

Use these to order rows within each partition:

```
  Ordering movies within the studio based on revenue:

  studio    title            revenue  ROW_NUMBER  RANK  DENSE_RANK
  ------    -----            -------  ----------  ----  ----------
  Marvel    Avengers         2797     1           1     1
  Marvel    Iron Man          585     2           2     2

  Warner    Dark Knight      1005     1           1     1
  Warner    Inception         836     2           2     2
  Warner    Tenet             363     3           3     3
```

| Function | Ties Handling | Gaps After Ties |
|----------|---------------|-----------------|
| ROW_NUMBER | Arbitrary (no ties) | N/A |
| RANK | Same rank for ties | Yes -- skips next |
| DENSE_RANK | Same rank for ties | No -- consecutive |
| NTILE(n) | Distributes rows into n buckets | N/A |

### LAG and LEAD

Access values from preceding or following rows:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead

w = Window.partitionBy("studio").orderBy("release_year")

df.withColumn("prev_revenue", lag("revenue", 1).over(w))
df.withColumn("next_revenue", lead("revenue", 1).over(w))
```

```
  studio    title            year  revenue  prev_revenue  next_revenue
  ------    -----            ----  -------  ------------  ------------
  Warner    Dark Knight      2008  1005     null          836
  Warner    Inception        2010   836     1005          363
  Warner    Tenet            2020   363      836          null
```

### Frame Specifications

The frame clause controls which rows are included in the window calculation:

```
  ROWS BETWEEN <start> AND <end>

  Options for start/end:
    UNBOUNDED PRECEDING   -- from the first row of the partition
    n PRECEDING           -- n rows before current
    CURRENT ROW           -- the current row
    n FOLLOWING           -- n rows after current
    UNBOUNDED FOLLOWING   -- to the last row of the partition
```

Common frame patterns:

```
  Running total:    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  Moving avg (3):   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  Entire partition: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  Centered (5):     ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
```

**ROWS vs RANGE**: `ROWS` counts physical row positions. `RANGE` considers the
logical value in the ORDER BY column, which matters when there are ties or when
dealing with date/numeric ranges.

### Running Totals and Moving Averages

```
  Moving average (window = 3 rows) for a studio:

  title            year  revenue   window_rows          moving_avg
  -----            ----  -------   -----------          ----------
  Dark Knight      2008  1005      [1005]               1005.0
  Inception        2010   836      [1005, 836]           920.5
  Tenet            2020   363      [1005, 836, 363]      734.7
  New Film         2022   500      [836, 363, 500]       566.3
                                    ^-- oldest drops off
```

## Hands-On Walkthrough

Open the companion notebook `03-window-functions_notebook.py` which covers:

1. Creating a movies/studios dataset with revenue and release years
2. ROW_NUMBER, RANK, DENSE_RANK comparisons
3. Top-N per group (e.g., top 2 movies per studio by revenue)
4. LAG and LEAD for year-over-year comparison
5. Running totals with UNBOUNDED PRECEDING
6. Moving average with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
7. NTILE for quartile/percentile bucketing
8. RANGE BETWEEN for date-based windows
9. SQL equivalent using temporary views

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| Window functions | All Spark 2.0+ | Photon-optimized | All Spark 2.0+ |
| RANGE BETWEEN with dates | Requires numeric cast | Supported natively on DBR | Requires numeric cast |
| Performance | Sort within partition | Photon columnar engine | Sort within partition |
| AQE optimization | Spark 3.0+ | Enabled by default | Spark 3.0+ |

## Certification Tip

Window functions appear frequently on the Databricks certification:
- Know the difference between RANK (gaps) and DENSE_RANK (no gaps)
- Understand that ROW_NUMBER always produces unique sequential numbers
- Be able to write a Top-N query using `ROW_NUMBER() ... WHERE rn <= N`
- Know that window functions are wide transformations (require sort within
  each partition)

## Key Takeaways

1. Window functions compute per-row values without collapsing the result set.
2. Every window requires PARTITION BY (grouping) and ORDER BY (sorting).
3. ROW_NUMBER gives unique ranks; RANK/DENSE_RANK handle ties differently.
4. LAG/LEAD access offset rows for period-over-period comparisons.
5. Frame specs (ROWS BETWEEN / RANGE BETWEEN) control the calculation window.
6. Running totals use UNBOUNDED PRECEDING AND CURRENT ROW.
7. Moving averages use N PRECEDING AND CURRENT ROW.
8. NTILE distributes rows into equal-sized buckets for percentile analysis.

## Next Steps

Proceed to **Topic 04 -- Complex Types** to learn how to work with arrays,
maps, structs, and nested JSON in Spark.
