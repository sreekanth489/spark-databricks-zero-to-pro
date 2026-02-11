# Aggregations

> Module 04 -- Topic 02 | Level: Intermediate | Time: 40 min

## Learning Objectives

- Use groupBy with agg() to compute count, sum, avg, min, max
- Perform multiple aggregations in a single pass
- Reshape data with pivot and unpivot operations
- Understand rollup, cube, and grouping sets for multi-level summaries
- Use approximate aggregations for speed on massive datasets
- Collect values into lists and sets with collect_list / collect_set

## Conceptual Overview

### How Aggregations Work in Spark

An aggregation collapses many rows into fewer rows by computing summary
statistics. In Spark, every aggregation is a **wide transformation** that
triggers a shuffle -- data with the same group key must end up on the same
partition before the aggregate function can produce a result.

```
  BEFORE groupBy("category")          AFTER shuffle + aggregate

  Partition 0       Partition 1        Partition 0       Partition 1
 +----------+      +----------+       +----------+      +----------+
 | Elec  50 |      | Elec  30 |       | Elec  50 |      | Books 20 |
 | Books 20 |      | Toys  15 |  ==>  | Elec  30 |      | Books 10 |
 | Elec  40 |      | Books 10 |       | Elec  40 |      +----------+
 +----------+      +----------+       +----------+        sum = 30

                                        sum = 120

                  Shuffle moves all "Elec" rows to one partition
                  and all "Books" rows to another partition
```

Remember: In Spark, DataFrames are **immutable**. `df.groupBy(...)` does not
modify the original DataFrame -- it returns a new `GroupedData` object that
you then call `.agg()` on to produce a new DataFrame.

Also remember that due to **lazy evaluation**, calling `groupBy().agg()` does
not immediately execute. Spark creates a plan and only executes it when you
trigger an action.

### groupBy + agg -- The Fundamental Pattern

```python
from pyspark.sql.functions import count, sum, avg, min, max

result = orders_df.groupBy("category").agg(
    count("order_id").alias("total_orders"),
    sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_order_value"),
    min("amount").alias("min_order"),
    max("amount").alias("max_order"),
)
```

You can compute as many aggregations as you need inside a single `.agg()` call.
Spark will compute them all in one pass over the data.

### Pivot -- Rows to Columns

Pivot rotates distinct values of a column into separate columns:

```
  BEFORE pivot                    AFTER pivot("quarter")
+------+---------+-------+      +------+----+----+----+----+
| store| quarter | sales |      | store| Q1 | Q2 | Q3 | Q4 |
+------+---------+-------+      +------+----+----+----+----+
| NYC  | Q1      | 100   |      | NYC  | 100| 200| .. | .. |
| NYC  | Q2      | 200   |      | LA   | 80 | 150| .. | .. |
| LA   | Q1      |  80   |      +------+----+----+----+----+
| LA   | Q2      | 150   |
+------+---------+-------+
```

**Performance tip**: always pass the distinct values list to `.pivot()` when
you know them in advance. Without it, Spark runs an extra job to discover the
distinct values.

### Unpivot -- Columns to Rows

The reverse of pivot. In Spark 3.4+, use the `unpivot()` method or the
`stack()` SQL function to convert wide-format data back to long-format.

### Rollup, Cube, and Grouping Sets

These produce multi-level summaries in a single query:

| Function | What It Computes |
|----------|------------------|
| `rollup("A", "B")` | Subtotals for A, then grand total |
| `cube("A", "B")` | Subtotals for every combination of A and B |
| Grouping sets (SQL) | Explicitly listed combinations |

```
  rollup("region", "category")

  region   category   total_sales
  ------   --------   -----------
  East     Elec       500         <-- group (East, Elec)
  East     Books      200         <-- group (East, Books)
  East     null       700         <-- subtotal for East
  West     Elec       300
  West     null       300         <-- subtotal for West
  null     null       1000        <-- grand total
```

### Approximate Aggregations

For very large datasets where exact counts are not critical:

```python
from pyspark.sql.functions import approx_count_distinct

df.select(approx_count_distinct("user_id", rsd=0.05))
```

Uses HyperLogLog with a relative standard deviation of 5%. Orders of magnitude
faster than `countDistinct` on billions of rows.

### collect_list and collect_set

These aggregate functions collect values into an array column:

```python
df.groupBy("category").agg(
    collect_list("product_name").alias("all_products"),   # allows duplicates
    collect_set("product_name").alias("unique_products"), # no duplicates
)
```

**Warning**: if the group has millions of values, the resulting array can
exhaust executor memory. Use these on reasonably-sized groups.

## Hands-On Walkthrough

Open the companion notebook `02-aggregations_notebook.py` which demonstrates:

1. Creating an e-commerce orders dataset
2. Basic groupBy with count, sum, avg, min, max
3. Multiple aggregations in one pass
4. Pivot (quarterly sales by store)
5. Unpivot back to long format
6. Rollup and cube for hierarchical summaries
7. approx_count_distinct vs countDistinct comparison
8. collect_list and collect_set examples
9. Temporary views and SQL aggregations

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| Partial aggregation | Spark default | Photon-accelerated | Spark default |
| approx_count_distinct | All Spark 2.1+ | All DBR | All Spark 2.1+ |
| unpivot() method | Spark 3.4+ | DBR 13.0+ | Spark 3.4+ |
| Adaptive partition coalescing | AQE config | Enabled by default | AQE config |

## Certification Tip

Expect questions on:
- The difference between `count("col")` (excludes nulls) and `count("*")` (includes nulls)
- Knowing that groupBy is a wide transformation that causes a shuffle
- Understanding when to use rollup vs cube
- Recognizing that pivot requires an aggregation function

## Key Takeaways

1. `groupBy().agg()` is the standard pattern for all aggregations.
2. Aggregations are wide transformations -- they cause shuffles.
3. Pivot converts row values to columns; pass distinct values for performance.
4. Rollup gives hierarchical subtotals; cube gives all-combination subtotals.
5. Use `approx_count_distinct` on large datasets when exact counts are unnecessary.
6. `collect_list` and `collect_set` gather grouped values into arrays.
7. Always check for null handling: `count(col)` skips nulls.

## Next Steps

Proceed to **Topic 03 -- Window Functions** to learn how to compute
aggregations without collapsing rows (running totals, rankings, etc.).
