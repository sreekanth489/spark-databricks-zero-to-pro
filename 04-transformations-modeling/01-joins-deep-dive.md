# Joins Deep Dive

> Module 04 -- Topic 01 | Level: Intermediate | Time: 50 min

## Learning Objectives

- Understand all seven Spark join types including left_semi and left_anti
- Explain why joins cause shuffles and how data moves between partitions
- Compare join strategies: broadcast hash join, sort-merge join, shuffle hash join
- Handle join skew, multi-column joins, and non-equi join conditions
- Read physical plans to verify which join strategy Spark selected

## Conceptual Overview

### Why Joins Are Expensive

A join combines rows from two datasets based on a matching condition. In a
single-machine tool like Pandas, both tables sit in the same memory space, so
the lookup is straightforward. In Spark, each table is split across many
partitions on different worker nodes.

Consider two tables -- `movies` and `studios` -- each spread across three
partitions:

```
  Node 1              Node 2              Node 3
+-----------+       +-----------+       +-----------+
| movies    |       | movies    |       | movies    |
| Avengers  |       | Inception |       | Frozen    |
| Iron Man  |       | Tenet     |       | Moana     |
+-----------+       +-----------+       +-----------+

+-----------+       +-----------+       +-----------+
| studios   |       | studios   |       | studios   |
| Marvel    |       | Disney    |       | Warner    |
| Pixar     |       | Lionsgate |       | Fox       |
+-----------+       +-----------+       +-----------+
```

The studio "Marvel" can be present in all these partitions, because when you
partition you did not partition by studio. To calculate average revenue of
Marvel movies, Spark has to move the data between these nodes/partitions so that
all Marvel records land on the same executor. This data movement is called a
**shuffle**, and it is the most expensive operation in distributed computing.

### Lazy Evaluation and Joins

In Pandas, writing `df1.merge(df2, on="key")` immediately executes the join and
produces a result DataFrame. In Spark, due to lazy evaluation, it will not
create a DataFrame -- it will just create a plan. The actual execution only
happens when you call an action like `.show()` or `.collect()`. This is why we
do lazy evaluation: for **performance** reasons and to be **memory efficient**.
Spark can analyze the entire plan and pick the optimal join strategy before
moving any data.

### The Seven Join Types

```
  Table A          Table B
+---------+      +---------+
| 1  foo  |      | 1  alpha|
| 2  bar  |      | 3  gamma|
| 3  baz  |      | 4  delta|
+---------+      +---------+

inner         =>  {1, 3}           Rows that match in BOTH tables
left          =>  {1, 2, 3}       All of A, matching B (or null)
right         =>  {1, 3, 4}       All of B, matching A (or null)
full (outer)  =>  {1, 2, 3, 4}   All rows from both, nulls where no match
cross         =>  A x B = 9 rows  Every combination (cartesian product)
left_semi     =>  {1, 3}          Rows in A that HAVE a match in B
left_anti     =>  {2}             Rows in A that have NO match in B
```

#### left_semi -- "List Known Customers"

A left_semi join returns only the rows from the left table that have a matching
row in the right table. It is equivalent to `WHERE EXISTS (...)` in SQL. Use
case: given a list of active subscribers, find all orders placed by known
customers.

#### left_anti -- "List Unknown Customers"

A left_anti join returns only the rows from the left table that do NOT have a
matching row in the right table. It is equivalent to `WHERE NOT EXISTS (...)`.
Use case: find all orders placed by customers not in our CRM system.

```
  movies                    studios
+----+------------+       +----+---------+
| id | title      |       | id | name    |
+----+------------+       +----+---------+
|  1 | Avengers   |       |  1 | Marvel  |
|  2 | Unknown    |       |  3 | Disney  |
|  3 | Frozen     |       +----+---------+
+----+------------+

left_semi  =>  movies with id IN (1, 3)     => Avengers, Frozen
               "List known movies (those with a studio)"

left_anti  =>  movies with id NOT IN (1, 3) => Unknown
               "List unknown movies (those WITHOUT a studio)"
```

### Narrow vs Wide Transformations

Not every Spark operation causes a shuffle:

| Type   | Examples                        | Shuffle? |
|--------|---------------------------------|----------|
| Narrow | select, filter, map, withColumn | No       |
| Wide   | join, groupBy, repartition, distinct | Yes  |

Wide transformations force data to move across the network. This is why a join
is orders of magnitude slower than a filter on the same data volume.

### Join Strategies

Spark selects a physical join strategy based on table sizes and configuration:

```
+------------------------------+------------------+-----------------------------+
| Strategy                     | When Used        | How It Works                |
+------------------------------+------------------+-----------------------------+
| Broadcast Hash Join (BHJ)    | One side < 10 MB | Small table broadcast to    |
|                              | (configurable)   | all executors; hash lookup  |
+------------------------------+------------------+-----------------------------+
| Sort-Merge Join (SMJ)        | Both sides large | Both sides sorted on key,   |
|                              |                  | then merged in linear scan  |
+------------------------------+------------------+-----------------------------+
| Shuffle Hash Join (SHJ)      | Medium + large   | Both sides shuffled by key, |
|                              |                  | hash table built on smaller |
+------------------------------+------------------+-----------------------------+
```

Broadcast hash join avoids shuffling the large table entirely:

```
  Driver broadcasts small table
  +--------+         +--------+         +--------+
  | Node 1 |         | Node 2 |         | Node 3 |
  | big_df |         | big_df |         | big_df |
  | +small |         | +small |         | +small |  <-- full copy on each node
  +--------+         +--------+         +--------+
      |                  |                  |
   local join         local join         local join   <-- no shuffle needed!
```

### Handling Skew

When one join key has far more rows than others (e.g., studio = "Marvel" has
10 million rows while others have 1,000), that partition becomes a bottleneck.
Strategies to handle skew:

1. **Salting** -- append a random suffix to the skewed key, join on the salted
   key, then aggregate. Spreads the hot key across multiple partitions.
2. **AQE Skew Join** -- Databricks Adaptive Query Execution automatically
   splits skewed partitions at runtime. Enable with
   `spark.sql.adaptive.skewJoin.enabled = true`.
3. **Broadcast the smaller side** -- if one table is small enough, broadcast
   it to avoid shuffles entirely.

### Repartitioning for Join Performance

```python
# Repartition by the join key before joining
df.repartition(6, "studio")
```

This will create 6 partitions but it will make sure that the records from the
same studio will be available only in a single partition. When both sides of a
join are pre-partitioned by the join key, Spark can skip the shuffle phase.

## Hands-On Walkthrough

Open the companion notebook `01-joins-deep-dive_notebook.py` which demonstrates:

1. Creating movies and studios sample data
2. All seven join types with output comparison
3. Using `explain(True)` to see physical plans and join strategies
4. Broadcast join hint vs automatic selection
5. Multi-column join example
6. Non-equi join (range join)
7. Skew detection and salting technique
8. Repartition-by-key optimization

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| AQE Skew Join | Spark 3.0+ config | Enabled by default on DBR 7.3+ | Spark 3.0+ config |
| Broadcast threshold | `spark.sql.autoBroadcastJoinThreshold` (10 MB default) | Same | Same |
| Join hints | All Spark hints supported | Additional Photon optimizations | All Spark hints supported |
| Cost-based optimizer | Manual ANALYZE TABLE | Auto-stats on Delta tables | Manual ANALYZE TABLE |

## Certification Tip

The Databricks Certified Data Engineer Associate exam frequently tests:
- Knowing when a broadcast join is used automatically (threshold setting)
- Understanding that left_semi is equivalent to `WHERE EXISTS`
- Recognizing that joins are wide transformations that trigger shuffles
- The difference between `explain()` output for broadcast vs sort-merge joins

## Key Takeaways

1. Joins are the most common source of shuffles in Spark applications.
2. left_semi finds rows WITH matches; left_anti finds rows WITHOUT matches.
3. Broadcast hash join eliminates shuffles when one side is small enough.
4. Use `explain(True)` to verify which join strategy Spark selected.
5. Pre-partitioning by the join key with `repartition(n, col)` can eliminate
   shuffle at join time if both sides share the same partitioning.
6. AQE skew join handling in Databricks automatically mitigates hot keys.
7. DataFrames are immutable -- every join produces a new DataFrame.

## Next Steps

Proceed to **Topic 02 -- Aggregations** to learn how `groupBy` operations also
cause shuffles and how to compute summary statistics efficiently.
