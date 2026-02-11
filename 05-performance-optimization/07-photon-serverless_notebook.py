# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 - Photon & Serverless
# MAGIC > Module 05 — Topic 07 | Photon C++ engine, serverless compute, cost optimization
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. Check if your cluster has Photon enabled
# MAGIC 2. Run benchmark queries to observe Photon's acceleration
# MAGIC 3. Understand the pipeline: SQL Query -> Catalyst Optimizer -> Photon Executor -> Results
# MAGIC 4. Identify which operations Photon accelerates (and which it does not)
# MAGIC 5. Cost analysis for cluster selection

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Photon Status
# MAGIC
# MAGIC Photon Query Engine is a high-performance vectorized query engine written in C++.
# MAGIC Databricks rewrote everything from Scala to C++ for better performance.

# COMMAND ----------

# Check if Photon is enabled on this cluster
try:
    photon_enabled = spark.conf.get("spark.databricks.photon.enabled", "false")
    print(f"Photon enabled: {photon_enabled}")
except Exception as e:
    print(f"Photon config not available (likely not a Databricks cluster): {e}")
    photon_enabled = "false"

if photon_enabled == "true":
    print("\nYour cluster has Photon. Scans, filters, aggregations, and joins")
    print("will execute in the native C++ engine for 2-8x performance improvement.")
else:
    print("\nPhoton is NOT enabled. This notebook will still work, but benchmark")
    print("results will reflect standard Spark JVM execution.")
    print("To enable: create a cluster with a Photon-enabled runtime.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup: Create Benchmark Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import random
import time

studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]
countries = ["USA", "UK", "Japan", "France", "Germany", "India", "Canada", "Australia",
             "South Korea", "Brazil", "Mexico", "Spain", "Italy", "China"]

# Large dataset for benchmarking
num_rows = 5_000_000
data = [
    (i, f"Movie_{i}", studios[i % len(studios)], genres[i % len(genres)],
     countries[i % len(countries)], random.randint(1960, 2024),
     round(random.uniform(1.0, 10.0), 1), random.randint(1_000_000, 500_000_000),
     random.randint(50_000_000, 400_000_000))
    for i in range(num_rows)
]

schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("studio", StringType(), False),
    StructField("genre", StringType(), False),
    StructField("country", StringType(), False),
    StructField("release_year", IntegerType(), False),
    StructField("rating", DoubleType(), False),
    StructField("revenue", IntegerType(), False),
    StructField("budget", IntegerType(), False),
])

# Write to Delta for realistic benchmark
delta_path = "/tmp/perf_module/photon_benchmark"
movies_df = spark.createDataFrame(data, schema=schema)
movies_df.write.format("delta").mode("overwrite").save(delta_path)

# Register as table for SQL queries
spark.sql(f"CREATE TABLE IF NOT EXISTS photon_movies USING DELTA LOCATION '{delta_path}'")
print(f"Created benchmark table with {num_rows:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The Photon Pipeline
# MAGIC
# MAGIC ```
# MAGIC SQL Query --> Catalyst Optimizer --> Optimized Plan --> Photon Executor --> Results
# MAGIC ```
# MAGIC
# MAGIC Photon is the executor. It goes to your table where the data is stored in
# MAGIC Parquet format, scans ONLY columns that are required, then applies the filter.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark 1: Scan + Filter
# MAGIC Photon excels at scanning Parquet columns and applying filters.

# COMMAND ----------

def benchmark(name, query_fn, iterations=3):
    """Run a query multiple times and report average time."""
    times = []
    for i in range(iterations):
        start = time.time()
        query_fn()
        elapsed = time.time() - start
        times.append(elapsed)
    avg = sum(times) / len(times)
    print(f"{name}: avg={avg:.2f}s (runs: {[f'{t:.2f}s' for t in times]})")
    return avg

# Scan + Filter: read from Delta, filter on studio
scan_filter_time = benchmark(
    "Scan + Filter",
    lambda: spark.sql("""
        SELECT movie_id, title, revenue
        FROM photon_movies
        WHERE studio = 'Disney' AND release_year >= 2010
    """).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark 2: Aggregation
# MAGIC Photon's vectorized aggregation processes batches of values in single CPU ops.

# COMMAND ----------

agg_time = benchmark(
    "Aggregation",
    lambda: spark.sql("""
        SELECT studio, genre,
               COUNT(*) as movie_count,
               AVG(rating) as avg_rating,
               SUM(revenue) as total_revenue,
               AVG(revenue - budget) as avg_profit
        FROM photon_movies
        GROUP BY studio, genre
        ORDER BY total_revenue DESC
    """).collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark 3: Join
# MAGIC Photon uses optimized C++ hash tables for joins.

# COMMAND ----------

# Create a dimension table for joining
studio_dim = spark.createDataFrame([
    ("Warner Bros", "Tier 1", "AT&T"),
    ("Disney", "Tier 1", "Disney Corp"),
    ("Universal", "Tier 1", "Comcast"),
    ("Paramount", "Tier 1", "Paramount Global"),
    ("Sony", "Tier 2", "Sony Group"),
    ("Lionsgate", "Tier 3", "Lionsgate"),
    ("MGM", "Tier 3", "Amazon"),
    ("Fox", "Tier 2", "Disney Corp"),
], ["studio", "tier", "parent_company"])

studio_dim.createOrReplaceTempView("studio_dim")

join_time = benchmark(
    "Join + Aggregate",
    lambda: spark.sql("""
        SELECT d.tier, d.parent_company,
               COUNT(*) as movie_count,
               SUM(m.revenue) as total_revenue
        FROM photon_movies m
        JOIN studio_dim d ON m.studio = d.studio
        WHERE m.release_year >= 2000
        GROUP BY d.tier, d.parent_company
        ORDER BY total_revenue DESC
    """).collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark 4: String Operations
# MAGIC Photon has optimized string processing (unlike JVM which creates objects per string).

# COMMAND ----------

string_time = benchmark(
    "String Ops",
    lambda: spark.sql("""
        SELECT studio,
               UPPER(title) as upper_title,
               LENGTH(title) as title_len,
               CONCAT(studio, ' - ', genre) as label
        FROM photon_movies
        WHERE title LIKE '%100%' OR title LIKE '%999%'
    """).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What Photon Does NOT Accelerate
# MAGIC
# MAGIC Python UDFs run in a separate Python process, not in Photon's C++ engine.
# MAGIC RDD operations bypass the DataFrame/SQL optimizer entirely.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF -- this runs in Python, NOT Photon
@udf(StringType())
def classify_revenue_udf(revenue):
    if revenue is None:
        return "Unknown"
    elif revenue > 300_000_000:
        return "Blockbuster"
    elif revenue > 100_000_000:
        return "Hit"
    else:
        return "Regular"

udf_time = benchmark(
    "Python UDF (NO Photon benefit)",
    lambda: spark.read.format("delta").load(delta_path)
        .withColumn("category", classify_revenue_udf(F.col("revenue")))
        .groupBy("category").count().collect()
)

# Compare: same logic using built-in functions (Photon CAN accelerate these)
builtin_time = benchmark(
    "Built-in functions (Photon accelerated)",
    lambda: spark.read.format("delta").load(delta_path)
        .withColumn("category", F.when(F.col("revenue") > 300_000_000, "Blockbuster")
                                  .when(F.col("revenue") > 100_000_000, "Hit")
                                  .otherwise("Regular"))
        .groupBy("category").count().collect()
)

print(f"\nBuilt-in functions are {udf_time/builtin_time:.1f}x faster than Python UDFs")
print("Lesson: replace Python UDFs with built-in functions to leverage Photon")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspecting Photon in Execution Plans
# MAGIC
# MAGIC When Photon is enabled, the SQL tab in Spark UI shows Photon-specific
# MAGIC operators (e.g., PhotonGroupingAgg, PhotonBroadcastHashJoin).

# COMMAND ----------

# Check the plan for a complex query
complex_query = spark.sql("""
    SELECT studio, genre,
           COUNT(*) as cnt,
           PERCENTILE_APPROX(rating, 0.5) as median_rating,
           SUM(CASE WHEN revenue > budget THEN 1 ELSE 0 END) as profitable_count
    FROM photon_movies
    WHERE release_year BETWEEN 2010 AND 2024
    GROUP BY studio, genre
    HAVING COUNT(*) > 100
    ORDER BY cnt DESC
""")

print("=== Execution Plan ===")
print("(On Photon clusters, you will see Photon* operators in the SQL tab)")
complex_query.explain("formatted")
complex_query.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Tuning Checklist
# MAGIC
# MAGIC When your queries are slow, check these three things:
# MAGIC
# MAGIC | Problem | Symptom | Solution |
# MAGIC |---------|---------|----------|
# MAGIC | **Skew** | One task takes much longer than others | Salt keys, AQE, repartition |
# MAGIC | **Need More Memory** | Spill to disk, OOM errors, high GC | Increase executor memory, reduce partitions |
# MAGIC | **Need to Move to Photon** | Standard Spark for SQL/DF workloads | Switch to Photon cluster (2-8x faster) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cost Analysis: Choosing the Right Compute

# COMMAND ----------

# Cost comparison framework
print("""
COST OPTIMIZATION = minimize (compute_time x hourly_rate + storage_cost)

=== Example Cost Comparison ===

Query: Complex analytics on 1 TB Delta table
Standard Spark: 20 min execution, $5/hr DBU rate = $1.67
Photon Cluster: 6 min execution, $10/hr DBU rate  = $1.00  (40% cheaper!)
Serverless:     5 min execution, $14/hr DBU rate  = $1.17  (30% cheaper, no idle cost)

Key insight:
  - Photon costs more per hour but finishes faster -> lower total cost
  - Serverless has highest rate but no idle cost -> best for bursty workloads
  - Standard Spark is cheapest per hour but slowest -> highest total cost

For daily batch jobs (predictable):   -> Photon cluster (best price/performance)
For ad-hoc analytics (bursty):        -> Serverless (no idle cost)
For ML training (custom code):        -> Standard cluster (UDFs don't benefit from Photon)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Benchmark Summary

# COMMAND ----------

print("=" * 60)
print("BENCHMARK SUMMARY")
print("=" * 60)
print(f"  Scan + Filter:    {scan_filter_time:.2f}s")
print(f"  Aggregation:      {agg_time:.2f}s")
print(f"  Join + Aggregate: {join_time:.2f}s")
print(f"  String Ops:       {string_time:.2f}s")
print(f"  Python UDF:       {udf_time:.2f}s  (not Photon-accelerated)")
print(f"  Built-in Funcs:   {builtin_time:.2f}s  (Photon-accelerated)")
print("=" * 60)
print(f"\nPhoton status: {'ENABLED' if photon_enabled == 'true' else 'DISABLED'}")
if photon_enabled != "true":
    print("Re-run on a Photon cluster to see 2-8x improvement on first 4 benchmarks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key Point |
# MAGIC |---------|-----------|
# MAGIC | Photon | C++ vectorized engine, 2-8x faster for SQL/DF ops |
# MAGIC | Pipeline | SQL -> Catalyst -> Optimized Plan -> Photon Executor -> Results |
# MAGIC | Accelerated | Scans, filters, aggregations, joins, sorts, strings |
# MAGIC | NOT accelerated | Python UDFs, Scala UDFs, RDD operations |
# MAGIC | Serverless | Instant start, no idle cost, auto-scaling |
# MAGIC | Cost | Photon/Serverless = higher rate but lower total cost |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS photon_movies")
dbutils.fs.rm("/tmp/perf_module/photon_benchmark", recurse=True)
print("Cleanup complete. Module 05 finished!")
