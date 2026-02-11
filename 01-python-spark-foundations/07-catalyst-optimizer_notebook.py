# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Catalyst Optimizer -- Hands-On
# MAGIC
# MAGIC Explore how Spark's Catalyst optimizer transforms your queries. Learn to read
# MAGIC execution plans, observe predicate pushdown, column pruning, and AQE in action.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Sample Data
# MAGIC
# MAGIC We create a moderately sized DataFrame to make optimizations observable.

# COMMAND ----------

from pyspark.sql.functions import col, rand, floor, lit, when, expr, round as spark_round

# Create a "sales" table with 500K rows
sales_df = (
    spark.range(0, 500000)
    .withColumn("customer_id", (col("id") % 1000).cast("int"))
    .withColumn("product_id", (col("id") % 50).cast("int"))
    .withColumn("store_id", (col("id") % 10).cast("int"))
    .withColumn("amount", spark_round(rand(seed=42) * 500 + 10, 2))
    .withColumn("quantity", (floor(rand(seed=99) * 10) + 1).cast("int"))
    .withColumn("region",
        when(col("store_id") < 3, "East")
        .when(col("store_id") < 6, "West")
        .when(col("store_id") < 8, "North")
        .otherwise("South")
    )
    .drop("id")
)

# Create a "products" table
products_df = spark.createDataFrame(
    [(i, f"Product_{i}", ["Electronics", "Clothing", "Food", "Home", "Sports"][i % 5],
      round(10.0 + i * 5.5, 2))
     for i in range(50)],
    ["product_id", "product_name", "category", "base_price"]
)

# Register views
sales_df.createOrReplaceTempView("sales")
products_df.createOrReplaceTempView("products")

print(f"Sales rows: {sales_df.count()}")
print(f"Product rows: {products_df.count()}")
sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic explain() -- Understanding the Physical Plan

# COMMAND ----------

# Simple filter and select
simple_query = sales_df.filter(col("amount") > 200).select("customer_id", "amount", "region")

print("=== Default explain() ===")
simple_query.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the Plan
# MAGIC
# MAGIC - `*` prefix = whole-stage code generation (fused operators)
# MAGIC - `FileScan` or `Scan` = data source read
# MAGIC - `Filter` = row filtering
# MAGIC - `Project` = column selection
# MAGIC - Operators with `(1)` share the same codegen stage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extended explain() -- All Plan Stages

# COMMAND ----------

print("=== Extended explain (all 4 stages) ===")
simple_query.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Formatted explain() -- Easiest to Read

# COMMAND ----------

aggregated = (
    sales_df
    .filter(col("region") == "East")
    .groupBy("customer_id")
    .agg({"amount": "sum", "quantity": "avg"})
    .filter(col("sum(amount)") > 1000)
)

print("=== Formatted explain ===")
aggregated.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Predicate Pushdown
# MAGIC
# MAGIC Catalyst pushes filters as close to the data source as possible.
# MAGIC Let us observe this in action.

# COMMAND ----------

# Without filter -- full scan
print("=== Full scan (no filter) ===")
sales_df.select("customer_id", "amount").explain()

# COMMAND ----------

# With filter -- predicate pushed to scan
print("=== With filter (predicate pushdown) ===")
sales_df.filter(col("store_id") == 5).select("customer_id", "amount").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predicate Pushdown with SQL

# COMMAND ----------

pushdown_sql = spark.sql("""
    SELECT customer_id, amount
    FROM sales
    WHERE region = 'East' AND amount > 300
""")

print("SQL predicate pushdown:")
pushdown_sql.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Column Pruning
# MAGIC
# MAGIC Catalyst reads only the columns needed for the final output.

# COMMAND ----------

# Selecting only 2 of 6 columns -- the scan should only read 2 columns
print("=== Column pruning ===")
sales_df.select("customer_id", "amount").explain(mode="formatted")
print(">> Notice: The scan reads only [customer_id, amount], not all 6 columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Constant Folding
# MAGIC
# MAGIC Constant expressions are evaluated at plan time.

# COMMAND ----------

# The expression (1 + 0.08) is folded into 1.08 at plan time
with_constant = sales_df.withColumn("with_tax", col("amount") * (1 + 0.08))
print("=== Constant folding ===")
with_constant.select("amount", "with_tax").explain()
print(">> Look for '1.08' in the plan -- the addition was computed at compile time.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Join Optimization
# MAGIC
# MAGIC Catalyst chooses the join strategy based on table sizes.
# MAGIC Small tables get broadcast for a BroadcastHashJoin.

# COMMAND ----------

# Join sales (500K rows) with products (50 rows)
joined = sales_df.join(products_df, "product_id")

print("=== Join plan ===")
joined.explain(mode="formatted")
print(">> The small 'products' table should be broadcast (BroadcastHashJoin).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast Threshold
# MAGIC
# MAGIC Spark broadcasts a table when its estimated size is below
# MAGIC `spark.sql.autoBroadcastJoinThreshold` (default: 10 MB).

# COMMAND ----------

print("Broadcast threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# Force a sort-merge join by disabling broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
print("\n=== With broadcast disabled ===")
sales_df.join(products_df, "product_id").explain()
print(">> Now you should see SortMergeJoin instead of BroadcastHashJoin.")

# Re-enable broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Comparing DataFrame API vs. SQL Plans
# MAGIC
# MAGIC Both produce identical optimized plans.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg

# DataFrame API version
df_version = (
    sales_df
    .filter(col("region") == "West")
    .groupBy("customer_id")
    .agg(_sum("amount").alias("total_amount"))
    .filter(col("total_amount") > 500)
    .orderBy(col("total_amount").desc())
)

# SQL version
sql_version = spark.sql("""
    SELECT customer_id, SUM(amount) AS total_amount
    FROM sales
    WHERE region = 'West'
    GROUP BY customer_id
    HAVING SUM(amount) > 500
    ORDER BY total_amount DESC
""")

print("=== DataFrame API plan ===")
df_version.explain()

print("\n=== SQL plan ===")
sql_version.explain()

print(">> Both plans should be identical (same optimized strategy).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-optimizes the plan at runtime based on actual data statistics.

# COMMAND ----------

# Check if AQE is enabled
aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
print(f"AQE enabled: {aqe_enabled}")

# Ensure AQE is on for this demo
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# COMMAND ----------

# Set a high number of shuffle partitions to see AQE coalesce them
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Run a groupBy that produces few output rows
aqe_demo = (
    sales_df
    .groupBy("region")
    .agg(_sum("amount").alias("total"))
    .orderBy("total")
)

# Force materialization
aqe_demo.show()

print(f"Output partitions: {aqe_demo.rdd.getNumPartitions()}")
print(">> With AQE, Spark may coalesce 200 shuffle partitions into fewer,")
print("   since only 4 regions exist (very small output).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Whole-Stage Code Generation
# MAGIC
# MAGIC Operators marked with `*` are fused into a single generated function.

# COMMAND ----------

codegen_query = (
    sales_df
    .filter(col("amount") > 100)
    .filter(col("region") == "East")
    .select("customer_id", "amount", "quantity")
    .withColumn("total", col("amount") * col("quantity"))
)

print("=== Whole-stage codegen markers (*) ===")
codegen_query.explain()
print("\n>> Operators with * prefix are fused into one codegen stage.")
print("   This avoids virtual function call overhead between operators.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Examining the Generated Code

# COMMAND ----------

# Show the generated Java code (verbose but educational)
codegen_query.explain(mode="codegen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Optimization Impact -- Before vs. After
# MAGIC
# MAGIC Let us compare what happens with and without Catalyst's push-down rules
# MAGIC by examining row counts at each step.

# COMMAND ----------

# Query: filter after join vs. filter before join
# Catalyst should push the filter before the join

query_with_pushdown = (
    sales_df
    .join(products_df, "product_id")
    .filter(col("category") == "Electronics")
    .select("customer_id", "product_name", "amount")
)

print("=== Filter after join (Catalyst will push down) ===")
query_with_pushdown.explain(mode="formatted")
print("\n>> Look for the Filter on 'category' -- it should appear BEFORE the join,")
print("   meaning Catalyst pushed the predicate down to the products scan.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Optimization Observations
# MAGIC
# MAGIC | Optimization | What to Look For in explain() |
# MAGIC |---|---|
# MAGIC | Predicate pushdown | Filter appears inside/next to Scan, not at top |
# MAGIC | Column pruning | Scan reads only needed columns |
# MAGIC | Constant folding | Expressions simplified (1+0.08 becomes 1.08) |
# MAGIC | Broadcast join | BroadcastHashJoin for small tables |
# MAGIC | AQE coalesce | Fewer output partitions than shuffle.partitions |
# MAGIC | Whole-stage codegen | Operators prefixed with * |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.catalog.dropTempView("sales")
spark.catalog.dropTempView("products")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
print("Views dropped. Configuration reset. Notebook complete.")
