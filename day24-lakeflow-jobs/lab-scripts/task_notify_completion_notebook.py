# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2: Pipeline Summary Report
# MAGIC
# MAGIC This notebook runs as the final task in a Lakeflow Job.
# MAGIC It queries the Gold layer views and prints a summary report.

# COMMAND ----------

print("Task 2: Pipeline Summary Report -- STARTED")

# COMMAND ----------

# Read task values from the upstream task (defaults for interactive runs)
total_orders = dbutils.jobs.taskValues.get(
    taskKey="build_gold_tables",
    key="total_orders",
    default=10,
    debugValue=10,
)
completed_orders = dbutils.jobs.taskValues.get(
    taskKey="build_gold_tables",
    key="completed_orders",
    default=8,
    debugValue=8,
)

print(f"Received from upstream task:")
print(f"  Total orders:     {total_orders}")
print(f"  Completed orders: {completed_orders}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top stores by net revenue
# MAGIC SELECT
# MAGIC     store_name,
# MAGIC     net_revenue,
# MAGIC     total_orders,
# MAGIC     avg_order_value
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY net_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall totals across all stores
# MAGIC SELECT
# MAGIC     SUM(total_orders)      AS total_completed_orders,
# MAGIC     SUM(unique_customers)  AS total_unique_customers,
# MAGIC     ROUND(SUM(net_revenue), 2)  AS total_net_revenue,
# MAGIC     ROUND(AVG(avg_order_value), 2) AS overall_avg_order_value
# MAGIC FROM gold_daily_revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Order status breakdown
# MAGIC SELECT
# MAGIC     status,
# MAGIC     order_count,
# MAGIC     total_value,
# MAGIC     ROUND(order_count * 100.0 / SUM(order_count) OVER (), 1) AS pct_of_total
# MAGIC FROM gold_order_status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# Print the final summary
cancellation_rate = round((total_orders - completed_orders) / total_orders * 100, 1) if total_orders > 0 else 0

print("=" * 50)
print("  PIPELINE RUN SUMMARY")
print("=" * 50)
print(f"  Total orders processed:  {total_orders}")
print(f"  Completed orders:        {completed_orders}")
print(f"  Cancellation rate:       {cancellation_rate}%")
print("=" * 50)

if cancellation_rate > 20:
    print("  WARNING: High cancellation rate detected!")
else:
    print("  STATUS: All metrics within normal range")

print()
print("Task 2: Pipeline Summary Report -- FINISHED")
