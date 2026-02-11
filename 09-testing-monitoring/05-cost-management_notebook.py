# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 - Cost Management
# MAGIC > Module 09 -- Topic 05 | DBU estimation, cluster sizing, and cost-efficient coding patterns
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Calculate DBU consumption estimates for different cluster configurations
# MAGIC 2. Compare cost of job clusters vs all-purpose clusters
# MAGIC 3. Demonstrate cost-efficient coding patterns (narrow vs wide transformations)
# MAGIC 4. Compare broadcast join vs shuffle join performance and cost
# MAGIC 5. Analyze storage costs: file sizes and partition strategy impact
# MAGIC 6. Build a cost estimation utility function
# MAGIC 7. Show tagging best practices for cost allocation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BooleanType
)
from datetime import datetime, timedelta
import random
import time

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: DBU Cost Estimation Calculator
# MAGIC
# MAGIC This utility helps estimate monthly Databricks costs for different configurations.
# MAGIC Prices are illustrative and based on publicly available list pricing.

# COMMAND ----------

# Illustrative DBU rates (actual rates vary by cloud, region, and contract)
DBU_RATES = {
    "jobs_compute": 0.15,          # per DBU
    "all_purpose_compute": 0.40,   # per DBU
    "sql_compute": 0.22,           # per DBU
    "dlt_compute": 0.20,           # per DBU (Core tier)
    "dlt_compute_pro": 0.25,       # per DBU (Pro tier)
    "dlt_compute_advanced": 0.36,  # per DBU (Advanced tier)
    "serverless_compute": 0.70,    # per DBU
}

# DBUs per hour by instance type (illustrative)
INSTANCE_DBUS = {
    "i3.xlarge": 1.0,      # 4 cores, 30.5 GB
    "i3.2xlarge": 2.0,     # 8 cores, 61 GB
    "m5.xlarge": 0.75,     # 4 cores, 16 GB
    "m5.2xlarge": 1.5,     # 8 cores, 32 GB
    "m5.4xlarge": 3.0,     # 16 cores, 64 GB
    "r5.xlarge": 0.75,     # 4 cores, 32 GB
    "r5.2xlarge": 1.5,     # 8 cores, 64 GB
    "c5.2xlarge": 1.0,     # 8 cores, 16 GB
    "c5.4xlarge": 2.0,     # 16 cores, 32 GB
}

# Cloud VM hourly costs (on-demand, illustrative for us-east-1)
VM_COSTS_ONDEMAND = {
    "i3.xlarge": 0.312,
    "i3.2xlarge": 0.624,
    "m5.xlarge": 0.192,
    "m5.2xlarge": 0.384,
    "m5.4xlarge": 0.768,
    "r5.xlarge": 0.252,
    "r5.2xlarge": 0.504,
    "c5.2xlarge": 0.340,
    "c5.4xlarge": 0.680,
}

# Spot discount (typical 60-80% off)
SPOT_DISCOUNT = 0.70  # 70% off on-demand

print("Cost reference data loaded.")
print(f"DBU rates: {len(DBU_RATES)} workload types")
print(f"Instance types: {len(INSTANCE_DBUS)} configurations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Estimation Function

# COMMAND ----------

def estimate_cluster_cost(
    instance_type: str,
    num_workers: int,
    hours_per_day: float,
    days_per_month: int,
    workload_type: str = "jobs_compute",
    spot_workers: bool = False,
    include_driver: bool = True,
) -> dict:
    """
    Estimate monthly Databricks cluster cost.

    Returns a breakdown of DBU cost, VM cost, and total.
    """
    dbu_per_instance = INSTANCE_DBUS.get(instance_type, 1.0)
    dbu_rate = DBU_RATES.get(workload_type, 0.15)
    vm_hourly = VM_COSTS_ONDEMAND.get(instance_type, 0.50)

    total_nodes = num_workers + (1 if include_driver else 0)
    total_hours = hours_per_day * days_per_month

    # DBU cost: all nodes consume DBUs
    total_dbus = dbu_per_instance * total_nodes * total_hours
    dbu_cost = total_dbus * dbu_rate

    # VM cost: driver on-demand, workers optionally on spot
    driver_vm_cost = vm_hourly * total_hours if include_driver else 0
    if spot_workers:
        worker_vm_cost = vm_hourly * (1 - SPOT_DISCOUNT) * num_workers * total_hours
    else:
        worker_vm_cost = vm_hourly * num_workers * total_hours

    total_vm_cost = driver_vm_cost + worker_vm_cost
    total_cost = dbu_cost + total_vm_cost

    return {
        "instance_type": instance_type,
        "num_workers": num_workers,
        "workload_type": workload_type,
        "spot_workers": spot_workers,
        "hours_per_day": hours_per_day,
        "days_per_month": days_per_month,
        "total_dbus": round(total_dbus, 1),
        "dbu_cost": round(dbu_cost, 2),
        "vm_cost": round(total_vm_cost, 2),
        "total_monthly_cost": round(total_cost, 2),
    }

print("Cost estimation function defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Configurations

# COMMAND ----------

configurations = [
    # Scenario 1: Production ETL (8 hrs/day, 30 days)
    estimate_cluster_cost("m5.2xlarge", 4, 8, 30, "jobs_compute", spot_workers=True),
    estimate_cluster_cost("m5.2xlarge", 4, 8, 30, "all_purpose_compute", spot_workers=False),

    # Scenario 2: Same workload, different sizing
    estimate_cluster_cost("m5.xlarge", 8, 8, 30, "jobs_compute", spot_workers=True),
    estimate_cluster_cost("m5.4xlarge", 2, 8, 30, "jobs_compute", spot_workers=True),

    # Scenario 3: Interactive development (10 hrs/day, 22 weekdays)
    estimate_cluster_cost("m5.2xlarge", 2, 10, 22, "all_purpose_compute", spot_workers=False),
    estimate_cluster_cost("m5.2xlarge", 2, 4, 22, "all_purpose_compute", spot_workers=False),
]

config_schema = StructType([
    StructField("instance_type", StringType()),
    StructField("num_workers", IntegerType()),
    StructField("workload_type", StringType()),
    StructField("spot_workers", BooleanType()),
    StructField("hours_per_day", DoubleType()),
    StructField("days_per_month", IntegerType()),
    StructField("total_dbus", DoubleType()),
    StructField("dbu_cost", DoubleType()),
    StructField("vm_cost", DoubleType()),
    StructField("total_monthly_cost", DoubleType()),
])

config_data = [
    (
        c["instance_type"], c["num_workers"], c["workload_type"],
        c["spot_workers"], c["hours_per_day"], c["days_per_month"],
        c["total_dbus"], c["dbu_cost"], c["vm_cost"], c["total_monthly_cost"]
    )
    for c in configurations
]

config_df = spark.createDataFrame(config_data, schema=config_schema)

print("Monthly Cost Comparison:")
config_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Insight: Job Cluster with Spot vs All-Purpose On-Demand

# COMMAND ----------

job_spot = configurations[0]
ap_ondemand = configurations[1]

savings = ap_ondemand["total_monthly_cost"] - job_spot["total_monthly_cost"]
savings_pct = savings / ap_ondemand["total_monthly_cost"] * 100

print(f"Job Cluster (spot workers):      ${job_spot['total_monthly_cost']:>10,.2f}/month")
print(f"All-Purpose (on-demand workers): ${ap_ondemand['total_monthly_cost']:>10,.2f}/month")
print(f"Monthly savings:                 ${savings:>10,.2f} ({savings_pct:.0f}%)")
print(f"Annual savings:                  ${savings * 12:>10,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Cost-Efficient Coding Patterns
# MAGIC
# MAGIC Generate a dataset to demonstrate how different coding patterns affect performance
# MAGIC (and therefore cost, since compute time = DBU consumption).

# COMMAND ----------

# Generate transaction data
num_transactions = 1_000_000

txn_data = [
    (
        i + 1,
        f"CUST_{random.randint(1, 10000):05d}",
        random.choice(["Electronics", "Clothing", "Food", "Books", "Home", "Auto", "Health"]),
        round(random.uniform(1.0, 5000.0), 2),
        random.randint(1, 20),
        datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364)),
        random.choice(["US", "CA", "UK", "DE", "FR", "JP"]),
        f"extra_field_{i % 100}",
    )
    for i in range(num_transactions)
]

txn_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("txn_date", TimestampType(), False),
    StructField("country", StringType(), False),
    StructField("extra_data", StringType(), False),
])

txn_df = spark.createDataFrame(txn_data, schema=txn_schema)
txn_df.cache()
txn_df.count()

print(f"Transaction dataset ready: {txn_df.count()} rows, {len(txn_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 1: Column Pruning (Select Only What You Need)

# COMMAND ----------

# EXPENSIVE: Read all columns then aggregate
start = time.time()
result_all_cols = (
    txn_df
    .groupBy("category")
    .agg(F.sum("amount").alias("total_amount"))
    .collect()
)
time_all_cols = time.time() - start

# CHEAPER: Select only needed columns first
start = time.time()
result_pruned = (
    txn_df
    .select("category", "amount")
    .groupBy("category")
    .agg(F.sum("amount").alias("total_amount"))
    .collect()
)
time_pruned = time.time() - start

print(f"All columns then aggregate: {time_all_cols:.3f}s")
print(f"Select first then aggregate: {time_pruned:.3f}s")
print(f"Difference: {abs(time_all_cols - time_pruned):.3f}s")
print("\nNote: With Delta + column pruning, Spark only reads required columns")
print("from storage. The impact grows with table width (many columns).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 2: Filter Early (Predicate Pushdown)

# COMMAND ----------

# EXPENSIVE: Aggregate everything, then filter results
start = time.time()
result_late_filter = (
    txn_df
    .groupBy("category", "country")
    .agg(F.sum("amount").alias("total"))
    .filter(F.col("country") == "US")
    .collect()
)
time_late = time.time() - start

# CHEAPER: Filter first, then aggregate (less data to shuffle)
start = time.time()
result_early_filter = (
    txn_df
    .filter(F.col("country") == "US")
    .groupBy("category")
    .agg(F.sum("amount").alias("total"))
    .collect()
)
time_early = time.time() - start

print(f"Aggregate then filter: {time_late:.3f}s")
print(f"Filter then aggregate: {time_early:.3f}s")
print(f"\nNote: Early filtering reduces the data volume BEFORE the shuffle,")
print("which means less network transfer and fewer tasks in the aggregation stage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 3: Broadcast Join vs Shuffle Join

# COMMAND ----------

# Create a small lookup table
category_details = spark.createDataFrame([
    ("Electronics", "Tech", 0.08),
    ("Clothing", "Retail", 0.10),
    ("Food", "Grocery", 0.02),
    ("Books", "Media", 0.05),
    ("Home", "Retail", 0.10),
    ("Auto", "Transport", 0.12),
    ("Health", "Medical", 0.00),
], ["category", "department", "tax_rate"])

# EXPENSIVE: Shuffle join (both sides shuffled across the network)
start = time.time()
shuffle_result = txn_df.join(category_details, "category")
shuffle_count = shuffle_result.count()
time_shuffle = time.time() - start

# CHEAPER: Broadcast the small table (no shuffle needed)
start = time.time()
broadcast_result = txn_df.join(F.broadcast(category_details), "category")
broadcast_count = broadcast_result.count()
time_broadcast = time.time() - start

print(f"Shuffle join: {time_shuffle:.3f}s ({shuffle_count} rows)")
print(f"Broadcast join: {time_broadcast:.3f}s ({broadcast_count} rows)")
print(f"\nBroadcast join sends the small table to all executors.")
print("This eliminates the shuffle of the large table entirely.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Plans: Broadcast vs Shuffle

# COMMAND ----------

print("=== Shuffle Join Plan ===")
txn_df.join(category_details, "category").explain("simple")

# COMMAND ----------

print("=== Broadcast Join Plan ===")
txn_df.join(F.broadcast(category_details), "category").explain("simple")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:**
# MAGIC - Shuffle join shows `Exchange hashpartitioning` (shuffle) on BOTH sides
# MAGIC - Broadcast join shows `BroadcastExchange` on the small table only
# MAGIC - The cost difference grows linearly with the size of the large table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern 4: Avoid Unnecessary Actions

# COMMAND ----------

# EXPENSIVE: Calling count() just to check if data exists
start = time.time()
if txn_df.filter(F.col("amount") > 10000).count() > 0:
    result = "Has high-value transactions"
time_count = time.time() - start

# CHEAPER: Use .first() or .head(1) to check existence
start = time.time()
first_row = txn_df.filter(F.col("amount") > 10000).head(1)
if len(first_row) > 0:
    result = "Has high-value transactions"
time_head = time.time() - start

print(f"count() > 0 check: {time_count:.3f}s (scans ALL matching data)")
print(f"head(1) check:     {time_head:.3f}s (stops after first match)")
print(f"\nhead(1) is sufficient to check existence. count() scans everything.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Storage Cost Analysis
# MAGIC
# MAGIC Demonstrate how partition strategy affects the number and size of files.

# COMMAND ----------

# Write data with different partition strategies to temp locations
import tempfile
import os

temp_base = tempfile.mkdtemp()

# Strategy 1: No partitioning
no_part_path = os.path.join(temp_base, "no_partition")
txn_df.write.format("parquet").mode("overwrite").save(no_part_path)

# Strategy 2: Partition by country (6 partitions)
country_part_path = os.path.join(temp_base, "country_partition")
txn_df.write.format("parquet").mode("overwrite").partitionBy("country").save(country_part_path)

# Strategy 3: Partition by country + category (42 partitions)
multi_part_path = os.path.join(temp_base, "multi_partition")
txn_df.write.format("parquet").mode("overwrite").partitionBy("country", "category").save(multi_part_path)

print("Data written with three partition strategies.")

# COMMAND ----------

# Analyze file counts and sizes
def analyze_storage(path, strategy_name):
    """Count files and total size for a parquet directory."""
    import os
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet"):
                file_count += 1
                total_size += os.path.getsize(os.path.join(root, f))
    avg_size_mb = (total_size / file_count / 1024 / 1024) if file_count > 0 else 0
    total_size_mb = total_size / 1024 / 1024
    return (strategy_name, file_count, round(total_size_mb, 2), round(avg_size_mb, 2))


storage_results = [
    analyze_storage(no_part_path, "No Partition"),
    analyze_storage(country_part_path, "By Country (6)"),
    analyze_storage(multi_part_path, "By Country+Category (42)"),
]

storage_schema = StructType([
    StructField("strategy", StringType()),
    StructField("file_count", IntegerType()),
    StructField("total_size_mb", DoubleType()),
    StructField("avg_file_size_mb", DoubleType()),
])

storage_df = spark.createDataFrame(storage_results, schema=storage_schema)
print("Storage Analysis by Partition Strategy:")
storage_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation:**
# MAGIC - More partitions = more files = more metadata overhead
# MAGIC - Over-partitioning creates many small files (the "small file problem")
# MAGIC - Each small file incurs fixed overhead for file listing, task scheduling, and I/O
# MAGIC - OPTIMIZE compacts small files back into larger, more efficient files
# MAGIC - Rule of thumb: each partition should contain at least 1 GB of data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Monthly Storage Cost Estimator

# COMMAND ----------

# Cloud storage pricing (per GB per month, illustrative)
STORAGE_PRICING = {
    "aws_s3_standard": 0.023,
    "aws_s3_infrequent": 0.0125,
    "azure_adls_hot": 0.02,
    "azure_adls_cool": 0.01,
    "gcp_gcs_standard": 0.02,
    "gcp_gcs_nearline": 0.01,
}

def estimate_storage_cost(
    current_data_gb: float,
    delta_versions_retained: int,
    avg_version_change_pct: float,
    storage_tier: str = "aws_s3_standard"
) -> dict:
    """
    Estimate monthly storage cost for a Delta table.

    Parameters:
        current_data_gb: Size of the current version of the table
        delta_versions_retained: Number of old versions kept (before VACUUM)
        avg_version_change_pct: Average % of data changed per version (0-1)
        storage_tier: Cloud storage pricing tier
    """
    price_per_gb = STORAGE_PRICING.get(storage_tier, 0.023)

    # Each old version stores only the changed files
    old_versions_gb = current_data_gb * avg_version_change_pct * delta_versions_retained
    total_storage_gb = current_data_gb + old_versions_gb
    monthly_cost = total_storage_gb * price_per_gb

    return {
        "current_data_gb": current_data_gb,
        "old_versions_gb": round(old_versions_gb, 1),
        "total_storage_gb": round(total_storage_gb, 1),
        "storage_tier": storage_tier,
        "monthly_cost": round(monthly_cost, 2),
    }


# Compare scenarios
scenarios = [
    # Table with daily updates, 30-day retention (no VACUUM)
    estimate_storage_cost(100, 30, 0.10, "aws_s3_standard"),
    # Same table with 7-day VACUUM
    estimate_storage_cost(100, 7, 0.10, "aws_s3_standard"),
    # Large table with frequent updates, no VACUUM
    estimate_storage_cost(1000, 30, 0.05, "aws_s3_standard"),
    # Large table with 7-day VACUUM
    estimate_storage_cost(1000, 7, 0.05, "aws_s3_standard"),
]

scenario_names = [
    "100GB, 30-day retention",
    "100GB, 7-day VACUUM",
    "1TB, 30-day retention",
    "1TB, 7-day VACUUM",
]

print("Storage Cost Comparison:")
print("-" * 80)
for name, s in zip(scenario_names, scenarios):
    print(f"  {name:<30s}  Current: {s['current_data_gb']:>8.0f} GB  "
          f"Old versions: {s['old_versions_gb']:>8.1f} GB  "
          f"Total: {s['total_storage_gb']:>8.1f} GB  "
          f"Cost: ${s['monthly_cost']:>8.2f}/mo")

vacuum_savings = scenarios[2]["monthly_cost"] - scenarios[3]["monthly_cost"]
print(f"\nVACUUM savings for 1TB table: ${vacuum_savings:.2f}/month (${vacuum_savings * 12:.2f}/year)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Comprehensive Cost Estimation Report

# COMMAND ----------

def generate_cost_report(
    pipeline_name: str,
    instance_type: str,
    num_workers: int,
    runtime_hours_per_day: float,
    days_per_month: int,
    data_size_gb: float,
    workload_type: str = "jobs_compute",
    spot_workers: bool = True,
    delta_retention_days: int = 7,
    daily_change_pct: float = 0.10,
) -> dict:
    """Generate a comprehensive cost estimate for a pipeline."""
    compute = estimate_cluster_cost(
        instance_type=instance_type,
        num_workers=num_workers,
        hours_per_day=runtime_hours_per_day,
        days_per_month=days_per_month,
        workload_type=workload_type,
        spot_workers=spot_workers,
    )

    storage = estimate_storage_cost(
        current_data_gb=data_size_gb,
        delta_versions_retained=delta_retention_days,
        avg_version_change_pct=daily_change_pct,
    )

    return {
        "pipeline": pipeline_name,
        "compute_monthly": compute["total_monthly_cost"],
        "storage_monthly": storage["monthly_cost"],
        "total_monthly": round(compute["total_monthly_cost"] + storage["monthly_cost"], 2),
        "total_annual": round((compute["total_monthly_cost"] + storage["monthly_cost"]) * 12, 2),
        "dbus_monthly": compute["total_dbus"],
    }


# Estimate costs for a typical data platform
platform_pipelines = [
    generate_cost_report("orders_etl", "m5.2xlarge", 4, 2, 30, 500, "jobs_compute", True),
    generate_cost_report("customer_sync", "m5.xlarge", 2, 1, 30, 100, "jobs_compute", True),
    generate_cost_report("analytics_build", "r5.2xlarge", 4, 3, 30, 1000, "jobs_compute", True),
    generate_cost_report("dev_workspace", "m5.2xlarge", 2, 8, 22, 50, "all_purpose_compute", False),
    generate_cost_report("sql_warehouse", "m5.4xlarge", 2, 10, 30, 200, "sql_compute", False),
]

report_schema = StructType([
    StructField("pipeline", StringType()),
    StructField("compute_monthly", DoubleType()),
    StructField("storage_monthly", DoubleType()),
    StructField("total_monthly", DoubleType()),
    StructField("total_annual", DoubleType()),
    StructField("dbus_monthly", DoubleType()),
])

report_data = [
    (p["pipeline"], p["compute_monthly"], p["storage_monthly"],
     p["total_monthly"], p["total_annual"], p["dbus_monthly"])
    for p in platform_pipelines
]

report_df = spark.createDataFrame(report_data, schema=report_schema)

print("Platform Cost Estimate:")
report_df.show(truncate=False)

# Total
total_monthly = sum(p["total_monthly"] for p in platform_pipelines)
total_annual = sum(p["total_annual"] for p in platform_pipelines)
print(f"Total Monthly: ${total_monthly:,.2f}")
print(f"Total Annual:  ${total_annual:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Tagging Best Practices
# MAGIC
# MAGIC Tags enable cost allocation and accountability. Here is a reference configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC   "cluster_tags": {
# MAGIC     "team": "data-engineering",
# MAGIC     "project": "customer-360",
# MAGIC     "environment": "production",
# MAGIC     "cost_center": "CC-4521",
# MAGIC     "owner": "data-platform-team@company.com",
# MAGIC     "sla_tier": "gold"
# MAGIC   },
# MAGIC   "job_tags": {
# MAGIC     "pipeline": "orders-bronze-to-silver",
# MAGIC     "schedule": "daily",
# MAGIC     "data_domain": "commerce"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cost Allocation Query Template

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Databricks Account Console: Cost by team and project
# MAGIC -- (requires access to system.billing.usage table)
# MAGIC
# MAGIC SELECT
# MAGIC   usage_metadata.cluster_tags['team'] as team,
# MAGIC   usage_metadata.cluster_tags['project'] as project,
# MAGIC   usage_metadata.cluster_tags['environment'] as environment,
# MAGIC   sku_name as workload_type,
# MAGIC   SUM(usage_quantity) as total_dbus,
# MAGIC   SUM(usage_quantity * list_price) as estimated_cost
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date BETWEEN '2024-01-01' AND '2024-01-31'
# MAGIC GROUP BY 1, 2, 3, 4
# MAGIC ORDER BY estimated_cost DESC;
# MAGIC
# MAGIC -- Top 10 most expensive clusters last month
# MAGIC SELECT
# MAGIC   usage_metadata.cluster_id,
# MAGIC   usage_metadata.cluster_tags['team'] as team,
# MAGIC   SUM(usage_quantity) as total_dbus,
# MAGIC   SUM(usage_quantity * list_price) as estimated_cost
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY estimated_cost DESC
# MAGIC LIMIT 10;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Cost Optimization Checklist

# COMMAND ----------

checklist = [
    ("Use job clusters for production", "HIGH", "50-70% compute savings"),
    ("Enable spot instances for workers", "HIGH", "60-90% VM cost savings"),
    ("Right-size clusters (start small, measure)", "HIGH", "20-40% compute savings"),
    ("Run VACUUM weekly (7-day retention)", "MEDIUM", "10-30% storage savings"),
    ("Run OPTIMIZE on frequently queried tables", "MEDIUM", "Faster queries, less I/O cost"),
    ("Use broadcast joins for small tables", "MEDIUM", "Eliminates large-side shuffle"),
    ("Select only needed columns", "MEDIUM", "Reduces I/O and memory usage"),
    ("Filter early in transformations", "MEDIUM", "Reduces shuffle volume"),
    ("Tag all clusters and jobs", "LOW", "Enables cost accountability"),
    ("Set cluster auto-termination (15-30 min)", "HIGH", "Prevents idle cluster waste"),
    ("Review and terminate unused clusters weekly", "HIGH", "Eliminates forgotten clusters"),
    ("Consider Photon for scan-heavy workloads", "MEDIUM", "Lower net cost despite higher DBU rate"),
]

checklist_schema = StructType([
    StructField("optimization", StringType()),
    StructField("impact", StringType()),
    StructField("expected_savings", StringType()),
])

checklist_df = spark.createDataFrame(checklist, schema=checklist_schema)
print("Cost Optimization Checklist:")
checklist_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up cached DataFrames and temp files
txn_df.unpersist()

import shutil
shutil.rmtree(temp_base, ignore_errors=True)

print("Notebook 05-cost-management complete.")
print("Cached DataFrames unpersisted. Temporary files removed.")
