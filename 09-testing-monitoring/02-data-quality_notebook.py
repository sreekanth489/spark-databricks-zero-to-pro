# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Data Quality Frameworks
# MAGIC > Module 09 -- Topic 02 | Build a custom data quality framework and quarantine pattern
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate a realistic dataset with intentional quality issues
# MAGIC 2. Build a reusable data quality check framework from scratch
# MAGIC 3. Implement checks across all six quality dimensions
# MAGIC 4. Create a quality report DataFrame that summarizes results
# MAGIC 5. Demonstrate the quarantine pattern for isolating bad records
# MAGIC 6. Show DLT expectations syntax as reference examples
# MAGIC 7. Build a data profiling utility

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data with Quality Issues

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, LongType
)
from datetime import datetime, timedelta
import random

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customer Orders Dataset
# MAGIC We intentionally inject quality problems: nulls, duplicates, invalid values,
# MAGIC stale records, and format inconsistencies.

# COMMAND ----------

num_records = 1000

order_data = []
for i in range(num_records):
    order_id = i + 1
    customer_id = random.randint(1000, 1200)
    email = f"user{customer_id}@example.com"
    amount = round(random.uniform(5.0, 5000.0), 2)
    quantity = random.randint(1, 50)
    product_category = random.choice(["Electronics", "Clothing", "Food", "Books", "Home"])
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    country_code = random.choice(["US", "CA", "UK", "DE", "FR", "JP"])
    phone = f"({random.randint(200,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}"

    # Inject quality issues:

    # Completeness: ~5% null emails, ~3% null amounts
    if i % 20 == 0:
        email = None
    if i % 33 == 0:
        amount = None

    # Validity: ~2% negative amounts, ~2% invalid emails
    if i % 50 == 0:
        amount = round(random.uniform(-500.0, -1.0), 2)
    if i % 45 == 0 and email is not None:
        email = "not-an-email"

    # Validity: ~1% future dates
    if i % 100 == 0:
        order_date = datetime(2025, 6, 15)

    # Validity: ~3% invalid country codes
    if i % 35 == 0:
        country_code = random.choice(["XX", "ZZ", "123"])

    # Consistency: ~2% quantity = 0 but amount > 0
    if i % 55 == 0:
        quantity = 0

    # Phone format inconsistencies
    if i % 30 == 0:
        phone = f"{random.randint(2000000000, 9999999999)}"
    if i % 60 == 0:
        phone = None

    order_data.append((
        order_id, customer_id, email, amount, quantity,
        product_category, order_date, country_code, phone
    ))

# Add explicit duplicates for uniqueness testing (duplicate order_ids)
for dup_id in [5, 10, 15, 20, 25]:
    original = order_data[dup_id - 1]
    order_data.append(original)

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("email", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), False),
    StructField("product_category", StringType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("country_code", StringType(), False),
    StructField("phone", StringType(), True),
])

orders_df = spark.createDataFrame(order_data, schema=orders_schema)
print(f"Orders dataset: {orders_df.count()} rows (includes intentional quality issues)")
orders_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Data Quality Check Framework
# MAGIC
# MAGIC Each check function returns a standardized dictionary with the check name,
# MAGIC target column, total rows, failing rows, pass rate, and pass/fail status.

# COMMAND ----------

from typing import Dict, List, Any

def quality_check_result(
    check_name: str,
    column: str,
    total_rows: int,
    failing_rows: int,
    threshold: float = 1.0
) -> Dict[str, Any]:
    """Create a standardized quality check result."""
    pass_rate = (total_rows - failing_rows) / total_rows if total_rows > 0 else 1.0
    return {
        "check_name": check_name,
        "column": column,
        "dimension": "",
        "total_rows": total_rows,
        "failing_rows": failing_rows,
        "pass_rate": round(pass_rate, 4),
        "threshold": threshold,
        "passed": pass_rate >= threshold,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Completeness Checks (Are all required values present?)

# COMMAND ----------

def check_not_null(df: DataFrame, column: str, threshold: float = 1.0) -> Dict:
    """Check that a column has no null values."""
    total = df.count()
    null_count = df.filter(F.col(column).isNull()).count()
    result = quality_check_result("not_null", column, total, null_count, threshold)
    result["dimension"] = "completeness"
    return result


def check_not_empty_string(df: DataFrame, column: str, threshold: float = 1.0) -> Dict:
    """Check that a string column has no empty strings."""
    total = df.count()
    empty_count = df.filter(
        (F.col(column).isNull()) | (F.trim(F.col(column)) == "")
    ).count()
    result = quality_check_result("not_empty_string", column, total, empty_count, threshold)
    result["dimension"] = "completeness"
    return result


# Run completeness checks
completeness_results = [
    check_not_null(orders_df, "email", threshold=0.95),
    check_not_null(orders_df, "amount", threshold=0.97),
    check_not_null(orders_df, "phone", threshold=0.95),
]

for r in completeness_results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"[{status}] {r['check_name']}({r['column']}): {r['pass_rate']:.2%} "
          f"(threshold: {r['threshold']:.2%}, failing: {r['failing_rows']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uniqueness Checks (Are there duplicates?)

# COMMAND ----------

def check_unique(df: DataFrame, column: str, threshold: float = 1.0) -> Dict:
    """Check that a column contains only unique values."""
    total = df.count()
    distinct_count = df.select(column).distinct().count()
    duplicate_count = total - distinct_count
    result = quality_check_result("unique", column, total, duplicate_count, threshold)
    result["dimension"] = "uniqueness"
    return result


def check_unique_composite(df: DataFrame, columns: List[str], threshold: float = 1.0) -> Dict:
    """Check uniqueness across a composite key."""
    total = df.count()
    distinct_count = df.select(columns).distinct().count()
    duplicate_count = total - distinct_count
    col_str = "+".join(columns)
    result = quality_check_result("unique_composite", col_str, total, duplicate_count, threshold)
    result["dimension"] = "uniqueness"
    return result


uniqueness_results = [
    check_unique(orders_df, "order_id"),
]

for r in uniqueness_results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"[{status}] {r['check_name']}({r['column']}): {r['pass_rate']:.2%} "
          f"(duplicates: {r['failing_rows']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validity Checks (Do values conform to business rules?)

# COMMAND ----------

def check_range(df: DataFrame, column: str, min_val, max_val, threshold: float = 1.0) -> Dict:
    """Check that numeric values fall within an expected range."""
    non_null_df = df.filter(F.col(column).isNotNull())
    total = non_null_df.count()
    out_of_range = non_null_df.filter(
        (F.col(column) < min_val) | (F.col(column) > max_val)
    ).count()
    result = quality_check_result(f"range({min_val},{max_val})", column, total, out_of_range, threshold)
    result["dimension"] = "validity"
    return result


def check_regex_pattern(df: DataFrame, column: str, pattern: str, threshold: float = 1.0) -> Dict:
    """Check that string values match a regex pattern."""
    non_null_df = df.filter(F.col(column).isNotNull())
    total = non_null_df.count()
    non_matching = non_null_df.filter(~F.col(column).rlike(pattern)).count()
    result = quality_check_result(f"regex({pattern[:30]})", column, total, non_matching, threshold)
    result["dimension"] = "validity"
    return result


def check_values_in_set(df: DataFrame, column: str, valid_values: List, threshold: float = 1.0) -> Dict:
    """Check that values are members of an allowed set."""
    non_null_df = df.filter(F.col(column).isNotNull())
    total = non_null_df.count()
    invalid_count = non_null_df.filter(~F.col(column).isin(valid_values)).count()
    result = quality_check_result("values_in_set", column, total, invalid_count, threshold)
    result["dimension"] = "validity"
    return result


def check_no_future_dates(df: DataFrame, column: str, threshold: float = 1.0) -> Dict:
    """Check that date/timestamp values are not in the future."""
    total = df.filter(F.col(column).isNotNull()).count()
    future_count = df.filter(F.col(column) > F.current_timestamp()).count()
    result = quality_check_result("no_future_dates", column, total, future_count, threshold)
    result["dimension"] = "validity"
    return result


valid_countries = ["US", "CA", "UK", "DE", "FR", "JP", "AU", "IN", "BR", "MX"]
email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

validity_results = [
    check_range(orders_df, "amount", 0, 50000, threshold=0.95),
    check_regex_pattern(orders_df, "email", email_pattern, threshold=0.95),
    check_values_in_set(orders_df, "country_code", valid_countries, threshold=0.95),
    check_no_future_dates(orders_df, "order_date", threshold=0.99),
]

for r in validity_results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"[{status}] {r['check_name']}({r['column']}): {r['pass_rate']:.2%} "
          f"(failing: {r['failing_rows']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consistency Checks (Do values agree across columns?)

# COMMAND ----------

def check_consistency(df: DataFrame, condition: str, check_name: str, threshold: float = 1.0) -> Dict:
    """Check a cross-column consistency rule using a SQL expression."""
    total = df.count()
    failing = df.filter(~F.expr(condition)).count()
    result = quality_check_result(check_name, "multi-column", total, failing, threshold)
    result["dimension"] = "consistency"
    return result


consistency_results = [
    check_consistency(
        orders_df,
        "(quantity > 0) OR (amount IS NULL)",
        "quantity_positive_when_has_amount",
        threshold=0.95
    ),
]

for r in consistency_results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"[{status}] {r['check_name']}: {r['pass_rate']:.2%} (failing: {r['failing_rows']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Quality Report DataFrame
# MAGIC
# MAGIC Consolidate all check results into a single DataFrame for analysis and tracking.

# COMMAND ----------

all_results = completeness_results + uniqueness_results + validity_results + consistency_results

report_schema = StructType([
    StructField("check_name", StringType(), False),
    StructField("column", StringType(), False),
    StructField("dimension", StringType(), False),
    StructField("total_rows", IntegerType(), False),
    StructField("failing_rows", IntegerType(), False),
    StructField("pass_rate", DoubleType(), False),
    StructField("threshold", DoubleType(), False),
    StructField("passed", BooleanType(), False),
])

report_data = [
    (
        r["check_name"], r["column"], r["dimension"],
        r["total_rows"], r["failing_rows"], r["pass_rate"],
        r["threshold"], r["passed"]
    )
    for r in all_results
]

quality_report_df = spark.createDataFrame(report_data, schema=report_schema)
quality_report_df = quality_report_df.withColumn("check_timestamp", F.current_timestamp())

print("Quality Report:")
quality_report_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary by Dimension

# COMMAND ----------

dimension_summary = (
    quality_report_df
    .groupBy("dimension")
    .agg(
        F.count("*").alias("total_checks"),
        F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("checks_passed"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("checks_failed"),
        F.avg("pass_rate").alias("avg_pass_rate"),
    )
    .orderBy("dimension")
)

print("Quality Summary by Dimension:")
dimension_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Quarantine Pattern
# MAGIC
# MAGIC Instead of failing the pipeline when bad records are found, route them to a
# MAGIC quarantine table for investigation while allowing good records to proceed.

# COMMAND ----------

def quarantine_split(
    df: DataFrame,
    checks: List[Dict[str, str]]
) -> tuple:
    """
    Split a DataFrame into good and quarantined records.

    Parameters:
        df: Input DataFrame
        checks: List of dicts with 'condition' (SQL expr) and 'reason' keys.
                Records that FAIL any condition are quarantined.

    Returns:
        Tuple of (good_records_df, quarantined_records_df)
    """
    # Build a combined condition: a record is bad if ANY check fails
    quarantine_reasons = []
    for check in checks:
        condition = check["condition"]
        reason = check["reason"]
        quarantine_reasons.append(
            F.when(~F.expr(condition), F.lit(reason))
        )

    # Create a column that collects all failure reasons
    df_with_reasons = df.withColumn(
        "_failure_reasons",
        F.array_compact(F.array(*quarantine_reasons))
    )

    # Good records have zero failure reasons
    good_records = (
        df_with_reasons
        .filter(F.size("_failure_reasons") == 0)
        .drop("_failure_reasons")
    )

    # Quarantined records have at least one failure reason
    bad_records = (
        df_with_reasons
        .filter(F.size("_failure_reasons") > 0)
        .withColumn("quarantine_reasons", F.col("_failure_reasons"))
        .withColumn("quarantine_timestamp", F.current_timestamp())
        .drop("_failure_reasons")
    )

    return good_records, bad_records

# COMMAND ----------

# Define quarantine rules
quarantine_checks = [
    {"condition": "email IS NOT NULL", "reason": "missing_email"},
    {"condition": "amount IS NOT NULL AND amount > 0", "reason": "invalid_amount"},
    {"condition": "country_code IN ('US','CA','UK','DE','FR','JP','AU','IN','BR','MX')", "reason": "invalid_country"},
    {"condition": "order_date <= current_timestamp()", "reason": "future_order_date"},
    {"condition": "quantity > 0", "reason": "zero_quantity"},
]

good_orders, quarantined_orders = quarantine_split(orders_df, quarantine_checks)

total = orders_df.count()
good_count = good_orders.count()
quarantined_count = quarantined_orders.count()

print(f"Total records:       {total}")
print(f"Good records:        {good_count} ({good_count/total:.1%})")
print(f"Quarantined records: {quarantined_count} ({quarantined_count/total:.1%})")
print(f"\nGood + Quarantined = {good_count + quarantined_count} (should equal {total})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Quarantined Records

# COMMAND ----------

print("Sample quarantined records:")
quarantined_orders.select(
    "order_id", "email", "amount", "country_code", "order_date",
    "quantity", "quarantine_reasons", "quarantine_timestamp"
).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine Reason Distribution

# COMMAND ----------

reason_distribution = (
    quarantined_orders
    .select(F.explode("quarantine_reasons").alias("reason"))
    .groupBy("reason")
    .count()
    .orderBy(F.desc("count"))
)

print("Quarantine Reason Distribution:")
reason_distribution.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Data Profiling Utility
# MAGIC
# MAGIC Profiling helps you understand the shape of your data before defining quality rules.

# COMMAND ----------

def profile_dataframe(df: DataFrame) -> DataFrame:
    """Generate a data profile for all columns in a DataFrame."""
    profile_rows = []
    total_rows = df.count()

    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)

        # Null count
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = null_count / total_rows if total_rows > 0 else 0

        # Distinct count
        distinct_count = df.select(col_name).distinct().count()
        distinct_pct = distinct_count / total_rows if total_rows > 0 else 0

        # Min/Max (as strings for universal display)
        stats = df.agg(
            F.min(col_name).cast("string").alias("min_val"),
            F.max(col_name).cast("string").alias("max_val"),
        ).first()

        profile_rows.append((
            col_name,
            col_type,
            total_rows,
            null_count,
            round(null_pct, 4),
            distinct_count,
            round(distinct_pct, 4),
            stats["min_val"],
            stats["max_val"],
        ))

    profile_schema = StructType([
        StructField("column_name", StringType()),
        StructField("data_type", StringType()),
        StructField("total_rows", IntegerType()),
        StructField("null_count", IntegerType()),
        StructField("null_pct", DoubleType()),
        StructField("distinct_count", IntegerType()),
        StructField("distinct_pct", DoubleType()),
        StructField("min_value", StringType()),
        StructField("max_value", StringType()),
    ])

    return spark.createDataFrame(profile_rows, schema=profile_schema)


profile_df = profile_dataframe(orders_df)
print("Data Profile:")
profile_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: DLT Expectations Reference
# MAGIC
# MAGIC The following shows how the same quality checks would be written as DLT
# MAGIC expectations. This code is for reference only -- it requires a DLT pipeline
# MAGIC runtime to execute.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # ----- DLT Pipeline Definition (Reference Only) -----
# MAGIC
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Bronze: raw ingestion with schema enforcement
# MAGIC @dlt.table(comment="Raw orders from source system")
# MAGIC @dlt.expect("valid_order_id", "order_id IS NOT NULL")
# MAGIC def bronze_orders():
# MAGIC     return spark.read.format("json").load("/data/raw/orders/")
# MAGIC
# MAGIC # Silver: cleaned data with quality expectations
# MAGIC @dlt.table(comment="Cleaned orders with quality checks applied")
# MAGIC @dlt.expect("valid_email", "email IS NOT NULL AND email LIKE '%@%.%'")
# MAGIC @dlt.expect_or_drop("positive_amount", "amount > 0")
# MAGIC @dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
# MAGIC def silver_orders():
# MAGIC     return (
# MAGIC         dlt.read("bronze_orders")
# MAGIC         .withColumn("processed_at", F.current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC # Multiple expectations using expect_all
# MAGIC @dlt.table(comment="Validated orders with all business rules")
# MAGIC @dlt.expect_all_or_drop({
# MAGIC     "valid_amount": "amount > 0 AND amount < 50000",
# MAGIC     "valid_country": "country_code IN ('US','CA','UK','DE','FR','JP')",
# MAGIC     "valid_quantity": "quantity > 0",
# MAGIC     "not_future_order": "order_date <= current_timestamp()",
# MAGIC })
# MAGIC def gold_validated_orders():
# MAGIC     return dlt.read("silver_orders")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Quality Monitoring Over Time
# MAGIC
# MAGIC In production, you would persist quality reports to a Delta table and track
# MAGIC trends. Here we simulate multiple pipeline runs.

# COMMAND ----------

from datetime import date

# Simulate quality metrics from multiple pipeline runs
historical_data = [
    ("2024-01-01", "not_null", "email", "completeness", 0.952, True),
    ("2024-01-01", "unique", "order_id", "uniqueness", 0.998, True),
    ("2024-01-01", "range(0,50000)", "amount", "validity", 0.978, True),
    ("2024-01-02", "not_null", "email", "completeness", 0.948, True),
    ("2024-01-02", "unique", "order_id", "uniqueness", 0.999, True),
    ("2024-01-02", "range(0,50000)", "amount", "validity", 0.982, True),
    ("2024-01-03", "not_null", "email", "completeness", 0.920, False),  # degradation!
    ("2024-01-03", "unique", "order_id", "uniqueness", 0.995, True),
    ("2024-01-03", "range(0,50000)", "amount", "validity", 0.965, True),
    ("2024-01-04", "not_null", "email", "completeness", 0.875, False),  # further degradation!
    ("2024-01-04", "unique", "order_id", "uniqueness", 0.997, True),
    ("2024-01-04", "range(0,50000)", "amount", "validity", 0.971, True),
]

historical_schema = StructType([
    StructField("run_date", StringType()),
    StructField("check_name", StringType()),
    StructField("column", StringType()),
    StructField("dimension", StringType()),
    StructField("pass_rate", DoubleType()),
    StructField("passed", BooleanType()),
])

historical_df = spark.createDataFrame(historical_data, schema=historical_schema)

# Show trend for email completeness -- notice the degradation
print("Email Completeness Trend (notice the degradation over time):")
(
    historical_df
    .filter(F.col("check_name") == "not_null")
    .filter(F.col("column") == "email")
    .orderBy("run_date")
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detect Anomalies in Quality Trends

# COMMAND ----------

from pyspark.sql.window import Window

# Calculate rolling average and detect drops
window_spec = Window.partitionBy("check_name", "column").orderBy("run_date").rowsBetween(-2, 0)

trend_analysis = (
    historical_df
    .withColumn("rolling_avg", F.avg("pass_rate").over(window_spec))
    .withColumn(
        "anomaly",
        F.when(
            F.col("pass_rate") < F.col("rolling_avg") - 0.03,
            F.lit("DEGRADATION")
        ).otherwise(F.lit("NORMAL"))
    )
)

print("Quality Trend Analysis with Anomaly Detection:")
trend_analysis.orderBy("check_name", "column", "run_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC This notebook used only in-memory DataFrames. No tables or temp views were created.

# COMMAND ----------

print("Notebook 02-data-quality complete.")
print(f"Final stats: {good_count} good records, {quarantined_count} quarantined records.")
