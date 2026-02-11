# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Testing Spark Code
# MAGIC > Module 09 -- Topic 01 | Write testable transformations and validate them with inline tests
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Write transformation functions that follow the pure-function pattern
# MAGIC 2. Build a lightweight DataFrame comparison utility
# MAGIC 3. Test transformations against edge cases: nulls, empty DataFrames, schema mismatches
# MAGIC 4. Test a UDF at both the pure-Python and DataFrame levels
# MAGIC 5. Demonstrate pytest-style inline assertions inside a Databricks notebook
# MAGIC 6. Implement a simple test runner that reports pass/fail results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data Generation

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from datetime import datetime, timedelta
import random

# Seed for reproducibility
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Sales Dataset
# MAGIC We create a realistic sales dataset with known characteristics so we can write
# MAGIC deterministic tests against it.

# COMMAND ----------

# Generate sample sales data
num_orders = 500
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headset"]
regions = ["North", "South", "East", "West"]
statuses = ["completed", "pending", "cancelled", "refunded"]

sales_data = []
for i in range(num_orders):
    order_id = i + 1
    product = products[i % len(products)]
    region = regions[i % len(regions)]
    status = statuses[i % len(statuses)]
    quantity = random.randint(1, 20)
    unit_price = round(random.uniform(10.0, 2000.0), 2)
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    # Intentionally introduce some nulls for testing
    if i % 50 == 0:
        product = None
    if i % 75 == 0:
        unit_price = None
    sales_data.append((order_id, product, region, status, quantity, unit_price, order_date))

sales_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("region", StringType(), False),
    StructField("status", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", TimestampType(), False),
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
print(f"Sales dataset: {sales_df.count()} rows, {len(sales_df.columns)} columns")
sales_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Writing Testable Transformation Functions
# MAGIC
# MAGIC The golden rule: **DataFrame in, DataFrame out.** No side effects.
# MAGIC No reading from tables. No writing to storage. Just pure transformations.

# COMMAND ----------

def calculate_revenue(df: DataFrame) -> DataFrame:
    """Calculate total revenue per order. Nulls in unit_price produce null revenue."""
    return df.withColumn(
        "revenue",
        F.col("quantity") * F.col("unit_price")
    )


def filter_completed_orders(df: DataFrame) -> DataFrame:
    """Keep only orders with status = 'completed'."""
    return df.filter(F.col("status") == "completed")


def aggregate_by_region(df: DataFrame) -> DataFrame:
    """Aggregate total revenue and order count by region."""
    return (
        df.groupBy("region")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("revenue").alias("avg_revenue")
        )
    )


def add_revenue_category(df: DataFrame) -> DataFrame:
    """Classify orders into revenue categories based on thresholds."""
    return df.withColumn(
        "revenue_category",
        F.when(F.col("revenue").isNull(), "unknown")
        .when(F.col("revenue") < 100, "low")
        .when(F.col("revenue") < 1000, "medium")
        .when(F.col("revenue") < 5000, "high")
        .otherwise("premium")
    )


def clean_phone_number(phone):
    """Pure Python function for cleaning phone numbers (used as UDF)."""
    if phone is None:
        return None
    digits = "".join(c for c in phone if c.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits

print("Transformation functions defined successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: DataFrame Comparison Utility
# MAGIC
# MAGIC In a real project you would use the `chispa` library. Here we build a manual
# MAGIC comparison utility that works without external dependencies.

# COMMAND ----------

class DataFrameTestResult:
    """Container for test results with detailed failure messages."""

    def __init__(self, passed, message=""):
        self.passed = passed
        self.message = message

    def __repr__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.message}"


def assert_schema_equal(actual: DataFrame, expected: DataFrame) -> DataFrameTestResult:
    """Compare schemas of two DataFrames."""
    if actual.schema == expected.schema:
        return DataFrameTestResult(True, "Schemas match")
    else:
        actual_fields = {f.name: f.dataType for f in actual.schema.fields}
        expected_fields = {f.name: f.dataType for f in expected.schema.fields}
        missing = set(expected_fields) - set(actual_fields)
        extra = set(actual_fields) - set(expected_fields)
        type_mismatches = {
            name: (actual_fields[name], expected_fields[name])
            for name in set(actual_fields) & set(expected_fields)
            if actual_fields[name] != expected_fields[name]
        }
        msg = f"Schema mismatch - Missing: {missing}, Extra: {extra}, Type mismatches: {type_mismatches}"
        return DataFrameTestResult(False, msg)


def assert_dataframes_equal(
    actual: DataFrame,
    expected: DataFrame,
    ignore_row_order: bool = True,
    ignore_nullable: bool = True
) -> DataFrameTestResult:
    """Compare two DataFrames for equality (schema + data)."""
    # Compare schemas (optionally ignoring nullable)
    if ignore_nullable:
        actual_types = [(f.name, f.dataType) for f in actual.schema.fields]
        expected_types = [(f.name, f.dataType) for f in expected.schema.fields]
        if actual_types != expected_types:
            return DataFrameTestResult(
                False,
                f"Schema mismatch:\n  actual:   {actual_types}\n  expected: {expected_types}"
            )
    else:
        schema_result = assert_schema_equal(actual, expected)
        if not schema_result.passed:
            return schema_result

    # Compare row counts
    actual_count = actual.count()
    expected_count = expected.count()
    if actual_count != expected_count:
        return DataFrameTestResult(
            False,
            f"Row count mismatch: actual={actual_count}, expected={expected_count}"
        )

    # Compare data
    if ignore_row_order:
        actual_rows = sorted([str(row) for row in actual.collect()])
        expected_rows = sorted([str(row) for row in expected.collect()])
    else:
        actual_rows = [str(row) for row in actual.collect()]
        expected_rows = [str(row) for row in expected.collect()]

    if actual_rows != expected_rows:
        # Find first difference
        for i, (a, e) in enumerate(zip(actual_rows, expected_rows)):
            if a != e:
                return DataFrameTestResult(
                    False, f"Data mismatch at row {i}:\n  actual:   {a}\n  expected: {e}"
                )

    return DataFrameTestResult(True, f"DataFrames match ({actual_count} rows)")


print("DataFrame comparison utilities defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Inline Test Runner
# MAGIC
# MAGIC We build a lightweight test runner that collects results and prints a summary.
# MAGIC This pattern works inside Databricks notebooks without needing pytest.

# COMMAND ----------

class NotebookTestRunner:
    """Simple test runner for inline notebook tests."""

    def __init__(self, suite_name):
        self.suite_name = suite_name
        self.results = []

    def run_test(self, test_name, test_fn):
        """Run a single test function and capture the result."""
        try:
            test_fn()
            self.results.append((test_name, True, ""))
        except AssertionError as e:
            self.results.append((test_name, False, str(e)))
        except Exception as e:
            self.results.append((test_name, False, f"Unexpected error: {type(e).__name__}: {e}"))

    def summary(self):
        """Print a formatted test summary."""
        passed = sum(1 for _, p, _ in self.results if p)
        failed = sum(1 for _, p, _ in self.results if not p)
        total = len(self.results)

        print(f"\n{'='*70}")
        print(f"  Test Suite: {self.suite_name}")
        print(f"  Results: {passed} passed, {failed} failed, {total} total")
        print(f"{'='*70}")
        for name, success, msg in self.results:
            status = "PASS" if success else "FAIL"
            print(f"  [{status}] {name}")
            if msg:
                print(f"         {msg}")
        print(f"{'='*70}\n")
        return failed == 0


runner = NotebookTestRunner("Spark Transformation Tests")
print("Test runner initialized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Test the Revenue Calculation

# COMMAND ----------

def test_calculate_revenue_basic():
    """Revenue = quantity * unit_price for normal rows."""
    input_df = spark.createDataFrame(
        [(1, "Laptop", "North", "completed", 3, 1000.0, datetime(2024, 1, 1))],
        schema=sales_schema
    )
    result = calculate_revenue(input_df)
    revenue_value = result.select("revenue").first()[0]
    assert revenue_value == 3000.0, f"Expected 3000.0, got {revenue_value}"


def test_calculate_revenue_null_price():
    """Revenue should be null when unit_price is null."""
    input_df = spark.createDataFrame(
        [(1, "Laptop", "North", "completed", 3, None, datetime(2024, 1, 1))],
        schema=sales_schema
    )
    result = calculate_revenue(input_df)
    revenue_value = result.select("revenue").first()[0]
    assert revenue_value is None, f"Expected None, got {revenue_value}"


def test_calculate_revenue_preserves_columns():
    """Revenue calculation should not drop any existing columns."""
    input_df = spark.createDataFrame(
        [(1, "Laptop", "North", "completed", 3, 100.0, datetime(2024, 1, 1))],
        schema=sales_schema
    )
    result = calculate_revenue(input_df)
    expected_cols = set(sales_schema.fieldNames()) | {"revenue"}
    actual_cols = set(result.columns)
    assert actual_cols == expected_cols, f"Column mismatch: {actual_cols} vs {expected_cols}"


runner.run_test("Revenue: basic calculation", test_calculate_revenue_basic)
runner.run_test("Revenue: null unit_price", test_calculate_revenue_null_price)
runner.run_test("Revenue: preserves columns", test_calculate_revenue_preserves_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Test the Filter Function

# COMMAND ----------

def test_filter_completed_only():
    """Only completed orders should survive the filter."""
    input_df = spark.createDataFrame([
        (1, "Laptop", "North", "completed", 1, 100.0, datetime(2024, 1, 1)),
        (2, "Phone", "South", "pending", 1, 100.0, datetime(2024, 1, 2)),
        (3, "Tablet", "East", "cancelled", 1, 100.0, datetime(2024, 1, 3)),
        (4, "Monitor", "West", "completed", 1, 100.0, datetime(2024, 1, 4)),
    ], schema=sales_schema)
    result = filter_completed_orders(input_df)
    assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
    statuses = [row["status"] for row in result.collect()]
    assert all(s == "completed" for s in statuses), f"Found non-completed: {statuses}"


def test_filter_empty_dataframe():
    """Filtering an empty DataFrame should return an empty DataFrame (not error)."""
    empty_df = spark.createDataFrame([], schema=sales_schema)
    result = filter_completed_orders(empty_df)
    assert result.count() == 0, "Expected 0 rows from empty input"
    assert result.schema == empty_df.schema, "Schema should be preserved"


def test_filter_no_matching_rows():
    """When no rows match, result should be empty but structurally valid."""
    input_df = spark.createDataFrame([
        (1, "Laptop", "North", "pending", 1, 100.0, datetime(2024, 1, 1)),
        (2, "Phone", "South", "cancelled", 1, 100.0, datetime(2024, 1, 2)),
    ], schema=sales_schema)
    result = filter_completed_orders(input_df)
    assert result.count() == 0, f"Expected 0, got {result.count()}"


runner.run_test("Filter: completed only", test_filter_completed_only)
runner.run_test("Filter: empty DataFrame", test_filter_empty_dataframe)
runner.run_test("Filter: no matching rows", test_filter_no_matching_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Test the Aggregation

# COMMAND ----------

def test_aggregate_by_region():
    """Aggregation should produce one row per region with correct totals."""
    input_df = spark.createDataFrame([
        (1, "Laptop", "North", "completed", 2, 100.0, datetime(2024, 1, 1)),
        (2, "Phone", "North", "completed", 3, 200.0, datetime(2024, 1, 2)),
        (3, "Tablet", "South", "completed", 1, 50.0, datetime(2024, 1, 3)),
    ], schema=sales_schema)
    with_revenue = calculate_revenue(input_df)
    result = aggregate_by_region(with_revenue)

    assert result.count() == 2, f"Expected 2 regions, got {result.count()}"

    north = result.filter(F.col("region") == "North").first()
    assert north["total_revenue"] == 800.0, f"North revenue: expected 800.0, got {north['total_revenue']}"
    assert north["order_count"] == 2, f"North count: expected 2, got {north['order_count']}"


def test_aggregate_single_region():
    """Single-region input should produce exactly one output row."""
    input_df = spark.createDataFrame([
        (1, "Laptop", "East", "completed", 5, 100.0, datetime(2024, 1, 1)),
    ], schema=sales_schema)
    with_revenue = calculate_revenue(input_df)
    result = aggregate_by_region(with_revenue)
    assert result.count() == 1, f"Expected 1 region, got {result.count()}"


runner.run_test("Aggregate: by region", test_aggregate_by_region)
runner.run_test("Aggregate: single region", test_aggregate_single_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Test the Revenue Category Classification

# COMMAND ----------

def test_revenue_category_boundaries():
    """Test all category boundaries including null."""
    input_df = spark.createDataFrame([
        (1, "A", "N", "completed", 1, 50.0, datetime(2024, 1, 1)),     # revenue=50 -> low
        (2, "B", "N", "completed", 1, 500.0, datetime(2024, 1, 1)),    # revenue=500 -> medium
        (3, "C", "N", "completed", 1, 2000.0, datetime(2024, 1, 1)),   # revenue=2000 -> high
        (4, "D", "N", "completed", 5, 2000.0, datetime(2024, 1, 1)),   # revenue=10000 -> premium
        (5, "E", "N", "completed", 1, None, datetime(2024, 1, 1)),     # revenue=null -> unknown
    ], schema=sales_schema)
    with_revenue = calculate_revenue(input_df)
    result = add_revenue_category(with_revenue)

    rows = {row["order_id"]: row["revenue_category"] for row in result.collect()}
    assert rows[1] == "low", f"Order 1: expected 'low', got '{rows[1]}'"
    assert rows[2] == "medium", f"Order 2: expected 'medium', got '{rows[2]}'"
    assert rows[3] == "high", f"Order 3: expected 'high', got '{rows[3]}'"
    assert rows[4] == "premium", f"Order 4: expected 'premium', got '{rows[4]}'"
    assert rows[5] == "unknown", f"Order 5: expected 'unknown', got '{rows[5]}'"


runner.run_test("Revenue Category: all boundaries", test_revenue_category_boundaries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Test a UDF (Pure Python + DataFrame Level)

# COMMAND ----------

# Level 1: Pure Python tests -- no Spark needed, instant execution
def test_clean_phone_standard():
    assert clean_phone_number("(555) 123-4567") == "5551234567"

def test_clean_phone_with_country_code():
    assert clean_phone_number("+1-555-123-4567") == "5551234567"

def test_clean_phone_none():
    assert clean_phone_number(None) is None

def test_clean_phone_short():
    assert clean_phone_number("12345") == "12345"

runner.run_test("UDF Pure: standard format", test_clean_phone_standard)
runner.run_test("UDF Pure: country code", test_clean_phone_with_country_code)
runner.run_test("UDF Pure: None input", test_clean_phone_none)
runner.run_test("UDF Pure: short number", test_clean_phone_short)

# Level 2: DataFrame-level UDF test
from pyspark.sql.functions import udf

clean_phone_udf = udf(clean_phone_number, StringType())

def test_clean_phone_udf_in_dataframe():
    phone_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("phone", StringType(), True),
    ])
    input_df = spark.createDataFrame([
        (1, "(555) 123-4567"),
        (2, None),
        (3, "+1-800-555-0199"),
    ], schema=phone_schema)

    result = input_df.withColumn("cleaned_phone", clean_phone_udf("phone"))
    rows = {row["id"]: row["cleaned_phone"] for row in result.collect()}
    assert rows[1] == "5551234567", f"Expected '5551234567', got '{rows[1]}'"
    assert rows[2] is None, f"Expected None, got '{rows[2]}'"
    assert rows[3] == "8005550199", f"Expected '8005550199', got '{rows[3]}'"


runner.run_test("UDF DataFrame: phone cleaning", test_clean_phone_udf_in_dataframe)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Test Schema Validation

# COMMAND ----------

def test_schema_match():
    """Verify that two DataFrames with identical schemas pass the check."""
    df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
    df2 = spark.createDataFrame([(2, "b")], ["id", "name"])
    result = assert_schema_equal(df1, df2)
    assert result.passed, f"Expected pass, got: {result.message}"


def test_schema_mismatch_detected():
    """Verify that mismatched schemas are caught."""
    df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
    df2 = spark.createDataFrame([(1, 100)], ["id", "value"])
    result = assert_schema_equal(df1, df2)
    assert not result.passed, "Expected schema mismatch to be detected"


def test_dataframe_equality_pass():
    """Verify full DataFrame equality check passes for identical data."""
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "name"])
    result = assert_dataframes_equal(df1, df2, ignore_row_order=True)
    assert result.passed, f"Expected pass, got: {result.message}"


def test_dataframe_equality_row_count_mismatch():
    """Verify that different row counts are detected."""
    df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
    df2 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = assert_dataframes_equal(df1, df2)
    assert not result.passed, "Expected row count mismatch"
    assert "Row count mismatch" in result.message


runner.run_test("Schema: match", test_schema_match)
runner.run_test("Schema: mismatch detected", test_schema_mismatch_detected)
runner.run_test("DataFrame Equality: pass", test_dataframe_equality_pass)
runner.run_test("DataFrame Equality: row count mismatch", test_dataframe_equality_row_count_mismatch)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Test a Full Mini-Pipeline

# COMMAND ----------

def test_full_pipeline():
    """End-to-end test: raw data -> revenue -> filter -> aggregate."""
    raw = spark.createDataFrame([
        (1, "Laptop", "North", "completed", 2, 500.0, datetime(2024, 3, 1)),
        (2, "Phone", "North", "pending", 1, 200.0, datetime(2024, 3, 2)),
        (3, "Tablet", "South", "completed", 3, 300.0, datetime(2024, 3, 3)),
        (4, "Mouse", "South", "completed", 10, 25.0, datetime(2024, 3, 4)),
    ], schema=sales_schema)

    # Run the pipeline
    with_revenue = calculate_revenue(raw)
    completed = filter_completed_orders(with_revenue)
    aggregated = aggregate_by_region(completed)

    # Verify
    assert aggregated.count() == 2, f"Expected 2 regions, got {aggregated.count()}"

    north = aggregated.filter(F.col("region") == "North").first()
    south = aggregated.filter(F.col("region") == "South").first()

    # North: order 1 only (completed), revenue = 2 * 500 = 1000
    assert north["total_revenue"] == 1000.0, f"North: expected 1000.0, got {north['total_revenue']}"
    assert north["order_count"] == 1

    # South: orders 3 and 4 (both completed), revenue = 900 + 250 = 1150
    assert south["total_revenue"] == 1150.0, f"South: expected 1150.0, got {south['total_revenue']}"
    assert south["order_count"] == 2


runner.run_test("Pipeline: end-to-end", test_full_pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: Test Results Summary

# COMMAND ----------

all_passed = runner.summary()

if all_passed:
    print("All tests passed! Your transformations are solid.")
else:
    print("Some tests failed. Review the output above for details.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 13: pytest-Style Patterns (Reference)
# MAGIC
# MAGIC When running tests outside Databricks (e.g., in a CI/CD pipeline), here is how
# MAGIC the same tests would look with pytest. This cell is for reference only.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # File: tests/conftest.py
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC @pytest.fixture(scope="session")
# MAGIC def spark():
# MAGIC     session = (
# MAGIC         SparkSession.builder
# MAGIC         .master("local[*]")
# MAGIC         .appName("unit-tests")
# MAGIC         .config("spark.sql.shuffle.partitions", "4")
# MAGIC         .config("spark.ui.enabled", "false")
# MAGIC         .getOrCreate()
# MAGIC     )
# MAGIC     yield session
# MAGIC     session.stop()
# MAGIC
# MAGIC # File: tests/integration/test_revenue.py
# MAGIC from transforms.revenue import calculate_revenue
# MAGIC
# MAGIC def test_calculate_revenue_basic(spark):
# MAGIC     input_df = spark.createDataFrame(
# MAGIC         [(1, 3, 1000.0)], ["order_id", "quantity", "unit_price"]
# MAGIC     )
# MAGIC     result = calculate_revenue(input_df)
# MAGIC     assert result.first()["revenue"] == 3000.0
# MAGIC
# MAGIC # Run: pytest tests/ -v --tb=short
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 14: Nutter Framework Reference
# MAGIC
# MAGIC The nutter framework enables notebook-level testing in Databricks CI/CD.
# MAGIC Below is a reference pattern (requires the nutter library installed on the cluster).

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Nutter test notebook -- runs inside Databricks
# MAGIC from runtime.nutterfixture import NutterFixture, tag
# MAGIC
# MAGIC class TestSalesPipeline(NutterFixture):
# MAGIC
# MAGIC     def assertion_revenue_column_exists(self):
# MAGIC         df = spark.sql("SELECT * FROM sales_with_revenue LIMIT 1")
# MAGIC         assert "revenue" in df.columns
# MAGIC
# MAGIC     def assertion_no_negative_revenue(self):
# MAGIC         bad_rows = spark.sql(
# MAGIC             "SELECT COUNT(*) as cnt FROM sales_with_revenue WHERE revenue < 0"
# MAGIC         ).first()["cnt"]
# MAGIC         assert bad_rows == 0, f"Found {bad_rows} rows with negative revenue"
# MAGIC
# MAGIC     @tag("slow")
# MAGIC     def assertion_row_count_in_range(self):
# MAGIC         count = spark.sql("SELECT COUNT(*) as cnt FROM sales_with_revenue").first()["cnt"]
# MAGIC         assert 400 <= count <= 600, f"Row count {count} out of expected range"
# MAGIC
# MAGIC result = TestSalesPipeline().execute_tests()
# MAGIC print(result.to_string())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC This notebook used only in-memory DataFrames, so no cleanup is required.
# MAGIC No tables, temp views, or files were created.

# COMMAND ----------

print("Notebook 01-testing-spark-code complete.")
