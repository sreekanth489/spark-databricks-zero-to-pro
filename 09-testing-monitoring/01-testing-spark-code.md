# Testing Spark Code
> Module 09 -- Topic 01 | Level: Intermediate | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:
1. Structure PySpark transformations as testable pure functions
2. Create pytest fixtures that provide a local SparkSession
3. Compare DataFrames for equality using both manual and library-based approaches
4. Test UDFs, edge cases (nulls, empty DataFrames), and schema mismatches
5. Organize tests into unit, integration, and end-to-end tiers
6. Mock external dependencies (databases, APIs, cloud storage)
7. Understand the nutter framework for Databricks notebook testing
8. Write inline test patterns inside Databricks notebooks

---

## Conceptual Overview

### Why Testing Spark Code Is Different

Testing Spark code introduces challenges that do not exist in standard Python:

- **Distributed execution** -- Code runs on a cluster, not a single process. A
  transformation that works on the driver may fail when executors serialize closures.
- **Schema enforcement** -- DataFrames have schemas. A test must validate both data
  values and column types/names.
- **Lazy evaluation** -- Transformations are not executed until an action triggers
  them. Errors may surface far from where they were introduced.
- **Heavy startup cost** -- SparkSession initialization takes seconds, so test suite
  design must minimize session creation.

The core principle is simple: **treat every transformation as a pure function**.
A function takes a DataFrame in and returns a DataFrame out. No side effects, no
reading from disk, no writing to tables. When you follow this pattern, Spark code
becomes as testable as any Python function.

```
  Testable Transformation Pattern
  ================================

  +------------------+      +-----------------------+      +------------------+
  | Input DataFrame  | ---> | Transformation Fn()   | ---> | Output DataFrame |
  +------------------+      +-----------------------+      +------------------+

  Tests provide the input, call the function, assert on the output.
  No cluster, no tables, no files -- just DataFrames in memory.
```

### The Testing Pyramid for Spark

```
              /\
             /  \          E2E Tests
            /    \         - Full pipeline runs on a cluster
           /------\        - Validate end-to-end data flow
          /        \       - Slowest, most expensive
         /----------\
        /            \     Integration Tests
       /              \    - Test with real SparkSession
      /                \   - Verify schema, joins, aggregations
     /------------------\  - Medium speed
    /                    \
   /                      \ Unit Tests
  /________________________\ - Pure Python logic (no Spark)
                             - UDF logic, config parsing, helpers
                             - Fastest, cheapest
```

**Recommended ratio**: 70% unit tests, 20% integration tests, 10% E2E tests.

---

## Testing Tools and Frameworks

### pytest with SparkSession Fixtures

The standard approach uses a shared SparkSession across the entire test session to
avoid the startup cost penalty:

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for the entire test run."""
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
```

Key configuration choices:
- `master("local[*]")` -- Run locally using all available cores
- `shuffle.partitions = 4` -- Reduce from default 200 for small test data
- `ui.enabled = false` -- Skip the Spark UI to save resources
- `scope="session"` -- One SparkSession shared across all test files

### DataFrame Comparison: chispa

The `chispa` library provides robust DataFrame comparison with clear diff output:

```python
from chispa import assert_df_equality

def test_revenue_calculation(spark):
    input_df = spark.createDataFrame(
        [(1, 100, 5)], ["id", "price", "quantity"]
    )
    expected = spark.createDataFrame(
        [(1, 100, 5, 500)], ["id", "price", "quantity", "revenue"]
    )
    result = calculate_revenue(input_df)
    assert_df_equality(result, expected, ignore_row_order=True)
```

If `chispa` is not available, manual comparison works:

```python
def assert_dataframes_equal(actual, expected):
    """Manual DataFrame comparison without external libraries."""
    assert actual.schema == expected.schema, (
        f"Schema mismatch:\n  actual:   {actual.schema}\n  expected: {expected.schema}"
    )
    actual_rows = sorted(actual.collect(), key=str)
    expected_rows = sorted(expected.collect(), key=str)
    assert actual_rows == expected_rows, "Row data mismatch"
```

### Testing UDFs

UDFs should be tested at two levels:

1. **Pure Python function** -- Test the inner logic without Spark
2. **Registered UDF** -- Test that it works inside a DataFrame transformation

```python
# The function
def clean_phone(phone):
    if phone is None:
        return None
    return "".join(c for c in phone if c.isdigit())[-10:]

# Level 1: pure Python test (fast, no Spark)
def test_clean_phone_pure():
    assert clean_phone("(555) 123-4567") == "5551234567"
    assert clean_phone(None) is None

# Level 2: UDF integration test (needs SparkSession)
def test_clean_phone_udf(spark):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    clean_phone_udf = udf(clean_phone, StringType())
    df = spark.createDataFrame([("(555) 123-4567",)], ["phone"])
    result = df.withColumn("cleaned", clean_phone_udf("phone"))
    assert result.first()["cleaned"] == "5551234567"
```

### Testing Edge Cases

Every transformation should be tested against these scenarios:

| Edge Case | What Can Go Wrong |
|-----------|-------------------|
| **Null values** | NullPointerException in UDFs, unexpected NULL propagation |
| **Empty DataFrame** | Division by zero in aggregations, missing columns after joins |
| **Schema mismatch** | Wrong column names, incompatible types after union |
| **Single row** | Window functions with insufficient partitions |
| **Duplicate keys** | Exploding row counts after joins |
| **Special characters** | Regex failures, encoding issues in string columns |

### Mocking External Dependencies

When transformations read from external sources, use dependency injection:

```python
# BAD: Hard-coded source -- untestable
def process_orders():
    df = spark.read.table("production.orders")
    return df.filter(col("status") == "active")

# GOOD: Injectable source -- testable
def process_orders(orders_df):
    return orders_df.filter(col("status") == "active")

# In production:
result = process_orders(spark.read.table("production.orders"))

# In tests:
test_input = spark.createDataFrame(
    [("active",), ("cancelled",)], ["status"]
)
result = process_orders(test_input)
assert result.count() == 1
```

---

## Test Organization

```
project/
  src/
    transforms/
      __init__.py
      revenue.py          # calculate_revenue(), apply_discounts()
      customers.py        # deduplicate_customers(), enrich_profiles()
    utils/
      __init__.py
      validators.py       # validate_schema(), check_nulls()
  tests/
    conftest.py           # SparkSession fixture
    unit/
      test_validators.py  # Pure Python tests (no Spark)
    integration/
      test_revenue.py     # DataFrame-level tests (uses Spark)
      test_customers.py
    e2e/
      test_pipeline.py    # Full pipeline run
```

### CI/CD Integration

```yaml
# GitHub Actions example
- name: Run Spark unit tests
  run: |
    pip install pytest pyspark chispa
    pytest tests/unit/ -v --tb=short

- name: Run Spark integration tests
  run: |
    pytest tests/integration/ -v --tb=short -x
```

### Nutter Framework for Databricks

Nutter is Databricks' testing framework designed for notebook testing:

```python
# In a Databricks notebook
from runtime.nutterfixture import NutterFixture, tag

class TestRevenueCalculation(NutterFixture):
    def assertion_revenue_positive(self):
        df = spark.sql("SELECT * FROM revenue_table WHERE revenue < 0")
        assert df.count() == 0, "Found negative revenue values"

    def assertion_row_count(self):
        df = spark.sql("SELECT COUNT(*) as cnt FROM revenue_table")
        assert df.first()["cnt"] > 0, "Revenue table is empty"

result = TestRevenueCalculation().execute_tests()
print(result.to_string())
```

Nutter integrates with Azure DevOps and GitHub Actions for automated testing of
notebooks in CI/CD pipelines.

---

## Hands-On Walkthrough

Open `01-testing-spark-code_notebook.py` to practice:
- Writing testable transformation functions
- Comparing DataFrames for equality (manual and library-based)
- Testing edge cases: nulls, empty DataFrames, schema mismatches
- Inline pytest-style patterns inside a Databricks notebook

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Local testing | `pyspark` from PyPI | `pyspark` from PyPI | `pyspark` from PyPI |
| CI/CD integration | GitHub Actions, CodeBuild | Azure DevOps, GitHub Actions | Cloud Build, GitHub Actions |
| Notebook testing (Nutter) | Supported via Databricks CLI | Native Azure DevOps integration | Supported via Databricks CLI |
| Cluster for E2E tests | Job cluster with `--json` config | Job cluster with ARM template | Job cluster with Terraform |
| Cost of test runs | ~$0.07/DBU (Jobs Compute) | ~$0.07/DBU (Jobs Compute) | ~$0.07/DBU (Jobs Compute) |

---

## Certification Tip

> **Databricks Certified Data Engineer Associate**: Expect questions about how to
> structure testable transformations, when to use `local[*]` mode for testing, and
> the difference between unit and integration tests for Spark. You may see questions
> about DLT expectations as a form of data validation testing (covered in Topic 02).
>
> **Key concept**: A SparkSession with `master("local[*]")` runs Spark in-process for
> testing. It uses all CPU cores but runs on a single JVM -- no cluster required.

---

## Key Takeaways

1. **Treat transformations as pure functions** -- DataFrame in, DataFrame out. This
   is the single most important design decision for testability.
2. **Share the SparkSession** across tests using a `session`-scoped pytest fixture to
   avoid repeated startup costs.
3. **Test at multiple levels**: pure Python for UDF logic, DataFrame comparison for
   transformations, full pipeline for end-to-end validation.
4. **Always test edge cases**: nulls, empty DataFrames, schema mismatches, duplicates,
   and single-row inputs cause the majority of production failures.
5. **Use dependency injection** to decouple transformations from data sources and
   make functions testable without mocking Spark internals.
6. **Nutter** enables notebook testing in CI/CD -- useful for teams that develop
   primarily in Databricks notebooks.

---

## Next Steps

- Proceed to **Topic 02: Data Quality Frameworks** to learn how DLT expectations and
  custom quality checks validate data at pipeline boundaries.
- Apply testing patterns to your own transformations from Modules 04-06.
