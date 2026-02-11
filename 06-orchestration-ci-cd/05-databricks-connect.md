# Databricks Connect
> Module 06 — Topic 05 | Level: Intermediate-Advanced | Time: 40 min

## Learning Objectives

By the end of this topic you will be able to:

1. Explain what Databricks Connect is and when to use it
2. Install and configure Databricks Connect v2
3. Run local Python/PySpark code against a remote Databricks cluster
4. Understand supported operations and limitations
5. Integrate Databricks Connect with pytest for local testing
6. Debug Spark code locally using your IDE

## Conceptual Overview

### What Is Databricks Connect?

Databricks Connect is a client library that lets you run Spark code from your
local IDE (VS Code, PyCharm, IntelliJ) while the actual computation happens on
a remote Databricks cluster. Your laptop sends the Spark plan to the cluster,
the cluster executes it, and results come back to your local environment.

```
  Your Laptop (IDE)                    Databricks Cluster
  +--------------------+               +-------------------+
  |                    |   Spark Plan   |                   |
  |  from databricks   | ------------> |  Execute plan on  |
  |    .connect import |               |  remote cluster   |
  |    DatabricksSession               |                   |
  |                    |   Results     |  Access Unity     |
  |  df = spark.sql(   | <------------ |  Catalog, DBFS,   |
  |    "SELECT ..."    |               |  Delta tables     |
  |  )                 |               |                   |
  |  df.show()         |               |                   |
  +--------------------+               +-------------------+
       Local Python                      Remote Compute
```

This is transformative for the development experience. Instead of editing
notebooks in a browser, you get:
- **Full IDE features**: autocomplete, refactoring, debugging, type hints
- **Version control**: work directly in Git repos
- **Local testing**: run pytest against real cluster data
- **Fast iteration**: no notebook cell-by-cell execution

### Databricks Connect v2 vs v1

| Feature | v1 (Legacy) | v2 (Current) |
|---------|-------------|-------------|
| API | `SparkSession.builder.remote()` | `DatabricksSession.builder` |
| Protocol | Spark's RPC protocol | Spark Connect (gRPC) |
| Spark version | 3.x specific | 13.3 LTS+ |
| Unity Catalog | Limited | Full support |
| Streaming | Limited | Supported |
| UDFs | Limited | Improved support |
| Installation | `databricks-connect==X.Y.Z` | `databricks-connect>=13.3` |

**Always use v2 for new projects.** v1 is maintained for backward compatibility only.

### When to Use Databricks Connect

```
  Use Databricks Connect:              Use Notebooks Instead:
  +----------------------------+       +----------------------------+
  | Local IDE development      |       | Quick ad-hoc exploration   |
  | Running pytest suites      |       | Visualization-heavy work   |
  | CI/CD integration tests    |       | Collaborative editing      |
  | Code that lives in Git     |       | DLT pipeline development   |
  | Complex refactoring        |       | Widgets / dashboards       |
  +----------------------------+       +----------------------------+
```

### Architecture: How It Works

```
  +------------------+
  | Your Python Code |
  | (local machine)  |
  +--------+---------+
           |
           | 1. Create DatabricksSession
           |    (gRPC connection)
           v
  +--------+---------+
  | Spark Connect    |
  | Client (local)   |
  | Builds logical   |
  | plan locally     |
  +--------+---------+
           |
           | 2. Send logical plan
           |    via gRPC
           v
  +--------+---------+
  | Spark Connect    |
  | Server (cluster) |
  | Optimizes and    |
  | executes plan    |
  +--------+---------+
           |
           | 3. Return results
           |    (Arrow format)
           v
  +--------+---------+
  | Your Python Code |
  | Receives results |
  | as Pandas/Spark  |
  +------------------+
```

## Hands-On Walkthrough

### Step 1: Install Databricks Connect v2

```bash
# Install the package (version must match your cluster's DBR version)
pip install databricks-connect==14.3.*

# Verify installation
databricks-connect test
```

The version **must match** your cluster's Databricks Runtime version. If your
cluster runs DBR 14.3, install `databricks-connect==14.3.*`.

### Step 2: Configure Authentication

There are three ways to configure the connection:

**Option A: Databricks CLI Profile (recommended)**

```bash
# Configure a profile
databricks configure --token --profile my-workspace

# The profile is stored in ~/.databrickscfg
# [my-workspace]
# host = https://myworkspace.cloud.databricks.com
# token = dapi...
```

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("my-workspace").getOrCreate()
```

**Option B: Environment Variables**

```bash
export DATABRICKS_HOST="https://myworkspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CLUSTER_ID="0123-456789-abcdef"
```

```python
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()  # reads env vars
```

**Option C: Direct Configuration**

```python
from databricks.connect import DatabricksSession

spark = (
    DatabricksSession.builder
        .host("https://myworkspace.cloud.databricks.com")
        .token("dapi...")
        .clusterId("0123-456789-abcdef")
        .getOrCreate()
)
```

### Step 3: Write and Run Local Code

```python
# my_etl.py — runs locally, executes on remote cluster
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("dev-workspace").getOrCreate()

# Read from Unity Catalog (data lives on the cluster)
df = spark.table("catalog.schema.sales_raw")

# Transformations are planned locally, executed remotely
df_clean = (
    df.filter(df.price > 0)
      .filter(df.quantity > 0)
      .withColumn("amount", df.price * df.quantity)
)

# Results come back to your local machine
print(f"Row count: {df_clean.count()}")
df_clean.show(10)

# Write back to Delta (executed on cluster)
df_clean.write.mode("overwrite").saveAsTable("catalog.schema.sales_clean")
```

Run this from your terminal:

```bash
python my_etl.py
```

### Step 4: Integrate with pytest

```python
# tests/conftest.py
import pytest
from databricks.connect import DatabricksSession

@pytest.fixture(scope="session")
def spark():
    """Connect to the remote Databricks cluster for testing."""
    session = (
        DatabricksSession.builder
            .profile("test-workspace")
            .getOrCreate()
    )
    yield session


@pytest.fixture
def sample_table(spark):
    """Ensure test data exists."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS test_catalog.test_schema.test_sales (
            order_id LONG,
            price DOUBLE,
            quantity INT
        ) USING DELTA
    """)
    return "test_catalog.test_schema.test_sales"
```

```python
# tests/test_etl.py
def test_silver_transform_drops_nulls(spark, sample_table):
    """Test that silver transform drops null prices."""
    # Insert test data
    spark.sql(f"""
        INSERT OVERWRITE {sample_table}
        VALUES (1, 29.99, 10), (2, NULL, 5), (3, 19.99, 3)
    """)

    # Run the transformation
    df = spark.table(sample_table)
    result = df.filter(df.price.isNotNull()).filter(df.price > 0)

    assert result.count() == 2
```

Run tests:

```bash
pytest tests/ -v
```

### Step 5: Debugging in Your IDE

With Databricks Connect, standard Python debugging works:

1. Set breakpoints in VS Code or PyCharm
2. Run your script in debug mode
3. Inspect DataFrame variables at breakpoints
4. Step through transformation logic

The key difference: `.show()`, `.collect()`, and `.count()` trigger remote
execution. Lazy transformations (`.filter()`, `.select()`) only build the plan
locally and execute instantly.

### Step 6: Supported and Unsupported Operations

**Supported:**
- DataFrame API (select, filter, join, groupBy, window functions)
- Spark SQL
- Reading/writing Delta tables
- Unity Catalog access
- Structured Streaming (basic)
- User-Defined Functions (UDFs)
- Pandas API on Spark

**Not Supported:**
- `dbutils` (use `databricks-sdk` instead)
- SparkContext / RDD operations
- Delta Live Tables
- MLflow experiment tracking (use `mlflow` client directly)
- Notebook widgets
- `display()` function (use `.show()` or `.toPandas()`)

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Auth method | PAT, OAuth M2M | PAT, Azure AD, SP | PAT, OAuth |
| Cluster endpoint | *.cloud.databricks.com | *.azuredatabricks.net | *.gcp.databricks.com |
| Unity Catalog | Supported | Supported | Supported |
| Network | Direct or via PrivateLink | Direct or via Private Endpoints | Direct or via PSC |

Databricks Connect works the same across all clouds. Only the authentication
method and workspace URL format differ.

## Certification Tip

Databricks Connect is less prominent on the Associate exam, but understanding
these concepts is valuable:
- The difference between local execution and remote execution
- Why Databricks Connect requires a running cluster
- The relationship between DatabricksSession and SparkSession
- Limitations (no dbutils, no RDD API)

## Key Takeaways

1. **Databricks Connect v2** lets you run local code on a remote cluster via gRPC
2. **Use your favorite IDE** with full debugging, autocomplete, and refactoring
3. **Version must match** your cluster's Databricks Runtime version
4. **pytest integration** enables real integration tests against cluster data
5. **Lazy operations** are planned locally; actions execute remotely
6. **No dbutils** — use the `databricks-sdk` for workspace operations instead

## Next Steps

Congratulations on completing Module 06! You now have the tools to:
- Orchestrate pipelines with Workflows
- Enforce data quality with DLT
- Package deployments with Asset Bundles
- Automate with CI/CD pipelines
- Develop locally with Databricks Connect

Continue to **Module 07 — Unity Catalog & Governance** to learn about data
access control, lineage, Lakehouse Federation, and PII protection.
