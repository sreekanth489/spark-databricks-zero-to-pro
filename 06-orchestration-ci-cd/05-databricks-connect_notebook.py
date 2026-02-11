# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Connect — Hands-On Notebook
# MAGIC > Module 06 — Topic 05 | Orchestration & CI/CD
# MAGIC
# MAGIC This notebook covers:
# MAGIC 1. What Databricks Connect is and how it works
# MAGIC 2. Installation and configuration steps
# MAGIC 3. A simple remote execution pattern
# MAGIC 4. Integration with pytest
# MAGIC 5. Supported operations and limitations
# MAGIC
# MAGIC **Note**: Databricks Connect is designed to run from a *local IDE*, not from
# MAGIC a Databricks notebook. This notebook uses reference code to show the patterns.
# MAGIC Cells marked `[RUNNABLE]` demonstrate concepts that work in the notebook.
# MAGIC Cells marked `[REFERENCE]` show code you would run locally.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: What Is Databricks Connect? [RUNNABLE]
# MAGIC Understanding the architecture.

# COMMAND ----------

print("""
=== Databricks Connect Architecture ===

  Your Laptop (IDE)                    Databricks Cluster
  +--------------------+               +-------------------+
  |                    |   Spark Plan   |                   |
  |  Python code with  | ------------> |  Execute plan on  |
  |  DatabricksSession |   (gRPC)      |  remote cluster   |
  |                    |               |                   |
  |  df = spark.table  |   Results     |  Access Delta     |
  |  df.filter(...)    | <------------ |  tables, Unity    |
  |  df.show()         |   (Arrow)     |  Catalog, DBFS    |
  +--------------------+               +-------------------+

Key Points:
  - Code is WRITTEN and DEBUGGED locally
  - Spark EXECUTION happens on the remote cluster
  - Results are transferred back via Apache Arrow (efficient)
  - You get full IDE features: autocomplete, breakpoints, refactoring
  - The cluster must be RUNNING for Databricks Connect to work
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Installation Steps [REFERENCE]
# MAGIC How to set up Databricks Connect v2 on your local machine.

# COMMAND ----------

print("""
=== Installation Guide ===

Step 1: Install the package (version MUST match your cluster DBR)
  $ pip install databricks-connect==14.3.*

  If your cluster runs DBR 15.1:
  $ pip install databricks-connect==15.1.*

Step 2: Verify the installation
  $ databricks-connect test

Step 3: Configure authentication (choose one method)

  Method A — CLI Profile (recommended):
    $ databricks configure --token --profile dev-workspace
    Enter host: https://myworkspace.cloud.databricks.com
    Enter token: dapi...

  Method B — Environment Variables:
    $ export DATABRICKS_HOST="https://myworkspace.cloud.databricks.com"
    $ export DATABRICKS_TOKEN="dapi..."
    $ export DATABRICKS_CLUSTER_ID="0123-456789-abcdef"

  Method C — Direct in code (not recommended for production):
    spark = DatabricksSession.builder \\
        .host("https://...") \\
        .token("dapi...") \\
        .clusterId("0123-...") \\
        .getOrCreate()

Step 4: Verify connection
  $ python -c "from databricks.connect import DatabricksSession; \\
    spark = DatabricksSession.builder.getOrCreate(); \\
    print(spark.sql('SELECT 1').collect())"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Basic Remote Execution Pattern [REFERENCE]
# MAGIC This code runs on your laptop; Spark executes on the cluster.

# COMMAND ----------

# -- REFERENCE CODE (run from your local IDE, NOT from this notebook) --
#
# from databricks.connect import DatabricksSession
#
# # Connect to the remote cluster
# spark = DatabricksSession.builder.profile("dev-workspace").getOrCreate()
#
# # Read from a Unity Catalog table (data is on the cluster)
# df = spark.table("my_catalog.my_schema.sales_raw")
#
# # Apply transformations (planned locally, executed remotely)
# df_clean = (
#     df.filter(df.price.isNotNull())
#       .filter(df.price > 0)
#       .filter(df.quantity > 0)
#       .withColumn("amount", df.price * df.quantity)
#       .select("order_id", "order_date", "product", "amount", "region")
# )
#
# # Trigger execution (this sends the plan to the cluster)
# print(f"Clean records: {df_clean.count()}")
# df_clean.show(10)
#
# # Write results back to Delta (executed on cluster)
# (
#     df_clean.write
#         .mode("overwrite")
#         .saveAsTable("my_catalog.my_schema.sales_clean")
# )
#
# print("Pipeline complete.")

print("Cell 3 is reference code. Run from your local IDE with Databricks Connect.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Simulating the Pattern in a Notebook [RUNNABLE]
# MAGIC The same logic, but using the notebook's built-in SparkSession.

# COMMAND ----------

from pyspark.sql import functions as F

# In a notebook, spark is already available (no DatabricksSession needed)
# This simulates the same transformation pattern

raw_data = [
    (1, "2024-01-15", "Widget A", 29.99, 10, "West"),
    (2, "2024-01-15", "Widget B", None,   5,  "East"),   # null price
    (3, "2024-01-15", "Widget C", 19.99, -3,  "West"),   # negative qty
    (4, "2024-01-16", "Widget D", 99.99,  2,  "East"),
    (5, "2024-01-16", "Widget E", 14.99, 50,  "West"),
]
columns = ["order_id", "order_date", "product", "price", "quantity", "region"]
df = spark.createDataFrame(raw_data, schema=columns)

# Same transformations you would write locally with Databricks Connect
df_clean = (
    df.filter(F.col("price").isNotNull())
      .filter(F.col("price") > 0)
      .filter(F.col("quantity") > 0)
      .withColumn("amount", F.round(F.col("price") * F.col("quantity"), 2))
      .select("order_id", "order_date", "product", "amount", "region")
)

print(f"Raw records: {df.count()}")
print(f"Clean records: {df_clean.count()}")
print()
df_clean.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: pytest Integration with Databricks Connect [REFERENCE]
# MAGIC How to write integration tests that run locally against remote data.

# COMMAND ----------

conftest_code = """
# tests/conftest.py
import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    '''Create a DatabricksSession for the entire test suite.'''
    session = (
        DatabricksSession.builder
            .profile("test-workspace")       # uses ~/.databrickscfg
            .getOrCreate()
    )
    yield session
    # Session cleanup is automatic


@pytest.fixture
def test_table(spark):
    '''Create and populate a test table, clean up after.'''
    table_name = "test_catalog.test_schema.test_sales"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f'''
        CREATE TABLE {table_name} (
            order_id LONG,
            price DOUBLE,
            quantity INT,
            region STRING
        ) USING DELTA
    ''')

    spark.sql(f'''
        INSERT INTO {table_name} VALUES
            (1, 29.99, 10, 'West'),
            (2, 49.99, 5,  'East'),
            (3, NULL,  3,  'West'),
            (4, 19.99, -1, 'East')
    ''')

    yield table_name

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
"""

test_code = """
# tests/test_etl.py

def test_null_prices_dropped(spark, test_table):
    '''Silver transform should drop records with null prices.'''
    df = spark.table(test_table)
    result = df.filter(df.price.isNotNull())
    assert result.count() == 3    # row with NULL price dropped


def test_negative_quantity_dropped(spark, test_table):
    '''Silver transform should drop negative quantities.'''
    df = spark.table(test_table)
    result = df.filter(df.price.isNotNull()).filter(df.quantity > 0)
    assert result.count() == 2    # NULL price + negative qty dropped


def test_amount_calculation(spark, test_table):
    '''Amount should equal price * quantity.'''
    df = spark.table(test_table)
    result = (
        df.filter(df.price.isNotNull())
          .filter(df.quantity > 0)
          .withColumn("amount", df.price * df.quantity)
    )
    rows = {r.order_id: r.amount for r in result.collect()}
    assert abs(rows[1] - 299.90) < 0.01
    assert abs(rows[2] - 249.95) < 0.01
"""

print("=== conftest.py ===")
print(conftest_code)
print("\n=== test_etl.py ===")
print(test_code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: v2 vs v1 Comparison [RUNNABLE]

# COMMAND ----------

comparison = [
    ("Package",         "databricks-connect==14.3.*",     "databricks-connect (legacy)"),
    ("Entry Point",     "DatabricksSession.builder",      "SparkSession.builder.remote()"),
    ("Protocol",        "gRPC (Spark Connect)",           "Spark RPC"),
    ("Min DBR Version", "13.3 LTS",                       "Varies"),
    ("Unity Catalog",   "Full support",                   "Limited"),
    ("Streaming",       "Supported",                      "Limited"),
    ("UDFs",            "Improved support",               "Limited"),
    ("RDD API",         "Not supported",                  "Supported (legacy)"),
    ("Status",          "ACTIVE development",             "Maintenance only"),
]

print(f"{'Feature':<20} {'v2 (Current)':<38} {'v1 (Legacy)'}")
print("-" * 85)
for feature, v2, v1 in comparison:
    print(f"{feature:<20} {v2:<38} {v1}")

print("\nRecommendation: Always use v2 for new projects.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Supported vs Unsupported Operations [RUNNABLE]

# COMMAND ----------

supported = [
    "DataFrame API (select, filter, join, groupBy, agg, window)",
    "Spark SQL (spark.sql('SELECT ...'))",
    "Reading / writing Delta tables",
    "Unity Catalog table access",
    "Structured Streaming (basic patterns)",
    "User-Defined Functions (Python UDFs)",
    "Pandas API on Spark",
    "Schema inspection and metadata queries",
]

unsupported = [
    "dbutils (use databricks-sdk instead)",
    "SparkContext / RDD operations",
    "Delta Live Tables (DLT)",
    "MLflow experiment tracking (use mlflow client directly)",
    "Notebook widgets (dbutils.widgets)",
    "display() function (use .show() or .toPandas())",
    "Spark UI direct access (use cluster UI)",
    "DBFS direct file operations",
]

print("=== SUPPORTED Operations ===")
for i, op in enumerate(supported, 1):
    print(f"  {i}. {op}")

print("\n=== NOT SUPPORTED ===")
for i, op in enumerate(unsupported, 1):
    print(f"  {i}. {op}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Replacing dbutils with databricks-sdk [REFERENCE]
# MAGIC Since dbutils is not available via Databricks Connect, use the SDK.

# COMMAND ----------

# -- REFERENCE CODE (runs locally with Databricks Connect) --
#
# from databricks.sdk import WorkspaceClient
#
# # Initialize the SDK client (reads same auth config as DatabricksSession)
# w = WorkspaceClient(profile="dev-workspace")
#
# # Instead of dbutils.secrets.get:
# secret = w.secrets.get_secret(scope="my-scope", key="my-key")
# print(f"Secret value: {secret.value}")
#
# # Instead of dbutils.fs.ls:
# files = w.dbfs.list("/mnt/data/")
# for f in files:
#     print(f"{f.path}  ({f.file_size} bytes)")
#
# # Instead of dbutils.notebook.run:
# # Use the Jobs API to trigger notebook runs
# run = w.jobs.submit(
#     run_name="triggered-from-local",
#     tasks=[{
#         "task_key": "my_task",
#         "notebook_task": {
#             "notebook_path": "/Repos/team/project/my_notebook"
#         },
#         "existing_cluster_id": "0123-456789-abcdef"
#     }]
# )
# print(f"Submitted run: {run.run_id}")

print("Cell 8 shows how to replace dbutils with databricks-sdk.")
print("Install locally: pip install databricks-sdk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Debugging Tips [RUNNABLE]
# MAGIC Practical tips for debugging with Databricks Connect.

# COMMAND ----------

print("""
=== Debugging Tips for Databricks Connect ===

1. LAZY vs EAGER operations:
   - Lazy (plan only, instant): .filter(), .select(), .withColumn(), .join()
   - Eager (triggers execution): .count(), .show(), .collect(), .write

   Set breakpoints BEFORE eager operations to inspect the plan.

2. Common errors:

   "CONNECT_TIMEOUT" -> Cluster is not running. Start it first.

   "VERSION_MISMATCH" -> Your local databricks-connect version does not
   match the cluster DBR version. Reinstall the correct version:
     pip install databricks-connect==<DBR_VERSION>.*

   "PERMISSION_DENIED" -> Your token does not have access to the cluster
   or catalog. Check Unity Catalog permissions.

   "TABLE_NOT_FOUND" -> Ensure the catalog and schema are accessible
   from the cluster configured in your profile.

3. Performance tip:
   Use .limit(100).toPandas() during development instead of .collect()
   to avoid pulling large datasets to your laptop.

4. Logging:
   import logging
   logging.getLogger("databricks.connect").setLevel(logging.DEBUG)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Local Development Workflow Summary [RUNNABLE]

# COMMAND ----------

print("""
=== Recommended Local Development Workflow ===

  +------------------+
  | 1. Install       |  pip install databricks-connect==14.3.*
  +--------+---------+
           |
  +--------v---------+
  | 2. Configure     |  databricks configure --token --profile dev
  +--------+---------+
           |
  +--------v---------+
  | 3. Write Code    |  Use DatabricksSession in your IDE
  +--------+---------+  Full autocomplete, type hints, refactoring
           |
  +--------v---------+
  | 4. Debug         |  Set breakpoints, inspect DataFrames
  +--------+---------+  Step through transformations
           |
  +--------v---------+
  | 5. Test          |  pytest tests/ -v
  +--------+---------+  Tests run locally, execute on cluster
           |
  +--------v---------+
  | 6. Commit & Push |  Standard Git workflow
  +--------+---------+
           |
  +--------v---------+
  | 7. CI/CD         |  Automated testing and deployment
  +------------------+  (Module 06, Topic 04)

This workflow gives you the best of both worlds:
  - IDE productivity of local development
  - Scalability and data access of Databricks clusters
""")
print("\nModule 06 complete. Proceed to Module 07: Unity Catalog & Governance.")
