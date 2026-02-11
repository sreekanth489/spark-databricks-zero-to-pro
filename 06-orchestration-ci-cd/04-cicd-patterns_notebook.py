# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # CI/CD Patterns for Databricks — Hands-On Notebook
# MAGIC > Module 06 — Topic 04 | Orchestration & CI/CD
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Unit testing patterns for Spark transformations
# MAGIC 2. Integration testing with DataFrame assertions
# MAGIC 3. CI pipeline YAML examples (GitHub Actions, Azure DevOps, GitLab)
# MAGIC 4. Secret scope usage for credential management
# MAGIC
# MAGIC Cells marked `[RUNNABLE]` can be executed in your Databricks workspace.
# MAGIC Cells marked `[REFERENCE]` show YAML/config examples.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Pure Python Functions (Testable Without Spark) [RUNNABLE]
# MAGIC Always extract business logic into pure functions that are easy to unit test.

# COMMAND ----------

def calculate_revenue(price: float, quantity: int) -> float:
    """Calculate revenue from price and quantity."""
    if price is None or quantity is None:
        return 0.0
    if price < 0 or quantity < 0:
        raise ValueError("Price and quantity must be non-negative")
    return round(price * quantity, 2)


def categorize_order(amount: float) -> str:
    """Categorize an order by its total amount."""
    if amount >= 1000:
        return "high"
    elif amount >= 100:
        return "medium"
    return "low"


def validate_email(email: str) -> bool:
    """Basic email validation."""
    if email is None:
        return False
    return "@" in email and "." in email.split("@")[-1]


# Quick sanity checks
assert calculate_revenue(29.99, 10) == 299.90
assert calculate_revenue(0, 5) == 0.0
assert categorize_order(1500) == "high"
assert categorize_order(50) == "low"
assert validate_email("user@example.com") == True
assert validate_email("invalid") == False

print("All pure function tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Unit Test Pattern with pytest [REFERENCE]
# MAGIC This is how your test file would look in `tests/test_transforms.py`.

# COMMAND ----------

test_file_content = '''
# tests/test_transforms.py
import pytest
from src.transforms import calculate_revenue, categorize_order, validate_email


class TestCalculateRevenue:
    """Tests for the calculate_revenue function."""

    def test_normal_calculation(self):
        assert calculate_revenue(29.99, 10) == 299.90

    def test_zero_price(self):
        assert calculate_revenue(0, 5) == 0.0

    def test_zero_quantity(self):
        assert calculate_revenue(29.99, 0) == 0.0

    def test_none_price(self):
        assert calculate_revenue(None, 10) == 0.0

    def test_negative_price_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            calculate_revenue(-5.0, 10)

    def test_large_numbers(self):
        assert calculate_revenue(9999.99, 10000) == 99999900.0


class TestCategorizeOrder:
    """Tests for the categorize_order function."""

    def test_high_value(self):
        assert categorize_order(1500.0) == "high"

    def test_boundary_high(self):
        assert categorize_order(1000.0) == "high"

    def test_medium_value(self):
        assert categorize_order(250.0) == "medium"

    def test_boundary_medium(self):
        assert categorize_order(100.0) == "medium"

    def test_low_value(self):
        assert categorize_order(50.0) == "low"

    def test_zero(self):
        assert categorize_order(0) == "low"


class TestValidateEmail:
    """Tests for the validate_email function."""

    def test_valid_email(self):
        assert validate_email("user@example.com") is True

    def test_missing_at(self):
        assert validate_email("userexample.com") is False

    def test_missing_domain_dot(self):
        assert validate_email("user@example") is False

    def test_none_input(self):
        assert validate_email(None) is False
'''

print(test_file_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Integration Testing with Spark DataFrames [RUNNABLE]
# MAGIC Test actual Spark transformations using the current session.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define a transformation function (what you would have in src/)
def apply_silver_transform(df):
    """Apply silver-layer cleaning and enrichment."""
    return (
        df
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .filter(F.col("quantity") > 0)
        .withColumn("amount", F.round(F.col("price") * F.col("quantity"), 2))
        .withColumn("category",
            F.when(F.col("price") * F.col("quantity") >= 1000, "high")
             .when(F.col("price") * F.col("quantity") >= 100, "medium")
             .otherwise("low")
        )
        .dropDuplicates(["order_id"])
    )

# --- Test Case 1: Normal data ---
test_data_1 = [
    (1, "2024-01-15", 29.99, 10),
    (2, "2024-01-15", 49.99, 5),
]
df_test_1 = spark.createDataFrame(test_data_1, ["order_id", "date", "price", "quantity"])
result_1 = apply_silver_transform(df_test_1)
assert result_1.count() == 2, f"Expected 2 rows, got {result_1.count()}"
print("Test 1 PASSED: Normal data returns correct row count")

# --- Test Case 2: Null prices should be dropped ---
test_data_2 = [
    (1, "2024-01-15", 29.99, 10),
    (2, "2024-01-15", None, 5),
]
df_test_2 = spark.createDataFrame(test_data_2, ["order_id", "date", "price", "quantity"])
result_2 = apply_silver_transform(df_test_2)
assert result_2.count() == 1, f"Expected 1 row, got {result_2.count()}"
print("Test 2 PASSED: Null prices are dropped")

# --- Test Case 3: Negative quantity should be dropped ---
test_data_3 = [
    (1, "2024-01-15", 29.99, 10),
    (2, "2024-01-15", 19.99, -3),
]
df_test_3 = spark.createDataFrame(test_data_3, ["order_id", "date", "price", "quantity"])
result_3 = apply_silver_transform(df_test_3)
assert result_3.count() == 1, f"Expected 1 row, got {result_3.count()}"
print("Test 3 PASSED: Negative quantities are dropped")

# --- Test Case 4: Duplicates should be removed ---
test_data_4 = [
    (1, "2024-01-15", 29.99, 10),
    (1, "2024-01-15", 29.99, 10),  # duplicate order_id
]
df_test_4 = spark.createDataFrame(test_data_4, ["order_id", "date", "price", "quantity"])
result_4 = apply_silver_transform(df_test_4)
assert result_4.count() == 1, f"Expected 1 row, got {result_4.count()}"
print("Test 4 PASSED: Duplicate order_ids are deduplicated")

# --- Test Case 5: Category assignment ---
test_data_5 = [
    (1, "2024-01-15", 200.0, 10),   # 2000 -> high
    (2, "2024-01-15", 50.0, 3),     # 150  -> medium
    (3, "2024-01-15", 5.0, 2),      # 10   -> low
]
df_test_5 = spark.createDataFrame(test_data_5, ["order_id", "date", "price", "quantity"])
result_5 = apply_silver_transform(df_test_5)
categories = {row["order_id"]: row["category"] for row in result_5.collect()}
assert categories[1] == "high", f"Expected 'high', got '{categories[1]}'"
assert categories[2] == "medium", f"Expected 'medium', got '{categories[2]}'"
assert categories[3] == "low", f"Expected 'low', got '{categories[3]}'"
print("Test 5 PASSED: Categories assigned correctly")

print("\nAll 5 integration tests PASSED.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: pytest conftest.py with SparkSession Fixture [REFERENCE]
# MAGIC Standard setup for running Spark tests locally with pytest.

# COMMAND ----------

conftest_content = '''
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for testing.
    scope="session" means one Spark session shared across all tests.
    """
    session = (
        SparkSession.builder
            .master("local[2]")
            .appName("pytest-spark")
            .config("spark.sql.shuffle.partitions", "2")   # fewer partitions = faster tests
            .config("spark.ui.enabled", "false")            # disable UI for speed
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_sales_df(spark):
    """Reusable test fixture for sales data."""
    data = [
        (1, "2024-01-15", "Widget A", 29.99, 10, "West"),
        (2, "2024-01-15", "Widget B", 49.99, 5, "East"),
        (3, "2024-01-16", "Widget C", 19.99, 25, "West"),
    ]
    return spark.createDataFrame(
        data, ["order_id", "order_date", "product", "price", "quantity", "region"]
    )
'''

print(conftest_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: GitHub Actions CI/CD Pipeline [REFERENCE]
# MAGIC Complete pipeline that tests on PR, deploys on merge.

# COMMAND ----------

github_actions_yaml = """
# .github/workflows/databricks-cicd.yml

name: Databricks CI/CD Pipeline

on:
  pull_request:
    branches: [main, staging]
  push:
    branches: [main, staging]

env:
  PYTHON_VERSION: "3.11"

jobs:
  # ---- Stage 1: Lint and Test ----
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest black ruff pyspark==3.5.0

      - name: Lint (ruff)
        run: ruff check src/ tests/

      - name: Format check (black)
        run: black --check src/ tests/

      - name: Unit tests
        run: pytest tests/ -v --tb=short -x

  # ---- Stage 2: Deploy to Staging ----
  deploy-staging:
    needs: test
    if: github.event_name == 'push' && github.ref == 'refs/heads/staging'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate and Deploy
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_WORKSPACE_URL }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}
        run: |
          databricks bundle validate --target staging
          databricks bundle deploy --target staging

  # ---- Stage 3: Deploy to Production ----
  deploy-prod:
    needs: test
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production         # Requires manual approval in GitHub
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate and Deploy
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_URL }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: |
          databricks bundle validate --target prod
          databricks bundle deploy --target prod
"""

print(github_actions_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Azure DevOps Pipeline [REFERENCE]

# COMMAND ----------

azure_devops_yaml = """
# azure-pipelines.yml

trigger:
  branches:
    include: [main, staging]

pool:
  vmImage: "ubuntu-latest"

stages:
  - stage: Test
    displayName: "Lint & Test"
    jobs:
      - job: RunTests
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"

          - script: |
              pip install -r requirements.txt
              pip install pytest black ruff pyspark==3.5.0
            displayName: "Install dependencies"

          - script: ruff check src/ tests/
            displayName: "Lint"

          - script: pytest tests/ -v --tb=short
            displayName: "Run tests"

  - stage: DeployStaging
    displayName: "Deploy to Staging"
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/staging'))
    dependsOn: Test
    jobs:
      - deployment: StagingDeploy
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    pip install databricks-cli
                    databricks bundle validate --target staging
                    databricks bundle deploy --target staging
                  env:
                    DATABRICKS_HOST: $(STAGING_HOST)
                    DATABRICKS_TOKEN: $(STAGING_TOKEN)
                  displayName: "Deploy bundle to staging"

  - stage: DeployProd
    displayName: "Deploy to Production"
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    dependsOn: Test
    jobs:
      - deployment: ProdDeploy
        environment: production       # Manual approval gate
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    pip install databricks-cli
                    databricks bundle validate --target prod
                    databricks bundle deploy --target prod
                  env:
                    DATABRICKS_HOST: $(PROD_HOST)
                    DATABRICKS_TOKEN: $(PROD_TOKEN)
                  displayName: "Deploy bundle to production"
"""

print(azure_devops_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: GitLab CI Pipeline [REFERENCE]

# COMMAND ----------

gitlab_ci_yaml = """
# .gitlab-ci.yml

stages:
  - test
  - deploy-staging
  - deploy-prod

variables:
  PYTHON_VERSION: "3.11"
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"

cache:
  paths:
    - .cache/pip

test:
  stage: test
  image: python:${PYTHON_VERSION}
  script:
    - pip install -r requirements.txt pytest black ruff pyspark==3.5.0
    - ruff check src/ tests/
    - black --check src/ tests/
    - pytest tests/ -v --tb=short
  rules:
    - if: $CI_MERGE_REQUEST_ID
    - if: $CI_COMMIT_BRANCH == "main" || $CI_COMMIT_BRANCH == "staging"

deploy-staging:
  stage: deploy-staging
  image: python:${PYTHON_VERSION}
  script:
    - pip install databricks-cli
    - databricks bundle validate --target staging
    - databricks bundle deploy --target staging
  rules:
    - if: $CI_COMMIT_BRANCH == "staging"
  environment:
    name: staging

deploy-prod:
  stage: deploy-prod
  image: python:${PYTHON_VERSION}
  script:
    - pip install databricks-cli
    - databricks bundle validate --target prod
    - databricks bundle deploy --target prod
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
  environment:
    name: production
  when: manual                      # Requires manual click to deploy
"""

print(gitlab_ci_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Secret Scope Usage [RUNNABLE]
# MAGIC How to securely access credentials in Databricks notebooks.

# COMMAND ----------

# Demonstrate secret scope patterns (read-only, won't fail if scope doesn't exist)
print("=== Databricks Secret Scopes ===\n")

print("Creating a secret scope (CLI):")
print("  $ databricks secrets create-scope my-secrets\n")

print("Adding secrets (CLI):")
print("  $ databricks secrets put-secret my-secrets db-password")
print("  $ databricks secrets put-secret my-secrets api-key\n")

print("Using secrets in notebooks (Python):")
print('  password = dbutils.secrets.get(scope="my-secrets", key="db-password")')
print('  api_key  = dbutils.secrets.get(scope="my-secrets", key="api-key")\n')

print("Security features:")
print("  - Secrets are REDACTED in notebook output")
print("  - Secrets are REDACTED in Spark driver logs")
print("  - Access controlled via ACLs on the scope")
print("  - Can be backed by cloud providers (Azure Key Vault, AWS Secrets Manager)")

# Example of how secrets would be used in a JDBC connection
jdbc_example = """
# Reading from PostgreSQL using secrets
password = dbutils.secrets.get(scope="etl-secrets", key="postgres-password")

df = (
    spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://host:5432/mydb")
        .option("dbtable", "public.customers")
        .option("user", "etl_user")
        .option("password", password)    # Secret — redacted in output
        .load()
)
"""
print(jdbc_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Environment Promotion Strategy [RUNNABLE]
# MAGIC Visualizing how code flows from development to production.

# COMMAND ----------

print("""
=== Environment Promotion Strategy ===

  Developer       Code Review       Automated Tests     Deployment
  +---------+     +---------+       +-------------+     +----------+
  | feature | --> | Pull    | ----> | CI Pipeline | --> | Deploy   |
  | branch  |     | Request |       | (lint+test) |     | to DEV   |
  +---------+     +---------+       +-------------+     +----------+
                       |                                      |
                       v                                      v
                  +----------+                          +-----------+
                  | Approved |                          | Manual or |
                  | & Merged |                          | Auto test |
                  | to stg   |                          | in DEV    |
                  +----------+                          +-----------+
                       |                                      |
                       v                                      v
                  +----------+     +-------------+     +-----------+
                  | staging  | --> | CI Pipeline | --> | Deploy    |
                  | branch   |     | (full test) |     | to STG    |
                  +----------+     +-------------+     +-----------+
                       |                                      |
                       v                                      v
                  +----------+     +-------------+     +-----------+
                  | Merge to | --> | CI Pipeline | --> | Deploy    |
                  | main     |     | + Approval  |     | to PROD   |
                  +----------+     +-------------+     +-----------+

Key Principles:
  1. Code flows ONE direction: dev -> staging -> prod
  2. Every promotion requires passing CI tests
  3. Production deployment requires manual approval
  4. Rollback = revert the merge commit and re-deploy
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Cleanup [RUNNABLE]

# COMMAND ----------

print("No persistent resources were created in this notebook.")
print()
print("Summary of CI/CD patterns covered:")
print("  1. Pure function unit tests (no Spark dependency)")
print("  2. Spark DataFrame integration tests (with local SparkSession)")
print("  3. GitHub Actions pipeline (lint -> test -> deploy)")
print("  4. Azure DevOps pipeline (with environment approvals)")
print("  5. GitLab CI pipeline (with manual production gate)")
print("  6. Secret scope usage for credential management")
print("  7. Environment promotion strategy (dev -> staging -> prod)")
