# CI/CD Patterns for Databricks
> Module 06 — Topic 04 | Level: Intermediate-Advanced | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:

1. Design a Git branching strategy for Databricks projects
2. Write unit and integration tests for Spark code
3. Configure CI/CD pipelines in GitHub Actions, Azure DevOps, and GitLab CI
4. Promote deployments safely across dev, staging, and production
5. Manage secrets and credentials securely across environments
6. Use Terraform to provision Databricks infrastructure

## Conceptual Overview

### Why CI/CD for Data Engineering?

Traditional data engineering often relies on manual deployments: someone clicks
through a UI to configure jobs, then hopes the staging version matches production.
This approach fails as teams and pipelines grow.

**"ETL pipelines based on pandas, not a scalable solution. Databricks addresses
this problem of scalability and performance."** But scalability applies to
*process* as well as *compute*. CI/CD makes your deployment process as scalable
as your Spark cluster.

```
  WITHOUT CI/CD:                      WITH CI/CD:
  +---------------------+             +---------------------+
  | Developer edits     |             | Developer pushes    |
  | notebook in UI      |             | code to Git         |
  | Manually runs in    |             | CI runs tests       |
  | prod                |             | CD deploys to dev   |
  | Hopes it works      |             | Promotes to staging |
  | No rollback plan    |             | Promotes to prod    |
  +---------------------+             +---------------------+
       Risky                              Reliable
```

### The CI/CD Pipeline for Spark Projects

```
  +--------+     +---------+     +---------+     +--------+     +------+
  | Commit | --> | Lint &  | --> | Unit    | --> | Deploy | --> | Int  |
  | & Push |     | Format  |     | Tests   |     | to Dev |     | Test |
  +--------+     +---------+     +---------+     +--------+     +------+
                                                      |
                                                      v
                                                 +---------+
                                                 | Deploy  |
                                                 | to Stg  |
                                                 +---------+
                                                      |
                                               Manual Approval
                                                      |
                                                      v
                                                 +---------+
                                                 | Deploy  |
                                                 | to Prod |
                                                 +---------+
```

### Git Branching Strategy

```
  main (production) ─────────────────────────────────────
       \                              /
        staging ─────────────────────
             \                  /
              feature/add-dlt ──
```

| Branch | Purpose | Deploys to |
|--------|---------|------------|
| `feature/*` | New features and bug fixes | Dev workspace |
| `staging` | Pre-production validation | Staging workspace |
| `main` | Production releases | Production workspace |

Pull requests enforce code review and must pass CI before merge.

## Hands-On Walkthrough

### Step 1: Testing Spark Code

#### Unit Tests (No Cluster Required)

Test pure transformation logic without Spark:

```python
# src/transforms.py
def calculate_revenue(price: float, quantity: int) -> float:
    """Calculate revenue from price and quantity."""
    if price < 0 or quantity < 0:
        raise ValueError("Price and quantity must be non-negative")
    return round(price * quantity, 2)

def categorize_order(amount: float) -> str:
    """Categorize order by total amount."""
    if amount >= 1000:
        return "high"
    elif amount >= 100:
        return "medium"
    return "low"
```

```python
# tests/test_transforms.py
import pytest
from src.transforms import calculate_revenue, categorize_order

def test_calculate_revenue():
    assert calculate_revenue(29.99, 10) == 299.90

def test_calculate_revenue_negative_price():
    with pytest.raises(ValueError):
        calculate_revenue(-5.0, 10)

def test_categorize_order_high():
    assert categorize_order(1500.0) == "high"

def test_categorize_order_medium():
    assert categorize_order(250.0) == "medium"

def test_categorize_order_low():
    assert categorize_order(50.0) == "low"
```

#### Integration Tests (With Spark Session)

Test DataFrame transformations using a local Spark session:

```python
# tests/test_spark_transforms.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
            .master("local[2]")
            .appName("unit-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
    )

def test_silver_transform(spark):
    """Test that silver layer correctly filters and enriches."""
    # Arrange
    data = [
        (1, "2024-01-15", 29.99, 10),
        (2, "2024-01-15", None, 5),     # should be dropped
        (3, "2024-01-15", 19.99, -3),   # should be dropped
    ]
    df = spark.createDataFrame(data, ["order_id", "date", "price", "qty"])

    # Act
    result = (
        df.filter(F.col("price").isNotNull())
          .filter(F.col("price") > 0)
          .filter(F.col("qty") > 0)
          .withColumn("amount", F.col("price") * F.col("qty"))
    )

    # Assert
    assert result.count() == 1
    row = result.first()
    assert row["order_id"] == 1
    assert row["amount"] == 299.90
```

### Step 2: GitHub Actions Pipeline

```yaml
# .github/workflows/ci-cd.yml
name: Databricks CI/CD

on:
  pull_request:
    branches: [main, staging]
  push:
    branches: [main, staging]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest black ruff

      - name: Lint with ruff
        run: ruff check src/ tests/

      - name: Format check with black
        run: black --check src/ tests/

      - name: Run unit tests
        run: pytest tests/ -v --tb=short

  deploy-staging:
    needs: lint-and-test
    if: github.ref == 'refs/heads/staging'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}
        run: |
          databricks bundle validate --target staging
          databricks bundle deploy --target staging

  deploy-prod:
    needs: lint-and-test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production    # Requires manual approval
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to production
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: |
          databricks bundle validate --target prod
          databricks bundle deploy --target prod
```

### Step 3: Azure DevOps Pipeline

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
      - staging

pool:
  vmImage: "ubuntu-latest"

stages:
  - stage: Test
    jobs:
      - job: LintAndTest
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: |
              pip install -r requirements.txt
              pip install pytest black ruff
            displayName: Install dependencies
          - script: ruff check src/ tests/
            displayName: Lint
          - script: pytest tests/ -v
            displayName: Run tests

  - stage: DeployStaging
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/staging')
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
                    databricks bundle deploy --target staging
                  env:
                    DATABRICKS_HOST: $(STAGING_HOST)
                    DATABRICKS_TOKEN: $(STAGING_TOKEN)

  - stage: DeployProd
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
    dependsOn: Test
    jobs:
      - deployment: ProdDeploy
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    pip install databricks-cli
                    databricks bundle deploy --target prod
                  env:
                    DATABRICKS_HOST: $(PROD_HOST)
                    DATABRICKS_TOKEN: $(PROD_TOKEN)
```

### Step 4: Secret Management

```python
# In Databricks notebooks — use Secret Scopes
db_password = dbutils.secrets.get(scope="etl-secrets", key="postgres-password")
api_token = dbutils.secrets.get(scope="etl-secrets", key="api-token")

# Secrets are redacted in notebook output (shows [REDACTED])
print(db_password)  # Output: [REDACTED]
```

```bash
# Create a secret scope backed by Databricks
databricks secrets create-scope etl-secrets

# Add secrets
databricks secrets put-secret etl-secrets postgres-password
databricks secrets put-secret etl-secrets api-token

# List scopes and secrets
databricks secrets list-scopes
databricks secrets list-secrets etl-secrets
```

For Azure Key Vault or AWS Secrets Manager integration, create a scope backed
by the cloud provider's secret service.

### Step 5: Terraform for Databricks

```hcl
# main.tf — Provision Databricks workspace resources
terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.35"
    }
  }
}

resource "databricks_cluster" "shared_etl" {
  cluster_name            = "shared-etl-${var.environment}"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = var.node_type
  autotermination_minutes = 30
  num_workers             = 2
}

resource "databricks_secret_scope" "etl" {
  name = "etl-secrets"
}

resource "databricks_permissions" "cluster_usage" {
  cluster_id = databricks_cluster.shared_etl.id
  access_control {
    group_name       = "data-engineers"
    permission_level = "CAN_RESTART"
  }
}
```

## Cloud Provider Notes

| Feature | GitHub Actions | Azure DevOps | GitLab CI |
|---------|---------------|--------------|-----------|
| Secrets | Repository Secrets | Variable Groups | CI/CD Variables |
| Approval gates | Environments | Environments | Manual jobs |
| Self-hosted runners | Yes | Agent pools | Yes |
| Databricks integration | CLI + PAT | CLI + PAT/SP | CLI + PAT |

## Certification Tip

For the Databricks Data Engineer Associate exam, know:
- How Databricks Repos integrates with Git providers
- The concept of promoting code between environments
- Secret scopes and how they protect credentials
- The difference between workspace-backed and cloud-backed secret scopes

## Key Takeaways

1. **Test Spark code** at two levels: pure unit tests and Spark integration tests
2. **CI pipelines** should lint, format-check, and test on every pull request
3. **CD pipelines** deploy to staging on merge, production with manual approval
4. **Secrets** belong in secret scopes, never in code or notebook output
5. **Terraform** can provision the Databricks infrastructure that bundles deploy into
6. **Environment promotion** (dev -> staging -> prod) ensures changes are validated

## Next Steps

Proceed to [05 - Databricks Connect](05-databricks-connect.md) to learn how to
develop and debug Spark code locally while executing against a remote cluster.
