# Databricks Free Edition Setup

> Sign up and start learning in under 10 minutes. No credit card. No cloud account.

---

## Step 1: Sign Up for Databricks Free Edition

1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com/)
2. You can sign up using:
   - **Google account** (fastest -- one click)
   - **Microsoft account**
   - **Email and password**

![Databricks Free Edition signup page](images/databricks-free-signup.png)

3. If using email, verify your email address via the confirmation link
4. Log in to your Databricks workspace

---

## Step 2: Create a Cluster

A cluster is the compute engine that runs your notebooks.

1. In the left sidebar, click **Compute**
2. Click **Create Compute**
3. Configure:
   - **Name**: `learning-cluster`
   - Leave all other defaults -- Free Edition provides a single-node cluster with:
     - 1 driver (no separate workers)
     - 15.3 GB memory
     - Databricks Runtime (latest version)
4. Click **Create Compute**
5. Wait 3-5 minutes for the cluster to start (status changes to **Running**)

**Note**: Free Edition clusters auto-terminate after 2 hours of inactivity. Just restart when needed.

---

## Step 3: Import Notebooks

1. In the left sidebar, click **Workspace**
2. Navigate to your user folder
3. Click the dropdown arrow next to your folder name and select **Import**
4. Choose **File** and upload any `_notebook.py` file from this repository
5. The notebook opens ready to run

**Alternative**: Import directly from a URL:
1. Click **Import** > **URL**
2. Paste the raw GitHub URL of the notebook (e.g., `https://raw.githubusercontent.com/sreekanth489/spark-databricks-zero-to-pro/main/day18-medallion-architecture/18-medallion-architecture_notebook.py`)

---

## Step 4: Run Your First Notebook

1. Open an imported notebook
2. Attach it to your running cluster (dropdown at the top)
3. Click **Run All** or run cells individually with `Shift+Enter`

---

## Limitations of Free Edition

| Feature | Free Edition | Databricks on AWS |
|---------|:------------:|:-----------------:|
| Spark notebooks | Yes | Yes |
| Delta Lake | Yes | Yes |
| Unity Catalog | No | Yes |
| External locations (S3) | No | Yes |
| Auto Loader | No | Yes |
| Managed file events | No | Yes |
| Multi-node clusters | No | Yes |
| Workflows/Jobs | No | Yes |
| SQL Warehouse | No | Yes |

For labs requiring Unity Catalog and S3 storage (Day 18+), see [00-databricks-aws-setup.md](00-databricks-aws-setup.md).
