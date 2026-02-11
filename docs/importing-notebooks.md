# Importing Notebooks into Databricks

All notebooks in this repository use the **Databricks source format** (`.py` files) which can be imported directly into any Databricks workspace.

---

## Method 1: File Upload (Recommended)

1. Download the `_notebook.py` file from GitHub (click **Raw**, then **Save As**)
2. In Databricks, click **Workspace** in the left sidebar
3. Navigate to your user folder (`/Users/your-email/`)
4. Right-click → **Import**
5. Select **File** and upload the `.py` file
6. Click **Import**

The notebook opens automatically with all cells ready to run.

## Method 2: Import from URL

1. On GitHub, navigate to the `_notebook.py` file
2. Click **Raw** to get the raw file URL
3. In Databricks, click **Workspace** → your folder → **Import**
4. Select **URL** and paste the raw GitHub URL
5. Click **Import**

## Method 3: Git Repo (Full Databricks Only)

> Not available on Community Edition

1. Click **Repos** in the left sidebar
2. Click **Add Repo**
3. Paste the repository URL
4. Click **Create Repo**

All notebooks appear in the repo folder and stay in sync with Git.

## Method 4: Bulk Import with Databricks CLI

```bash
# Install the CLI
pip install databricks-cli

# Configure
databricks configure --token

# Import a single notebook
databricks workspace import \
  01-python-spark-foundations/01-python-essentials_notebook.py \
  /Users/your-email/spark-zero-to-pro/01-python-essentials \
  --language PYTHON --format SOURCE

# Import an entire module directory
databricks workspace import_dir \
  01-python-spark-foundations/ \
  /Users/your-email/spark-zero-to-pro/01-python-spark-foundations/
```

---

## Understanding the Notebook Format

Each `_notebook.py` file follows this structure:

```python
# Databricks notebook source          ← Required first line

# COMMAND ----------                   ← Cell separator

# MAGIC %md                            ← Markdown cell
# MAGIC # Title                        ← (each line prefixed with # MAGIC)

# COMMAND ----------                   ← Cell separator

# Python code cell                     ← Regular Python code
df = spark.read.format("csv").load(path)

# COMMAND ----------                   ← Cell separator

# MAGIC %sql                           ← SQL cell
# MAGIC SELECT * FROM my_table LIMIT 10
```

### Cell Types

| Prefix | Cell Type | Example |
|--------|-----------|---------|
| *(none)* | Python | `df.show()` |
| `# MAGIC %md` | Markdown | `# MAGIC %md ## Section Title` |
| `# MAGIC %sql` | SQL | `# MAGIC %sql SELECT count(*) FROM t` |
| `# MAGIC %sh` | Shell | `# MAGIC %sh ls /dbfs/` |
| `# MAGIC %r` | R | `# MAGIC %r library(SparkR)` |
| `# MAGIC %scala` | Scala | `# MAGIC %scala val x = 1` |

---

## After Importing

1. **Attach a cluster**: Click the cluster dropdown at the top of the notebook
2. **Run all cells**: Click **Run All** or press **Ctrl+Shift+Enter**
3. **Run one cell**: Click the cell and press **Shift+Enter**

Every notebook generates its own sample data, so you can run any notebook independently — no need to run prior notebooks first.
