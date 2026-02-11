# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Databricks Repos & Git Integration
# MAGIC **Module 00: Setup and Basics**
# MAGIC
# MAGIC This notebook explores the workspace file system layout, the
# MAGIC notebook context, and how `dbutils.notebook` enables notebook
# MAGIC workflows. We also demonstrate how to inspect the Repos environment.
# MAGIC
# MAGIC **Cluster requirement:** Any cluster with DBR 13.3 LTS or later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook Context — Where Am I?
# MAGIC
# MAGIC Every running notebook has access to context information that tells
# MAGIC you the notebook path, cluster ID, and more.

# COMMAND ----------

# The notebook context provides metadata about the current execution
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "N/A")
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "N/A")

print(f"Notebook path  : {notebook_path}")
print(f"Cluster ID     : {cluster_id}")
print(f"Workspace URL  : {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explore the Workspace File System Layout
# MAGIC
# MAGIC The Databricks workspace organizes files into well-known paths:
# MAGIC - `/Repos/` — Git-integrated repositories
# MAGIC - `/Users/` — Personal workspace folders
# MAGIC - `/Shared/` — Team-accessible folders

# COMMAND ----------

# List top-level workspace sections via DBFS-like paths
# Note: workspace files and DBFS are different file systems
# This lists DBFS root — not workspace objects

print("=== DBFS Root Contents ===")
for item in dbutils.fs.ls("/"):
    item_type = "DIR " if item.isDir() else "FILE"
    print(f"  {item_type}  {item.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Understanding Notebook Source Format
# MAGIC
# MAGIC When stored in Git via Repos, notebooks are saved as plain text files.
# MAGIC A Python notebook starts with `# Databricks notebook source` and uses
# MAGIC `# COMMAND ----------` to separate cells.
# MAGIC
# MAGIC This means:
# MAGIC - Git diffs show meaningful line-by-line changes
# MAGIC - Standard code review tools (GitHub PRs, etc.) work well
# MAGIC - No binary blob (unlike `.ipynb` format)

# COMMAND ----------

# Let us demonstrate by building a notebook source string programmatically
# This shows you exactly what a Databricks notebook file looks like in Git

sample_notebook_source = '''# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # My Sample Notebook
# MAGIC This is how it looks in Git.

# COMMAND ----------

print("Hello from a code cell!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS test_value
'''

print("=== What a Databricks Notebook Looks Like in Git ===")
print(sample_notebook_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dbutils.notebook — Orchestrating Notebooks
# MAGIC
# MAGIC `dbutils.notebook.run()` executes another notebook and returns a
# MAGIC string result. This is the foundation of notebook workflows.
# MAGIC
# MAGIC Since we cannot guarantee a child notebook exists in every workspace,
# MAGIC we will demonstrate the API pattern without actually calling `run()`.

# COMMAND ----------

# --- Pattern: Running a child notebook ---
# Uncomment and modify the path to run a real child notebook in your workspace

# result = dbutils.notebook.run(
#     path="./child_notebook",         # relative or absolute path
#     timeout_seconds=300,             # fail if child takes > 5 minutes
#     arguments={                      # parameters passed as widget values
#         "environment": "dev",
#         "start_date": "2024-01-01"
#     }
# )
# print(f"Child notebook returned: {result}")

# --- Pattern: Exiting a notebook with a return value ---
# In the CHILD notebook, the last cell would contain:
# dbutils.notebook.exit("SUCCESS: processed 1000 rows")

# Demonstrate exit value format (do NOT run dbutils.notebook.exit here
# because it would stop THIS notebook)
exit_value = "SUCCESS: processed 1000 rows"
print(f"Example exit value: {exit_value}")
print(f"Return type is always str: {type(exit_value)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Simulating a Multi-Notebook Workflow
# MAGIC
# MAGIC In production, you might chain notebooks like this:
# MAGIC
# MAGIC ```
# MAGIC Orchestrator Notebook
# MAGIC  |
# MAGIC  +-- Step 1: run("./ingest")       -> "SUCCESS"
# MAGIC  +-- Step 2: run("./transform")    -> "SUCCESS"
# MAGIC  +-- Step 3: run("./publish")      -> "SUCCESS"
# MAGIC  +-- Step 4: run("./validate")     -> "PASS: 0 failures"
# MAGIC ```
# MAGIC
# MAGIC Let us simulate this pattern entirely within this notebook.

# COMMAND ----------

import time

def simulate_notebook_run(name, duration_seconds=1):
    """Simulate running a child notebook."""
    print(f"  Starting: {name}...")
    time.sleep(duration_seconds)
    result = f"SUCCESS: {name} completed"
    print(f"  Finished: {result}")
    return result

# Simulate the orchestration workflow
print("=" * 50)
print("ORCHESTRATOR: Starting pipeline")
print("=" * 50)

steps = ["01_ingest", "02_transform", "03_publish", "04_validate"]
results = {}

for step in steps:
    try:
        results[step] = simulate_notebook_run(step, duration_seconds=0.5)
    except Exception as e:
        results[step] = f"FAILED: {e}"
        print(f"  Pipeline stopped at {step}")
        break

print("=" * 50)
print("ORCHESTRATOR: Pipeline complete")
print("=" * 50)
for step, result in results.items():
    status = "PASS" if "SUCCESS" in result else "FAIL"
    print(f"  [{status}] {step}: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Repos API — Programmatic Access
# MAGIC
# MAGIC The Repos REST API allows CI/CD tools to clone, update, and manage
# MAGIC repositories. Here are the key endpoints (for reference):
# MAGIC
# MAGIC | Method | Endpoint | Purpose |
# MAGIC |--------|----------|---------|
# MAGIC | GET | `/api/2.0/repos` | List all repos |
# MAGIC | POST | `/api/2.0/repos` | Clone a new repo |
# MAGIC | GET | `/api/2.0/repos/{id}` | Get repo details |
# MAGIC | PATCH | `/api/2.0/repos/{id}` | Pull or switch branch |
# MAGIC | DELETE | `/api/2.0/repos/{id}` | Delete a repo |
# MAGIC
# MAGIC Example using the `requests` library (requires a PAT):
# MAGIC ```python
# MAGIC import requests
# MAGIC headers = {"Authorization": f"Bearer {token}"}
# MAGIC resp = requests.get(f"{workspace_url}/api/2.0/repos", headers=headers)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Best Practices Summary
# MAGIC
# MAGIC | Practice | Reason |
# MAGIC |----------|--------|
# MAGIC | Use feature branches | Keeps `main` stable |
# MAGIC | Extract code to `.py` modules | Enables unit testing and reuse |
# MAGIC | Keep notebooks focused | One task per notebook |
# MAGIC | Do not store data in Git | Use cloud storage instead |
# MAGIC | Use `.gitignore` | Prevent artifacts from being committed |
# MAGIC | Code review via PRs | Catches bugs before production |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clean Up
# MAGIC
# MAGIC This notebook did not create any tables, views, or files.
# MAGIC No cleanup is needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next:** Import `05-dbfs-and-volumes_notebook.py` to explore the
# MAGIC Databricks File System and Unity Catalog Volumes.
