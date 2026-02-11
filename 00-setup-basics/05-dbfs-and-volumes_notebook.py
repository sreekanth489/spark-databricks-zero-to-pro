# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — DBFS and Volumes
# MAGIC **Module 00: Setup and Basics**
# MAGIC
# MAGIC This notebook explores the Databricks File System (DBFS), demonstrates
# MAGIC file operations with `dbutils.fs`, and compares DBFS paths with Unity
# MAGIC Catalog Volume paths.
# MAGIC
# MAGIC **Cluster requirement:** Any cluster with DBR 13.3 LTS or later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore DBFS Root
# MAGIC
# MAGIC The DBFS root is the top-level directory. Every workspace has standard
# MAGIC directories like `/FileStore/`, `/databricks-datasets/`, and `/tmp/`.

# COMMAND ----------

# List the root of DBFS
print("=== DBFS Root Directory ===")
print(f"{'Type':<6} {'Name':<40} {'Size':>12}")
print("-" * 60)

for item in dbutils.fs.ls("/"):
    item_type = "DIR" if item.isDir() else "FILE"
    size_str = f"{item.size:,}" if item.size > 0 else "-"
    print(f"{item_type:<6} {item.name:<40} {size_str:>12}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explore Sample Datasets
# MAGIC
# MAGIC Databricks provides sample datasets at `/databricks-datasets/`.
# MAGIC These are great for learning and prototyping.

# COMMAND ----------

# List available sample datasets
print("=== Sample Datasets Available ===")
datasets = dbutils.fs.ls("/databricks-datasets/")

# Show the first 15 datasets
for item in datasets[:15]:
    print(f"  {item.name}")

print(f"\n... and {len(datasets) - 15} more" if len(datasets) > 15 else "")
print(f"\nTotal sample datasets: {len(datasets)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read a Sample File with dbutils.fs.head()
# MAGIC
# MAGIC `head()` reads the first N bytes of a file. Useful for quick previews.

# COMMAND ----------

# Read the first 500 bytes of a sample CSV file
try:
    sample_path = "/databricks-datasets/wine-quality/winequality-red.csv"
    content = dbutils.fs.head(sample_path, 500)
    print(f"=== First 500 bytes of {sample_path} ===")
    print(content)
except Exception as e:
    # Fallback if that dataset is not available
    print(f"Sample dataset not available: {e}")
    print("This is normal on some workspace configurations.")
    print("We will create our own files in the next section.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create, Read, and Delete Files
# MAGIC
# MAGIC Let us use `dbutils.fs.put()` to write files and then explore them.
# MAGIC We will use `/tmp/` which is available in every workspace.

# COMMAND ----------

# Define a temp directory for our experiments
temp_dir = "/tmp/module00_dbfs_demo"

# Create the directory
dbutils.fs.mkdirs(temp_dir)
print(f"Created directory: {temp_dir}")

# Write a text file
dbutils.fs.put(f"{temp_dir}/hello.txt", "Hello from DBFS!\nThis is line 2.\nThis is line 3.", True)
print(f"Wrote: {temp_dir}/hello.txt")

# Write a CSV file
csv_content = """id,name,department,salary
1,Alice,Engineering,95000
2,Bob,Marketing,82000
3,Charlie,Engineering,105000
4,Diana,Data Science,98000
5,Eve,Marketing,78000"""

dbutils.fs.put(f"{temp_dir}/employees.csv", csv_content, True)
print(f"Wrote: {temp_dir}/employees.csv")

# List the directory to verify
print(f"\n=== Contents of {temp_dir} ===")
for item in dbutils.fs.ls(temp_dir):
    print(f"  {item.name:<25} {item.size:>8} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read Files with Spark
# MAGIC
# MAGIC Now let us read the CSV file we created using Spark. This demonstrates
# MAGIC how DBFS paths work seamlessly with Spark.

# COMMAND ----------

# Read the CSV file we created — Spark uses dbfs:/ paths by default
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{temp_dir}/employees.csv")
)

print("=== DataFrame from our DBFS CSV file ===")
display(df)

# Show the schema to verify type inference
print("\nSchema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DBFS Paths vs. Local Paths
# MAGIC
# MAGIC This is a common source of confusion. Let us clarify the path types.
# MAGIC
# MAGIC | Context | DBFS Path | Local Mount Path |
# MAGIC |---------|-----------|------------------|
# MAGIC | `dbutils.fs` | `/tmp/file.txt` | N/A (always DBFS) |
# MAGIC | Spark | `/tmp/file.txt` or `dbfs:/tmp/file.txt` | N/A (always DBFS) |
# MAGIC | `%sh` (shell) | N/A | `/dbfs/tmp/file.txt` |
# MAGIC | Python `open()` | N/A | `/dbfs/tmp/file.txt` |

# COMMAND ----------

# Demonstrate the path difference

# Method 1: dbutils.fs (uses DBFS paths)
content_via_dbutils = dbutils.fs.head(f"{temp_dir}/hello.txt", 100)
print("Via dbutils.fs:")
print(f"  Path: {temp_dir}/hello.txt")
print(f"  Content: {content_via_dbutils[:50]}...")

print()

# Method 2: Python open() (uses local FUSE mount /dbfs/)
try:
    local_path = f"/dbfs{temp_dir}/hello.txt"
    with open(local_path, "r") as f:
        content_via_python = f.read()
    print("Via Python open():")
    print(f"  Path: {local_path}")
    print(f"  Content: {content_via_python[:50]}...")
except FileNotFoundError:
    print(f"Note: /dbfs FUSE mount not available at {local_path}")
    print("This is expected on some cluster types (e.g., shared access mode).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Copy and Move Files

# COMMAND ----------

# Copy a file
dbutils.fs.cp(f"{temp_dir}/hello.txt", f"{temp_dir}/hello_copy.txt")
print(f"Copied hello.txt -> hello_copy.txt")

# Create a subdirectory and move a file into it
dbutils.fs.mkdirs(f"{temp_dir}/archive")
dbutils.fs.mv(f"{temp_dir}/hello_copy.txt", f"{temp_dir}/archive/hello_copy.txt")
print(f"Moved hello_copy.txt -> archive/hello_copy.txt")

# Verify the directory structure
print(f"\n=== Updated contents of {temp_dir} ===")
for item in dbutils.fs.ls(temp_dir):
    print(f"  {item.name}")
    if item.isDir():
        for sub_item in dbutils.fs.ls(item.path):
            print(f"    {sub_item.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. List Mount Points
# MAGIC
# MAGIC Mounts map DBFS paths to external cloud storage. Let us see what
# MAGIC mounts exist in this workspace.

# COMMAND ----------

# List all current mount points
mounts = dbutils.fs.mounts()

print(f"=== Mount Points ({len(mounts)} total) ===")
print(f"{'Mount Point':<40} {'Source':<60}")
print("-" * 100)

for mount in mounts:
    print(f"{mount.mountPoint:<40} {mount.source:<60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Unity Catalog Volumes (If Available)
# MAGIC
# MAGIC If your workspace has Unity Catalog enabled, you can access files
# MAGIC via the `/Volumes/` path. This section demonstrates the path format.
# MAGIC
# MAGIC **Note:** This cell may produce errors if Unity Catalog is not
# MAGIC configured. That is expected on Community Edition.

# COMMAND ----------

# Check if Unity Catalog is available by trying to list catalogs
try:
    catalogs = spark.sql("SHOW CATALOGS").collect()
    print("=== Unity Catalog is Available ===")
    print("Catalogs:")
    for cat in catalogs:
        print(f"  {cat[0]}")

    print("\nVolume path format: /Volumes/<catalog>/<schema>/<volume>/")
    print("Example: /Volumes/my_catalog/default/my_volume/data.csv")

    # Try to list volumes in the current catalog
    try:
        volumes = spark.sql("SHOW VOLUMES").collect()
        if volumes:
            print(f"\nVolumes in current schema:")
            for vol in volumes:
                print(f"  {vol}")
        else:
            print("\nNo volumes found in the current schema.")
    except Exception:
        print("\nCould not list volumes (permissions or schema not set).")

except Exception as e:
    print("Unity Catalog is not available in this workspace.")
    print("This is expected on Databricks Community Edition.")
    print(f"\nTo use Volumes, you need:")
    print(f"  - Databricks Premium or Enterprise tier")
    print(f"  - Unity Catalog metastore assigned to the workspace")
    print(f"  - Appropriate permissions (CREATE VOLUME)")
    print(f"\nVolume path format: /Volumes/<catalog>/<schema>/<volume>/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Compare Path Formats — Summary
# MAGIC
# MAGIC | Storage Type | Path Example | Governed? |
# MAGIC |---|---|---|
# MAGIC | DBFS root | `dbfs:/FileStore/data.csv` | No |
# MAGIC | DBFS mount | `dbfs:/mnt/datalake/data.csv` | No |
# MAGIC | Managed Volume | `/Volumes/catalog/schema/vol/data.csv` | Yes |
# MAGIC | External Volume | `/Volumes/catalog/schema/vol/data.csv` | Yes |
# MAGIC | Driver local | `file:/tmp/data.csv` | No |
# MAGIC
# MAGIC **Recommendation**: Use `/Volumes/` paths for all new work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Clean Up
# MAGIC
# MAGIC Remove all temporary files and directories we created.

# COMMAND ----------

# Remove the entire temp directory recursively
dbutils.fs.rm(temp_dir, recurse=True)
print(f"Removed: {temp_dir} (and all contents)")

# Verify it is gone
try:
    dbutils.fs.ls(temp_dir)
    print("WARNING: Directory still exists!")
except Exception:
    print("Confirmed: directory has been deleted.")

print("\nClean up complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** You have completed all notebooks in Module 00.
# MAGIC
# MAGIC Proceed to **Module 01: DataFrames and Transformations** to start
# MAGIC building real Spark pipelines.
