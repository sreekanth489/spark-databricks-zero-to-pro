# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # COPY INTO -- Idempotent File Loading
# MAGIC
# MAGIC **Module 02 -- Topic 03 | Databricks Zero-to-Pro**
# MAGIC
# MAGIC This notebook demonstrates the `COPY INTO` SQL command for loading files
# MAGIC into Delta tables with built-in idempotency. All data is generated inline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Temp paths
LANDING_DIR = "/tmp/m02_copy_into/landing"
TARGET_TABLE = "m02_copy_into_demo"

# Clean up from any prior run
dbutils.fs.rm("/tmp/m02_copy_into", recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
dbutils.fs.mkdirs(LANDING_DIR)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the Target Delta Table
# MAGIC
# MAGIC `COPY INTO` requires the target table to already exist. We create an empty
# MAGIC Delta table with the expected schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an empty Delta table with explicit schema
# MAGIC CREATE TABLE IF NOT EXISTS m02_copy_into_demo (
# MAGIC     id INT,
# MAGIC     product STRING,
# MAGIC     amount DOUBLE,
# MAGIC     sale_date STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Sample CSV Files to the Landing Directory

# COMMAND ----------

# Batch 1 -- initial sales data
batch1 = """id,product,amount,sale_date
1,Widget A,19.99,2024-01-10
2,Widget B,24.50,2024-01-11
3,Widget C,7.25,2024-01-12
"""

# Batch 2 -- more sales data
batch2 = """id,product,amount,sale_date
4,Widget A,19.99,2024-01-13
5,Widget D,55.00,2024-01-14
6,Widget B,24.50,2024-01-15
"""

dbutils.fs.put(f"{LANDING_DIR}/sales_001.csv", batch1, overwrite=True)
dbutils.fs.put(f"{LANDING_DIR}/sales_002.csv", batch2, overwrite=True)

print("Files in landing directory:")
for f in dbutils.fs.ls(LANDING_DIR):
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. First COPY INTO -- Load All Files

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO m02_copy_into_demo
# MAGIC FROM '/tmp/m02_copy_into/landing'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data
# MAGIC SELECT * FROM m02_copy_into_demo ORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count FROM m02_copy_into_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Idempotency -- Run COPY INTO Again
# MAGIC
# MAGIC Running the same `COPY INTO` command again should load **zero** new rows,
# MAGIC because the files have already been tracked.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same command, second run
# MAGIC COPY INTO m02_copy_into_demo
# MAGIC FROM '/tmp/m02_copy_into/landing'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count should still be 6 (no duplicates)
# MAGIC SELECT COUNT(*) AS row_count FROM m02_copy_into_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Incremental Loading -- Add New Files
# MAGIC
# MAGIC When new files appear in the landing directory, the next `COPY INTO` run
# MAGIC picks them up while skipping previously loaded files.

# COMMAND ----------

# Batch 3 -- new file arrives
batch3 = """id,product,amount,sale_date
7,Widget E,39.99,2024-01-16
8,Widget A,19.99,2024-01-17
9,Widget F,12.75,2024-01-18
"""
dbutils.fs.put(f"{LANDING_DIR}/sales_003.csv", batch3, overwrite=True)
print("Added sales_003.csv to landing directory.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Third run: only sales_003.csv is new
# MAGIC COPY INTO m02_copy_into_demo
# MAGIC FROM '/tmp/m02_copy_into/landing'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should now have 9 rows (6 from before + 3 new)
# MAGIC SELECT COUNT(*) AS row_count FROM m02_copy_into_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m02_copy_into_demo ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The `force` Option -- Reprocessing All Files
# MAGIC
# MAGIC The `force = true` copy option tells COPY INTO to ignore the tracking
# MAGIC metadata and reload ALL files. Use this only when you intentionally want
# MAGIC to reprocess (e.g., after fixing source data).
# MAGIC
# MAGIC **Warning:** This WILL create duplicate rows unless you truncate the table
# MAGIC first.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, let's check the current count
# MAGIC SELECT COUNT(*) AS before_force FROM m02_copy_into_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Force reload all files (will create duplicates!)
# MAGIC COPY INTO m02_copy_into_demo
# MAGIC FROM '/tmp/m02_copy_into/landing'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC     'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'force' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now we have duplicates: 9 original + 9 reloaded = 18
# MAGIC SELECT COUNT(*) AS after_force FROM m02_copy_into_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. COPY INTO with Transformations (Subquery)
# MAGIC
# MAGIC You can apply column renaming, casting, and metadata enrichment during
# MAGIC the COPY INTO load using a subquery.

# COMMAND ----------

# New landing directory for the subquery demo
LANDING_DIR_V2 = "/tmp/m02_copy_into/landing_v2"
TARGET_TABLE_V2 = "m02_copy_into_enriched"

dbutils.fs.mkdirs(LANDING_DIR_V2)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_V2}")

# Write a headerless CSV
raw_csv = """101,Gadget X,89.99,2024-02-01
102,Gadget Y,45.50,2024-02-02
103,Gadget Z,120.00,2024-02-03
"""
dbutils.fs.put(f"{LANDING_DIR_V2}/raw_001.csv", raw_csv, overwrite=True)

# Create target table with enriched schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE_V2} (
        sale_id INT,
        product_name STRING,
        sale_amount DOUBLE,
        sale_date STRING,
        source_file STRING,
        loaded_at TIMESTAMP
    ) USING DELTA
""")

print("Ready for subquery demo.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COPY INTO with a subquery that transforms and enriches
# MAGIC COPY INTO m02_copy_into_enriched
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         _c0::INT           AS sale_id,
# MAGIC         _c1::STRING        AS product_name,
# MAGIC         _c2::DOUBLE        AS sale_amount,
# MAGIC         _c3::STRING        AS sale_date,
# MAGIC         _metadata.file_name AS source_file,
# MAGIC         current_timestamp() AS loaded_at
# MAGIC     FROM '/tmp/m02_copy_into/landing_v2'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m02_copy_into_enriched ORDER BY sale_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Run | Files Processed | Total Rows | Notes |
# MAGIC |-----|----------------|------------|-------|
# MAGIC | 1st COPY INTO | sales_001, sales_002 | 6 | Initial load |
# MAGIC | 2nd COPY INTO | (none) | 6 | Idempotent -- same files skipped |
# MAGIC | 3rd COPY INTO | sales_003 | 9 | Incremental -- only new file |
# MAGIC | 4th COPY INTO (force) | all 3 files | 18 | Force reloaded everything |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_V2}")
dbutils.fs.rm("/tmp/m02_copy_into", recurse=True)
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **04 -- External Sources** to learn how to ingest from JDBC,
# MAGIC Kafka, and cloud storage APIs.
