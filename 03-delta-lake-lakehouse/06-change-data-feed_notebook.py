# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Change Data Feed (CDF)
# MAGIC > Module 03 -- Topic 06 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a Delta table with CDF enabled
# MAGIC 2. Perform INSERT, UPDATE, and DELETE operations
# MAGIC 3. Read the change feed and inspect metadata columns
# MAGIC 4. Filter changes by type
# MAGIC 5. Demonstrate incremental processing with version ranges

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Table with CDF Enabled

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.customers_cdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table with Change Data Feed enabled
# MAGIC CREATE TABLE module03.customers_cdf (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   membership_tier STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Initial Insert (Version 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 1: Initial customer data
# MAGIC INSERT INTO module03.customers_cdf VALUES
# MAGIC   (1, 'Alice Johnson', 'alice@email.com', 'Seattle', 'Gold'),
# MAGIC   (2, 'Bob Smith', 'bob@email.com', 'Portland', 'Silver'),
# MAGIC   (3, 'Carol White', 'carol@email.com', 'Denver', 'Bronze'),
# MAGIC   (4, 'David Brown', 'david@email.com', 'Austin', 'Gold'),
# MAGIC   (5, 'Eve Davis', 'eve@email.com', 'Chicago', 'Silver');
# MAGIC
# MAGIC SELECT * FROM module03.customers_cdf ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- UPDATE Operation (Version 2)
# MAGIC
# MAGIC Upgrade Bob and Carol's membership tiers.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 2: Membership upgrades
# MAGIC UPDATE module03.customers_cdf
# MAGIC SET membership_tier = 'Gold'
# MAGIC WHERE customer_id IN (2, 3);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- DELETE Operation (Version 3)
# MAGIC
# MAGIC David has closed his account.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 3: Customer departure
# MAGIC DELETE FROM module03.customers_cdf
# MAGIC WHERE customer_id = 4;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- More INSERTs (Version 4)
# MAGIC
# MAGIC New customers join.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 4: New customers
# MAGIC INSERT INTO module03.customers_cdf VALUES
# MAGIC   (6, 'Frank Miller', 'frank@email.com', 'Miami', 'Bronze'),
# MAGIC   (7, 'Grace Lee', 'grace@email.com', 'Boston', 'Silver');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Read the Full Change Feed
# MAGIC
# MAGIC Now let's read ALL changes since version 1 (the first insert).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read all changes from version 1 to latest
# MAGIC SELECT *
# MAGIC FROM table_changes('module03.customers_cdf', 1)
# MAGIC ORDER BY _commit_version, customer_id, _change_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- Understanding Change Types
# MAGIC
# MAGIC Let's break down each change type.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERTS: New rows added
# MAGIC SELECT customer_id, name, membership_tier,
# MAGIC        _change_type, _commit_version, _commit_timestamp
# MAGIC FROM table_changes('module03.customers_cdf', 1)
# MAGIC WHERE _change_type = 'insert'
# MAGIC ORDER BY _commit_version, customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATES: Before and after images
# MAGIC -- update_preimage = the row BEFORE the update
# MAGIC -- update_postimage = the row AFTER the update
# MAGIC SELECT customer_id, name, membership_tier,
# MAGIC        _change_type, _commit_version
# MAGIC FROM table_changes('module03.customers_cdf', 1)
# MAGIC WHERE _change_type LIKE 'update%'
# MAGIC ORDER BY customer_id, _change_type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETES: Rows that were removed
# MAGIC SELECT customer_id, name, membership_tier,
# MAGIC        _change_type, _commit_version
# MAGIC FROM table_changes('module03.customers_cdf', 1)
# MAGIC WHERE _change_type = 'delete'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 -- Read Change Feed Using DataFrame API

# COMMAND ----------

# Read changes between specific versions
changes_df = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .option("endingVersion", 3)
    .table("module03.customers_cdf"))

print("Changes in versions 2-3 (updates and deletes):")
changes_df.select(
    "customer_id", "name", "membership_tier",
    "_change_type", "_commit_version"
).orderBy("_commit_version", "customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 -- Incremental Processing Pattern
# MAGIC
# MAGIC Process only changes since the last checkpoint version.

# COMMAND ----------

# Simulate incremental processing
# Assume we last processed version 2
last_processed_version = 2

# Read only new changes (version 3 onwards)
new_changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed_version + 1)
    .table("module03.customers_cdf"))

print(f"New changes since version {last_processed_version}:")
new_changes.select(
    "customer_id", "name", "_change_type", "_commit_version"
).show(truncate=False)

# Separate by change type for downstream processing
inserts = new_changes.filter("_change_type = 'insert'")
updates = new_changes.filter("_change_type = 'update_postimage'")
deletes = new_changes.filter("_change_type = 'delete'")

print(f"New inserts:  {inserts.count()}")
print(f"New updates:  {updates.count()}")
print(f"New deletes:  {deletes.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 -- CDF Summary by Version

# COMMAND ----------

from pyspark.sql import functions as F

all_changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("module03.customers_cdf"))

summary = (all_changes
    .groupBy("_commit_version", "_change_type")
    .count()
    .orderBy("_commit_version", "_change_type"))

print("Change summary by version:")
summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 -- Verify Table History Matches CDF
# MAGIC
# MAGIC The table history and CDF are complementary -- history shows operations,
# MAGIC CDF shows the actual row-level changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY module03.customers_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.customers_cdf")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
