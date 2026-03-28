# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 1: Build Gold Layer Tables
# MAGIC
# MAGIC This notebook runs as a task inside a Lakeflow Job.
# MAGIC It creates sample Bronze data, then builds Gold aggregations via SQL.

# COMMAND ----------

print("Task 1: Build Gold Layer Tables -- STARTED")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary Bronze table with sample order data
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_orders AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('ORD-001', 'CUST-01', 'Store NYC', 'completed', 250.00, 10.00, DATE '2025-06-15'),
# MAGIC   ('ORD-002', 'CUST-02', 'Store LA',  'completed', 175.50, 5.00,  DATE '2025-06-15'),
# MAGIC   ('ORD-003', 'CUST-03', 'Store CHI', 'completed', 320.00, 20.00, DATE '2025-06-15'),
# MAGIC   ('ORD-004', 'CUST-01', 'Store NYC', 'cancelled', 90.00,  0.00,  DATE '2025-06-15'),
# MAGIC   ('ORD-005', 'CUST-04', 'Store LA',  'completed', 410.75, 15.00, DATE '2025-06-15'),
# MAGIC   ('ORD-006', 'CUST-05', 'Store NYC', 'completed', 88.25,  0.00,  DATE '2025-06-15'),
# MAGIC   ('ORD-007', 'CUST-02', 'Store CHI', 'completed', 560.00, 30.00, DATE '2025-06-15'),
# MAGIC   ('ORD-008', 'CUST-06', 'Store LA',  'pending',   200.00, 0.00,  DATE '2025-06-15'),
# MAGIC   ('ORD-009', 'CUST-03', 'Store NYC', 'completed', 145.00, 5.00,  DATE '2025-06-14'),
# MAGIC   ('ORD-010', 'CUST-07', 'Store CHI', 'completed', 330.00, 12.00, DATE '2025-06-14')
# MAGIC AS orders(order_id, customer_id, store_name, status, order_total, discount, order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick look at the Bronze data
# MAGIC SELECT * FROM bronze_orders

# COMMAND ----------

print("Bronze data created: 10 sample orders across 3 stores")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Table 1: Daily Revenue by Store (only completed orders)
# MAGIC CREATE OR REPLACE TEMP VIEW gold_daily_revenue AS
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     store_name,
# MAGIC     COUNT(*)                              AS total_orders,
# MAGIC     COUNT(DISTINCT customer_id)           AS unique_customers,
# MAGIC     ROUND(SUM(order_total), 2)            AS gross_revenue,
# MAGIC     ROUND(SUM(discount), 2)               AS total_discounts,
# MAGIC     ROUND(SUM(order_total - discount), 2) AS net_revenue,
# MAGIC     ROUND(AVG(order_total), 2)            AS avg_order_value
# MAGIC FROM bronze_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY order_date, store_name
# MAGIC ORDER BY order_date DESC, net_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display Gold daily revenue
# MAGIC SELECT * FROM gold_daily_revenue

# COMMAND ----------

print("Gold daily revenue table built successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold Table 2: Order Status Summary
# MAGIC CREATE OR REPLACE TEMP VIEW gold_order_status AS
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*)                   AS order_count,
# MAGIC     ROUND(SUM(order_total), 2) AS total_value
# MAGIC FROM bronze_orders
# MAGIC GROUP BY status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display order status breakdown
# MAGIC SELECT * FROM gold_order_status

# COMMAND ----------

print("Gold order status table built successfully")

# COMMAND ----------

# Pass metrics to downstream tasks via task values
total_orders = spark.sql("SELECT COUNT(*) AS cnt FROM bronze_orders").collect()[0]["cnt"]
completed = spark.sql("SELECT COUNT(*) AS cnt FROM bronze_orders WHERE status = 'completed'").collect()[0]["cnt"]

dbutils.jobs.taskValues.set(key="total_orders", value=total_orders)
dbutils.jobs.taskValues.set(key="completed_orders", value=completed)

print(f"Task values set -> total_orders: {total_orders}, completed_orders: {completed}")
print("Task 1: Build Gold Layer Tables -- FINISHED")
