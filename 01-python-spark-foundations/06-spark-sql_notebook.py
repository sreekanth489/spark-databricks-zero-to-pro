# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL -- Hands-On
# MAGIC
# MAGIC Practice SQL queries in Spark: temp views, aggregations, CTEs, subqueries,
# MAGIC built-in functions, and catalog exploration.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Sample Data
# MAGIC
# MAGIC We create two DataFrames and register them as temporary views.

# COMMAND ----------

from pyspark.sql.functions import col, rand, floor, lit, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import date, timedelta

# Employees data
employees_data = [
    (1, "Alice", "Engineering", 95000.0, date(2019, 3, 15)),
    (2, "Bob", "Marketing", 72000.0, date(2020, 7, 1)),
    (3, "Charlie", "Engineering", 120000.0, date(2017, 1, 10)),
    (4, "Diana", "Marketing", 68000.0, date(2021, 5, 20)),
    (5, "Eve", "Data Science", 85000.0, date(2020, 11, 1)),
    (6, "Frank", "Data Science", 110000.0, date(2018, 6, 15)),
    (7, "Grace", "Engineering", 105000.0, date(2019, 9, 1)),
    (8, "Henry", "Marketing", 78000.0, date(2022, 2, 14)),
    (9, "Ivy", "Data Science", 92000.0, date(2021, 8, 1)),
    (10, "Jack", "Engineering", 88000.0, date(2023, 1, 15)),
]

employees_df = spark.createDataFrame(
    employees_data,
    ["emp_id", "name", "department", "salary", "hire_date"]
)

# Departments data
departments_data = [
    ("Engineering", 500000, "Building A"),
    ("Marketing", 200000, "Building B"),
    ("Data Science", 350000, "Building A"),
    ("HR", 150000, "Building C"),
]

departments_df = spark.createDataFrame(
    departments_data,
    ["dept_name", "budget", "location"]
)

# Register as temp views
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")

print("Views created: employees, departments")
employees_df.show()
departments_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic SELECT and WHERE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select specific columns
# MAGIC SELECT name, department, salary
# MAGIC FROM employees
# MAGIC WHERE salary > 80000
# MAGIC ORDER BY salary DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Multiple conditions
# MAGIC SELECT name, department, salary
# MAGIC FROM employees
# MAGIC WHERE department = 'Engineering'
# MAGIC   AND salary >= 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregations with GROUP BY and HAVING

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     department,
# MAGIC     COUNT(*) AS headcount,
# MAGIC     ROUND(AVG(salary), 2) AS avg_salary,
# MAGIC     MIN(salary) AS min_salary,
# MAGIC     MAX(salary) AS max_salary,
# MAGIC     ROUND(SUM(salary), 2) AS total_salary
# MAGIC FROM employees
# MAGIC GROUP BY department
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY avg_salary DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Joins

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inner join employees with departments
# MAGIC SELECT
# MAGIC     e.name,
# MAGIC     e.department,
# MAGIC     e.salary,
# MAGIC     d.budget,
# MAGIC     d.location
# MAGIC FROM employees e
# MAGIC INNER JOIN departments d ON e.department = d.dept_name
# MAGIC ORDER BY e.name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Left join to see departments without employees
# MAGIC SELECT
# MAGIC     d.dept_name,
# MAGIC     d.budget,
# MAGIC     COUNT(e.emp_id) AS employee_count
# MAGIC FROM departments d
# MAGIC LEFT JOIN employees e ON d.dept_name = e.department
# MAGIC GROUP BY d.dept_name, d.budget
# MAGIC ORDER BY employee_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Common Table Expressions (CTEs)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dept_stats AS (
# MAGIC     SELECT
# MAGIC         department,
# MAGIC         AVG(salary) AS avg_salary,
# MAGIC         COUNT(*) AS headcount
# MAGIC     FROM employees
# MAGIC     GROUP BY department
# MAGIC ),
# MAGIC dept_ranked AS (
# MAGIC     SELECT *,
# MAGIC            RANK() OVER (ORDER BY avg_salary DESC) AS salary_rank
# MAGIC     FROM dept_stats
# MAGIC )
# MAGIC SELECT * FROM dept_ranked

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Subqueries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Scalar subquery: compare each salary to the overall average
# MAGIC SELECT
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     ROUND(salary - (SELECT AVG(salary) FROM employees), 2) AS diff_from_avg
# MAGIC FROM employees
# MAGIC ORDER BY diff_from_avg DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Correlated subquery: employees earning above their department average
# MAGIC SELECT name, department, salary
# MAGIC FROM employees e
# MAGIC WHERE salary > (
# MAGIC     SELECT AVG(salary)
# MAGIC     FROM employees
# MAGIC     WHERE department = e.department
# MAGIC )
# MAGIC ORDER BY department, salary DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. String Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     name,
# MAGIC     UPPER(name) AS name_upper,
# MAGIC     LOWER(name) AS name_lower,
# MAGIC     LENGTH(name) AS name_length,
# MAGIC     SUBSTRING(name, 1, 3) AS first_three,
# MAGIC     CONCAT(name, ' (', department, ')') AS name_with_dept
# MAGIC FROM employees
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Date Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     name,
# MAGIC     hire_date,
# MAGIC     YEAR(hire_date) AS hire_year,
# MAGIC     MONTH(hire_date) AS hire_month,
# MAGIC     DATEDIFF(CURRENT_DATE(), hire_date) AS days_employed,
# MAGIC     ROUND(DATEDIFF(CURRENT_DATE(), hire_date) / 365.25, 1) AS years_employed,
# MAGIC     DATE_ADD(hire_date, 365) AS one_year_anniversary,
# MAGIC     DATE_FORMAT(hire_date, 'MMMM dd, yyyy') AS formatted_date
# MAGIC FROM employees
# MAGIC ORDER BY hire_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conditional Functions (CASE, IF, COALESCE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     name,
# MAGIC     salary,
# MAGIC     CASE
# MAGIC         WHEN salary >= 110000 THEN 'Band 4 - Senior'
# MAGIC         WHEN salary >= 90000  THEN 'Band 3 - Mid-Senior'
# MAGIC         WHEN salary >= 75000  THEN 'Band 2 - Mid'
# MAGIC         ELSE 'Band 1 - Junior'
# MAGIC     END AS salary_band,
# MAGIC     IF(salary > 100000, 'Above 100K', 'Below 100K') AS above_100k
# MAGIC FROM employees
# MAGIC ORDER BY salary DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Window Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     name,
# MAGIC     department,
# MAGIC     salary,
# MAGIC     RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
# MAGIC     DENSE_RANK() OVER (ORDER BY salary DESC) AS overall_rank,
# MAGIC     SUM(salary) OVER (PARTITION BY department) AS dept_total_salary,
# MAGIC     ROUND(salary / SUM(salary) OVER (PARTITION BY department) * 100, 1) AS pct_of_dept_total
# MAGIC FROM employees
# MAGIC ORDER BY department, dept_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Mixing DataFrame API and SQL

# COMMAND ----------

from pyspark.sql.functions import col, desc

# Step 1: DataFrame API -- filter
senior_employees = employees_df.filter(col("salary") > 80000)

# Step 2: Register as a view for SQL
senior_employees.createOrReplaceTempView("senior_employees")

# Step 3: SQL query
result_sql = spark.sql("""
    SELECT department, COUNT(*) AS senior_count, ROUND(AVG(salary), 2) AS avg_salary
    FROM senior_employees
    GROUP BY department
""")

# Step 4: Back to DataFrame API for final ordering
final = result_sql.orderBy(desc("avg_salary"))
final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Set Operations

# COMMAND ----------

# Create two employee groups
engineering = spark.sql("SELECT name FROM employees WHERE department = 'Engineering'")
high_earners = spark.sql("SELECT name FROM employees WHERE salary > 90000")

engineering.createOrReplaceTempView("engineering_names")
high_earners.createOrReplaceTempView("high_earner_names")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INTERSECT: engineers who are also high earners
# MAGIC SELECT name FROM engineering_names
# MAGIC INTERSECT
# MAGIC SELECT name FROM high_earner_names

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXCEPT: engineers who are NOT high earners
# MAGIC SELECT name FROM engineering_names
# MAGIC EXCEPT
# MAGIC SELECT name FROM high_earner_names

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Exploring the Catalog

# COMMAND ----------

# List all tables and views in the current session
tables = spark.catalog.listTables()
print("=== Tables and Views ===")
for t in tables:
    print(f"  {t.name:30s} type={t.tableType:15s} isTemporary={t.isTemporary}")

# COMMAND ----------

# List columns of the employees view
columns = spark.catalog.listColumns("employees")
print("=== Columns of 'employees' ===")
for c in columns:
    print(f"  {c.name:15s} {c.dataType:15s} nullable={c.nullable}")

# COMMAND ----------

# Check if a table exists
print(f"'employees' exists: {spark.catalog.tableExists('employees')}")
print(f"'orders' exists: {spark.catalog.tableExists('orders')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop temporary views
spark.catalog.dropTempView("employees")
spark.catalog.dropTempView("departments")
spark.catalog.dropTempView("senior_employees")
spark.catalog.dropTempView("engineering_names")
spark.catalog.dropTempView("high_earner_names")
print("All temporary views dropped. Notebook complete.")
