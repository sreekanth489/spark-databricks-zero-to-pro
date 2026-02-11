# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Notebook Fundamentals
# MAGIC **Module 00: Setup and Basics**
# MAGIC
# MAGIC This notebook demonstrates cell types, magic commands, widgets,
# MAGIC display functions, and dbutils. Run each cell in order.
# MAGIC
# MAGIC **Cluster requirement:** Any cluster with DBR 13.3 LTS or later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cell Types Demo
# MAGIC
# MAGIC This cell is **Markdown**. It supports:
# MAGIC - **Bold**, *italic*, `code`
# MAGIC - [Links](https://docs.databricks.com)
# MAGIC - Tables, lists, headings
# MAGIC
# MAGIC The cells below demonstrate Python, SQL, and Shell.

# COMMAND ----------

# --- Python Cell (default language) ---
# Create sample data that we will reuse throughout this notebook

from pyspark.sql import Row

employees = spark.createDataFrame([
    Row(emp_id=1, name="Alice",   dept="Engineering", salary=95000, city="Seattle"),
    Row(emp_id=2, name="Bob",     dept="Marketing",   salary=82000, city="New York"),
    Row(emp_id=3, name="Charlie", dept="Engineering", salary=105000, city="Seattle"),
    Row(emp_id=4, name="Diana",   dept="Data Science", salary=98000, city="Austin"),
    Row(emp_id=5, name="Eve",     dept="Marketing",   salary=78000, city="New York"),
    Row(emp_id=6, name="Frank",   dept="Engineering", salary=110000, city="Austin"),
    Row(emp_id=7, name="Grace",   dept="Data Science", salary=102000, city="Seattle"),
    Row(emp_id=8, name="Hank",    dept="Marketing",   salary=85000, city="Austin"),
])

# Register as a temp view so SQL cells can query it
employees.createOrReplaceTempView("employees")

print(f"Created 'employees' DataFrame with {employees.count()} rows")
print(f"Columns: {employees.columns}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Cell: query the temp view we created in the Python cell above
# MAGIC -- Notice how languages share the same SparkSession context
# MAGIC
# MAGIC SELECT dept,
# MAGIC        COUNT(*)       AS headcount,
# MAGIC        ROUND(AVG(salary), 0) AS avg_salary
# MAGIC FROM employees
# MAGIC GROUP BY dept
# MAGIC ORDER BY avg_salary DESC;

# COMMAND ----------

# MAGIC %sh
# MAGIC # Shell Cell: runs on the driver node only
# MAGIC echo "=== Driver Node Info ==="
# MAGIC echo "Hostname : $(hostname)"
# MAGIC echo "OS       : $(uname -s)"
# MAGIC echo "Python   : $(python --version 2>&1)"
# MAGIC echo "Working directory : $(pwd)"
# MAGIC echo ""
# MAGIC echo "=== Disk Usage ==="
# MAGIC df -h / 2>/dev/null | head -2 || echo "(df not available)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The display() Function
# MAGIC
# MAGIC `display()` renders rich, interactive tables with built-in charting.
# MAGIC After running the cell below, click the chart icon (bar chart) to
# MAGIC visualize the data.

# COMMAND ----------

# display() provides an interactive table with sort, filter, and chart options
# Compare this with df.show() which gives plain text output
display(employees)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. displayHTML() — Custom HTML Output
# MAGIC
# MAGIC For custom formatted output, use `displayHTML()`.

# COMMAND ----------

# Build an HTML summary of our data
dept_counts = employees.groupBy("dept").count().collect()

html_rows = ""
for row in sorted(dept_counts, key=lambda r: r["count"], reverse=True):
    bar_width = row["count"] * 60  # pixels per employee
    html_rows += f"""
    <tr>
      <td style="padding: 8px; font-weight: bold;">{row["dept"]}</td>
      <td style="padding: 8px;">{row["count"]}</td>
      <td style="padding: 8px;">
        <div style="background: #1B8BD1; width: {bar_width}px; height: 20px; border-radius: 4px;"></div>
      </td>
    </tr>
    """

html = f"""
<h3>Department Headcount</h3>
<table style="border-collapse: collapse; font-family: sans-serif;">
  <tr style="border-bottom: 2px solid #333;">
    <th style="padding: 8px; text-align: left;">Department</th>
    <th style="padding: 8px; text-align: left;">Count</th>
    <th style="padding: 8px; text-align: left;">Visual</th>
  </tr>
  {html_rows}
</table>
"""

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Widgets — Parameterize Your Notebooks
# MAGIC
# MAGIC Widgets let you create interactive inputs. When this notebook runs
# MAGIC as a job, widget values can be passed as parameters.

# COMMAND ----------

# Create widgets — these appear at the top of the notebook
dbutils.widgets.dropdown("department", "All", ["All", "Engineering", "Marketing", "Data Science"], "Department Filter")
dbutils.widgets.text("min_salary", "0", "Minimum Salary")

# COMMAND ----------

# Read widget values and apply them
selected_dept = dbutils.widgets.get("department")
min_salary = int(dbutils.widgets.get("min_salary"))

print(f"Selected department : {selected_dept}")
print(f"Minimum salary      : ${min_salary:,}")

# Apply filters based on widget values
from pyspark.sql.functions import col

filtered = employees

if selected_dept != "All":
    filtered = filtered.filter(col("dept") == selected_dept)

if min_salary > 0:
    filtered = filtered.filter(col("salary") >= min_salary)

print(f"\nFiltered results ({filtered.count()} rows):")
display(filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dbutils.fs — File System Operations Preview
# MAGIC
# MAGIC `dbutils.fs` provides access to DBFS (Databricks File System).
# MAGIC We will cover DBFS in depth in Topic 05. Here is a quick preview.

# COMMAND ----------

# List the root of DBFS
print("Contents of dbfs:/")
for item in dbutils.fs.ls("/"):
    item_type = "DIR " if item.isDir() else "FILE"
    print(f"  {item_type}  {item.name:<30}  {item.size:>10} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. dbutils Help — Discover Available Modules
# MAGIC
# MAGIC Each dbutils module has a `.help()` method that documents
# MAGIC all available functions.

# COMMAND ----------

# Top-level help
dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. show() vs display() Comparison
# MAGIC
# MAGIC Run the next two cells and compare the output format.

# COMMAND ----------

# show() — plain text, fixed-width, limited formatting
print("=== df.show() output ===")
employees.show(truncate=False)

# COMMAND ----------

# display() — interactive HTML table (try clicking column headers to sort)
# After running, click the "+" button below the table to add a visualization
display(employees.orderBy("salary", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clean Up
# MAGIC
# MAGIC Remove the temp view and widgets created during this notebook.

# COMMAND ----------

# Drop the temporary view
spark.catalog.dropTempView("employees")
print("Dropped temp view: employees")

# Remove all widgets
dbutils.widgets.removeAll()
print("Removed all widgets")

print("\nClean up complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Next:** Import `04-databricks-repos-git_notebook.py` to learn about
# MAGIC Git integration in Databricks.
