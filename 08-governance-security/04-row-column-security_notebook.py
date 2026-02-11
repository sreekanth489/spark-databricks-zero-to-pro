# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Row & Column Security
# MAGIC > Module 08 — Topic 04 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - Creating sample data with sensitive columns (PII, financial data)
# MAGIC - Implementing dynamic views for row-level security
# MAGIC - Implementing dynamic views for column masking
# MAGIC - ROW FILTER and COLUMN MASK DDL syntax demonstration
# MAGIC - Testing common masking patterns (full, partial, hash)
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog for ROW FILTER / COLUMN MASK
# MAGIC - Community Edition users: dynamic views work; native features are shown as reference
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup — Create Sensitive Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m08_security_demo
# MAGIC COMMENT 'Row and column security demonstrations';
# MAGIC USE m08_security_demo;

# COMMAND ----------

# Create employee data with sensitive columns
from pyspark.sql import Row
from datetime import date

employees = [
    Row(1,  "Alice",   "Johnson",  "Engineering", "North", "123-45-6789", 125000.00, "alice.johnson@company.com",  date(1988, 3, 15)),
    Row(2,  "Bob",     "Smith",    "Marketing",   "South", "234-56-7890",  95000.00, "bob.smith@company.com",      date(1992, 7, 20)),
    Row(3,  "Carol",   "Williams", "Engineering", "North", "345-67-8901", 135000.00, "carol.williams@company.com", date(1985, 11, 8)),
    Row(4,  "David",   "Brown",    "Finance",     "East",  "456-78-9012", 110000.00, "david.brown@company.com",    date(1990, 5, 25)),
    Row(5,  "Eve",     "Davis",    "Engineering", "West",  "567-89-0123", 140000.00, "eve.davis@company.com",      date(1987, 9, 12)),
    Row(6,  "Frank",   "Miller",   "Marketing",   "South", "678-90-1234",  88000.00, "frank.miller@company.com",   date(1995, 1, 3)),
    Row(7,  "Grace",   "Wilson",   "Finance",     "East",  "789-01-2345", 105000.00, "grace.wilson@company.com",   date(1991, 4, 17)),
    Row(8,  "Henry",   "Moore",    "HR",          "North", "890-12-3456",  92000.00, "henry.moore@company.com",    date(1993, 8, 29)),
    Row(9,  "Irene",   "Taylor",   "Engineering", "West",  "901-23-4567", 130000.00, "irene.taylor@company.com",   date(1986, 12, 6)),
    Row(10, "Jack",    "Anderson", "HR",          "North", "012-34-5678",  97000.00, "jack.anderson@company.com",  date(1994, 2, 14)),
    Row(11, "Karen",   "Thomas",   "Marketing",   "East",  "111-22-3333",  91000.00, "karen.thomas@company.com",   date(1989, 6, 21)),
    Row(12, "Leo",     "Jackson",  "Finance",     "West",  "222-33-4444", 115000.00, "leo.jackson@company.com",    date(1988, 10, 30)),
]

schema = """employee_id INT, first_name STRING, last_name STRING,
            department STRING, region STRING, ssn STRING,
            salary DECIMAL(10,2), email STRING, birth_date DATE"""

emp_df = spark.createDataFrame(employees, schema)
emp_df.write.mode("overwrite").saveAsTable("m08_security_demo.employees")

print(f"Created employees table with {emp_df.count()} rows containing sensitive data.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the raw data (all columns visible - this is what we want to restrict)
# MAGIC SELECT * FROM m08_security_demo.employees ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Dynamic Views for Row-Level Security
# MAGIC
# MAGIC Dynamic views use `current_user()` and `is_member()` to filter rows
# MAGIC based on who is running the query.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, check who we are
# MAGIC SELECT current_user() AS my_identity;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Row Filter by Department
# MAGIC
# MAGIC Only show employees in the querying user's department.
# MAGIC Since we cannot actually assign users to departments in a demo,
# MAGIC we simulate with a parameter.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dynamic view: Row-level security by department
# MAGIC -- In production, you would use current_user() or is_member() checks
# MAGIC CREATE OR REPLACE VIEW m08_security_demo.v_employees_by_dept AS
# MAGIC SELECT *
# MAGIC FROM m08_security_demo.employees
# MAGIC WHERE
# MAGIC     -- HR admin group can see all rows
# MAGIC     is_member('hr_admin')
# MAGIC     -- Department managers see only their department
# MAGIC     OR department = 'Engineering';  -- Simulated: pretend current user is in Engineering
# MAGIC
# MAGIC -- NOTE: In a real scenario, you would map current_user() to their department
# MAGIC -- using a lookup table:
# MAGIC --   OR department = (SELECT dept FROM user_department_map WHERE user = current_user())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This view only shows Engineering employees (simulated filter)
# MAGIC SELECT * FROM m08_security_demo.v_employees_by_dept ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Row Filter by Region (Multi-Tenant Pattern)
# MAGIC
# MAGIC Common in multi-tenant applications where each user/team sees
# MAGIC only their region's data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dynamic view: Multi-tenant row-level security
# MAGIC CREATE OR REPLACE VIEW m08_security_demo.v_employees_north_region AS
# MAGIC SELECT *
# MAGIC FROM m08_security_demo.employees
# MAGIC WHERE
# MAGIC     region = 'North'  -- Simulated: user belongs to North region
# MAGIC     OR is_member('global_admin');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only North region employees visible
# MAGIC SELECT employee_id, first_name, last_name, department, region
# MAGIC FROM m08_security_demo.v_employees_north_region
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Dynamic Views for Column Masking
# MAGIC
# MAGIC Column masking hides or transforms sensitive column values while
# MAGIC still allowing access to non-sensitive columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dynamic view with column masking
# MAGIC CREATE OR REPLACE VIEW m08_security_demo.v_employees_masked AS
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     department,
# MAGIC     region,
# MAGIC
# MAGIC     -- SSN: partial mask (show last 4 digits only)
# MAGIC     CASE
# MAGIC         WHEN is_member('hr_admin') THEN ssn
# MAGIC         ELSE CONCAT('XXX-XX-', SUBSTRING(ssn, 8, 4))
# MAGIC     END AS ssn,
# MAGIC
# MAGIC     -- Salary: null mask for non-finance users
# MAGIC     CASE
# MAGIC         WHEN is_member('finance_team') OR is_member('hr_admin') THEN salary
# MAGIC         ELSE NULL
# MAGIC     END AS salary,
# MAGIC
# MAGIC     -- Email: show domain only for non-HR
# MAGIC     CASE
# MAGIC         WHEN is_member('hr_admin') THEN email
# MAGIC         ELSE CONCAT('****@', SUBSTRING_INDEX(email, '@', -1))
# MAGIC     END AS email,
# MAGIC
# MAGIC     -- Birth date: show year only
# MAGIC     CASE
# MAGIC         WHEN is_member('hr_admin') THEN birth_date
# MAGIC         ELSE DATE_TRUNC('year', birth_date)
# MAGIC     END AS birth_date
# MAGIC
# MAGIC FROM m08_security_demo.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View masked data (as a non-admin user would see it)
# MAGIC -- Note: is_member() returns FALSE on Community Edition for custom groups,
# MAGIC -- so the masked versions are shown by default (which is the safe behavior).
# MAGIC SELECT * FROM m08_security_demo.v_employees_masked ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Common Masking Patterns — Implementation

# COMMAND ----------

# Demonstrate all masking patterns using PySpark
from pyspark.sql.functions import (
    col, lit, concat, substring, sha2, when, length,
    regexp_replace, date_trunc, lpad
)

source_df = spark.table("m08_security_demo.employees")

# Pattern 1: Full Mask
full_mask_df = source_df.select(
    col("employee_id"),
    col("first_name"),
    regexp_replace(col("ssn"), ".", "*").alias("ssn_full_mask")
)
print("Pattern 1: Full Mask")
full_mask_df.show(5, truncate=False)

# COMMAND ----------

# Pattern 2: Partial Mask (last 4 characters visible)
partial_mask_df = source_df.select(
    col("employee_id"),
    col("first_name"),
    concat(lit("XXX-XX-"), substring(col("ssn"), 8, 4)).alias("ssn_partial_mask")
)
print("Pattern 2: Partial Mask (last 4 visible)")
partial_mask_df.show(5, truncate=False)

# COMMAND ----------

# Pattern 3: Hash Mask (SHA-256)
hash_mask_df = source_df.select(
    col("employee_id"),
    col("email"),
    sha2(col("email"), 256).alias("email_hash")
)
print("Pattern 3: Hash Mask (SHA-256)")
print("Use case: Join on hashed values without exposing the original")
hash_mask_df.show(5, truncate=False)

# COMMAND ----------

# Pattern 4: Null Mask
null_mask_df = source_df.select(
    col("employee_id"),
    col("first_name"),
    lit(None).cast("decimal(10,2)").alias("salary_null_mask"),
    lit(None).cast("string").alias("ssn_null_mask")
)
print("Pattern 4: Null Mask")
null_mask_df.show(5, truncate=False)

# COMMAND ----------

# Pattern 5: Range/Bucket Mask
from pyspark.sql.functions import floor, year, months_between, current_date

range_mask_df = source_df.select(
    col("employee_id"),
    col("first_name"),
    col("salary"),
    concat(
        (floor(col("salary") / 10000) * 10000).cast("int").cast("string"),
        lit(" - "),
        ((floor(col("salary") / 10000) + 1) * 10000).cast("int").cast("string")
    ).alias("salary_range"),
    col("birth_date"),
    concat(
        floor(months_between(current_date(), col("birth_date")) / 12 / 10).cast("int") * 10,
        lit("-"),
        (floor(months_between(current_date(), col("birth_date")) / 12 / 10).cast("int") * 10 + 9)
    ).alias("age_range")
)
print("Pattern 5: Range/Bucket Mask")
range_mask_df.show(5, truncate=False)

# COMMAND ----------

# Pattern 6: Date Truncation
date_trunc_df = source_df.select(
    col("employee_id"),
    col("first_name"),
    col("birth_date").alias("original_date"),
    date_trunc("year", col("birth_date")).cast("date").alias("truncated_to_year"),
    date_trunc("month", col("birth_date")).cast("date").alias("truncated_to_month")
)
print("Pattern 6: Date Truncation")
date_trunc_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Native ROW FILTER Syntax (Unity Catalog)
# MAGIC
# MAGIC ROW FILTER is a Unity Catalog feature that applies a boolean filter
# MAGIC function directly on the table. Unlike dynamic views, this cannot be
# MAGIC bypassed — every query goes through the filter.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ROW FILTER SYNTAX (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Step 1: Create a row filter function
# MAGIC -- The function takes column values as parameters and returns BOOLEAN
# MAGIC --
# MAGIC -- CREATE OR REPLACE FUNCTION m08_security_demo.department_filter(dept STRING)
# MAGIC -- RETURNS BOOLEAN
# MAGIC -- RETURN (
# MAGIC --     -- Admins see all rows
# MAGIC --     is_member('hr_admin')
# MAGIC --     -- Other users see only their department (looked up from a mapping table)
# MAGIC --     OR dept = (
# MAGIC --         SELECT department
# MAGIC --         FROM user_department_mapping
# MAGIC --         WHERE user_email = current_user()
# MAGIC --     )
# MAGIC -- );
# MAGIC --
# MAGIC -- Step 2: Apply the filter to the table
# MAGIC -- ALTER TABLE m08_security_demo.employees
# MAGIC -- SET ROW FILTER m08_security_demo.department_filter ON (department);
# MAGIC --
# MAGIC -- Step 3: Now ALL queries on the table are filtered automatically
# MAGIC -- SELECT * FROM m08_security_demo.employees;
# MAGIC -- ^ Only returns rows where the filter function returns TRUE
# MAGIC --
# MAGIC -- Remove a row filter:
# MAGIC -- ALTER TABLE m08_security_demo.employees DROP ROW FILTER;
# MAGIC --
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT 'See comments above for ROW FILTER syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulated ROW FILTER Behavior

# COMMAND ----------

# Simulate ROW FILTER behavior
def simulated_row_filter(df, filter_column, allowed_value, is_admin=False):
    """
    Simulates the behavior of a Unity Catalog ROW FILTER.
    In production, the filter is applied transparently by the engine.
    """
    if is_admin:
        print(f"[ROW FILTER] User is admin -> returning all {df.count()} rows")
        return df
    else:
        filtered = df.filter(col(filter_column) == allowed_value)
        print(f"[ROW FILTER] Filtering {filter_column} = '{allowed_value}'")
        print(f"  Before filter: {df.count()} rows")
        print(f"  After filter:  {filtered.count()} rows")
        return filtered

emp_df = spark.table("m08_security_demo.employees")

# Simulating a non-admin user in the Engineering department
print("=== Non-admin user (Engineering department) ===")
filtered_df = simulated_row_filter(emp_df, "department", "Engineering", is_admin=False)
filtered_df.select("employee_id", "first_name", "last_name", "department").show()

print("\n=== Admin user (sees everything) ===")
admin_df = simulated_row_filter(emp_df, "department", "Engineering", is_admin=True)
admin_df.select("employee_id", "first_name", "last_name", "department").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Native COLUMN MASK Syntax (Unity Catalog)
# MAGIC
# MAGIC COLUMN MASK is a Unity Catalog feature that transforms column values
# MAGIC based on the querying user's identity. Applied directly on the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- COLUMN MASK SYNTAX (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Step 1: Create a masking function
# MAGIC -- The function takes the column value as input and returns the masked value
# MAGIC --
# MAGIC -- CREATE OR REPLACE FUNCTION m08_security_demo.mask_ssn(ssn_value STRING)
# MAGIC -- RETURNS STRING
# MAGIC -- RETURN CASE
# MAGIC --     WHEN is_member('hr_admin') THEN ssn_value
# MAGIC --     ELSE CONCAT('XXX-XX-', RIGHT(ssn_value, 4))
# MAGIC -- END;
# MAGIC --
# MAGIC -- CREATE OR REPLACE FUNCTION m08_security_demo.mask_salary(salary_value DECIMAL(10,2))
# MAGIC -- RETURNS DECIMAL(10,2)
# MAGIC -- RETURN CASE
# MAGIC --     WHEN is_member('finance_team') OR is_member('hr_admin') THEN salary_value
# MAGIC --     ELSE NULL
# MAGIC -- END;
# MAGIC --
# MAGIC -- CREATE OR REPLACE FUNCTION m08_security_demo.mask_email(email_value STRING)
# MAGIC -- RETURNS STRING
# MAGIC -- RETURN CASE
# MAGIC --     WHEN is_member('hr_admin') THEN email_value
# MAGIC --     ELSE CONCAT('****@', SUBSTRING_INDEX(email_value, '@', -1))
# MAGIC -- END;
# MAGIC --
# MAGIC -- Step 2: Apply masks to specific columns
# MAGIC -- ALTER TABLE m08_security_demo.employees
# MAGIC -- ALTER COLUMN ssn SET MASK m08_security_demo.mask_ssn;
# MAGIC --
# MAGIC -- ALTER TABLE m08_security_demo.employees
# MAGIC -- ALTER COLUMN salary SET MASK m08_security_demo.mask_salary;
# MAGIC --
# MAGIC -- ALTER TABLE m08_security_demo.employees
# MAGIC -- ALTER COLUMN email SET MASK m08_security_demo.mask_email;
# MAGIC --
# MAGIC -- Step 3: Now ALL queries automatically apply the masks
# MAGIC -- SELECT * FROM m08_security_demo.employees;
# MAGIC -- ^ SSN, salary, and email are masked based on user's group membership
# MAGIC --
# MAGIC -- Remove a column mask:
# MAGIC -- ALTER TABLE m08_security_demo.employees ALTER COLUMN ssn DROP MASK;
# MAGIC --
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT 'See comments above for COLUMN MASK syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulated COLUMN MASK Behavior

# COMMAND ----------

from pyspark.sql.functions import concat, lit, substring, sha2, when

def simulated_column_masks(df, user_groups=None):
    """
    Simulates the behavior of Unity Catalog COLUMN MASKs.
    In production, masks are applied transparently by the engine.
    """
    if user_groups is None:
        user_groups = []

    is_hr = "hr_admin" in user_groups
    is_finance = "finance_team" in user_groups

    masked_df = df.select(
        col("employee_id"),
        col("first_name"),
        col("last_name"),
        col("department"),
        col("region"),
        # SSN mask
        when(lit(is_hr), col("ssn"))
            .otherwise(concat(lit("XXX-XX-"), substring(col("ssn"), 8, 4)))
            .alias("ssn"),
        # Salary mask
        when(lit(is_hr) | lit(is_finance), col("salary"))
            .otherwise(lit(None).cast("decimal(10,2)"))
            .alias("salary"),
        # Email mask
        when(lit(is_hr), col("email"))
            .otherwise(concat(lit("****@"), substring_index(col("email"), "@", -1)))
            .alias("email"),
        # Birth date mask
        when(lit(is_hr), col("birth_date"))
            .otherwise(date_trunc("year", col("birth_date")).cast("date"))
            .alias("birth_date"),
    )
    return masked_df

from pyspark.sql.functions import substring_index, date_trunc

emp_df = spark.table("m08_security_demo.employees")

# Scenario 1: Regular employee (no special groups)
print("=== Regular Employee (no special groups) ===")
print("SSN: partial mask | Salary: NULL | Email: domain only | Birth date: year only")
masked1 = simulated_column_masks(emp_df, user_groups=[])
masked1.show(5, truncate=False)

# COMMAND ----------

# Scenario 2: Finance team member
print("=== Finance Team Member ===")
print("SSN: partial mask | Salary: VISIBLE | Email: domain only | Birth date: year only")
masked2 = simulated_column_masks(emp_df, user_groups=["finance_team"])
masked2.show(5, truncate=False)

# COMMAND ----------

# Scenario 3: HR Admin (sees everything)
print("=== HR Admin (full access) ===")
print("SSN: VISIBLE | Salary: VISIBLE | Email: VISIBLE | Birth date: VISIBLE")
masked3 = simulated_column_masks(emp_df, user_groups=["hr_admin"])
masked3.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Combined Row Filter + Column Mask
# MAGIC
# MAGIC In production, you typically combine both: filter rows so users see
# MAGIC only relevant data, AND mask sensitive columns within those rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- COMBINED APPROACH (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Apply both a row filter and column masks to the same table:
# MAGIC --
# MAGIC -- ALTER TABLE employees SET ROW FILTER dept_filter ON (department);
# MAGIC -- ALTER TABLE employees ALTER COLUMN ssn SET MASK mask_ssn;
# MAGIC -- ALTER TABLE employees ALTER COLUMN salary SET MASK mask_salary;
# MAGIC --
# MAGIC -- Now a query like:
# MAGIC --   SELECT * FROM employees;
# MAGIC --
# MAGIC -- 1. First, the ROW FILTER removes rows the user cannot see
# MAGIC -- 2. Then, COLUMN MASKs transform sensitive values in remaining rows
# MAGIC -- 3. User receives filtered AND masked results
# MAGIC --
# MAGIC -- This layered security ensures defense-in-depth.
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT 'See comments above for combined approach' AS note;

# COMMAND ----------

# Demonstrate the combined approach
print("=" * 60)
print("COMBINED: Row Filter + Column Mask")
print("=" * 60)
print()
print("User: Marketing department manager (not in hr_admin or finance_team)")
print()

emp_df = spark.table("m08_security_demo.employees")

# Step 1: Row filter (only Marketing department)
print("Step 1: ROW FILTER -> department = 'Marketing'")
row_filtered = emp_df.filter(col("department") == "Marketing")
print(f"  Rows after filter: {row_filtered.count()}")

# Step 2: Column masks (partial SSN, no salary, masked email)
print("Step 2: COLUMN MASK -> SSN partial, salary NULL, email domain only")
final_df = row_filtered.select(
    col("employee_id"),
    col("first_name"),
    col("last_name"),
    col("department"),
    concat(lit("XXX-XX-"), substring(col("ssn"), 8, 4)).alias("ssn"),
    lit(None).cast("decimal(10,2)").alias("salary"),
    concat(lit("****@"), substring_index(col("email"), "@", -1)).alias("email"),
)
print("\nFinal result (filtered + masked):")
final_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Security Strategy Summary

# COMMAND ----------

# Print a decision guide for choosing the right approach
print("=" * 70)
print("DECISION GUIDE: Which security approach to use?")
print("=" * 70)
print()
print("Question 1: Do you have Unity Catalog?")
print("  NO  -> Use Dynamic Views (only option)")
print("  YES -> Continue to Question 2")
print()
print("Question 2: Must security be enforceable at the table level?")
print("  YES -> Use ROW FILTER and/or COLUMN MASK (cannot be bypassed)")
print("  NO  -> Dynamic Views are simpler for read-only use cases")
print()
print("Question 3: Do you need complex logic (joins, subqueries)?")
print("  YES -> Dynamic Views support full SQL (row filters have limits)")
print("  NO  -> ROW FILTER / COLUMN MASK are cleaner and more maintainable")
print()
print("Question 4: How many tables need the same policy?")
print("  MANY -> Create reusable MASK/FILTER functions, apply to each table")
print("  FEW  -> Either approach works fine")
print()
print("Best practice: Use native ROW FILTER + COLUMN MASK when available,")
print("fall back to dynamic views for complex scenarios or non-UC environments.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS m08_security_demo.v_employees_by_dept;
# MAGIC DROP VIEW IF EXISTS m08_security_demo.v_employees_north_region;
# MAGIC DROP VIEW IF EXISTS m08_security_demo.v_employees_masked;
# MAGIC DROP TABLE IF EXISTS m08_security_demo.employees;
# MAGIC DROP DATABASE IF EXISTS m08_security_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. Dynamic views: row/column security via current_user() and is_member()")
print("  2. ROW FILTER: table-level boolean filter function (cannot be bypassed)")
print("  3. COLUMN MASK: table-level column transformation (cannot be bypassed)")
print("  4. Masking patterns: full, partial, hash, null, range, date truncation")
print("  5. Combine row filters + column masks for defense-in-depth")
print("  6. Native UC features preferred; dynamic views for legacy/complex cases")
print()
print("Next: 05-delta-sharing_notebook.py")
