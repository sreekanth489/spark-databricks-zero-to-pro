# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 11: Unity Catalog Security — Part 2: Native Row Filters & Column Masks
# MAGIC
# MAGIC **Objective**: Implement native Row Filters and Column Masks — the modern, table-level approach
# MAGIC to data security that replaces the "regional views" pattern.
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Understand WHY native policies replace dynamic views
# MAGIC 2. Create and attach Row Filter functions to a table
# MAGIC 3. Create and attach Column Mask functions to specific columns
# MAGIC 4. Combine Row Filters + Column Masks on the same table
# MAGIC 5. Implement the regional data pattern with Row Filters
# MAGIC 6. Migrate from the old regional views pattern
# MAGIC 7. Inspect applied policies with DESCRIBE EXTENDED
# MAGIC 8. Remove filters and masks
# MAGIC
# MAGIC **Feature availability**: DBR 12.2+ | Unity Catalog required
# MAGIC
# MAGIC **Why this matters**:
# MAGIC ```
# MAGIC  OLD WAY (Dynamic Views)            NEW WAY (Native Policies)
# MAGIC  ─────────────────────────          ────────────────────────────────
# MAGIC  3 views for 3 regions:             1 Row Filter on the TABLE:
# MAGIC  apac_sales_vw                      SELECT * FROM sales;
# MAGIC  emea_sales_vw         ──────▶      (filtered automatically)
# MAGIC  amer_sales_vw
# MAGIC
# MAGIC  Users query different views        Users query ONE table
# MAGIC  View can be bypassed               Cannot be bypassed
# MAGIC  3 objects to maintain              1 function to maintain
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS/Azure with Unity Catalog (DBR 12.2+)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uc_rowfilter_lab
# MAGIC COMMENT 'Day 11b: Native Row Filters and Column Masks lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA uc_rowfilter_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create the Base Table
# MAGIC
# MAGIC This table has:
# MAGIC - `region` column: used for row-level filtering
# MAGIC - `email`, `ssn`, `salary` columns: used for column masking

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id INT,
# MAGIC   first_name  STRING,
# MAGIC   last_name   STRING,
# MAGIC   email       STRING   COMMENT 'PII: Work email',
# MAGIC   ssn         STRING   COMMENT 'PII: Social Security Number',
# MAGIC   department  STRING,
# MAGIC   region      STRING   COMMENT 'Geographic region: APAC, EMEA, AMER',
# MAGIC   salary      DOUBLE   COMMENT 'PII: Annual salary',
# MAGIC   hire_date   DATE,
# MAGIC   is_active   BOOLEAN
# MAGIC )
# MAGIC COMMENT 'Employees table — Row Filters and Column Masks applied here'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1,  'Alice',  'Johnson',  'alice@company.com',  '123-45-6789', 'Engineering', 'AMER', 120000.0, '2020-03-15', true),
# MAGIC   (2,  'Bob',    'Smith',    'bob@company.com',    '234-56-7890', 'Engineering', 'AMER', 135000.0, '2019-07-01', true),
# MAGIC   (3,  'Carol',  'Williams', 'carol@company.com',  '345-67-8901', 'Marketing',   'AMER', 95000.0,  '2021-01-10', true),
# MAGIC   (4,  'David',  'Brown',    'david@company.com',  '456-78-9012', 'Finance',     'EMEA', 110000.0, '2018-11-20', true),
# MAGIC   (5,  'Eve',    'Davis',    'eve@company.com',    '567-89-0123', 'Engineering', 'EMEA', 115000.0, '2022-06-05', true),
# MAGIC   (6,  'Frank',  'Miller',   'frank@company.com',  '678-90-1234', 'Marketing',   'EMEA', 98000.0,  '2020-09-12', false),
# MAGIC   (7,  'Grace',  'Wilson',   'grace@company.com',  '789-01-2345', 'Finance',     'APAC', 105000.0, '2021-04-18', true),
# MAGIC   (8,  'Hank',   'Moore',    'hank@company.com',   '890-12-3456', 'HR',          'APAC', 88000.0,  '2017-02-28', true),
# MAGIC   (9,  'Ivy',    'Taylor',   'ivy@company.com',    '901-23-4567', 'HR',          'APAC', 82000.0,  '2023-01-15', true),
# MAGIC   (10, 'Jack',   'Anderson', 'jack@company.com',   '012-34-5678', 'Engineering', 'AMER', 128000.0, '2021-08-22', true)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all 10 rows visible (no filter yet)
# MAGIC SELECT employee_id, first_name, department, region, salary
# MAGIC FROM employees
# MAGIC ORDER BY region, department

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create a Row Filter Function
# MAGIC
# MAGIC A Row Filter function:
# MAGIC - Returns **BOOLEAN** (true = include row, false = exclude row)
# MAGIC - Takes **column values** as parameters
# MAGIC - Uses `is_account_group_member()` or `current_user()` for identity-based logic
# MAGIC
# MAGIC ```
# MAGIC Function signature:
# MAGIC   CREATE FUNCTION fn_name(col_name TYPE) RETURNS BOOLEAN RETURN <expr>
# MAGIC
# MAGIC The parameter name maps to a column name when you do:
# MAGIC   ALTER TABLE t SET ROW FILTER fn_name ON (column_name)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row Filter: Department-based access
# MAGIC -- Users see rows where department matches their group
# MAGIC -- Admins see all rows
# MAGIC CREATE OR REPLACE FUNCTION dept_row_filter(dept STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   is_account_group_member('admins')
# MAGIC   OR is_account_group_member(lower(dept))
# MAGIC   -- e.g., engineering group sees rows where dept = 'Engineering'
# MAGIC   -- because lower('Engineering') = 'engineering' = group name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function in isolation before attaching it
# MAGIC SELECT
# MAGIC   dept_row_filter('Engineering') AS engineering_dept_result,
# MAGIC   dept_row_filter('Finance')     AS finance_dept_result,
# MAGIC   dept_row_filter('Marketing')   AS marketing_dept_result
# MAGIC -- As admin, all return true. As engineering user, only Engineering returns true.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach the row filter to the employees table
# MAGIC -- ON (department) maps the 'department' column to the 'dept' parameter
# MAGIC ALTER TABLE employees
# MAGIC   SET ROW FILTER dept_row_filter ON (department);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the TABLE directly — row filter is applied automatically
# MAGIC -- As admin/owner: all 10 rows visible
# MAGIC -- As engineering user: only Engineering rows
# MAGIC SELECT employee_id, first_name, department, region
# MAGIC FROM employees
# MAGIC ORDER BY department

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify the filter is applied** by checking DESCRIBE EXTENDED:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the table — look for Row Filter section
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Regional Row Filter (Replaces Regional Views)
# MAGIC
# MAGIC This is the key pattern that replaces creating 3 separate views for APAC/EMEA/AMER.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, remove the department filter (we'll apply a different one)
# MAGIC ALTER TABLE employees DROP ROW FILTER

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row Filter: Regional access
# MAGIC -- Convention: group names match region names in lowercase
# MAGIC --   APAC team is in 'apac_team' group
# MAGIC --   EMEA team is in 'emea_team' group
# MAGIC --   AMER team is in 'amer_team' group
# MAGIC --   Global team ('global_data_team') sees all
# MAGIC CREATE OR REPLACE FUNCTION region_row_filter(region_col STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   is_account_group_member('admins')
# MAGIC   OR is_account_group_member('global_data_team')
# MAGIC   OR is_account_group_member(concat(lower(region_col), '_team'))
# MAGIC   -- concat('apac', '_team') = 'apac_team'
# MAGIC   -- User in apac_team sees APAC rows, emea_team sees EMEA rows, etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function
# MAGIC SELECT
# MAGIC   region_row_filter('APAC') AS would_see_apac,
# MAGIC   region_row_filter('EMEA') AS would_see_emea,
# MAGIC   region_row_filter('AMER') AS would_see_amer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach regional row filter to the table
# MAGIC ALTER TABLE employees
# MAGIC   SET ROW FILTER region_row_filter ON (region)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All regions visible as admin; only your region if in apac/emea/amer group
# MAGIC SELECT employee_id, first_name, region, department
# MAGIC FROM employees
# MAGIC ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC **This replaces the old pattern**:
# MAGIC
# MAGIC ```sql
# MAGIC -- OLD: 3 views to maintain
# MAGIC CREATE VIEW apac_employees_vw AS SELECT * FROM employees WHERE region = 'APAC';
# MAGIC CREATE VIEW emea_employees_vw AS SELECT * FROM employees WHERE region = 'EMEA';
# MAGIC CREATE VIEW amer_employees_vw AS SELECT * FROM employees WHERE region = 'AMER';
# MAGIC -- APAC users query apac_employees_vw, EMEA users query emea_employees_vw, etc.
# MAGIC
# MAGIC -- NEW: 1 function + 1 ALTER TABLE. Everyone queries employees.
# MAGIC ALTER TABLE employees SET ROW FILTER region_row_filter ON (region);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Create Column Mask Functions
# MAGIC
# MAGIC A Column Mask function:
# MAGIC - Takes ONE parameter (the column value of the column being masked)
# MAGIC - Returns a value of the **same type** as the column
# MAGIC - Is attached per-column with `ALTER TABLE ... ALTER COLUMN ... SET MASK`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column mask for email addresses
# MAGIC CREATE OR REPLACE FUNCTION mask_email_col(email_val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr')
# MAGIC       OR is_account_group_member('admins')
# MAGIC       THEN email_val                                          -- unmasked
# MAGIC     ELSE concat(left(email_val, 2), '***@***')               -- masked
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column mask for SSN
# MAGIC CREATE OR REPLACE FUNCTION mask_ssn_col(ssn_val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr')
# MAGIC       OR is_account_group_member('admins')
# MAGIC       THEN ssn_val                                            -- full SSN
# MAGIC     ELSE concat('***-**-', right(ssn_val, 4))                -- last 4 only
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column mask for salary
# MAGIC -- Return NULL if user is not in finance/hr/admins group
# MAGIC CREATE OR REPLACE FUNCTION mask_salary_col(salary_val DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('finance')
# MAGIC       OR is_account_group_member('hr')
# MAGIC       OR is_account_group_member('admins')
# MAGIC       THEN salary_val                                         -- full salary
# MAGIC     ELSE NULL                                                 -- hidden
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attach column masks to the table
# MAGIC ALTER TABLE employees ALTER COLUMN email  SET MASK mask_email_col;
# MAGIC ALTER TABLE employees ALTER COLUMN ssn    SET MASK mask_ssn_col;
# MAGIC ALTER TABLE employees ALTER COLUMN salary SET MASK mask_salary_col

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the table — column masks are applied automatically
# MAGIC -- As admin: unmasked data
# MAGIC -- As analyst: al***@***, ***-**-6789, NULL
# MAGIC SELECT employee_id, first_name, email, ssn, salary
# MAGIC FROM employees
# MAGIC ORDER BY employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Row Filter + Column Mask Together
# MAGIC
# MAGIC Both can be active on the same table simultaneously.
# MAGIC When you query:
# MAGIC 1. Row filter runs first → selects qualifying rows
# MAGIC 2. Column masks run → transforms column values in qualifying rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify both are active
# MAGIC DESCRIBE EXTENDED employees
# MAGIC -- Look for: Row Filter, Column Masks sections

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One query, two security layers applied transparently
# MAGIC SELECT employee_id, first_name, last_name, email, ssn, region, salary
# MAGIC FROM employees

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Grant Required Permissions
# MAGIC
# MAGIC When you have Row Filters and Column Masks, users also need `EXECUTE` on the functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Full permission grant pattern for native row filters + column masks:
# MAGIC
# MAGIC -- Navigation prerequisites
# MAGIC GRANT USE CATALOG ON CATALOG databricks_pro   TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA uc_rowfilter_lab  TO `analysts`;
# MAGIC
# MAGIC -- Data access on the TABLE (not a view)
# MAGIC GRANT SELECT      ON TABLE employees           TO `analysts`;
# MAGIC
# MAGIC -- Execute permissions on filter/mask functions
# MAGIC GRANT EXECUTE ON FUNCTION region_row_filter    TO `analysts`;
# MAGIC GRANT EXECUTE ON FUNCTION mask_email_col       TO `analysts`;
# MAGIC GRANT EXECUTE ON FUNCTION mask_ssn_col         TO `analysts`;
# MAGIC GRANT EXECUTE ON FUNCTION mask_salary_col      TO `analysts`;
# MAGIC
# MAGIC -- Now analysts can query employees directly:
# MAGIC --   SELECT * FROM employees;
# MAGIC -- They see only their region's rows, with PII masked.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Inspecting Applied Policies

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full metadata including row filter and column masks
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check row filters via information_schema
# MAGIC SELECT
# MAGIC   table_catalog, table_schema, table_name, row_filter
# MAGIC FROM databricks_pro.information_schema.tables
# MAGIC WHERE table_schema = 'uc_rowfilter_lab'
# MAGIC   AND row_filter IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check column masks via information_schema
# MAGIC SELECT
# MAGIC   table_name, column_name, mask_catalog, mask_schema, mask_name
# MAGIC FROM databricks_pro.information_schema.columns
# MAGIC WHERE table_schema = 'uc_rowfilter_lab'
# MAGIC   AND mask_name IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Remove Filters and Masks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove row filter from table
# MAGIC ALTER TABLE employees DROP ROW FILTER

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove column masks
# MAGIC ALTER TABLE employees ALTER COLUMN email  DROP MASK;
# MAGIC ALTER TABLE employees ALTER COLUMN ssn    DROP MASK;
# MAGIC ALTER TABLE employees ALTER COLUMN salary DROP MASK

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all data visible now (no filter, no mask)
# MAGIC SELECT employee_id, first_name, email, ssn, region, salary
# MAGIC FROM employees
# MAGIC ORDER BY employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Migration Pattern Summary
# MAGIC
# MAGIC When migrating from regional dynamic views to native Row Filters:

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Migration Checklist: Regional Views → Row Filters
# MAGIC ─────────────────────────────────────────────────
# MAGIC
# MAGIC  1. Audit existing views
# MAGIC     SELECT table_name, view_definition
# MAGIC     FROM information_schema.views
# MAGIC     WHERE view_definition LIKE '%region%'
# MAGIC        OR view_definition LIKE '%current_user()%';
# MAGIC
# MAGIC  2. Extract the WHERE clause logic into a Row Filter function
# MAGIC     Old: WHERE region = 'APAC'  (hard-coded per view)
# MAGIC     New: RETURN is_account_group_member(concat(lower(region_col), '_team'))
# MAGIC
# MAGIC  3. Attach Row Filter to the BASE TABLE
# MAGIC     ALTER TABLE sales SET ROW FILTER region_filter ON (region);
# MAGIC
# MAGIC  4. Extract column masking CASE logic into Column Mask functions
# MAGIC     Old: CASE WHEN current_user() IN (...) THEN ssn ELSE '***' END
# MAGIC     New: CREATE FUNCTION mask_ssn(v STRING) RETURNS STRING
# MAGIC          ALTER TABLE sales ALTER COLUMN ssn SET MASK mask_ssn;
# MAGIC
# MAGIC  5. Update grants — now grant SELECT on TABLE (not views)
# MAGIC     GRANT SELECT ON TABLE sales TO `analysts`;
# MAGIC     GRANT EXECUTE ON FUNCTION region_filter TO `analysts`;
# MAGIC
# MAGIC  6. Test: analysts query TABLE directly and see filtered+masked data
# MAGIC     SELECT * FROM sales;  -- filter and masks apply automatically
# MAGIC
# MAGIC  7. Deprecate old views with tags, then drop after cutover
# MAGIC     ALTER VIEW apac_sales_vw SET TBLPROPERTIES ('status' = 'deprecated');
# MAGIC     DROP VIEW apac_sales_vw;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Dynamic Views vs Row Filters vs Column Masks Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Comparison Matrix
# MAGIC ─────────────────────────────────────────────────────────────────────────────────
# MAGIC                      Dynamic Views       Row Filters         Column Masks
# MAGIC ─────────────────────────────────────────────────────────────────────────────────
# MAGIC Object type          VIEW                FUNCTION            FUNCTION
# MAGIC Applied to           VIEW                TABLE               TABLE COLUMN
# MAGIC User queries         the VIEW            the TABLE           the TABLE
# MAGIC Bypassable?          YES (if TABLE grant) NO                 NO
# MAGIC Maintenance          N views for N cases  1 fn per filter    1 fn per column
# MAGIC Stacking             Complex              Combine both       Multiple columns
# MAGIC DBR requirement      Any                 DBR 12.2+ (GA)     DBR 12.2+ (GA)
# MAGIC UC required?         No (works in HMS)   Yes                Yes
# MAGIC Visible in metadata  View definition      DESCRIBE EXTENDED  DESCRIBE EXTENDED
# MAGIC ─────────────────────────────────────────────────────────────────────────────────
# MAGIC
# MAGIC When to use Dynamic Views:
# MAGIC   - Legacy Hive Metastore (no UC)
# MAGIC   - Very complex multi-table join logic in the security condition
# MAGIC   - When you want a named "secure view" as a product (e.g., published to consumers)
# MAGIC
# MAGIC When to use Native Row Filters:
# MAGIC   - Replacing regional views (APAC/EMEA/AMER patterns)
# MAGIC   - Department-based access patterns
# MAGIC   - Any row-level filter that maps group → column value
# MAGIC   - New projects on UC (always prefer this over views)
# MAGIC
# MAGIC When to use Native Column Masks:
# MAGIC   - Replacing column CASE expressions in views
# MAGIC   - PII protection (email, SSN, phone, salary)
# MAGIC   - When the masking logic is reused across many tables
# MAGIC   - Consistent masking across all consumers of a table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop functions first (must drop masks before dropping table)
# MAGIC DROP FUNCTION IF EXISTS region_row_filter;
# MAGIC DROP FUNCTION IF EXISTS dept_row_filter;
# MAGIC DROP FUNCTION IF EXISTS mask_email_col;
# MAGIC DROP FUNCTION IF EXISTS mask_ssn_col;
# MAGIC DROP FUNCTION IF EXISTS mask_salary_col;
# MAGIC
# MAGIC DROP TABLE  IF EXISTS employees;
# MAGIC DROP SCHEMA IF EXISTS uc_rowfilter_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What You Learned |
# MAGIC |---------|-----------------|
# MAGIC | Row Filter function | RETURNS BOOLEAN, maps column → filter logic |
# MAGIC | Attaching row filter | `ALTER TABLE t SET ROW FILTER fn ON (col)` |
# MAGIC | Column mask function | Returns same type as column, transforms values |
# MAGIC | Attaching column mask | `ALTER TABLE t ALTER COLUMN c SET MASK fn` |
# MAGIC | Regional views → Row Filters | 1 filter function replaces N regional views |
# MAGIC | Bypass protection | Native policies cannot be bypassed (unlike views) |
# MAGIC | Grant pattern | SELECT on TABLE + EXECUTE on functions |
# MAGIC | Inspection | DESCRIBE EXTENDED, information_schema.columns |
# MAGIC | Removal | DROP ROW FILTER, DROP MASK |
# MAGIC
# MAGIC **Next**: [Day 12: Managed vs External Tables](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day12-managed-vs-external-tables)
