# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 11: Unity Catalog Security — Part 1: GRANT/REVOKE & Dynamic Views
# MAGIC
# MAGIC **Objective**: Master Unity Catalog access control, privilege hierarchy, and dynamic views
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Explore the privilege prerequisite chain (USE CATALOG → USE SCHEMA → SELECT)
# MAGIC 2. Grant, revoke, and show privileges
# MAGIC 3. Implement row-level security with dynamic views (legacy pattern)
# MAGIC 4. Implement column masking with dynamic views
# MAGIC 5. Create reusable masking UDFs
# MAGIC 6. Manage table ownership
# MAGIC 7. Query privilege metadata via information_schema
# MAGIC
# MAGIC **Security Model**:
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  UC Security Model                               │
# MAGIC │                                                                  │
# MAGIC │  GRANT privilege ON securable_object TO principal               │
# MAGIC │                                                                  │
# MAGIC │  WHO (Principals)     WHAT (Privileges)   WHERE (Objects)       │
# MAGIC │  Users                SELECT               METASTORE            │
# MAGIC │  Service Principals   MODIFY               CATALOG              │
# MAGIC │  Groups               CREATE               SCHEMA               │
# MAGIC │                       USE CATALOG          TABLE / VIEW         │
# MAGIC │                       USE SCHEMA           VOLUME               │
# MAGIC │                       READ/WRITE VOLUME    FUNCTION             │
# MAGIC │                       EXECUTE              EXT LOCATION         │
# MAGIC │                                                                  │
# MAGIC │  Prerequisite Chain:                                             │
# MAGIC │    USE CATALOG ──▶ USE SCHEMA ──▶ SELECT/MODIFY/etc.            │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS/Azure with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uc_security_lab
# MAGIC COMMENT 'Day 11: Unity Catalog Security lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA uc_security_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create Sample Tables with PII Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id INT,
# MAGIC   first_name  STRING,
# MAGIC   last_name   STRING,
# MAGIC   email       STRING   COMMENT 'PII: Work email',
# MAGIC   ssn         STRING   COMMENT 'PII: Social Security Number',
# MAGIC   department  STRING,
# MAGIC   region      STRING,
# MAGIC   salary      DOUBLE   COMMENT 'PII: Annual salary',
# MAGIC   hire_date   DATE,
# MAGIC   manager_id  INT,
# MAGIC   is_active   BOOLEAN
# MAGIC )
# MAGIC COMMENT 'Employee records with PII — security lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1,  'Alice',  'Johnson',  'alice@company.com',  '123-45-6789', 'Engineering', 'AMER', 120000.0, '2020-03-15', NULL, true),
# MAGIC   (2,  'Bob',    'Smith',    'bob@company.com',    '234-56-7890', 'Engineering', 'AMER', 135000.0, '2019-07-01', 1,    true),
# MAGIC   (3,  'Carol',  'Williams', 'carol@company.com',  '345-67-8901', 'Marketing',   'AMER', 95000.0,  '2021-01-10', NULL, true),
# MAGIC   (4,  'David',  'Brown',    'david@company.com',  '456-78-9012', 'Finance',     'EMEA', 110000.0, '2018-11-20', NULL, true),
# MAGIC   (5,  'Eve',    'Davis',    'eve@company.com',    '567-89-0123', 'Engineering', 'EMEA', 115000.0, '2022-06-05', 1,    true),
# MAGIC   (6,  'Frank',  'Miller',   'frank@company.com',  '678-90-1234', 'Marketing',   'EMEA', 98000.0,  '2020-09-12', 3,    false),
# MAGIC   (7,  'Grace',  'Wilson',   'grace@company.com',  '789-01-2345', 'Finance',     'APAC', 105000.0, '2021-04-18', 4,    true),
# MAGIC   (8,  'Hank',   'Moore',    'hank@company.com',   '890-12-3456', 'HR',          'APAC', 88000.0,  '2017-02-28', NULL, true),
# MAGIC   (9,  'Ivy',    'Taylor',   'ivy@company.com',    '901-23-4567', 'HR',          'APAC', 82000.0,  '2023-01-15', 8,    true),
# MAGIC   (10, 'Jack',   'Anderson', 'jack@company.com',   '012-34-5678', 'Engineering', 'AMER', 128000.0, '2021-08-22', 1,    true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE departments (
# MAGIC   department_name STRING,
# MAGIC   department_head STRING,
# MAGIC   cost_center     STRING,
# MAGIC   budget          DOUBLE,
# MAGIC   location        STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO departments VALUES
# MAGIC   ('Engineering', 'Alice Johnson',  'CC-ENG-001', 500000.0, 'San Francisco'),
# MAGIC   ('Marketing',   'Carol Williams', 'CC-MKT-001', 250000.0, 'New York'),
# MAGIC   ('Finance',     'David Brown',    'CC-FIN-001', 300000.0, 'Chicago'),
# MAGIC   ('HR',          'Hank Moore',     'CC-HR-001',  150000.0, 'San Francisco')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Understanding GRANT and REVOKE
# MAGIC
# MAGIC **Critical concept — prerequisite chain**:
# MAGIC To SELECT from `databricks_pro.uc_security_lab.employees`, a user needs:
# MAGIC
# MAGIC ```
# MAGIC Step 1: GRANT USE CATALOG  ON CATALOG databricks_pro  TO `group`
# MAGIC Step 2: GRANT USE SCHEMA   ON SCHEMA uc_security_lab  TO `group`
# MAGIC Step 3: GRANT SELECT       ON TABLE employees         TO `group`
# MAGIC
# MAGIC  USE CATALOG alone does NOT give data access — only navigation
# MAGIC  USE SCHEMA alone does NOT give data access — only navigation
# MAGIC  SELECT is what actually gives data access
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check who the current user is
# MAGIC SELECT current_user() AS me

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show current grants on our schema
# MAGIC SHOW GRANTS ON SCHEMA uc_security_lab

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show grants on the employees table
# MAGIC SHOW GRANTS ON TABLE employees

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant/Revoke Syntax Reference
# MAGIC
# MAGIC The cells below show the syntax. Run them only if you have other users/groups to test with.
# MAGIC Wrap group names in backticks, emails in backticks.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Minimal read access for analysts group:
# MAGIC GRANT USE CATALOG ON CATALOG databricks_pro    TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA uc_security_lab    TO `analysts`;
# MAGIC GRANT SELECT      ON TABLE employees            TO `analysts`;
# MAGIC
# MAGIC -- Shortcut: GRANT on schema covers all tables in that schema
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA uc_security_lab TO `analysts`;
# MAGIC
# MAGIC -- Write access
# MAGIC GRANT MODIFY ON TABLE employees TO `data_engineers`;
# MAGIC
# MAGIC -- Create new tables in schema
# MAGIC GRANT CREATE TABLE ON SCHEMA uc_security_lab TO `data_engineers`;
# MAGIC
# MAGIC -- Full schema access
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA uc_security_lab TO `schema_owner_group`;
# MAGIC
# MAGIC -- Volume access
# MAGIC GRANT READ VOLUME  ON VOLUME raw_files TO `analysts`;
# MAGIC GRANT WRITE VOLUME ON VOLUME raw_files TO `data_engineers`;
# MAGIC
# MAGIC -- Revoke
# MAGIC REVOKE SELECT ON TABLE employees FROM `analysts`;
# MAGIC
# MAGIC -- Transfer ownership (best practice: assign to group, not individual)
# MAGIC ALTER TABLE employees SET OWNER TO `data_platform_team`;
# MAGIC ALTER SCHEMA uc_security_lab SET OWNER TO `data_platform_team`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Row-Level Security with Dynamic Views (Legacy Pattern)
# MAGIC
# MAGIC This is the **traditional approach** — still relevant for Hive Metastore
# MAGIC and for complex multi-condition logic.
# MAGIC
# MAGIC **Problem being solved**:
# MAGIC - Engineering team should only see Engineering rows
# MAGIC - Marketing team should only see Marketing rows
# MAGIC - Admins see everything
# MAGIC
# MAGIC ```
# MAGIC  User query  ──▶  Dynamic View  ──▶  WHERE (is_account_group_member check)
# MAGIC                                  ──▶  Filtered rows returned
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check group membership for current user
# MAGIC -- (This will return true only if you're in that group)
# MAGIC SELECT
# MAGIC   current_user()                           AS user,
# MAGIC   is_account_group_member('admins')        AS is_admin,
# MAGIC   is_account_group_member('engineering')   AS is_engineering,
# MAGIC   is_account_group_member('marketing')     AS is_marketing,
# MAGIC   is_account_group_member('finance')       AS is_finance,
# MAGIC   is_account_group_member('hr')            AS is_hr

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-Level Security: Department-based access via dynamic view
# MAGIC CREATE OR REPLACE VIEW dept_secure_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id, first_name, last_name, email,
# MAGIC   department, region, salary, hire_date, is_active
# MAGIC FROM employees
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('admins')      THEN true          -- see all
# MAGIC     WHEN is_account_group_member('engineering') AND department = 'Engineering' THEN true
# MAGIC     WHEN is_account_group_member('marketing')   AND department = 'Marketing'   THEN true
# MAGIC     WHEN is_account_group_member('finance')     AND department = 'Finance'     THEN true
# MAGIC     WHEN is_account_group_member('hr')          AND department = 'HR'          THEN true
# MAGIC     ELSE false                                                     -- no access
# MAGIC   END
# MAGIC COMMENT 'Row-level security: users see only their department rows'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the secure view — as admin/owner you see all rows
# MAGIC SELECT * FROM dept_secure_employees_vw ORDER BY department

# COMMAND ----------

# MAGIC %md
# MAGIC **Regional Views Pattern** — the old way, now replaced by Row Filters:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OLD PATTERN: One view per region (maintenance burden)
# MAGIC CREATE OR REPLACE VIEW amer_employees_vw AS
# MAGIC   SELECT * FROM employees WHERE region = 'AMER';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW emea_employees_vw AS
# MAGIC   SELECT * FROM employees WHERE region = 'EMEA';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW apac_employees_vw AS
# MAGIC   SELECT * FROM employees WHERE region = 'APAC';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OLD PATTERN: User knows to query their region view
# MAGIC SELECT * FROM amer_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC **Problem**: If you add a new region (say LATAM), you must create another view.
# MAGIC If a user changes from AMER to EMEA team, they need access to a different view.
# MAGIC The NEW way (native Row Filters) is in the next notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Column Masking with Dynamic Views
# MAGIC
# MAGIC Protect sensitive columns (PII, salary, SSN) based on group membership.
# MAGIC
# MAGIC ```
# MAGIC  Data in table              What HR user sees      What analyst sees
# MAGIC  ─────────────────────      ─────────────────────  ─────────────────────
# MAGIC  email: alice@co.com   →   alice@co.com (full)    al***@*** (masked)
# MAGIC  ssn:   123-45-6789   →   123-45-6789 (full)     ***-**-6789 (partial)
# MAGIC  salary: 120000       →   120000.0    (full)     NULL (hidden)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW masked_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC
# MAGIC   -- Email: visible to HR and admins, masked for others
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN email
# MAGIC     ELSE concat(left(email, 2), '***@***')
# MAGIC   END AS email,
# MAGIC
# MAGIC   -- SSN: last 4 digits only unless HR/admin
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN ssn
# MAGIC     ELSE concat('***-**-', right(ssn, 4))
# MAGIC   END AS ssn,
# MAGIC
# MAGIC   department,
# MAGIC   region,
# MAGIC
# MAGIC   -- Salary: only finance, HR, admins see actual value
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('finance')
# MAGIC       OR is_account_group_member('hr')
# MAGIC       OR is_account_group_member('admins')
# MAGIC       THEN salary
# MAGIC     ELSE NULL
# MAGIC   END AS salary,
# MAGIC
# MAGIC   hire_date,
# MAGIC   is_active
# MAGIC FROM employees
# MAGIC WHERE is_active = true
# MAGIC COMMENT 'Column masking: PII hidden based on group membership'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As admin/owner you see unmasked data
# MAGIC SELECT * FROM masked_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Column masking summary:
# MAGIC
# MAGIC | Column | HR Group | Finance Group | Engineering/Marketing | Admin |
# MAGIC |--------|----------|---------------|-----------------------|-------|
# MAGIC | email  | Full     | Masked        | Masked                | Full  |
# MAGIC | ssn    | Full     | Masked        | Masked                | Full  |
# MAGIC | salary | Full     | Full          | NULL                  | Full  |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Combined Row + Column Security (Dynamic View)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combined: department row filter + PII column masking in one view
# MAGIC CREATE OR REPLACE VIEW fully_secure_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN email
# MAGIC     ELSE concat(left(email, 2), '***@***')
# MAGIC   END AS email,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN ssn
# MAGIC     ELSE concat('***-**-', right(ssn, 4))
# MAGIC   END AS ssn,
# MAGIC   department,
# MAGIC   region,
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('finance')
# MAGIC       OR is_account_group_member('hr')
# MAGIC       OR is_account_group_member('admins')
# MAGIC       THEN salary
# MAGIC     ELSE NULL
# MAGIC   END AS salary,
# MAGIC   hire_date,
# MAGIC   is_active
# MAGIC FROM employees
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('admins')      THEN true
# MAGIC     WHEN is_account_group_member('engineering') AND department = 'Engineering' THEN true
# MAGIC     WHEN is_account_group_member('marketing')   AND department = 'Marketing'   THEN true
# MAGIC     WHEN is_account_group_member('finance')     AND department = 'Finance'     THEN true
# MAGIC     WHEN is_account_group_member('hr')          AND department = 'HR'          THEN true
# MAGIC     ELSE false
# MAGIC   END
# MAGIC COMMENT 'Row + column security combined in one view'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fully_secure_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC **Grant pattern for view-based security**:
# MAGIC ```sql
# MAGIC -- Grant on the VIEW, NOT on the base TABLE
# MAGIC GRANT USE CATALOG ON CATALOG databricks_pro TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA uc_security_lab  TO `analysts`;
# MAGIC GRANT SELECT      ON VIEW fully_secure_employees_vw TO `analysts`;
# MAGIC
# MAGIC -- Do NOT run this — it would bypass the view security:
# MAGIC -- GRANT SELECT ON TABLE employees TO `analysts`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Reusable Masking UDFs
# MAGIC
# MAGIC Encapsulate masking logic in UDFs for reuse across multiple views.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reusable email masking function
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email_val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN email_val
# MAGIC     ELSE concat(left(email_val, 2), '***@', split(email_val, '@')[1])
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reusable SSN masking function
# MAGIC CREATE OR REPLACE FUNCTION mask_ssn(ssn_val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN ssn_val
# MAGIC     ELSE concat('***-**-', right(ssn_val, 4))
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View using the reusable UDFs (cleaner, consistent masking)
# MAGIC CREATE OR REPLACE VIEW clean_masked_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   mask_email(email) AS email,
# MAGIC   mask_ssn(ssn)     AS ssn,
# MAGIC   department,
# MAGIC   hire_date,
# MAGIC   is_active
# MAGIC FROM employees
# MAGIC WHERE is_active = true
# MAGIC COMMENT 'Uses reusable masking UDFs'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clean_masked_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Querying Privileges via Information Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all table privileges in this schema
# MAGIC SELECT grantor, grantee, privilege_type, table_name, is_grantable
# MAGIC FROM databricks_pro.information_schema.table_privileges
# MAGIC WHERE table_schema = 'uc_security_lab'
# MAGIC ORDER BY table_name, grantee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all routine (function) privileges in this schema
# MAGIC SELECT grantor, grantee, privilege_type, routine_name
# MAGIC FROM databricks_pro.information_schema.routine_privileges
# MAGIC WHERE routine_schema = 'uc_security_lab'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Privilege Cheat Sheet

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC Scenario                          SQL
# MAGIC ─────────────────────────────────────────────────────────────────────────────
# MAGIC Read one table                    GRANT USE CATALOG ON CATALOG c TO g;
# MAGIC                                   GRANT USE SCHEMA ON SCHEMA s TO g;
# MAGIC                                   GRANT SELECT ON TABLE t TO g;
# MAGIC
# MAGIC Read all tables in schema         GRANT USE SCHEMA, SELECT ON SCHEMA s TO g;
# MAGIC
# MAGIC Write access                      GRANT MODIFY ON TABLE t TO g;
# MAGIC
# MAGIC Create tables                     GRANT CREATE TABLE ON SCHEMA s TO g;
# MAGIC
# MAGIC Full schema access                GRANT ALL PRIVILEGES ON SCHEMA s TO g;
# MAGIC
# MAGIC Volume read                       GRANT READ VOLUME ON VOLUME v TO g;
# MAGIC
# MAGIC External location read            GRANT READ FILES ON EXTERNAL LOCATION loc TO g;
# MAGIC
# MAGIC Run UDF                           GRANT EXECUTE ON FUNCTION f TO g;
# MAGIC
# MAGIC View grants                       SHOW GRANTS ON TABLE t;
# MAGIC
# MAGIC View user's grants                SHOW GRANTS `user@company.com`;
# MAGIC
# MAGIC Transfer ownership to group       ALTER TABLE t SET OWNER TO `group_name`;
# MAGIC
# MAGIC Revoke access                     REVOKE SELECT ON TABLE t FROM g;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS mask_email;
# MAGIC DROP FUNCTION IF EXISTS mask_ssn;
# MAGIC DROP VIEW IF EXISTS dept_secure_employees_vw;
# MAGIC DROP VIEW IF EXISTS amer_employees_vw;
# MAGIC DROP VIEW IF EXISTS emea_employees_vw;
# MAGIC DROP VIEW IF EXISTS apac_employees_vw;
# MAGIC DROP VIEW IF EXISTS masked_employees_vw;
# MAGIC DROP VIEW IF EXISTS fully_secure_employees_vw;
# MAGIC DROP VIEW IF EXISTS clean_masked_employees_vw;
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC DROP TABLE IF EXISTS departments;
# MAGIC DROP SCHEMA IF EXISTS uc_security_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What You Learned |
# MAGIC |---------|-----------------|
# MAGIC | Prerequisite chain | USE CATALOG + USE SCHEMA required before SELECT |
# MAGIC | GRANT/REVOKE | SQL syntax for managing privileges on UC objects |
# MAGIC | Dynamic views (row) | WHERE clause with `is_account_group_member()` |
# MAGIC | Dynamic views (col) | CASE expressions in SELECT to mask PII |
# MAGIC | Regional views | Old pattern: 1 view per region (now replaced by Row Filters) |
# MAGIC | Combined security | Row + column filtering in a single view |
# MAGIC | Reusable UDFs | Encapsulate masking logic for consistency |
# MAGIC | Ownership | Every object has an owner — assign to groups |
# MAGIC | Information schema | Query privilege metadata programmatically |
# MAGIC
# MAGIC **Next**: Day 11 Part 2 — [`11b-row-filters-column-masks_notebook.py`]
# MAGIC → Native Row Filters and Column Masks (DBR 12.2+, the modern approach)
