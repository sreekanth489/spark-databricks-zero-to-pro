# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 11: Unity Catalog Security -- Hands-On Lab
# MAGIC
# MAGIC **Objective**: Master Unity Catalog access control, dynamic views for row/column security
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Create tables with sample data for security exercises
# MAGIC 2. Explore GRANT, REVOKE, and SHOW GRANTS syntax
# MAGIC 3. Implement row-level security using dynamic views
# MAGIC 4. Implement column masking using dynamic views
# MAGIC 5. Explore table ownership and transfer
# MAGIC 6. Query the information_schema for privilege metadata
# MAGIC 7. Clean up all lab resources
# MAGIC
# MAGIC **Unity Catalog Security Model**:
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │                    UC Security Model                                 │
# MAGIC │                                                                      │
# MAGIC │  Principals (WHO)          Privileges (WHAT)     Objects (WHERE)     │
# MAGIC │  ┌──────────────┐         ┌──────────────┐      ┌──────────────┐   │
# MAGIC │  │ Users        │         │ SELECT       │      │ CATALOG      │   │
# MAGIC │  │ Service      │  GRANT  │ MODIFY       │  ON  │ SCHEMA       │   │
# MAGIC │  │  Principals  │ ──────▶ │ CREATE       │ ───▶ │ TABLE        │   │
# MAGIC │  │ Groups       │         │ USE CATALOG  │      │ VIEW         │   │
# MAGIC │  │              │         │ USE SCHEMA   │      │ VOLUME       │   │
# MAGIC │  │              │         │ EXECUTE      │      │ FUNCTION     │   │
# MAGIC │  │              │         │ ALL PRIVS    │      │ EXT LOCATION │   │
# MAGIC │  └──────────────┘         └──────────────┘      └──────────────┘   │
# MAGIC │                                                                      │
# MAGIC │  Prerequisite Chain:                                                 │
# MAGIC │    USE CATALOG ──▶ USE SCHEMA ──▶ SELECT/MODIFY on TABLE            │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Privilege Hierarchy**:
# MAGIC ```
# MAGIC   Metastore Admin
# MAGIC   └── Can grant on: everything
# MAGIC       Catalog Owner
# MAGIC       └── Can grant on: all objects in that catalog
# MAGIC           Schema Owner
# MAGIC           └── Can grant on: all objects in that schema
# MAGIC               Table Owner
# MAGIC               └── Can grant on: that specific table
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**: See [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/blob/main/day00-environment-setup/00-databricks-cloud-setup.md) for AWS + Databricks + S3 configuration.

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
# MAGIC ## Step 1: Create Sample Tables
# MAGIC
# MAGIC We'll create an HR dataset to demonstrate security controls.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   ssn STRING,
# MAGIC   department STRING,
# MAGIC   salary DOUBLE,
# MAGIC   hire_date DATE,
# MAGIC   manager_id INT,
# MAGIC   is_active BOOLEAN
# MAGIC )
# MAGIC COMMENT 'Employee records with PII -- used for security lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1, 'Alice', 'Johnson', 'alice@company.com', '123-45-6789', 'Engineering', 120000.0, '2020-03-15', NULL, true),
# MAGIC   (2, 'Bob', 'Smith', 'bob@company.com', '234-56-7890', 'Engineering', 135000.0, '2019-07-01', 1, true),
# MAGIC   (3, 'Carol', 'Williams', 'carol@company.com', '345-67-8901', 'Marketing', 95000.0, '2021-01-10', NULL, true),
# MAGIC   (4, 'David', 'Brown', 'david@company.com', '456-78-9012', 'Finance', 110000.0, '2018-11-20', NULL, true),
# MAGIC   (5, 'Eve', 'Davis', 'eve@company.com', '567-89-0123', 'Engineering', 115000.0, '2022-06-05', 1, true),
# MAGIC   (6, 'Frank', 'Miller', 'frank@company.com', '678-90-1234', 'Marketing', 98000.0, '2020-09-12', 3, false),
# MAGIC   (7, 'Grace', 'Wilson', 'grace@company.com', '789-01-2345', 'Finance', 105000.0, '2021-04-18', 4, true),
# MAGIC   (8, 'Hank', 'Moore', 'hank@company.com', '890-12-3456', 'HR', 88000.0, '2017-02-28', NULL, true),
# MAGIC   (9, 'Ivy', 'Taylor', 'ivy@company.com', '901-23-4567', 'HR', 82000.0, '2023-01-15', 8, true),
# MAGIC   (10, 'Jack', 'Anderson', 'jack@company.com', '012-34-5678', 'Engineering', 128000.0, '2021-08-22', 1, true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE departments (
# MAGIC   department_name STRING,
# MAGIC   department_head STRING,
# MAGIC   cost_center STRING,
# MAGIC   budget DOUBLE,
# MAGIC   location STRING
# MAGIC )
# MAGIC COMMENT 'Department reference data'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO departments VALUES
# MAGIC   ('Engineering', 'Alice Johnson', 'CC-ENG-001', 500000.0, 'San Francisco'),
# MAGIC   ('Marketing', 'Carol Williams', 'CC-MKT-001', 250000.0, 'New York'),
# MAGIC   ('Finance', 'David Brown', 'CC-FIN-001', 300000.0, 'Chicago'),
# MAGIC   ('HR', 'Hank Moore', 'CC-HR-001', 150000.0, 'San Francisco')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Understanding GRANT and REVOKE
# MAGIC
# MAGIC The core syntax for managing privileges:
# MAGIC ```sql
# MAGIC GRANT privilege ON securable_object TO principal;
# MAGIC REVOKE privilege ON securable_object FROM principal;
# MAGIC SHOW GRANTS ON securable_object;
# MAGIC ```
# MAGIC
# MAGIC **Prerequisite chain** -- to SELECT from a table, a user needs:
# MAGIC 1. `USE CATALOG` on the parent catalog
# MAGIC 2. `USE SCHEMA` on the parent schema
# MAGIC 3. `SELECT` on the table itself

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
# MAGIC ### Granting Privileges (Demonstration)
# MAGIC
# MAGIC The following commands show the syntax for granting privileges.
# MAGIC These require you to be a catalog/schema owner or metastore admin.
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Allow navigating to the catalog
# MAGIC GRANT USE CATALOG ON CATALOG databricks_pro TO `analysts`;
# MAGIC
# MAGIC -- Step 2: Allow navigating to the schema
# MAGIC GRANT USE SCHEMA ON SCHEMA uc_security_lab TO `analysts`;
# MAGIC
# MAGIC -- Step 3: Grant read access on the table
# MAGIC GRANT SELECT ON TABLE employees TO `analysts`;
# MAGIC
# MAGIC -- Grant multiple privileges at once
# MAGIC GRANT SELECT, MODIFY ON TABLE employees TO `data_engineers`;
# MAGIC
# MAGIC -- Grant on entire schema (all current and future tables)
# MAGIC GRANT SELECT ON SCHEMA uc_security_lab TO `analysts`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revoking Privileges (Demonstration)
# MAGIC
# MAGIC ```sql
# MAGIC -- Revoke read access
# MAGIC REVOKE SELECT ON TABLE employees FROM `analysts`;
# MAGIC
# MAGIC -- Revoke all privileges
# MAGIC REVOKE ALL PRIVILEGES ON TABLE employees FROM `data_engineers`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Row-Level Security with Dynamic Views
# MAGIC
# MAGIC Unity Catalog does not have built-in row filters (though they are in preview).
# MAGIC The standard approach is to create **dynamic views** that filter rows based on
# MAGIC the calling user's group membership.
# MAGIC
# MAGIC ```
# MAGIC  User Request
# MAGIC       │
# MAGIC       ▼
# MAGIC  ┌─────────────────────────────────┐
# MAGIC  │  Dynamic View                    │
# MAGIC  │  ┌───────────────────────────┐   │
# MAGIC  │  │ is_account_group_member() │   │
# MAGIC  │  │ current_user()            │   │
# MAGIC  │  └───────────────────────────┘   │
# MAGIC  │         │                        │
# MAGIC  │         ▼                        │
# MAGIC  │  WHERE department = user's dept  │
# MAGIC  └─────────────────────────────────┘
# MAGIC       │
# MAGIC       ▼
# MAGIC  Filtered Results (only rows user is allowed to see)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check your current identity
# MAGIC SELECT current_user() AS current_user

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-Level Security: Department-based access
# MAGIC -- Engineering group sees only Engineering rows
# MAGIC -- HR group sees only HR rows
# MAGIC -- Admins see all rows
# MAGIC CREATE OR REPLACE VIEW secure_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id, first_name, last_name, email,
# MAGIC   department, salary, hire_date, is_active
# MAGIC FROM employees
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('admins') THEN true
# MAGIC     WHEN is_account_group_member('engineering') AND department = 'Engineering' THEN true
# MAGIC     WHEN is_account_group_member('marketing') AND department = 'Marketing' THEN true
# MAGIC     WHEN is_account_group_member('finance') AND department = 'Finance' THEN true
# MAGIC     WHEN is_account_group_member('hr') AND department = 'HR' THEN true
# MAGIC     ELSE false
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the secure view
# MAGIC -- As workspace owner/admin, you see all rows
# MAGIC SELECT * FROM secure_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC **How it works**:
# MAGIC - `is_account_group_member('group_name')` returns TRUE if the current user belongs to that group
# MAGIC - The view filters rows based on group membership
# MAGIC - Users are granted `SELECT` on the VIEW, not on the underlying TABLE
# MAGIC - This prevents direct table access while allowing governed data access

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Column Masking with Dynamic Views
# MAGIC
# MAGIC Protect sensitive columns (PII, salary, SSN) by masking them based on
# MAGIC the user's group membership.
# MAGIC
# MAGIC ```
# MAGIC  Original Data                    Masked Data (non-HR user)
# MAGIC  ┌────────────────────────┐      ┌────────────────────────┐
# MAGIC  │ email: alice@co.com    │      │ email: al***@***       │
# MAGIC  │ ssn: 123-45-6789      │ ───▶ │ ssn: ***-**-6789       │
# MAGIC  │ salary: 120000        │      │ salary: NULL            │
# MAGIC  └────────────────────────┘      └────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Column Masking: Protect PII and salary information
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
# MAGIC   -- SSN: last 4 digits only for HR, fully masked for others
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN ssn
# MAGIC     ELSE concat('***-**-', right(ssn, 4))
# MAGIC   END AS ssn,
# MAGIC
# MAGIC   department,
# MAGIC
# MAGIC   -- Salary: visible to finance, HR, and admins only
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
# MAGIC
# MAGIC FROM employees
# MAGIC WHERE is_active = true

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the masked view
# MAGIC -- As admin/owner you see unmasked data
# MAGIC SELECT * FROM masked_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC **Column masking summary**:
# MAGIC
# MAGIC | Column | HR Group | Finance Group | Engineering Group | Admin |
# MAGIC |--------|----------|---------------|-------------------|-------|
# MAGIC | email | Full | Masked | Masked | Full |
# MAGIC | ssn | Full | Masked | Masked | Full |
# MAGIC | salary | Full | Full | NULL | Full |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Combined Row + Column Security
# MAGIC
# MAGIC You can combine row-level filtering AND column masking in a single view.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combined: department-based row filter + PII column masking
# MAGIC CREATE OR REPLACE VIEW fully_secure_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC
# MAGIC   -- Column masking
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN email
# MAGIC     ELSE concat(left(email, 2), '***@***')
# MAGIC   END AS email,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr') OR is_account_group_member('admins')
# MAGIC       THEN ssn
# MAGIC     ELSE concat('***-**-', right(ssn, 4))
# MAGIC   END AS ssn,
# MAGIC
# MAGIC   department,
# MAGIC
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
# MAGIC
# MAGIC FROM employees
# MAGIC WHERE
# MAGIC   -- Row-level security
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('admins') THEN true
# MAGIC     WHEN is_account_group_member('engineering') AND department = 'Engineering' THEN true
# MAGIC     WHEN is_account_group_member('marketing') AND department = 'Marketing' THEN true
# MAGIC     WHEN is_account_group_member('finance') AND department = 'Finance' THEN true
# MAGIC     WHEN is_account_group_member('hr') AND department = 'HR' THEN true
# MAGIC     ELSE false
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fully_secure_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC **Security pattern**:
# MAGIC ```
# MAGIC  Grant flow for governed access:
# MAGIC
# MAGIC  1. GRANT USE CATALOG on catalog ──▶ to group
# MAGIC  2. GRANT USE SCHEMA on schema   ──▶ to group
# MAGIC  3. GRANT SELECT on secure VIEW  ──▶ to group
# MAGIC
# MAGIC  Do NOT grant SELECT on the underlying TABLE.
# MAGIC  Users access data only through the secured view.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Table Ownership
# MAGIC
# MAGIC Every object has an **owner** who has full control.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table ownership
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transferring Ownership (Demonstration)
# MAGIC
# MAGIC ```sql
# MAGIC -- Transfer table ownership to a group (best practice)
# MAGIC ALTER TABLE employees SET OWNER TO `data_platform_team`;
# MAGIC
# MAGIC -- Transfer schema ownership
# MAGIC ALTER SCHEMA uc_security_lab SET OWNER TO `data_platform_team`;
# MAGIC ```
# MAGIC
# MAGIC **Best practice**: Assign ownership to **groups**, not individual users,
# MAGIC so ownership persists when team members leave.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Querying Privileges via Information Schema
# MAGIC
# MAGIC Unity Catalog exposes privilege metadata through the `information_schema`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all table privileges in this schema
# MAGIC SELECT *
# MAGIC FROM databricks_pro.information_schema.table_privileges
# MAGIC WHERE table_schema = 'uc_security_lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all schema privileges in this catalog
# MAGIC SELECT *
# MAGIC FROM databricks_pro.information_schema.schema_privileges
# MAGIC WHERE schema_name = 'uc_security_lab'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: UDF-Based Security Helper
# MAGIC
# MAGIC You can create UDFs that encapsulate security logic for reuse across views.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a reusable masking function for emails
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
# MAGIC -- Create a reusable masking function for SSN
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
# MAGIC -- Use the UDFs in a cleaner view definition
# MAGIC CREATE OR REPLACE VIEW clean_masked_employees_vw AS
# MAGIC SELECT
# MAGIC   employee_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   mask_email(email) AS email,
# MAGIC   mask_ssn(ssn) AS ssn,
# MAGIC   department,
# MAGIC   hire_date,
# MAGIC   is_active
# MAGIC FROM employees
# MAGIC WHERE is_active = true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clean_masked_employees_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Using UDFs for masking logic keeps your views clean and ensures consistent
# MAGIC masking behavior across multiple views.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Privilege Patterns Cheat Sheet
# MAGIC
# MAGIC | Scenario | SQL |
# MAGIC |----------|-----|
# MAGIC | Read access to one table | `GRANT USE CATALOG ON CATALOG c TO g; GRANT USE SCHEMA ON SCHEMA s TO g; GRANT SELECT ON TABLE t TO g;` |
# MAGIC | Read access to all tables in schema | `GRANT SELECT ON SCHEMA s TO g;` |
# MAGIC | Write access | `GRANT MODIFY ON TABLE t TO g;` |
# MAGIC | Create tables in schema | `GRANT CREATE TABLE ON SCHEMA s TO g;` |
# MAGIC | Full schema access | `GRANT ALL PRIVILEGES ON SCHEMA s TO g;` |
# MAGIC | Volume read access | `GRANT READ VOLUME ON VOLUME v TO g;` |
# MAGIC | External file access | `GRANT READ FILES ON EXTERNAL LOCATION loc TO g;` |
# MAGIC | View grants | `SHOW GRANTS ON TABLE t;` |
# MAGIC | View user grants | `` SHOW GRANTS `user@company.com`; `` |
# MAGIC | Transfer ownership | `` ALTER TABLE t SET OWNER TO `group_name`; `` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS mask_email;
# MAGIC DROP FUNCTION IF EXISTS mask_ssn;
# MAGIC DROP VIEW IF EXISTS secure_employees_vw;
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
# MAGIC | GRANT/REVOKE | SQL syntax for managing privileges on UC objects |
# MAGIC | Prerequisite chain | USE CATALOG + USE SCHEMA required before SELECT |
# MAGIC | Row-level security | Dynamic views with `is_account_group_member()` |
# MAGIC | Column masking | CASE expressions in views to hide PII |
# MAGIC | Combined security | Row + column filtering in a single view |
# MAGIC | Ownership | Every object has an owner with full control |
# MAGIC | UDF masking | Reusable functions for consistent masking |
# MAGIC | Information schema | Query privilege metadata programmatically |
# MAGIC
# MAGIC **Next**: [Day 12: Managed vs External Tables](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day12-managed-vs-external-tables) -- deep dive into table types and storage
