# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Access Control & Permissions
# MAGIC > Module 08 — Topic 02 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - GRANT and REVOKE SQL commands
# MAGIC - Viewing current privileges with SHOW GRANTS
# MAGIC - Understanding privilege inheritance
# MAGIC - Ownership inspection and transfer
# MAGIC - Simulated permission checks for Community Edition
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog (recommended)
# MAGIC - Community Edition users: follow simulated alternatives
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup — Create Demo Objects
# MAGIC
# MAGIC We need tables and schemas to demonstrate access control.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m08_access_demo
# MAGIC COMMENT 'Access control demonstration schema';
# MAGIC
# MAGIC USE m08_access_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create sample tables representing different sensitivity levels
# MAGIC CREATE OR REPLACE TABLE m08_access_demo.public_products (
# MAGIC     product_id   INT,
# MAGIC     product_name STRING,
# MAGIC     category     STRING,
# MAGIC     price        DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Public product catalog — low sensitivity';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE m08_access_demo.customer_orders (
# MAGIC     order_id      INT,
# MAGIC     customer_id   INT,
# MAGIC     customer_name STRING,
# MAGIC     order_total   DECIMAL(10,2),
# MAGIC     order_date    DATE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Customer orders — medium sensitivity (contains PII)';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE m08_access_demo.employee_payroll (
# MAGIC     employee_id   INT,
# MAGIC     employee_name STRING,
# MAGIC     ssn           STRING,
# MAGIC     salary        DECIMAL(10,2),
# MAGIC     bank_account  STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Employee payroll — HIGH sensitivity (PII + financial)';

# COMMAND ----------

# Insert sample data into all three tables
from pyspark.sql import Row
from datetime import date

# Public products
products = [
    Row(1, "Widget A", "Electronics", 29.99),
    Row(2, "Widget B", "Electronics", 49.99),
    Row(3, "Gadget X", "Accessories", 15.99),
    Row(4, "Tool Pro", "Hardware", 89.99),
]
spark.createDataFrame(products, "product_id INT, product_name STRING, category STRING, price DECIMAL(10,2)") \
    .write.mode("overwrite").saveAsTable("m08_access_demo.public_products")

# Customer orders
orders = [
    Row(101, 1001, "Alice Johnson", 259.97, date(2024, 1, 15)),
    Row(102, 1002, "Bob Smith",     149.99, date(2024, 1, 16)),
    Row(103, 1003, "Carol Williams", 89.99, date(2024, 1, 17)),
    Row(104, 1001, "Alice Johnson", 179.98, date(2024, 2, 1)),
]
spark.createDataFrame(orders, "order_id INT, customer_id INT, customer_name STRING, order_total DECIMAL(10,2), order_date DATE") \
    .write.mode("overwrite").saveAsTable("m08_access_demo.customer_orders")

# Employee payroll
payroll = [
    Row(1, "Alice Johnson",  "XXX-XX-1234", 125000.00, "****5678"),
    Row(2, "Bob Smith",      "XXX-XX-5678", 95000.00,  "****9012"),
    Row(3, "Carol Williams", "XXX-XX-9012", 135000.00, "****3456"),
]
spark.createDataFrame(payroll, "employee_id INT, employee_name STRING, ssn STRING, salary DECIMAL(10,2), bank_account STRING") \
    .write.mode("overwrite").saveAsTable("m08_access_demo.employee_payroll")

print("Created 3 demo tables with sample data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: GRANT and REVOKE Commands
# MAGIC
# MAGIC These are the core commands for managing access in Unity Catalog.
# MAGIC
# MAGIC **Note:** GRANT/REVOKE require Unity Catalog. On Community Edition,
# MAGIC we show the syntax and simulate the behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GRANT SYNTAX REFERENCE (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Grant SELECT on a specific table
# MAGIC -- GRANT SELECT ON TABLE m08_access_demo.public_products TO `analysts`;
# MAGIC --
# MAGIC -- Grant SELECT on all tables in a schema
# MAGIC -- GRANT SELECT ON SCHEMA m08_access_demo TO `analysts`;
# MAGIC --
# MAGIC -- Grant ability to create tables
# MAGIC -- GRANT CREATE TABLE ON SCHEMA m08_access_demo TO `data_engineers`;
# MAGIC --
# MAGIC -- Grant all privileges on a schema
# MAGIC -- GRANT ALL PRIVILEGES ON SCHEMA m08_access_demo TO `schema_admins`;
# MAGIC --
# MAGIC -- Grant USE CATALOG (required to see schemas in a catalog)
# MAGIC -- GRANT USE CATALOG ON CATALOG main TO `all_users`;
# MAGIC --
# MAGIC -- Grant USE SCHEMA (required to see tables in a schema)
# MAGIC -- GRANT USE SCHEMA ON SCHEMA m08_access_demo TO `analysts`;
# MAGIC
# MAGIC SELECT 'See comments above for GRANT syntax reference' AS note;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- REVOKE SYNTAX REFERENCE (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Revoke SELECT from a group
# MAGIC -- REVOKE SELECT ON TABLE m08_access_demo.employee_payroll FROM `interns`;
# MAGIC --
# MAGIC -- Revoke all privileges on a schema
# MAGIC -- REVOKE ALL PRIVILEGES ON SCHEMA m08_access_demo FROM `temp_contractors`;
# MAGIC --
# MAGIC -- Revoke CREATE TABLE (prevent creating new tables)
# MAGIC -- REVOKE CREATE TABLE ON SCHEMA m08_access_demo FROM `analysts`;
# MAGIC
# MAGIC SELECT 'See comments above for REVOKE syntax reference' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Viewing Current Privileges
# MAGIC
# MAGIC SHOW GRANTS displays what privileges have been assigned on an object.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SHOW GRANTS SYNTAX (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Show all grants on a table
# MAGIC -- SHOW GRANTS ON TABLE m08_access_demo.public_products;
# MAGIC --
# MAGIC -- Show all grants on a schema
# MAGIC -- SHOW GRANTS ON SCHEMA m08_access_demo;
# MAGIC --
# MAGIC -- Show grants for a specific principal (user or group)
# MAGIC -- SHOW GRANTS `analysts` ON SCHEMA m08_access_demo;
# MAGIC --
# MAGIC -- Show grants on a catalog
# MAGIC -- SHOW GRANTS ON CATALOG main;
# MAGIC --
# MAGIC -- Show your own grants
# MAGIC -- SHOW GRANTS `current_user@company.com` ON TABLE m08_access_demo.public_products;
# MAGIC
# MAGIC SELECT current_user() AS current_user_identity;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Privilege Inheritance — Simulated Demonstration
# MAGIC
# MAGIC Since Community Edition does not support GRANT/REVOKE, let us simulate
# MAGIC the privilege inheritance model to understand how it works.

# COMMAND ----------

# Simulate the privilege inheritance hierarchy
class PrivilegeSimulator:
    """Simulates Unity Catalog privilege inheritance for learning purposes."""

    def __init__(self):
        # Grants stored as: (principal, privilege, securable_type, securable_name)
        self.grants = []

    def grant(self, privilege, securable_type, securable_name, principal):
        self.grants.append((principal, privilege, securable_type, securable_name))
        print(f"GRANTED {privilege} ON {securable_type} {securable_name} TO `{principal}`")

    def revoke(self, privilege, securable_type, securable_name, principal):
        self.grants = [
            g for g in self.grants
            if not (g[0] == principal and g[1] == privilege
                    and g[2] == securable_type and g[3] == securable_name)
        ]
        print(f"REVOKED {privilege} ON {securable_type} {securable_name} FROM `{principal}`")

    def show_grants(self, securable_type=None, securable_name=None, principal=None):
        results = self.grants
        if securable_type:
            results = [g for g in results if g[2] == securable_type]
        if securable_name:
            results = [g for g in results if g[3] == securable_name]
        if principal:
            results = [g for g in results if g[0] == principal]
        return results

    def check_access(self, principal, privilege, catalog, schema=None, table=None):
        """Check if a principal has a privilege, considering inheritance."""
        # Check direct grant at the exact level
        if table and schema:
            fqn = f"{catalog}.{schema}.{table}"
            for g in self.grants:
                if g[0] == principal and g[1] in (privilege, "ALL PRIVILEGES"):
                    if g[3] == fqn and g[2] == "TABLE":
                        return True, f"Direct grant on TABLE {fqn}"
            # Check schema-level inheritance
            for g in self.grants:
                if g[0] == principal and g[1] in (privilege, "ALL PRIVILEGES"):
                    if g[3] == f"{catalog}.{schema}" and g[2] == "SCHEMA":
                        return True, f"Inherited from SCHEMA {catalog}.{schema}"
            # Check catalog-level inheritance
            for g in self.grants:
                if g[0] == principal and g[1] in (privilege, "ALL PRIVILEGES"):
                    if g[3] == catalog and g[2] == "CATALOG":
                        return True, f"Inherited from CATALOG {catalog}"
        return False, "No matching grant found"


sim = PrivilegeSimulator()
print("Privilege Simulator initialized.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Scenario: Setting Up Analyst Access

# COMMAND ----------

# Simulate granting access to analysts
print("=" * 60)
print("SCENARIO: Setting up read-only access for analysts")
print("=" * 60)
print()

# Analysts need: USE CATALOG + USE SCHEMA + SELECT
sim.grant("USE CATALOG", "CATALOG", "prod", "analysts")
sim.grant("USE SCHEMA", "SCHEMA", "prod.sales", "analysts")
sim.grant("SELECT", "SCHEMA", "prod.sales", "analysts")

print()
print("Now checking if analyst can query prod.sales.transactions...")
has_access, reason = sim.check_access("analysts", "SELECT", "prod", "sales", "transactions")
print(f"  Access: {'ALLOWED' if has_access else 'DENIED'}")
print(f"  Reason: {reason}")

print()
print("Can analyst query prod.hr.salaries?")
has_access, reason = sim.check_access("analysts", "SELECT", "prod", "hr", "salaries")
print(f"  Access: {'ALLOWED' if has_access else 'DENIED'}")
print(f"  Reason: {reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Scenario: Catalog-Level Inheritance

# COMMAND ----------

print("=" * 60)
print("SCENARIO: Granting SELECT at the CATALOG level")
print("=" * 60)
print()

sim2 = PrivilegeSimulator()
sim2.grant("USE CATALOG", "CATALOG", "prod", "power_users")
sim2.grant("SELECT", "CATALOG", "prod", "power_users")

print()
tables_to_check = [
    ("prod", "sales", "transactions"),
    ("prod", "sales", "customers"),
    ("prod", "marketing", "campaigns"),
    ("prod", "hr", "employees"),
]

print("Checking access to multiple tables:")
for cat, schema, table in tables_to_check:
    has_access, reason = sim2.check_access("power_users", "SELECT", cat, schema, table)
    status = "ALLOWED" if has_access else "DENIED"
    print(f"  {cat}.{schema}.{table:15s} -> {status} ({reason})")

print()
print("Key insight: A single GRANT SELECT ON CATALOG covers ALL tables!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Scenario: The Three Required Privileges

# COMMAND ----------

print("=" * 60)
print("SCENARIO: Why you need USE CATALOG + USE SCHEMA + SELECT")
print("=" * 60)
print()

# User with only SELECT but missing USE CATALOG and USE SCHEMA
sim3 = PrivilegeSimulator()
print("Case 1: Only SELECT granted (missing USE CATALOG and USE SCHEMA)")
sim3.grant("SELECT", "TABLE", "prod.sales.transactions", "user_a")
print("  Result: User can reference the table in SQL but CANNOT")
print("  navigate to it via the UI or see it in SHOW TABLES.")
print()

print("Case 2: All three privileges granted")
sim3.grant("USE CATALOG", "CATALOG", "prod", "user_b")
sim3.grant("USE SCHEMA", "SCHEMA", "prod.sales", "user_b")
sim3.grant("SELECT", "TABLE", "prod.sales.transactions", "user_b")
print("  Result: User can navigate, discover, AND query the table.")
print()

print("Best practice: Grant USE CATALOG and USE SCHEMA broadly,")
print("then control data access with SELECT at the table or schema level.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Ownership Model

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- OWNERSHIP COMMANDS (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Check who owns a table
# MAGIC -- DESCRIBE TABLE EXTENDED m08_access_demo.public_products;
# MAGIC -- (Look for the "Owner" field in the output)
# MAGIC --
# MAGIC -- Transfer ownership
# MAGIC -- ALTER TABLE m08_access_demo.public_products SET OWNER TO `data_team`;
# MAGIC --
# MAGIC -- Transfer schema ownership
# MAGIC -- ALTER SCHEMA m08_access_demo SET OWNER TO `platform_team`;
# MAGIC --
# MAGIC -- Transfer catalog ownership
# MAGIC -- ALTER CATALOG prod SET OWNER TO `data_governance_team`;
# MAGIC
# MAGIC -- Community Edition: check table owner via DESCRIBE EXTENDED
# MAGIC DESCRIBE TABLE EXTENDED m08_access_demo.public_products;

# COMMAND ----------

# Demonstrate ownership concepts
print("=" * 60)
print("OWNERSHIP MODEL SUMMARY")
print("=" * 60)
print()
print("Who can be an owner?")
print("  - Individual users (email-based identity)")
print("  - Groups (account-level groups)")
print("  - Service principals")
print()
print("What can owners do?")
print("  - All privileges on the object (SELECT, MODIFY, etc.)")
print("  - GRANT privileges to others")
print("  - ALTER the object (rename, add columns, etc.)")
print("  - DROP the object")
print("  - Transfer ownership to another principal")
print()
print("Ownership rules:")
print("  1. Creator is the initial owner")
print("  2. Only the owner or metastore admin can DROP")
print("  3. Ownership can be transferred with ALTER ... SET OWNER TO")
print("  4. Group ownership enables team-based management")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Access Control Patterns for Real-World Scenarios

# COMMAND ----------

# Real-world access control patterns
patterns = [
    {
        "scenario": "Data Engineering Team",
        "description": "Full access to dev, read+write to staging, read-only to prod",
        "grants": [
            "GRANT ALL PRIVILEGES ON CATALOG dev TO `data_engineers`",
            "GRANT USE CATALOG ON CATALOG staging TO `data_engineers`",
            "GRANT ALL PRIVILEGES ON CATALOG staging TO `data_engineers`",
            "GRANT USE CATALOG ON CATALOG prod TO `data_engineers`",
            "GRANT SELECT ON CATALOG prod TO `data_engineers`",
        ]
    },
    {
        "scenario": "Business Analysts",
        "description": "Read-only access to curated data in prod",
        "grants": [
            "GRANT USE CATALOG ON CATALOG prod TO `analysts`",
            "GRANT USE SCHEMA ON SCHEMA prod.curated TO `analysts`",
            "GRANT SELECT ON SCHEMA prod.curated TO `analysts`",
        ]
    },
    {
        "scenario": "Data Science Team",
        "description": "Read from prod, full access to ML schemas",
        "grants": [
            "GRANT USE CATALOG ON CATALOG prod TO `data_scientists`",
            "GRANT SELECT ON CATALOG prod TO `data_scientists`",
            "GRANT ALL PRIVILEGES ON SCHEMA prod.ml_features TO `data_scientists`",
            "GRANT ALL PRIVILEGES ON SCHEMA prod.ml_models TO `data_scientists`",
        ]
    },
    {
        "scenario": "ETL Service Principal",
        "description": "Automated pipeline with write access to specific schemas",
        "grants": [
            "GRANT USE CATALOG ON CATALOG prod TO `etl_sp`",
            "GRANT ALL PRIVILEGES ON SCHEMA prod.raw TO `etl_sp`",
            "GRANT ALL PRIVILEGES ON SCHEMA prod.bronze TO `etl_sp`",
            "GRANT ALL PRIVILEGES ON SCHEMA prod.silver TO `etl_sp`",
        ]
    },
]

for p in patterns:
    print("=" * 60)
    print(f"PATTERN: {p['scenario']}")
    print(f"  {p['description']}")
    print("-" * 60)
    for g in p['grants']:
        print(f"  {g};")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Identity Management Reference

# COMMAND ----------

# Identity management reference
print("=" * 60)
print("IDENTITY MANAGEMENT IN UNITY CATALOG")
print("=" * 60)
print()
print("Three types of principals:")
print()
print("1. USERS")
print("   - Individual human identities")
print("   - Identified by email address")
print("   - Synced from identity provider (Azure AD, Okta, etc.)")
print("   - Example: alice.johnson@company.com")
print()
print("2. GROUPS")
print("   - Collections of users, service principals, or nested groups")
print("   - Account-level groups (recommended) vs workspace-level (legacy)")
print("   - Example: data_engineers, analysts, ml_team")
print("   - Best practice: Grant privileges to groups, not individual users")
print()
print("3. SERVICE PRINCIPALS")
print("   - Non-human identities for automated workloads")
print("   - Used by jobs, pipelines, and API integrations")
print("   - Have their own client ID and secret")
print("   - Example: etl_pipeline_sp, ml_training_sp")
print()
print("Best practices:")
print("  - Use account-level groups for all Unity Catalog grants")
print("  - Never grant privileges to individual users in production")
print("  - Use service principals for all automated workloads")
print("  - Sync groups from your identity provider via SCIM")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Auditing Access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- AUDITING COMMANDS (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- List all grants on a specific table
# MAGIC -- SHOW GRANTS ON TABLE m08_access_demo.employee_payroll;
# MAGIC --
# MAGIC -- List all grants for a specific group
# MAGIC -- SHOW GRANTS `analysts`;
# MAGIC --
# MAGIC -- Query the audit log (available via system tables)
# MAGIC -- SELECT * FROM system.access.audit
# MAGIC -- WHERE action_name IN ('createTable', 'grantPermission', 'revokePermission')
# MAGIC -- AND event_date >= current_date() - INTERVAL 7 DAYS
# MAGIC -- ORDER BY event_time DESC;
# MAGIC --
# MAGIC -- Query privilege assignments from information_schema
# MAGIC -- SELECT * FROM system.information_schema.table_privileges
# MAGIC -- WHERE table_schema = 'm08_access_demo';
# MAGIC
# MAGIC SELECT 'See comments above for auditing commands' AS note;

# COMMAND ----------

# Build a summary of the access control model
summary_data = [
    ("GRANT ... TO `principal`",     "Add a privilege",              "GRANT SELECT ON TABLE t TO `g`"),
    ("REVOKE ... FROM `principal`",  "Remove a privilege",           "REVOKE SELECT ON TABLE t FROM `g`"),
    ("SHOW GRANTS ON ...",           "View grants on an object",     "SHOW GRANTS ON SCHEMA s"),
    ("SHOW GRANTS `principal`",      "View grants for a principal",  "SHOW GRANTS `analysts`"),
    ("ALTER ... SET OWNER TO",       "Transfer ownership",           "ALTER TABLE t SET OWNER TO `team`"),
    ("DESCRIBE ... EXTENDED",        "View owner and metadata",      "DESCRIBE TABLE EXTENDED t"),
]

summary_df = spark.createDataFrame(summary_data,
    "command STRING, purpose STRING, example STRING")
summary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS m08_access_demo.public_products;
# MAGIC DROP TABLE IF EXISTS m08_access_demo.customer_orders;
# MAGIC DROP TABLE IF EXISTS m08_access_demo.employee_payroll;
# MAGIC DROP DATABASE IF EXISTS m08_access_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. GRANT/REVOKE with SQL syntax controls all access in Unity Catalog")
print("  2. Privilege inheritance: grant on catalog covers all schemas and tables")
print("  3. Three privileges needed to query: USE CATALOG + USE SCHEMA + SELECT")
print("  4. Owners have full control; creator is the default owner")
print("  5. Use account-level groups, not individual users, for grants")
print("  6. Service principals provide identity for automated workloads")
print()
print("Next: 03-data-lineage_notebook.py")
