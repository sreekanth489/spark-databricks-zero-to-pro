# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Sharing
# MAGIC > Module 08 — Topic 05 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - SQL commands for creating shares, recipients, and providers
# MAGIC - Adding tables to shares with various options
# MAGIC - Recipient access patterns and credential management
# MAGIC - Configuration templates for production sharing
# MAGIC - Understanding audit logging for shared data
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog for actual sharing
# MAGIC - Community Edition users: all SQL is shown as reference with explanations
# MAGIC
# MAGIC **Note:** Delta Sharing basics were covered in Module 03. This notebook
# MAGIC focuses on production configuration, cross-org sharing, and advanced features.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup — Create Data to Share

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m08_sharing_demo
# MAGIC COMMENT 'Delta Sharing demonstration schema';
# MAGIC USE m08_sharing_demo;

# COMMAND ----------

# Create realistic tables that an organization might share
from pyspark.sql import Row
from datetime import date, timedelta
import random

random.seed(42)

# Product catalog (low sensitivity - shareable broadly)
products = [
    Row(pid=i,
        name=f"Product_{i:03d}",
        category=random.choice(["Electronics", "Clothing", "Food", "Home"]),
        price=round(random.uniform(10, 500), 2),
        in_stock=random.choice([True, False]))
    for i in range(1, 51)
]
prod_df = spark.createDataFrame(products)
prod_df.write.mode("overwrite").saveAsTable("m08_sharing_demo.product_catalog")

# Aggregated sales (medium sensitivity - shareable with partners)
sales = []
for day_offset in range(30):
    for region in ["North", "South", "East", "West"]:
        sales.append(Row(
            sale_date=date(2024, 1, 1) + timedelta(days=day_offset),
            region=region,
            total_orders=random.randint(50, 500),
            total_revenue=round(random.uniform(5000, 50000), 2),
            avg_order_value=round(random.uniform(50, 200), 2)
        ))
sales_df = spark.createDataFrame(sales)
sales_df.write.mode("overwrite").saveAsTable("m08_sharing_demo.regional_sales_summary")

# Market research data (shareable with specific partners)
market = [
    Row(quarter="2024-Q1", segment="Enterprise",   market_share=23.5, growth_rate=5.2),
    Row(quarter="2024-Q1", segment="Mid-Market",    market_share=18.3, growth_rate=8.1),
    Row(quarter="2024-Q1", segment="SMB",           market_share=12.7, growth_rate=12.3),
    Row(quarter="2024-Q1", segment="Consumer",      market_share=31.2, growth_rate=3.8),
    Row(quarter="2024-Q2", segment="Enterprise",   market_share=24.1, growth_rate=5.5),
    Row(quarter="2024-Q2", segment="Mid-Market",    market_share=19.0, growth_rate=7.9),
    Row(quarter="2024-Q2", segment="SMB",           market_share=13.5, growth_rate=11.8),
    Row(quarter="2024-Q2", segment="Consumer",      market_share=30.8, growth_rate=4.1),
]
market_df = spark.createDataFrame(market)
market_df.write.mode("overwrite").saveAsTable("m08_sharing_demo.market_research")

print("Created 3 tables for sharing demonstration:")
print("  - product_catalog (50 rows) — low sensitivity")
print("  - regional_sales_summary (120 rows) — medium sensitivity")
print("  - market_research (8 rows) — partner-only access")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN m08_sharing_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Creating Shares
# MAGIC
# MAGIC A SHARE is a named container of tables, views, or schemas that you want
# MAGIC to make available to external recipients.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- CREATING SHARES (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Create a share for public data (product catalog)
# MAGIC -- CREATE SHARE IF NOT EXISTS public_data_share
# MAGIC -- COMMENT 'Public product catalog available to all partners';
# MAGIC --
# MAGIC -- Create a share for partner analytics data
# MAGIC -- CREATE SHARE IF NOT EXISTS partner_analytics_share
# MAGIC -- COMMENT 'Regional sales data shared with analytics partners';
# MAGIC --
# MAGIC -- Create a share for strategic partners
# MAGIC -- CREATE SHARE IF NOT EXISTS strategic_partner_share
# MAGIC -- COMMENT 'Market research data shared with strategic partners only';
# MAGIC --
# MAGIC -- List all shares
# MAGIC -- SHOW SHARES;
# MAGIC --
# MAGIC -- Describe a share
# MAGIC -- DESCRIBE SHARE public_data_share;
# MAGIC
# MAGIC SELECT 'See comments above for CREATE SHARE syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Adding Tables to Shares
# MAGIC
# MAGIC After creating a share, you add specific tables (or entire schemas) to it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ADDING TABLES TO SHARES (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Add a full table to a share
# MAGIC -- ALTER SHARE public_data_share
# MAGIC -- ADD TABLE m08_sharing_demo.product_catalog;
# MAGIC --
# MAGIC -- Add a table with an alias (recipients see the alias name)
# MAGIC -- ALTER SHARE partner_analytics_share
# MAGIC -- ADD TABLE m08_sharing_demo.regional_sales_summary
# MAGIC -- AS shared_analytics.regional_sales;
# MAGIC --
# MAGIC -- Add a table with partition filter (share only subset of data)
# MAGIC -- ALTER SHARE partner_analytics_share
# MAGIC -- ADD TABLE m08_sharing_demo.regional_sales_summary
# MAGIC -- PARTITION (region = 'North');
# MAGIC --
# MAGIC -- Add a table with history (allows time travel by recipients)
# MAGIC -- ALTER SHARE partner_analytics_share
# MAGIC -- ADD TABLE m08_sharing_demo.regional_sales_summary
# MAGIC -- WITH HISTORY;
# MAGIC --
# MAGIC -- Add a table with change data feed
# MAGIC -- ALTER SHARE partner_analytics_share
# MAGIC -- ADD TABLE m08_sharing_demo.regional_sales_summary
# MAGIC -- WITH CHANGE DATA FEED;
# MAGIC --
# MAGIC -- Add an entire schema (all current and future tables)
# MAGIC -- ALTER SHARE strategic_partner_share
# MAGIC -- ADD SCHEMA m08_sharing_demo;
# MAGIC --
# MAGIC -- Remove a table from a share
# MAGIC -- ALTER SHARE public_data_share
# MAGIC -- REMOVE TABLE m08_sharing_demo.product_catalog;
# MAGIC --
# MAGIC -- Show what is in a share
# MAGIC -- SHOW ALL IN SHARE public_data_share;
# MAGIC
# MAGIC SELECT 'See comments above for adding tables to shares' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Creating Recipients
# MAGIC
# MAGIC A RECIPIENT represents an external entity that will access your shared data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- CREATING RECIPIENTS (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Method 1: Open sharing (token-based, for non-Databricks recipients)
# MAGIC -- CREATE RECIPIENT IF NOT EXISTS partner_acme_corp
# MAGIC -- COMMENT 'Acme Corporation - analytics partner';
# MAGIC -- This generates an activation link that you send to the recipient.
# MAGIC -- The recipient downloads a .share credentials file from the link.
# MAGIC --
# MAGIC -- Method 2: Databricks-to-Databricks sharing (no tokens needed)
# MAGIC -- CREATE RECIPIENT IF NOT EXISTS partner_bigco
# MAGIC -- USING ID '<recipient-metastore-sharing-identifier>'
# MAGIC -- COMMENT 'BigCo Inc - uses Databricks, authenticated via metastore';
# MAGIC --
# MAGIC -- Set token expiration (open sharing only)
# MAGIC -- ALTER RECIPIENT partner_acme_corp
# MAGIC -- SET PROPERTIES ('token_lifetime_seconds' = '86400');
# MAGIC --
# MAGIC -- Rotate recipient token
# MAGIC -- ALTER RECIPIENT partner_acme_corp ROTATE TOKEN;
# MAGIC --
# MAGIC -- List all recipients
# MAGIC -- SHOW RECIPIENTS;
# MAGIC --
# MAGIC -- Describe a recipient
# MAGIC -- DESCRIBE RECIPIENT partner_acme_corp;
# MAGIC
# MAGIC SELECT 'See comments above for CREATE RECIPIENT syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Granting Shares to Recipients
# MAGIC
# MAGIC Connect shares to recipients to enable data access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GRANTING SHARES TO RECIPIENTS (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Grant a share to a recipient
# MAGIC -- GRANT SELECT ON SHARE public_data_share TO RECIPIENT partner_acme_corp;
# MAGIC --
# MAGIC -- Grant multiple shares to one recipient
# MAGIC -- GRANT SELECT ON SHARE partner_analytics_share TO RECIPIENT partner_acme_corp;
# MAGIC -- GRANT SELECT ON SHARE strategic_partner_share TO RECIPIENT partner_acme_corp;
# MAGIC --
# MAGIC -- Revoke access
# MAGIC -- REVOKE SELECT ON SHARE partner_analytics_share FROM RECIPIENT partner_acme_corp;
# MAGIC --
# MAGIC -- Show grants on a share
# MAGIC -- SHOW GRANTS ON SHARE public_data_share;
# MAGIC
# MAGIC SELECT 'See comments above for granting shares' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Recipient Access Patterns
# MAGIC
# MAGIC How do recipients actually access the shared data?

# COMMAND ----------

# Demonstrate how recipients access shared data with different clients
print("=" * 70)
print("RECIPIENT ACCESS PATTERNS")
print("=" * 70)
print()

print("=" * 70)
print("Pattern 1: Python (pandas) — Open Sharing")
print("=" * 70)
print("""
import delta_sharing

# Load the credentials file (downloaded from activation link)
profile = "config.share"

# List available shares
client = delta_sharing.SharingClient(profile)
shares = client.list_shares()
for share in shares:
    print(f"Share: {share.name}")
    for schema in client.list_schemas(share):
        for table in client.list_tables(schema):
            print(f"  Table: {table.name}")

# Read a shared table into pandas
table_url = f"{profile}#public_data_share.default.product_catalog"
df = delta_sharing.load_as_pandas(table_url)
print(df.head())
""")

print("=" * 70)
print("Pattern 2: Apache Spark — Open Sharing")
print("=" * 70)
print("""
# Read shared data into a Spark DataFrame
df = (spark.read
    .format("deltaSharing")
    .load("config.share#public_data_share.default.product_catalog")
)
df.show()

# Read with change data feed
cdf = (spark.read
    .format("deltaSharing")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("config.share#partner_analytics_share.default.regional_sales")
)
""")

print("=" * 70)
print("Pattern 3: Databricks-to-Databricks (Managed Sharing)")
print("=" * 70)
print("""
-- The shared data appears as a foreign catalog in Unity Catalog
-- No credentials file needed — authentication is automatic

-- List shared catalogs
SHOW CATALOGS;
-- You will see the provider's shared catalog

-- Query shared data directly
SELECT * FROM provider_shared_catalog.schema.table;

-- Create a local reference for convenience
CREATE TABLE local_catalog.schema.product_catalog
AS SELECT * FROM provider_shared_catalog.default.product_catalog;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Providers (Accessing Data Shared WITH You)
# MAGIC
# MAGIC When another organization shares data with you, they appear as a PROVIDER.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- PROVIDERS: Accessing Data Shared WITH You (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- List providers who have shared data with you
# MAGIC -- SHOW PROVIDERS;
# MAGIC --
# MAGIC -- Describe a provider
# MAGIC -- DESCRIBE PROVIDER acme_data_provider;
# MAGIC --
# MAGIC -- List shares from a provider
# MAGIC -- SHOW SHARES IN PROVIDER acme_data_provider;
# MAGIC --
# MAGIC -- Create a catalog from a provider's share (Databricks-to-Databricks)
# MAGIC -- CREATE CATALOG acme_shared_data
# MAGIC -- USING SHARE acme_data_provider.public_data_share;
# MAGIC --
# MAGIC -- Now query the shared data as a regular catalog
# MAGIC -- SELECT * FROM acme_shared_data.default.product_catalog;
# MAGIC
# MAGIC SELECT 'See comments above for PROVIDER syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Production Configuration Templates

# COMMAND ----------

# Print production configuration templates
print("=" * 70)
print("PRODUCTION CONFIGURATION: Open-Source Delta Sharing Server")
print("=" * 70)
print()
print("Server configuration (delta-sharing-server.yaml):")
print("""
# Delta Sharing Server Configuration
version: 1
shares:
  - name: "public_data_share"
    schemas:
      - name: "default"
        tables:
          - name: "product_catalog"
            location: "s3://my-bucket/delta/product_catalog"
          - name: "regional_sales"
            location: "s3://my-bucket/delta/regional_sales"

  - name: "partner_analytics_share"
    schemas:
      - name: "analytics"
        tables:
          - name: "aggregated_metrics"
            location: "s3://my-bucket/delta/aggregated_metrics"

# Server hosting configuration
host: "0.0.0.0"
port: 8443
endpoint: "/delta-sharing"

# Authentication
authorization:
  bearerToken:
    tokens:
      - id: "token_acme_corp"
        token: "<generated-bearer-token>"
        expirationTime: "2025-12-31T23:59:59Z"

# Cloud storage credentials
cloudStorage:
  s3:
    region: "us-east-1"
    # Uses IAM role attached to the server instance
""")

print("=" * 70)
print("PRODUCTION CONFIGURATION: Recipient Credentials File (.share)")
print("=" * 70)
print()
print("Credentials file format (config.share):")
print("""
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing-server.company.com:8443/delta-sharing/",
  "bearerToken": "<bearer-token-from-provider>",
  "expirationTime": "2025-12-31T23:59:59.000Z"
}

For Databricks-managed sharing, the credentials file is auto-generated
and downloaded via the activation link.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Audit Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- AUDIT LOGGING FOR DELTA SHARING (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Query sharing audit events
# MAGIC -- SELECT
# MAGIC --     event_time,
# MAGIC --     action_name,
# MAGIC --     request_params.share_name,
# MAGIC --     request_params.recipient_name,
# MAGIC --     request_params.table_name,
# MAGIC --     source_ip_address,
# MAGIC --     user_identity.email
# MAGIC -- FROM system.access.audit
# MAGIC -- WHERE service_name = 'unityCatalog'
# MAGIC --   AND action_name LIKE '%Share%'
# MAGIC -- ORDER BY event_time DESC
# MAGIC -- LIMIT 50;
# MAGIC --
# MAGIC -- Common audit events for Delta Sharing:
# MAGIC -- - createShare
# MAGIC -- - updateShare
# MAGIC -- - deleteShare
# MAGIC -- - createRecipient
# MAGIC -- - deleteRecipient
# MAGIC -- - getSharePermissions
# MAGIC -- - updateSharePermissions
# MAGIC -- - getActivationUrl (token downloaded)
# MAGIC -- - deltaSharingQueryTable (data accessed)
# MAGIC -- - deltaSharingListTables (schema browsed)
# MAGIC
# MAGIC SELECT 'See comments above for audit logging queries' AS note;

# COMMAND ----------

# Simulate a sharing audit log
from datetime import datetime

audit_events = [
    {"time": "2024-01-15 09:00:00", "action": "createShare",             "actor": "admin@company.com",     "detail": "Share: public_data_share"},
    {"time": "2024-01-15 09:05:00", "action": "updateShare",             "actor": "admin@company.com",     "detail": "Added table: product_catalog"},
    {"time": "2024-01-15 09:10:00", "action": "createRecipient",         "actor": "admin@company.com",     "detail": "Recipient: partner_acme_corp"},
    {"time": "2024-01-15 09:15:00", "action": "updateSharePermissions",  "actor": "admin@company.com",     "detail": "GRANT SELECT to partner_acme_corp"},
    {"time": "2024-01-16 14:30:00", "action": "getActivationUrl",        "actor": "partner_acme_corp",     "detail": "Credentials downloaded"},
    {"time": "2024-01-17 10:00:00", "action": "deltaSharingListTables",  "actor": "partner_acme_corp",     "detail": "Listed tables in public_data_share"},
    {"time": "2024-01-17 10:05:00", "action": "deltaSharingQueryTable",  "actor": "partner_acme_corp",     "detail": "Read product_catalog (50 rows)"},
    {"time": "2024-01-18 08:00:00", "action": "deltaSharingQueryTable",  "actor": "partner_acme_corp",     "detail": "Read product_catalog (50 rows)"},
    {"time": "2024-01-20 16:00:00", "action": "rotateRecipientToken",    "actor": "admin@company.com",     "detail": "Rotated token for partner_acme_corp"},
]

print("=" * 90)
print("SIMULATED DELTA SHARING AUDIT LOG")
print("=" * 90)
print(f"\n{'Timestamp':<22s} {'Action':<28s} {'Actor':<25s} {'Detail'}")
print("-" * 90)
for event in audit_events:
    print(f"{event['time']:<22s} {event['action']:<28s} {event['actor']:<25s} {event['detail']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Sharing Strategy Decision Guide

# COMMAND ----------

print("=" * 70)
print("DELTA SHARING DECISION GUIDE")
print("=" * 70)
print()
print("Question 1: Is the recipient using Databricks?")
print("  YES -> Use Databricks-to-Databricks sharing (simplest)")
print("  NO  -> Use open sharing (token-based)")
print()
print("Question 2: How sensitive is the data?")
print("  LOW  -> Share full tables, consider sharing entire schema")
print("  MED  -> Share specific tables, consider partition filters")
print("  HIGH -> Share views with row/column security built in")
print()
print("Question 3: Does the recipient need historical data?")
print("  YES -> Add WITH HISTORY (enables time travel)")
print("  NO  -> Default (latest snapshot only)")
print()
print("Question 4: Does the recipient need incremental updates?")
print("  YES -> Add WITH CHANGE DATA FEED")
print("  NO  -> Default (full snapshot reads)")
print()
print("Question 5: Is this cross-cloud sharing?")
print("  YES -> Delta Sharing handles this natively (no extra config)")
print("  NO  -> Same cloud, same benefits")
print()
print("Security checklist:")
print("  [ ] Set token expiration for open sharing recipients")
print("  [ ] Rotate tokens on a regular schedule")
print("  [ ] Monitor audit logs for unusual access patterns")
print("  [ ] Use views (not raw tables) for sensitive data")
print("  [ ] Review share contents quarterly")
print("  [ ] Remove recipients who no longer need access")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS m08_sharing_demo.product_catalog;
# MAGIC DROP TABLE IF EXISTS m08_sharing_demo.regional_sales_summary;
# MAGIC DROP TABLE IF EXISTS m08_sharing_demo.market_research;
# MAGIC DROP DATABASE IF EXISTS m08_sharing_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. Delta Sharing is an open protocol for cross-org data sharing")
print("  2. Three core objects: SHARE (container), PROVIDER (data owner), RECIPIENT (consumer)")
print("  3. No data copying: recipients read directly from provider's storage")
print("  4. Two auth methods: token-based (open) and metastore identity (Databricks-to-Databricks)")
print("  5. Can share tables, views, schemas, and volumes")
print("  6. Cross-cloud sharing works natively")
print("  7. Comprehensive audit logging tracks all shared data access")
print()
print("Next: 06-lakehouse-federation_notebook.py")
