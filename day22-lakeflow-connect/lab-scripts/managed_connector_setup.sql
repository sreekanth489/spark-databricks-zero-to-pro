-- managed_connector_setup.sql
-- SQL commands for setting up Managed Connectors in Lakeflow Connect.
--
-- Managed Connectors are no-code, serverless connectors that use CDC
-- (Change Data Capture) for efficient incremental ingestion from
-- databases and SaaS applications.
--
-- These commands can be run in a Databricks SQL editor or notebook.
-- They require appropriate Unity Catalog permissions.


-- ============================================================================
-- EXAMPLE 1: PostgreSQL Managed Connector
-- ============================================================================

-- Step 1: Create a connection to the PostgreSQL database
-- The connection object is stored in Unity Catalog and can be shared/governed.
CREATE CONNECTION IF NOT EXISTS ecommerce_postgres
TYPE postgresql
OPTIONS (
    host 'ecommerce-db.example.com',
    port '5432',
    user secret('jdbc-secrets', 'pg-username'),
    password secret('jdbc-secrets', 'pg-password')
);

-- Step 2: Verify the connection
DESCRIBE CONNECTION ecommerce_postgres;

-- Step 3: Create streaming tables that ingest via CDC
-- Each table reads the change feed from the source and applies changes
-- incrementally (inserts, updates, deletes).

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.pg_customers
COMMENT 'Customers from PostgreSQL via Managed Connector (CDC)'
AS SELECT * FROM STREAM read_changefeed(
    'ecommerce_postgres',
    'public.customers'
);

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.pg_orders
COMMENT 'Orders from PostgreSQL via Managed Connector (CDC)'
AS SELECT * FROM STREAM read_changefeed(
    'ecommerce_postgres',
    'public.orders'
);

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.pg_products
COMMENT 'Products from PostgreSQL via Managed Connector (CDC)'
AS SELECT * FROM STREAM read_changefeed(
    'ecommerce_postgres',
    'public.products'
);


-- ============================================================================
-- EXAMPLE 2: MySQL Managed Connector
-- ============================================================================

CREATE CONNECTION IF NOT EXISTS ecommerce_mysql
TYPE mysql
OPTIONS (
    host 'mysql-replica.example.com',
    port '3306',
    user secret('jdbc-secrets', 'mysql-username'),
    password secret('jdbc-secrets', 'mysql-password')
);

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.mysql_inventory
COMMENT 'Inventory from MySQL via Managed Connector (CDC)'
AS SELECT * FROM STREAM read_changefeed(
    'ecommerce_mysql',
    'inventory.stock_levels'
);


-- ============================================================================
-- EXAMPLE 3: Salesforce Managed Connector
-- ============================================================================

-- SaaS connectors use OAuth or API key authentication
CREATE CONNECTION IF NOT EXISTS salesforce_conn
TYPE salesforce
OPTIONS (
    host 'https://mycompany.my.salesforce.com',
    user secret('salesforce-secrets', 'sf-username'),
    password secret('salesforce-secrets', 'sf-password'),
    securityToken secret('salesforce-secrets', 'sf-token')
);

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.sf_accounts
COMMENT 'Accounts from Salesforce via Managed Connector'
AS SELECT * FROM STREAM read_changefeed(
    'salesforce_conn',
    'Account'
);

CREATE STREAMING TABLE IF NOT EXISTS ecommerce.bronze.sf_opportunities
COMMENT 'Opportunities from Salesforce via Managed Connector'
AS SELECT * FROM STREAM read_changefeed(
    'salesforce_conn',
    'Opportunity'
);


-- ============================================================================
-- GOVERNANCE: Grant access to ingested tables
-- ============================================================================

-- Data engineers can manage connections
GRANT CREATE CONNECTION ON CATALOG ecommerce TO `data-engineers`;

-- Analysts can read ingested bronze tables
GRANT SELECT ON SCHEMA ecommerce.bronze TO `data-analysts`;

-- View all connections in the catalog
SHOW CONNECTIONS;


-- ============================================================================
-- MONITORING: Check pipeline health
-- ============================================================================

-- View streaming table status
DESCRIBE EXTENDED ecommerce.bronze.pg_customers;

-- Check data freshness (when was the last ingestion?)
SELECT MAX(_commit_timestamp) AS last_ingestion
FROM ecommerce.bronze.pg_customers;


-- ============================================================================
-- CLEANUP (uncomment to remove)
-- ============================================================================

-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.pg_customers;
-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.pg_orders;
-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.pg_products;
-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.mysql_inventory;
-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.sf_accounts;
-- DROP STREAMING TABLE IF EXISTS ecommerce.bronze.sf_opportunities;
-- DROP CONNECTION IF EXISTS ecommerce_postgres;
-- DROP CONNECTION IF EXISTS ecommerce_mysql;
-- DROP CONNECTION IF EXISTS salesforce_conn;
