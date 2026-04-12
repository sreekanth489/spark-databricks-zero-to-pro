# Unity Catalog Security
> Module: Data Governance | Day 11 | Level: Intermediate | Time: 120 min

## Learning Objectives

After completing this session, you will be able to:
- Explain the Unity Catalog security model and how it differs from Hive Metastore
- Manage identities: users, service principals, and groups
- Grant, revoke, and deny privileges on data objects
- Implement row-level security using **native Row Filters** (GA)
- Apply **native Column Masks** to protect sensitive data (GA)
- Understand the migration from regional dynamic views to native Row Filters
- Configure storage credentials and external locations
- Set up cross-workspace data access patterns

---

## The Security Problem Before Unity Catalog

### How Access Was Managed Before

```
  BEFORE UNITY CATALOG — The "Regional Views" Pattern
  ┌───────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  Problem: Different divisions (mid_west, west_division) should    │
  │  only see their own data. Also, PII columns must be hidden from   │
  │  division users — only admin1 sees full data.                     │
  │                                                                    │
  │  Solution teams used:                                             │
  │                                                                    │
  │  1. Create a BASE table with all data                             │
  │     CREATE TABLE hive_metastore.sales.transactions (...)          │
  │                                                                    │
  │  2. Create DIVISION VIEWS to filter rows                          │
  │     CREATE VIEW mid_west_transactions_vw AS                       │
  │       SELECT * FROM transactions WHERE region = 'mid_west';      │
  │     CREATE VIEW west_division_transactions_vw AS                  │
  │       SELECT * FROM transactions WHERE region = 'west_division'; │
  │                                                                    │
  │  3. Create MASKED VIEWS to hide PII columns                       │
  │     CREATE VIEW transactions_masked_vw AS                         │
  │       SELECT id,                                                  │
  │              CASE WHEN current_user() = 'analyst@co.com'         │
  │                   THEN '***-**-' || right(ssn,4)                 │
  │                   ELSE ssn END AS ssn,                           │
  │              amount FROM transactions;                            │
  │                                                                    │
  │  Problems with this approach:                                     │
  │  ✗ N regions = N views to maintain                                │
  │  ✗ When user changes group, views must be updated                │
  │  ✗ Users need to KNOW which view to query                        │
  │  ✗ Easy to accidentally bypass by querying base table            │
  │  ✗ No governance on the view itself (who created it?)            │
  │  ✗ Can't combine row+column security cleanly                     │
  │  ✗ workspace-local: must duplicate in every workspace            │
  └───────────────────────────────────────────────────────────────────┘
```

### The Unity Catalog Security Solution

```
  WITH UNITY CATALOG — Native Row Filters + Column Masks
  ┌───────────────────────────────────────────────────────────────────┐
  │                                                                    │
  │  Same requirement — but now managed AT THE TABLE LEVEL            │
  │                                                                    │
  │  1. Create a row filter FUNCTION (once)                           │
  │     CREATE FUNCTION region_filter(region STRING)                  │
  │       RETURNS BOOLEAN                                             │
  │       RETURN is_account_group_member('admin1')                    │
  │           OR is_account_group_member(lower(region));                  │
  │                                                                    │
  │  2. ATTACH it to the table (once)                                 │
  │     ALTER TABLE transactions                                      │
  │       SET ROW FILTER region_filter ON (region);                   │
  │                                                                    │
  │  3. Users query the TABLE DIRECTLY — filter is transparent        │
  │     SELECT * FROM prod_catalog.sales.transactions;                │
  │     -- Automatically filtered by user's region group!             │
  │                                                                    │
  │  Benefits:                                                        │
  │  ✓ One function, one attachment — no N-views problem             │
  │  ✓ Filter follows group membership (auto-updated)                │
  │  ✓ Users query the TABLE, not a view — simpler mental model       │
  │  ✓ Cannot be bypassed — filter applies to ALL queries             │
  │  ✓ Works across workspaces — defined once at account level       │
  │  ✓ Audited: who attached the filter, when, what function         │
  └───────────────────────────────────────────────────────────────────┘
```

---

## UC Security Model Overview

```
  Unity Catalog Security Model
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━

  GRANT privilege ON securable_object TO principal

  Principals (WHO)
  ┌─────────────────────────────────────────────────────┐
  │  Users           individual email-based identities  │
  │  Service Princ.  application IDs for automation     │
  │  Groups          collections (can be nested)        │
  └─────────────────────────────────────────────────────┘

  Securable Objects (WHERE)
  ┌─────────────────────────────────────────────────────┐
  │  METASTORE                                           │
  │  └── CATALOG                                         │
  │      └── SCHEMA                                      │
  │          ├── TABLE / VIEW                            │
  │          ├── VOLUME                                  │
  │          ├── FUNCTION (incl. row filters, col masks) │
  │          └── MODEL                                   │
  │  STORAGE CREDENTIAL                                  │
  │  EXTERNAL LOCATION                                   │
  │  SHARE / RECIPIENT                                   │
  └─────────────────────────────────────────────────────┘

  Privileges (WHAT)
  ┌─────────────────────────────────────────────────────┐
  │  USE CATALOG, USE SCHEMA  (prerequisite chain)       │
  │  SELECT, MODIFY           (data access)              │
  │  CREATE TABLE/SCHEMA/CATALOG (creation rights)       │
  │  READ VOLUME, WRITE VOLUME  (file access)            │
  │  READ FILES, WRITE FILES    (external locations)     │
  │  EXECUTE                    (functions/procedures)   │
  │  APPLY TAG                  (tagging assets)         │
  │  ALL PRIVILEGES             (shorthand for all above)│
  └─────────────────────────────────────────────────────┘
```

---

## Privilege Prerequisite Chain

This is a critical concept for the exam and for production use:

```
  To SELECT from prod_catalog.hr_db.employees:

  Step 1: USE CATALOG ON CATALOG prod_catalog    ← Navigate to catalog
  Step 2: USE SCHEMA ON SCHEMA hr_db             ← Navigate to schema
  Step 3: SELECT ON TABLE employees              ← Read the table

  ┌──────────────────────────────────────────────────────────────────┐
  │  USE CATALOG   ──▶  USE SCHEMA  ──▶  SELECT/MODIFY/etc.          │
  │  (I can see the catalog)                                         │
  │                     (I can see the schema)                       │
  │                                           (I can read/write data)│
  │                                                                  │
  │  NOTE: USE CATALOG does NOT mean you can read data.              │
  │  It only allows you to see that the catalog exists.              │
  │  Same for USE SCHEMA — it does NOT grant data access.            │
  └──────────────────────────────────────────────────────────────────┘
```

```sql
-- Read access for division groups
GRANT USE CATALOG ON CATALOG prod_catalog TO `mid_west`;
GRANT USE SCHEMA ON SCHEMA prod_catalog.hr_db TO `mid_west`;
GRANT SELECT ON TABLE prod_catalog.hr_db.employees TO `mid_west`;

GRANT USE CATALOG ON CATALOG prod_catalog TO `west_division`;
GRANT USE SCHEMA ON SCHEMA prod_catalog.hr_db TO `west_division`;
GRANT SELECT ON TABLE prod_catalog.hr_db.employees TO `west_division`;

-- Full access for admin1
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG prod_catalog TO `admin1`;
```

---

## Dynamic Views for Row/Column Security (Legacy Pattern)

Dynamic views were the standard approach before native Row Filters. Understanding them is important because:
- Many existing workspaces still use them
- They are more flexible for complex logic
- Still needed for Hive Metastore governance

### Row-Level Security via Dynamic View

```sql
-- Only show employees from the user's own division
CREATE OR REPLACE VIEW prod_catalog.hr_db.secure_employees_vw AS
SELECT *
FROM prod_catalog.hr_db.employees
WHERE
  CASE
    WHEN is_account_group_member('admin1')        THEN true
    WHEN is_account_group_member('mid_west')      AND region = 'mid_west'      THEN true
    WHEN is_account_group_member('west_division') AND region = 'west_division' THEN true
    ELSE false
  END;

-- Grant on VIEW, not on TABLE
GRANT SELECT ON VIEW prod_catalog.hr_db.secure_employees_vw TO `mid_west`;
GRANT SELECT ON VIEW prod_catalog.hr_db.secure_employees_vw TO `west_division`;
-- Do NOT grant SELECT on prod_catalog.hr_db.employees to division groups
```

### Column Masking via Dynamic View

```sql
CREATE OR REPLACE VIEW prod_catalog.hr_db.masked_employees_vw AS
SELECT
  employee_id,
  first_name,
  last_name,

  -- Email: visible to admin1 only, masked for division users
  CASE
    WHEN is_account_group_member('admin1')
      THEN email
    ELSE concat(left(email, 2), '***@***')
  END AS email,

  -- SSN: last 4 digits only unless admin1
  CASE
    WHEN is_account_group_member('admin1')
      THEN ssn
    ELSE concat('***-**-', right(ssn, 4))
  END AS ssn,

  department,

  -- Salary: visible to admin1 only
  CASE
    WHEN is_account_group_member('admin1')
      THEN salary
    ELSE NULL
  END AS salary

FROM prod_catalog.hr_db.employees
WHERE is_active = true;
```

### Limitations of Dynamic Views

```
  Dynamic View Limitations
  ┌────────────────────────────────────────────────────────────────┐
  │  ✗ Users must query the VIEW, not the TABLE                    │
  │    → Mental overhead, documentation burden                     │
  │                                                                │
  │  ✗ If someone has SELECT on the base TABLE, view is bypassed   │
  │    → Must carefully manage both table AND view grants          │
  │                                                                │
  │  ✗ Maintenance: logic duplicated across many views             │
  │    → N regional views × M column masks = N×M views            │
  │                                                                │
  │  ✗ No single place to see "what security policies apply?"     │
  │    → Must inspect each view definition individually            │
  │                                                                │
  │  ✗ Cannot stack: one view for rows, another for columns       │
  │    → Combined security requires one complex view               │
  └────────────────────────────────────────────────────────────────┘
```

---

## Native Row Filters (Current Best Practice)

**Row Filters** are functions that are ATTACHED to a table and automatically applied to every query. No view required.

```
  How Row Filters Work
  ┌────────────────────────────────────────────────────────────────┐
  │                                                                │
  │  SELECT * FROM employees;                                      │
  │           │                                                    │
  │           ▼                                                    │
  │  ┌─────────────────────────────────────────────┐              │
  │  │  Databricks SQL Engine                       │              │
  │  │                                             │              │
  │  │  1. Intercepts the query                    │              │
  │  │  2. Evaluates row_filter_fn(department)     │              │
  │  │     for current_user()                      │              │
  │  │  3. Automatically adds WHERE clause         │              │
  │  │  4. Returns only qualifying rows            │              │
  │  └─────────────────────────────────────────────┘              │
  │           │                                                    │
  │           ▼                                                    │
  │  Only rows the current user is allowed to see                  │
  │  (transparent to the user — they query the TABLE directly)     │
  └────────────────────────────────────────────────────────────────┘
```

### Implementing Native Row Filters

```sql
-- Step 1: Create a row filter function
-- The function must return BOOLEAN
-- Parameters map to columns in the filtered table
CREATE OR REPLACE FUNCTION prod_catalog.hr_db.division_row_filter(div STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('admin1')          -- admin1 sees all
  OR is_account_group_member(lower(div));    -- users see their division rows
  -- lower('mid_west') = 'mid_west' = group name
  -- lower('west_division') = 'west_division' = group name

-- Step 2: Attach the row filter to the table
-- ON (region) maps the column `region` to the function parameter `div`
ALTER TABLE prod_catalog.hr_db.employees
  SET ROW FILTER prod_catalog.hr_db.division_row_filter ON (region);

-- Step 3: Grant EXECUTE on the function to users who query the table
-- (They need to execute the filter function as part of their query)
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.division_row_filter
  TO `mid_west`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.division_row_filter
  TO `west_division`;

-- Now query the TABLE directly — filter is automatic
SELECT * FROM prod_catalog.hr_db.employees;
-- mid_west user sees only region='mid_west' rows
-- west_division user sees only region='west_division' rows
-- admin1 sees all rows

-- Step 4: Remove row filter when no longer needed
ALTER TABLE prod_catalog.hr_db.employees DROP ROW FILTER;
```

### Row Filter: Division Data Pattern

This replaces the old "create a view per division" pattern:

```sql
-- OLD WAY: 2 views (mid_west, west_division)
CREATE VIEW mid_west_sales_vw     AS SELECT * FROM sales WHERE region = 'mid_west';
CREATE VIEW west_division_sales_vw AS SELECT * FROM sales WHERE region = 'west_division';
-- Users query different views, maintenance burden grows with divisions

-- NEW WAY: 1 row filter function + 1 table
CREATE OR REPLACE FUNCTION prod_catalog.sales_db.region_row_filter(region_col STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('admin1')              -- admin1 sees all
  OR is_account_group_member(lower(region_col)); -- group name matches region value
  -- lower('mid_west') = 'mid_west' group sees mid_west rows
  -- lower('west_division') = 'west_division' group sees west_division rows

ALTER TABLE prod_catalog.sales_db.transactions
  SET ROW FILTER prod_catalog.sales_db.region_row_filter ON (region);

-- ALL users query the SAME table — filter is transparent
SELECT * FROM prod_catalog.sales_db.transactions;
-- mid_west member sees only region='mid_west' rows
-- west_division member sees only region='west_division' rows
-- admin1 sees all rows
```

---

## Native Column Masks (Current Best Practice)

**Column Masks** are functions that transform individual column values before returning them to the user. Applied at the column level on the table.

```
  Column Mask Execution Flow
  ┌────────────────────────────────────────────────────────────────┐
  │                                                                │
  │  SELECT employee_id, email, salary FROM employees;             │
  │           │                                                    │
  │           ▼                                                    │
  │  For each row, for each masked column:                         │
  │  ┌─────────────────────────────────────────────────────────┐  │
  │  │  email column:   mask_email_fn(email)                   │  │
  │  │    → HR user:    alice@company.com  (unmasked)          │  │
  │  │    → Analyst:    al***@***          (masked)            │  │
  │  │                                                         │  │
  │  │  salary column:  mask_salary_fn(salary)                 │  │
  │  │    → Finance:    120000.0           (unmasked)          │  │
  │  │    → Analyst:    NULL               (masked)            │  │
  │  └─────────────────────────────────────────────────────────┘  │
  │                                                                │
  │  User sees transformed values transparently                    │
  └────────────────────────────────────────────────────────────────┘
```

### Implementing Native Column Masks

```sql
-- Step 1: Create masking functions
-- Return type must match the column type being masked

CREATE OR REPLACE FUNCTION prod_catalog.hr_db.mask_email(email_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('admin1')
      THEN email_val
    ELSE concat(left(email_val, 2), '***@***')
  END;

CREATE OR REPLACE FUNCTION prod_catalog.hr_db.mask_ssn(ssn_val STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('admin1')
      THEN ssn_val
    ELSE concat('***-**-', right(ssn_val, 4))
  END;

CREATE OR REPLACE FUNCTION prod_catalog.hr_db.mask_salary(salary_val DOUBLE)
RETURNS DOUBLE
RETURN
  CASE
    WHEN is_account_group_member('admin1')
      THEN salary_val
    ELSE NULL
  END;

-- Step 2: Attach column masks to the table
ALTER TABLE prod_catalog.hr_db.employees
  ALTER COLUMN email   SET MASK prod_catalog.hr_db.mask_email;

ALTER TABLE prod_catalog.hr_db.employees
  ALTER COLUMN ssn     SET MASK prod_catalog.hr_db.mask_ssn;

ALTER TABLE prod_catalog.hr_db.employees
  ALTER COLUMN salary  SET MASK prod_catalog.hr_db.mask_salary;

-- Step 3: Grant EXECUTE on masking functions
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_email   TO `mid_west`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_email   TO `west_division`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_ssn     TO `mid_west`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_ssn     TO `west_division`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_salary  TO `mid_west`;
GRANT EXECUTE ON FUNCTION prod_catalog.hr_db.mask_salary  TO `west_division`;

-- Now all users query the TABLE directly — masking is automatic
SELECT employee_id, email, ssn, salary FROM prod_catalog.hr_db.employees;
-- admin1:        alice@company.com | 123-45-6789 | 120000.0  (unmasked)
-- mid_west user: al***@***        | ***-**-6789 | NULL       (masked)

-- Step 4: Remove a column mask
ALTER TABLE prod_catalog.hr_db.employees ALTER COLUMN email DROP MASK;
```

---

## Migration: Regional Views → Native Row Filters

For teams migrating from the old regional views pattern:

```
  Migration Strategy
  ─────────────────

  Phase 1: Identify (Audit existing views)
  ┌────────────────────────────────────────────────────────────────┐
  │  SELECT table_name, view_definition                            │
  │  FROM information_schema.views                                 │
  │  WHERE view_definition LIKE '%region%'                        │
  │     OR view_definition LIKE '%current_user()%';               │
  └────────────────────────────────────────────────────────────────┘

  Phase 2: Create equivalent Row Filter functions
  ┌────────────────────────────────────────────────────────────────┐
  │  -- For each WHERE clause pattern in old views, create a fn   │
  │  CREATE FUNCTION region_filter(r STRING)                       │
  │  RETURNS BOOLEAN                                               │
  │  RETURN is_account_group_member('admin1') OR                   │
  │         is_account_group_member(lower(r));                    │
  │  -- lower('mid_west') = 'mid_west', lower('west_division') =  │
  │  -- 'west_division' — group names match column values exactly  │
  └────────────────────────────────────────────────────────────────┘

  Phase 3: Attach to tables
  ┌────────────────────────────────────────────────────────────────┐
  │  ALTER TABLE transactions SET ROW FILTER region_filter ON (r);│
  └────────────────────────────────────────────────────────────────┘

  Phase 4: Update grants (grant on TABLE, revoke from views)
  ┌────────────────────────────────────────────────────────────────┐
  │  GRANT USE CATALOG, USE SCHEMA, SELECT ON TABLE transactions  │
  │    TO `mid_west`, `west_division`;                            │
  │  GRANT EXECUTE ON FUNCTION region_filter                       │
  │    TO `mid_west`, `west_division`;                            │
  └────────────────────────────────────────────────────────────────┘

  Phase 5: Deprecate views (keep temporarily, then drop)
  ┌────────────────────────────────────────────────────────────────┐
  │  ALTER VIEW mid_west_sales_vw                                  │
  │    SET TBLPROPERTIES ('deprecated' = 'true',                  │
  │    'migrate_to' = 'transactions');                             │
  │  -- After cutover:                                             │
  │  DROP VIEW mid_west_sales_vw;                                  │
  │  DROP VIEW west_division_sales_vw;                             │
  └────────────────────────────────────────────────────────────────┘
```

---

## Dynamic Views vs Native Row Filters vs Native Column Masks

| Aspect | Dynamic Views | Native Row Filters | Native Column Masks |
|--------|--------------|-------------------|---------------------|
| Object type | VIEW | FUNCTION attached to TABLE | FUNCTION attached to COLUMN |
| User experience | Query the VIEW | Query the TABLE | Query the TABLE |
| Bypassable? | Yes (if user has TABLE SELECT) | No (always enforced) | No (always enforced) |
| Maintenance | N views for N filter variations | 1 function, 1 ALTER TABLE | 1 function per column |
| Stacking | Complex | Row filter + Column mask on same table | Multiple masks on diff columns |
| Visibility | Hidden behind views | Visible in table metadata | Visible in column metadata |
| Availability | All DBR versions | DBR 12.2+ (GA), UC required | DBR 12.2+ (GA), UC required |

---

## Storage Credentials and External Locations

To access data in cloud storage, UC uses a two-layer model:

```
  Storage Credential
  (Authentication — HOW to connect)
  ┌──────────────────────────────────────────────────────────────┐
  │  Azure: Access Connector for Azure Databricks (Managed ID)   │
  │  AWS:   IAM Role ARN                                         │
  │  GCP:   Service Account JSON key                             │
  └──────────────────────────────────────────────────────────────┘
                           │
                           │ Used by
                           ▼
  External Location
  (Path — WHAT path is accessible)
  ┌──────────────────────────────────────────────────────────────┐
  │  abfss://raw-data@mystorage.dfs.core.windows.net/bronze/     │
  │  s3://my-data-lake/silver/                                   │
  │  gs://my-gcs-bucket/gold/                                    │
  └──────────────────────────────────────────────────────────────┘
                           │
                           │ Governed by
                           ▼
  GRANT READ FILES / WRITE FILES ON EXTERNAL LOCATION
  (WHO can access the path)
```

```sql
-- Create storage credential (Azure example)
-- Usually done via UI or Terraform by metastore admin
CREATE STORAGE CREDENTIAL azure_adls_cred
WITH (
  AZURE_MANAGED_IDENTITY = (
    CREDENTIAL = '/subscriptions/<sub>/resourceGroups/<rg>/providers/
                  Microsoft.Databricks/accessConnectors/uc-connector'
  )
);

-- Validate the credential works
VALIDATE STORAGE CREDENTIAL azure_adls_cred
ON LOCATION 'abfss://data@mystorage.dfs.core.windows.net/';

-- Create external location
CREATE EXTERNAL LOCATION bronze_zone
  URL 'abfss://raw@mystorage.dfs.core.windows.net/bronze/'
  WITH (STORAGE CREDENTIAL azure_adls_cred)
  COMMENT 'Bronze landing zone for raw ingestion';

-- Grant file access to data engineers
GRANT READ FILES, WRITE FILES
  ON EXTERNAL LOCATION bronze_zone
  TO `data_engineers`;

-- Grant read-only to analysts
GRANT READ FILES
  ON EXTERNAL LOCATION bronze_zone
  TO `analysts`;
```

---

## Complete Security Blueprint

A production security pattern for a multi-team lakehouse:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  Production Security Architecture                                    │
  │                                                                      │
  │  Storage Layer                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │  External Locations (governed by Storage Credentials)         │  │
  │  │  bronze_zone (READ FILES: de_team, READ FILES: ingestion_sp)  │  │
  │  │  silver_zone (READ FILES: analysts, WRITE FILES: de_team)     │  │
  │  │  gold_zone   (READ FILES: bi_team, SELECT: dashboard_sp)      │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  │                                                                      │
  │  Catalog Layer                                                       │
  │  ┌───────────────────────────────────────────────────────────────┐  │
  │  │  prod_catalog                                                  │  │
  │  │  Owner: admin1                                                │  │
  │  │  USE CATALOG: mid_west, west_division, admin1                │  │
  │  │                                                               │  │
  │  │  ├── Schema: raw_db (admin1 only)                             │  │
  │  │  │   Owner: admin1                                           │  │
  │  │  │   USE SCHEMA + SELECT: admin1                             │  │
  │  │  │                                                            │  │
  │  │  ├── Schema: curated_db (divisions read, admin1 write)        │  │
  │  │  │   Owner: admin1                                           │  │
  │  │  │   USE SCHEMA + SELECT: mid_west, west_division, admin1   │  │
  │  │  │   MODIFY: admin1                                          │  │
  │  │  │   Row Filter: region_filter ON region column              │  │
  │  │  │     → mid_west sees mid_west rows only                    │  │
  │  │  │     → west_division sees west_division rows only          │  │
  │  │  │   Column Mask: mask_ssn ON ssn, mask_salary ON salary     │  │
  │  │  │     → admin1 sees full values, divisions see NULL/masked  │  │
  │  │  │                                                            │  │
  │  │  └── Schema: reporting_db (read by all groups)                │  │
  │  │      Owner: admin1                                           │  │
  │  │      USE SCHEMA + SELECT: mid_west, west_division, admin1   │  │
  │  └───────────────────────────────────────────────────────────────┘  │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## Querying Existing Security Policies

```sql
-- View all grants on a table
SHOW GRANTS ON TABLE prod_catalog.hr_db.employees;

-- View all grants on a schema
SHOW GRANTS ON SCHEMA prod_catalog.hr_db;

-- View grants for a specific user or group
SHOW GRANTS `analysts`;

-- View grants on a storage credential
SHOW GRANTS ON STORAGE CREDENTIAL azure_adls_cred;

-- View row filters and column masks on a table
DESCRIBE EXTENDED prod_catalog.hr_db.employees;
-- Look for: Row Filter, Column Masks sections

-- List all functions (including row filter / mask functions) in a schema
SHOW FUNCTIONS IN prod_catalog.hr_db;

-- Check which tables have row filters via information_schema
SELECT table_catalog, table_schema, table_name, row_filter
FROM prod_catalog.information_schema.tables
WHERE row_filter IS NOT NULL;
```

---

## Audit Logging via System Tables

```sql
-- See all permission changes (GRANT/REVOKE events)
SELECT
  event_time,
  user_identity.email AS performed_by,
  action_name,
  request_params.securable_type,
  request_params.securable_full_name,
  request_params.changes
FROM system.access.audit
WHERE action_name IN ('grantPermission', 'revokePermission', 'updatePermissions')
ORDER BY event_time DESC
LIMIT 50;

-- See who accessed sensitive tables
SELECT
  event_time,
  user_identity.email,
  action_name,
  request_params.full_name_arg AS table_accessed
FROM system.access.audit
WHERE request_params.full_name_arg = 'prod_catalog.hr_db.employees'
  AND action_name = 'getTable'
ORDER BY event_time DESC;
```

---

## Hands-On Walkthrough

### Lab 1 — Day 11 Main Notebook
[`11-unity-catalog-security_notebook.py`](11-unity-catalog-security_notebook.py)

Covers:
1. GRANT/REVOKE/SHOW GRANTS syntax
2. Dynamic views for row-level security (legacy pattern)
3. Dynamic views for column masking
4. Combined row + column security view
5. Reusable masking UDFs
6. Table ownership management

### Lab 2 — Row Filters and Column Masks (Native)
[`11b-row-filters-column-masks_notebook.py`](11b-row-filters-column-masks_notebook.py)

Covers:
1. Creating row filter functions
2. Attaching/detaching row filters to tables
3. Creating column mask functions
4. Attaching/detaching column masks
5. Migration from dynamic views to native policies
6. Inspecting applied policies via DESCRIBE EXTENDED

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Storage Credential type | IAM Role ARN | Access Connector (Managed Identity) | Service Account JSON |
| Identity provider | SCIM + AWS SSO / Okta | Azure AD (Entra ID) + SCIM | Google Identity + SCIM |
| Account console | accounts.cloud.databricks.com | accounts.azuredatabricks.net | accounts.gcp.databricks.com |
| External location path | `s3://bucket/path/` | `abfss://container@account.dfs.core.windows.net/path/` | `gs://bucket/path/` |
| Storage permission for UC | `s3:GetObject`, `s3:PutObject` on bucket | Storage Blob Data Contributor on ADLS | Storage Object Admin on GCS |

---

## Certification Tip

**Databricks Certified Data Engineer Associate / Professional** exams test:
- GRANT/REVOKE/DENY syntax and the prerequisite chain (`USE CATALOG` → `USE SCHEMA` → `SELECT`)
- Difference between Hive Metastore ACLs and Unity Catalog privileges
- Ownership and who can grant privileges (metastore admin, catalog owner, schema owner, object owner)
- Dynamic views for row-level security using `is_account_group_member()`
- Storage credentials and external locations
- Native Row Filters and Column Masks (DBR 12.2+)
- `current_user()` and `is_account_group_member()` functions

---

## Key Takeaways

1. **Before UC**: Regional views were the only way to filter rows — N regions = N views = maintenance burden
2. **Native Row Filters**: Attach a function to a TABLE — filter is transparent, cannot be bypassed
3. **Native Column Masks**: Attach a masking function to a COLUMN — transforms values transparently
4. **Migration path**: View the WHERE clause logic in old views → create Row Filter functions → ALTER TABLE SET ROW FILTER → update grants
5. **Prerequisite chain**: `USE CATALOG` → `USE SCHEMA` → `SELECT` (each step required)
6. **Storage Credentials + External Locations**: Two-layer model to govern cloud storage access
7. **Audit logs**: Query `system.access.audit` to see who accessed/changed what
8. **Groups over individuals**: Always assign privileges to groups, not individual users

---

## Next Steps

- [Day 12: Managed vs External Tables](../day12-managed-vs-external-tables/) — deep dive into table types
- [Day 13: Volumes in Databricks](../day13-volumes-in-databricks/) — governed file access patterns
