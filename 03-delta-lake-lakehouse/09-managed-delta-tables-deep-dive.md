# Managed Delta Tables Deep Dive

> Module 03 -- Topic 09 | Level: Beginner to Advanced | Time: 60 min

## Learning Objectives

- Understand managed vs external Delta tables and when to use each
- Create catalogs, schemas, and managed Delta tables with Unity Catalog
- Perform full CRUD operations (INSERT, UPDATE, DELETE) on managed tables
- Use Time Travel to query historical versions by number and timestamp
- Implement MERGE (upsert) patterns for incremental data loads
- Apply schema enforcement, schema evolution, and CHECK constraints
- Optimize table performance with OPTIMIZE, Z-ORDER, and VACUUM
- Enable and query Change Data Feed (CDF) for auditing
- Use the DeltaTable Python API and DESCRIBE commands for metadata inspection

## Conceptual Overview

### Managed vs External Tables

In Databricks, every Delta table falls into one of two categories:

```
  +------------------------------------------------------------------+
  |                      Delta Table Types                            |
  +------------------------------------------------------------------+
  |                                                                    |
  |   MANAGED TABLE                     EXTERNAL TABLE                 |
  |   ==============                    ==============                 |
  |   - Databricks owns data + metadata - You own data storage        |
  |   - Stored in catalog's managed     - Points to YOUR path         |
  |     storage location                  (S3, ADLS, GCS)             |
  |   - DROP TABLE = deletes everything - DROP TABLE = metadata only  |
  |   - Best for curated Lakehouse data - Best for shared/raw data    |
  |   - Unity Catalog governs fully     - You manage lifecycle        |
  |                                                                    |
  +------------------------------------------------------------------+
```

| Aspect | Managed Table | External Table |
|--------|--------------|----------------|
| **Data Location** | Catalog's managed storage | User-specified path (LOCATION clause) |
| **DROP TABLE** | Deletes data + metadata | Deletes metadata only; data remains |
| **Governance** | Fully governed by Unity Catalog | Metadata governed; data access depends on storage permissions |
| **Use Case** | Production Lakehouse tables | Landing zones, shared datasets, cross-platform data |
| **CREATE syntax** | `CREATE TABLE t (...)` | `CREATE TABLE t (...) LOCATION 's3://...'` |

**This topic focuses on managed tables.** For external tables and advanced storage
behavior, see [Topic 10 -- External Delta Tables on S3](10-external-delta-tables-s3.md).

### Unity Catalog Three-Level Namespace

Managed tables live inside Unity Catalog's three-level namespace:

```
  CATALOG  .  SCHEMA  .  TABLE
  -------     ------     -----
  databricks_pro . employee . employee

  +-------------------+
  |     CATALOG       |   <-- Top-level container (e.g., databricks_pro)
  |   +-------------+ |
  |   |   SCHEMA    | |   <-- Logical grouping (e.g., employee)
  |   | +---------+ | |
  |   | | TABLE   | | |   <-- Delta table (e.g., employee)
  |   | +---------+ | |
  |   | | TABLE   | | |   <-- Another table
  |   | +---------+ | |
  |   +-------------+ |
  |   +-------------+ |
  |   |   SCHEMA    | |   <-- Another schema (e.g., finance)
  |   +-------------+ |
  +-------------------+
```

```sql
-- Create the hierarchy
CREATE CATALOG databricks_pro;
USE CATALOG databricks_pro;
CREATE SCHEMA IF NOT EXISTS employee;
USE SCHEMA employee;
```

### Managed Table Lifecycle

When you create a managed table, Databricks handles the entire data lifecycle:

```
  CREATE TABLE employee (...)
        |
        v
  +---------------------------------------------+
  | Unity Catalog registers metadata             |
  | - Table name, schema, properties             |
  | - Managed storage location assigned          |
  +---------------------------------------------+
        |
        v
  +---------------------------------------------+
  | Delta files written to managed storage       |
  | - Parquet data files                         |
  | - _delta_log/ transaction log                |
  +---------------------------------------------+
        |
        v
  +---------------------------------------------+
  | DML Operations (INSERT, UPDATE, DELETE)      |
  | - Each operation = new version in log        |
  | - ACID guarantees on every operation         |
  +---------------------------------------------+
        |
        v  (when no longer needed)
  +---------------------------------------------+
  | DROP TABLE employee                          |
  | - Metadata removed from catalog              |
  | - Data files DELETED from storage            |
  | - Complete cleanup (managed only!)           |
  +---------------------------------------------+
```

## Feature Deep Dive: Beginner to Advanced

### Level 1: Beginner -- Create and Query

**Creating a managed Delta table** is straightforward. Delta is the default format
in Databricks, so `USING DELTA` is optional:

```sql
CREATE TABLE IF NOT EXISTS employee (
  employee_id   INT,
  first_name    STRING,
  last_name     STRING,
  department    STRING,
  salary        INT,
  hire_date     DATE
);
```

**Insert data** directly with SQL:

```sql
INSERT INTO employee VALUES
  (1, 'John',    'Doe',      'Engineering', 80000, '2022-01-01'),
  (2, 'Jane',    'Smith',    'Engineering', 75000, '2022-02-01'),
  (3, 'Bob',     'Johnson',  'Marketing',  60000, '2022-03-01'),
  (4, 'Alice',   'Williams', 'Marketing',  65000, '2022-04-01'),
  (5, 'Charlie', 'Brown',    'Engineering', 85000, '2022-05-01');
```

**Query the table:**

```sql
SELECT * FROM employee;
```

**Inspect metadata** with DESCRIBE commands:

```sql
-- Table-level metadata: location, format, size, number of files
DESCRIBE DETAIL employee;

-- Full commit history: who did what, when, which version
DESCRIBE HISTORY employee;
```

### Level 2: Intermediate -- DML Operations

#### UPDATE

Delta rewrites only the Parquet files that contain affected rows:

```sql
UPDATE employee
SET salary = salary + 5000
WHERE department = 'Engineering';
```

After this UPDATE, `DESCRIBE HISTORY` shows a new version with operation `UPDATE`.

#### DELETE

```sql
DELETE FROM employee
WHERE employee_id = 5;
```

With **deletion vectors enabled** (default in Databricks), DELETE does not rewrite
files. Instead, it marks rows as deleted in a lightweight side-file. This makes
deletes much faster. See [Topic 10](10-external-delta-tables-s3.md) for the full
deletion vector deep dive.

#### MERGE (Upsert)

MERGE is the most powerful DML operation. It atomically matches rows between a
source and target, then applies different actions:

```sql
-- Create a source table with updates
CREATE OR REPLACE TABLE employee_updates (
  employee_id INT,
  salary      INT
);

INSERT INTO employee_updates VALUES
  (1, 95000),    -- existing employee: update salary
  (6, 70000);    -- new employee: insert

-- MERGE: update existing, insert new
MERGE INTO employee e
USING employee_updates u
ON e.employee_id = u.employee_id
WHEN MATCHED THEN
  UPDATE SET e.salary = u.salary
WHEN NOT MATCHED THEN
  INSERT (employee_id, salary)
  VALUES (u.employee_id, u.salary);
```

```
  MERGE Logic Flow:
  =================

  Source Row              Target Match?        Action
  ----------              -------------        ------
  (1, 95000)      --->    employee_id=1 found  --> UPDATE salary to 95000
  (6, 70000)      --->    employee_id=6 NOT    --> INSERT new row
                          found
```

### Level 3: Intermediate -- Time Travel

Every DML operation creates a new version. You can query any historical version:

```sql
-- By version number (from DESCRIBE HISTORY output)
SELECT * FROM employee VERSION AS OF 1;

-- By timestamp
SELECT * FROM employee TIMESTAMP AS OF '2026-02-27T20:30:00.000+00:00';
```

```
  Time Travel Versions:
  =====================

  Version 0: CREATE + INSERT (5 rows)
  Version 1: UPDATE (salary +5000 for Engineering)
  Version 2: DELETE (employee_id = 5)
  Version 3: MERGE (update id=1, insert id=6)
  ...

  You can query ANY of these versions at any time
  (as long as the underlying files have not been VACUUMed)
```

### Level 4: Intermediate-Advanced -- Schema Enforcement and Evolution

#### Schema Enforcement (Default Behavior)

Delta Lake **rejects** writes that do not match the table schema. This prevents
data corruption:

```sql
-- This FAILS because 'wrong_salary' is a STRING, not INT
INSERT INTO employee VALUES
  (7, 'Test', 'User', 'Engineering', 'wrong_salary', '2022-06-01');
-- Error: cannot cast STRING to INT
```

#### Schema Evolution

You can safely add new columns without breaking existing data:

```sql
-- Add a new column
ALTER TABLE employee ADD COLUMNS (email STRING);

-- Now insert with the new column
INSERT INTO employee
VALUES (8, 'Tom', 'Hardy', 'Finance', 90000, '2022-06-01', 'tom@email.com');

-- Existing rows have NULL for the new column
```

#### CHECK Constraints

Enforce business rules at the table level:

```sql
-- Add a constraint: salary must be positive
ALTER TABLE employee
ADD CONSTRAINT salary_positive CHECK (salary > 0);

-- This INSERT fails because salary is negative
INSERT INTO employee VALUES
  (9, 'Bad', 'Salary', 'HR', -5000, '2022-07-01', NULL);
-- Error: CHECK constraint salary_positive violated
```

```
  Schema Enforcement Stack:
  =========================

  +---------------------------+
  | CHECK Constraints         |  <-- Business rules (salary > 0)
  +---------------------------+
  | NOT NULL Constraints      |  <-- Required fields
  +---------------------------+
  | Data Type Enforcement     |  <-- INT stays INT, no silent cast
  +---------------------------+
  | Schema Match Validation   |  <-- Column names/types must match
  +---------------------------+
  | Delta Transaction Log     |  <-- Schema stored in metaData action
  +---------------------------+
```

### Level 5: Advanced -- Optimization

#### OPTIMIZE (File Compaction)

Over time, many small DML operations create small files. OPTIMIZE compacts them:

```sql
-- Before: many small files
DESCRIBE DETAIL employee;  -- check numFiles

-- Compact small files into larger ones (target ~1 GB per file)
OPTIMIZE employee;

-- After: fewer, larger files = faster reads
DESCRIBE DETAIL employee;
```

#### Z-ORDER (Data Skipping Enhancement)

Z-ORDER co-locates related data in the same files, dramatically improving query
performance on filtered columns:

```sql
OPTIMIZE employee ZORDER BY (department);
```

```
  Without Z-ORDER:                  With Z-ORDER BY (department):
  ==================                =============================

  File 1: Eng, Mktg, Fin, HR       File 1: Engineering, Engineering
  File 2: Eng, Mktg, Fin           File 2: Marketing, Marketing
  File 3: HR, Eng, Mktg            File 3: Finance, HR

  WHERE department = 'Engineering'  WHERE department = 'Engineering'
  --> Must scan ALL files           --> Scan File 1 ONLY (data skipping)
```

#### VACUUM (Stale File Cleanup)

After OPTIMIZE, UPDATE, or DELETE, old Parquet files remain on disk (needed for
time travel). VACUUM removes files no longer referenced by recent versions:

```sql
-- Default: retain files for 7 days (168 hours)
VACUUM employee;

-- Aggressive: retain 0 hours (CAUTION: breaks time travel!)
-- Requires: SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM employee RETAIN 0 HOURS;
```

```
  VACUUM Decision:
  ================

  File still referenced by       Keep or Remove?
  current table version?
  ----------------------------   ---------------
  YES                            KEEP (active file)
  NO, removed < 7 days ago       KEEP (time travel safety)
  NO, removed >= 7 days ago      REMOVE (stale, reclaimable)

  WARNING: After VACUUM, you CANNOT time travel to versions
  that relied on the removed files!
```

### Level 6: Advanced -- Change Data Feed (CDF)

CDF captures row-level changes for downstream consumers:

```sql
-- Enable CDF on the table
ALTER TABLE employee
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Make a change (this is tracked now)
UPDATE employee
SET salary = salary + 1000
WHERE employee_id = 1;

-- Query the change feed (shows before/after images)
SELECT * FROM table_changes('employee', 17);
-- Returns: _change_type, _commit_version, _commit_timestamp
```

```
  Change Data Feed Output:
  ========================

  _change_type       | employee_id | salary | _commit_version
  -------------------|-------------|--------|----------------
  update_preimage    |      1      | 95000  |       17
  update_postimage   |      1      | 96000  |       17

  Change types:
  - insert           : New row added
  - update_preimage  : Row BEFORE update
  - update_postimage : Row AFTER update
  - delete           : Row removed
```

### Level 7: Advanced -- Python API and Metadata Inspection

#### DeltaTable Python API

```python
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "employee")
dt.history().display(truncate=False)
```

#### Table Properties

```sql
-- All table properties (Delta config, CDF, constraints, etc.)
SHOW TBLPROPERTIES employee;

-- Extended metadata (columns, storage info, table type)
DESCRIBE EXTENDED employee;
```

## Feature Summary: Beginner to Advanced

```
  Level        Feature                  What It Does
  -----        -------                  ------------
  Beginner     CREATE TABLE             Create managed Delta table
               INSERT INTO              Add rows
               SELECT                   Query data
               DESCRIBE DETAIL          Inspect metadata

  Intermediate UPDATE / DELETE           Modify/remove rows
               MERGE INTO               Upsert (update + insert)
               TIME TRAVEL              Query historical versions
               DESCRIBE HISTORY         View full audit trail

  Advanced     Schema Evolution         ADD COLUMNS safely
               CHECK Constraints        Enforce business rules
               OPTIMIZE + Z-ORDER       Compact files, improve skipping
               VACUUM                   Reclaim stale file storage
               Change Data Feed         Track row-level changes
               DeltaTable Python API    Programmatic table operations
```

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Managed Storage | S3 bucket (catalog root) | ADLS Gen2 container | GCS bucket |
| Unity Catalog | Fully supported | Fully supported | Fully supported |
| Default Format | Delta | Delta | Delta |
| OPTIMIZE Engine | Photon recommended | Photon recommended | Photon recommended |

## Certification Tip

The **Databricks Data Engineer Associate** exam heavily tests managed table concepts:

- "What happens when you DROP a managed table?" -- Both data and metadata are deleted
- "What happens when you DROP an external table?" -- Only metadata is removed; data stays
- "How does schema enforcement work?" -- Writes that violate the schema are rejected
- "What does OPTIMIZE do?" -- Compacts small files into larger ones for read performance
- "What does VACUUM do?" -- Removes stale files no longer referenced by the table
- "Can you time travel after VACUUM?" -- Only to versions whose files were retained

Know the difference between managed and external tables cold -- this is a guaranteed
exam question.

## Key Takeaways

1. **Managed tables** are fully governed by Databricks -- DROP TABLE deletes both
   data and metadata. Use them for curated Lakehouse data.
2. Unity Catalog's **three-level namespace** (catalog.schema.table) organizes all
   managed tables with fine-grained access control.
3. Delta's **schema enforcement** prevents bad data from entering tables, while
   **CHECK constraints** enforce business rules.
4. **MERGE** is the workhorse for incremental loads -- it atomically updates
   existing rows and inserts new ones in a single operation.
5. **Time travel** lets you query any historical version, but **VACUUM** removes
   the underlying files, so balance retention needs against storage costs.
6. **OPTIMIZE + Z-ORDER** dramatically improves read performance by compacting
   files and co-locating related data for data skipping.
7. **Change Data Feed** captures row-level changes (insert/update/delete) for
   downstream consumers and audit trails.

## Next Steps

Proceed to [10 -- External Delta Tables on S3 & Deletion Vectors](10-external-delta-tables-s3.md)
to learn how external tables work, how data lives on S3, and how deletion vectors
change UPDATE/DELETE behavior.
