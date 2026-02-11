# Data Modeling Patterns

> Module 04 -- Topic 07 | Level: Intermediate | Time: 55 min

## Learning Objectives

- Understand the difference between managed and external tables in Databricks
- Design star schemas with fact and dimension tables
- Implement Slowly Changing Dimensions (SCD Type 1 and Type 2)
- Apply the medallion architecture (Bronze, Silver, Gold) for progressive data refinement
- Choose appropriate partitioning strategies for tables
- Use DESCRIBE EXTENDED to inspect table metadata

## Conceptual Overview

### Managed vs External Tables

Every table in Databricks has two things: the **data** and the **metadata**.
The key question is: who manages each piece?

```
  +-------------------+------------------------------+
  |                   | MANAGED TABLE                |
  +-------------------+------------------------------+
  | Data location     | Databricks-managed storage   |
  | Metadata          | Unity Catalog / Hive metastore|
  | Who manages both? | Databricks manages BOTH      |
  +-------------------+------------------------------+

  +-------------------+------------------------------+
  |                   | EXTERNAL TABLE               |
  +-------------------+------------------------------+
  | Data location     | S3, ADLS, GCS (you control)  |
  | Metadata          | Unity Catalog / Hive metastore|
  | Who manages data? | YOU manage the data files     |
  | Who manages meta? | Databricks manages metadata   |
  +-------------------+------------------------------+
```

#### What Happens When You DROP a Table?

```
  DROP TABLE managed_table;
  --> Deletes BOTH the metadata AND the data files
  --> Data is gone (unless you have Time Travel / backups)

  DROP TABLE external_table;
  --> Deletes ONLY the metadata from Unity Catalog
  --> The actual data in S3/ADLS is NOT deleted
  --> You can re-register the external table later
```

This is the critical difference: if you delete an external table, it will
delete the metadata but it will NOT delete the actual content which might be
stored in AWS S3 or Azure ADLS.

#### When to Use Each

| Scenario | Recommendation |
|----------|---------------|
| Quick analysis, fully governed data | **Managed table** -- data governance is easy because Unity Catalog controls everything |
| Data already in S3/ADLS for years | **External table** -- point to existing data without moving it |
| Shared across multiple compute engines | **External table** -- Spark, Presto, Trino can all read from S3 |
| Strict data lifecycle control | **Managed table** -- DROP TABLE cleans up everything |
| Cross-team data sharing | **Managed table** -- Unity Catalog handles permissions |

For managed tables, data governance is easy because Unity Catalog controls
everything. When you are doing quick analysis and fully governed data, go for
managed tables. If you already have data stored in S3 or ADLS for years, then
use external tables.

### Inspecting Tables with DESCRIBE EXTENDED

```sql
DESCRIBE EXTENDED my_catalog.my_schema.my_table;
```

This command shows:
- Column names, types, and comments
- Table type (MANAGED or EXTERNAL)
- Data location (DBFS path or external URI)
- Provider (delta, parquet, etc.)
- Table properties (partitioning, Delta features)
- Statistics (row count, size)

### Star Schema

The star schema is the most common analytical data model. It consists of:

- **Fact table**: stores measurable events (sales, clicks, transactions)
- **Dimension tables**: store descriptive attributes (customers, products, time)

```
                    +---------------+
                    |  dim_product  |
                    +-------+-------+
                            |
  +---------------+  +------+--------+  +---------------+
  |  dim_customer |--| fact_sales    |--| dim_date      |
  +---------------+  +------+--------+  +---------------+
                            |
                    +-------+-------+
                    |  dim_store    |
                    +---------------+

  Fact table (center):
    sale_id, customer_key, product_key, store_key, date_key,
    quantity, unit_price, total_amount

  Dimension tables (points of the star):
    dim_customer: customer_key, name, email, segment, city
    dim_product:  product_key, name, category, brand
    dim_store:    store_key, name, region, state
    dim_date:     date_key, date, year, quarter, month, day_of_week
```

Benefits:
- Simple queries (one fact table + N dimension joins)
- Optimized for aggregations (measure columns in fact table)
- Easy for business users to understand

### Slowly Changing Dimensions (SCD)

Dimension data changes over time (customer moves, product gets recategorized).
SCD patterns handle these changes:

#### SCD Type 1 -- Overwrite

Simply update the existing record. History is lost.

```
  BEFORE:  customer_key=1, name="Alice", city="Portland"
  AFTER:   customer_key=1, name="Alice", city="Seattle"   (overwritten)
```

Use when: you do not need history, only the current state matters.

#### SCD Type 2 -- Add New Row with Versioning

Insert a new row and mark the old one as expired. Full history is preserved.

```
  customer_key  name    city       valid_from  valid_to    is_current
  1             Alice   Portland   2020-01-01  2024-06-14  false
  1             Alice   Seattle    2024-06-15  9999-12-31  true
```

Use when: you need full audit history (financial, compliance, analytics).

Implementation with Delta Lake MERGE:

```sql
MERGE INTO dim_customer AS target
USING updates AS source
ON target.customer_key = source.customer_key AND target.is_current = true
WHEN MATCHED AND target.city <> source.city THEN
  UPDATE SET target.is_current = false, target.valid_to = current_date()
WHEN NOT MATCHED THEN
  INSERT (customer_key, name, city, valid_from, valid_to, is_current)
  VALUES (source.customer_key, source.name, source.city, current_date(), '9999-12-31', true);
-- Then insert the new version as a separate statement or CTE
```

### Medallion Architecture

The medallion architecture is a data design pattern used to logically organize
data in a lakehouse. When you are doing this transformation, you have to do it
**incrementally and progressively** to improve the structure and quality of data.

```
  +-------------------+    +-------------------+    +-------------------+
  |     BRONZE        |    |     SILVER        |    |      GOLD         |
  |   (Raw Layer)     |--->|  (Cleaned Layer)  |--->| (Business Layer)  |
  +-------------------+    +-------------------+    +-------------------+
  |                   |    |                   |    |                   |
  | - Raw ingestion   |    | - Deduplicated    |    | - Star schemas    |
  | - Append-only     |    | - Validated       |    | - Aggregated      |
  | - No transforms   |    | - Standardized    |    | - Business metrics|
  | - Full fidelity   |    | - Joined/enriched |    | - Ready for BI    |
  | - Schema on read  |    | - Schema enforced |    | - Curated views   |
  +-------------------+    +-------------------+    +-------------------+

  Data quality improves as it flows left to right:
  ================================================>
  Raw              Validated           Business-ready
```

#### Bronze (Raw)

- Ingest data exactly as received from source systems
- Append-only (never delete or modify raw records)
- Store in Delta format for time travel and schema evolution
- Include metadata columns: ingestion timestamp, source system, batch ID

#### Silver (Cleaned / Conformed)

- Deduplicate records
- Validate and enforce schemas
- Standardize column names, data types, units
- Join with reference data for enrichment
- Apply business rules (e.g., filter test records)

#### Gold (Business / Aggregated)

- Star schemas with fact and dimension tables
- Pre-computed aggregations for dashboards
- Department-specific data marts
- Optimized for query performance (partitioned, Z-ordered)

### Table Partitioning Strategies

Partitioning divides table data into subdirectories by column values:

```sql
CREATE TABLE fact_sales (
    sale_id BIGINT,
    sale_date DATE,
    region STRING,
    amount DOUBLE
)
USING DELTA
PARTITIONED BY (region, sale_date);
```

```
  fact_sales/
  +-- region=East/
  |   +-- sale_date=2024-01-15/
  |   |   +-- part-00000.parquet
  |   +-- sale_date=2024-01-16/
  |       +-- part-00000.parquet
  +-- region=West/
      +-- sale_date=2024-01-15/
          +-- part-00000.parquet
```

Partitioning guidelines:
- Partition by columns used in WHERE filters (date, region)
- Each partition should be at least 1 GB (avoid too many small partitions)
- High-cardinality columns (user_id) are bad partition keys
- For Delta tables, consider Z-ORDER instead of (or in addition to)
  partitioning for multi-column filter patterns

### Naming Conventions

| Layer | Pattern | Example |
|-------|---------|---------|
| Bronze | `bronze_<source>_<entity>` | `bronze_salesforce_contacts` |
| Silver | `silver_<domain>_<entity>` | `silver_crm_customers` |
| Gold | `gold_<domain>_<metric/entity>` | `gold_sales_daily_revenue` |
| Fact | `fact_<event>` | `fact_orders` |
| Dimension | `dim_<entity>` | `dim_customer` |

## Hands-On Walkthrough

Open the companion notebook `07-data-modeling-patterns_notebook.py` which covers:

1. Creating managed vs external tables and comparing behavior
2. Using DESCRIBE EXTENDED to inspect table metadata
3. Building a star schema (fact_sales + dim_customer, dim_product, dim_date)
4. SCD Type 1 implementation with Delta MERGE (overwrite)
5. SCD Type 2 implementation with Delta MERGE (versioned history)
6. Bronze -> Silver -> Gold pipeline with progressive enrichment
7. Table partitioning and Z-ORDER
8. Temporary views for SQL analysis

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| Managed tables | Hive metastore | Unity Catalog | Hive metastore |
| External table data | S3 | ADLS Gen2 / Blob | GCS |
| DESCRIBE EXTENDED | All Spark | All DBR | All Spark |
| Delta MERGE (SCD) | Delta Lake OSS | Built-in | Delta Lake OSS |
| Unity Catalog governance | N/A | Full support | N/A |
| Z-ORDER | Delta Lake OSS 2.0+ | Built-in | Delta Lake OSS 2.0+ |

## Certification Tip

Data modeling questions are common on the Databricks exam:
- Know the difference between managed and external tables (what happens on DROP)
- Understand that Unity Catalog manages metadata for both table types
- Be able to identify Bronze/Silver/Gold layers from a description
- Know that SCD Type 2 preserves full history with versioning columns
- Understand that DESCRIBE EXTENDED shows table type and location
- Know when to partition vs when to Z-ORDER

## Key Takeaways

1. **Managed tables**: Databricks manages both data and metadata. DROP deletes
   everything. Best for governed, self-contained analytics.
2. **External tables**: you manage data in S3/ADLS; Databricks manages metadata
   only. DROP removes metadata but not data files.
3. **Star schema**: fact table at center, dimension tables around it. Optimized
   for analytical queries.
4. **SCD Type 1**: overwrite -- no history. **SCD Type 2**: new row with
   versioning -- full history preserved.
5. **Medallion architecture**: Bronze (raw) -> Silver (cleaned) -> Gold
   (business-ready). Transform incrementally and progressively.
6. Partition by low-cardinality filter columns. Use Z-ORDER for high-cardinality.
7. Use DESCRIBE EXTENDED to inspect table type, location, and properties.
8. Naming conventions (bronze_, silver_, gold_, fact_, dim_) keep the
   lakehouse organized.

## Next Steps

You have completed Module 04. Proceed to **Module 05 -- Performance Tuning &
Optimization** to learn how to make your transformations run faster at scale.
