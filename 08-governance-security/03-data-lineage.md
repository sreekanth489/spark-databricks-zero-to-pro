# Data Lineage
> Module 08 — Topic 03 | Level: Intermediate | Time: 35 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Explain what data lineage captures and why it matters.
2. Describe how Unity Catalog automatically tracks table-level and column-level
   lineage.
3. Use the Unity Catalog UI to visualize lineage graphs.
4. Query lineage metadata through the Lineage API and INFORMATION_SCHEMA.
5. Apply lineage for impact analysis, compliance (GDPR/CCPA), and debugging.
6. Understand how lineage works across notebooks, jobs, and SQL queries.

---

## Conceptual Overview

### What Is Data Lineage?

Data lineage is the record of how data moves through your systems — where it
originates, how it is transformed, and where it ends up. It answers three
fundamental questions:

1. **Where did this data come from?** (upstream lineage)
2. **What transformations were applied?** (processing history)
3. **What depends on this data?** (downstream lineage / impact analysis)

```
  Data Lineage Example:
  =====================

  [raw_events]        [user_profiles]
       |                     |
       +--------+  +---------+
                |  |
                v  v
          [enriched_events]
                |
       +--------+---------+
       |                  |
       v                  v
  [daily_summary]   [user_activity_report]
       |
       v
  [executive_dashboard]
```

### Table-Level vs Column-Level Lineage

Unity Catalog tracks lineage at two granularities:

**Table-level lineage** records which tables are read and written by each
operation. This tells you "Table A feeds into Table B."

**Column-level lineage** goes deeper — it records which specific columns in the
source tables contribute to which columns in the target table.

```
  Column-Level Lineage Example:
  =============================

  Source: raw_orders                 Target: order_summary
  +-----------------+               +-------------------+
  | order_id        |----+--------->| order_id          |
  | customer_id     |----|---+----->| customer_id       |
  | item_price      |----|---|--+   | total_amount      | <-- SUM(item_price)
  | quantity         |----|---|--+-->|                   |
  | order_date       |----|-+-+-+-->| order_month       | <-- DATE_TRUNC('month', order_date)
  +-----------------+    | | | |   +-------------------+
                         | | | |
  Source: customers      | | | |
  +-----------------+    | | | |
  | customer_id     |----+ | | |
  | customer_name   |------|-|---->| customer_name     |
  | region          |------|-|---->| region            |
  +-----------------+      | |     +-------------------+
```

### How Unity Catalog Captures Lineage

Lineage capture in Unity Catalog is **automatic** — there is no instrumentation
or configuration required. It works by analyzing the query plans of Spark jobs.

```
  Lineage Capture Flow:
  =====================

  User runs query/notebook/job
        |
        v
  Spark creates query plan
        |
        v
  Unity Catalog analyzes the plan
        |
        +-- Identifies source tables/columns (reads)
        +-- Identifies target tables/columns (writes)
        +-- Records the transformation relationship
        |
        v
  Lineage stored in Unity Catalog metadata
        |
        v
  Available via UI, API, and INFORMATION_SCHEMA
```

Lineage is captured from:
- **Notebooks** — interactive Spark SQL and PySpark operations
- **Jobs** — scheduled Spark jobs and workflows
- **Delta Live Tables** — DLT pipeline transformations
- **SQL Queries** — queries run in the SQL warehouse
- **Structured Streaming** — streaming write operations

### Lineage in the Unity Catalog UI

The Unity Catalog UI provides an interactive lineage graph for every table,
view, and column:

```
  Unity Catalog UI — Lineage Tab:
  ===============================

  +--------------------------------------------------+
  |  Table: prod.sales.order_summary                 |
  |  [Overview] [Schema] [Sample Data] [Lineage]     |
  +--------------------------------------------------+
  |                                                   |
  |  Upstream (sources)          Downstream (targets) |
  |                                                   |
  |  [raw_orders] ---+                                |
  |                  +--> [order_summary] --+--> [dashboard_view] |
  |  [customers] ---+                      |    |
  |                                        +--> [monthly_report]  |
  |                                             |
  +--------------------------------------------------+
  |  Click any table to see column-level detail       |
  +--------------------------------------------------+
```

### The Lineage REST API

For programmatic access, Unity Catalog provides a REST API:

```
GET /api/2.1/unity-catalog/lineage/table-lineage
  ?table_name=prod.sales.order_summary
  &include_entity_lineage=true

Response:
{
  "upstreams": [
    {"table_info": {"name": "raw_orders", ...}},
    {"table_info": {"name": "customers", ...}}
  ],
  "downstreams": [
    {"table_info": {"name": "dashboard_view", ...}},
    {"table_info": {"name": "monthly_report", ...}}
  ]
}

GET /api/2.1/unity-catalog/lineage/column-lineage
  ?table_name=prod.sales.order_summary
  &column_name=total_amount

Response:
{
  "upstream_cols": [
    {"table_name": "raw_orders", "name": "item_price"},
    {"table_name": "raw_orders", "name": "quantity"}
  ]
}
```

### Lineage for Compliance (GDPR / CCPA)

Data lineage is essential for privacy compliance. When a user exercises their
"right to be forgotten" under GDPR, you need to know every table where their
data exists.

```
  GDPR "Right to Erasure" Workflow:
  =================================

  1. User requests deletion of their data
  2. Query lineage for the source table containing user data
  3. Identify ALL downstream tables that contain derived user data
  4. Delete/anonymize user data from every table in the lineage graph
  5. Verify deletion using the lineage trail as an audit checklist
```

Column-level lineage is particularly valuable because it tells you exactly which
columns in downstream tables were derived from the user's personal information.

### Lineage for Impact Analysis

Before modifying a table schema, lineage tells you what will break:

```
  Impact Analysis Example:
  ========================

  Question: "What happens if I drop the 'region' column from customers?"

  Lineage shows downstream dependencies:
    customers.region
        |
        +--> order_summary.region
        |      |
        |      +--> regional_dashboard
        |      +--> monthly_sales_by_region
        |
        +--> customer_segments.region
               |
               +--> marketing_targets

  Impact: 5 downstream objects would break.
  Action: Update all downstream objects before dropping the column.
```

---

## Hands-On Walkthrough

Open the companion notebook `03-data-lineage_notebook.py` and follow along.
The notebook covers:

1. Creating a multi-step transformation pipeline
2. How Unity Catalog tracks lineage at each step
3. Querying INFORMATION_SCHEMA for metadata
4. Simulated lineage output for Community Edition
5. Impact analysis demonstration

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Lineage UI | Available in workspace UI | Available in workspace UI | Available in workspace UI |
| Lineage API | REST API via workspace URL | REST API via workspace URL | REST API via workspace URL |
| Lineage retention | 1 year by default | 1 year by default | 1 year by default |
| Cross-workspace lineage | Tracked via shared metastore | Tracked via shared metastore | Tracked via shared metastore |
| System tables for lineage | `system.access.table_lineage` | `system.access.table_lineage` | `system.access.table_lineage` |

---

## Certification Tip

For the Databricks Certified Data Engineer Associate exam, know that:

- Lineage in Unity Catalog is **automatic** — no manual setup required
- Both **table-level** and **column-level** lineage are tracked
- Lineage is captured from notebooks, jobs, DLT pipelines, and SQL queries
- The lineage graph is accessible from the **Lineage tab** in the Unity Catalog UI
- Lineage helps with **impact analysis** (what breaks if I change this table?)
  and **compliance** (where does PII flow?)
- Lineage information is available via the **REST API** and **system tables**

---

## Key Takeaways

1. **Data lineage** records where data comes from, how it is transformed, and
   where it goes — answering "What happens if this changes?"
2. Unity Catalog captures lineage **automatically** by analyzing Spark query plans.
3. **Table-level lineage** shows which tables read/write to each other.
4. **Column-level lineage** traces individual columns through transformations.
5. Lineage is critical for **GDPR/CCPA compliance** (tracking PII flow) and
   **impact analysis** (understanding change consequences).
6. The **Lineage UI**, **REST API**, and **system tables** provide access to
   lineage data.
7. Lineage spans all workloads: notebooks, jobs, DLT, SQL, and streaming.

---

## Next Steps

Proceed to [04 — Row & Column Security](04-row-column-security.md) to learn
how to apply fine-grained data access controls with dynamic views, row filters,
and column masking.
