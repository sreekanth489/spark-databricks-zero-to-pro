# Unity Catalog Fundamentals
> Module: Data Governance | Day 10 | Level: Beginner | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain what Unity Catalog is and why it replaces the Hive Metastore
- Navigate the 3-level namespace: Catalog → Schema → Table
- Distinguish between Unity Catalog metastore and Hive Metastore
- Understand managed vs external storage in Unity Catalog
- Create and manage catalogs, schemas, tables, and views
- Use Unity Catalog Volumes for file governance
- Explore data lineage and search capabilities

---

## Conceptual Overview

### What is Unity Catalog?

**Unity Catalog** is Databricks' centralized governance solution for all data and AI assets across every workspace and cloud. It provides a single place to manage permissions, discover data, and track lineage -- replacing the workspace-scoped Hive Metastore with an account-level governance layer.

```
   Before Unity Catalog                    With Unity Catalog
  ┌──────────────────────┐          ┌──────────────────────────────┐
  │  Workspace A         │          │       Account Console        │
  │  ┌────────────────┐  │          │  ┌────────────────────────┐  │
  │  │ Hive Metastore │  │          │  │   Unity Catalog        │  │
  │  │ (local users)  │  │          │  │   Metastore            │  │
  │  │ (local ACLs)   │  │          │  │                        │  │
  │  └────────────────┘  │          │  │  Users & Groups        │  │
  └──────────────────────┘          │  │  (account-level)       │  │
  ┌──────────────────────┐          │  │                        │  │
  │  Workspace B         │          │  │  Shared ACLs           │  │
  │  ┌────────────────┐  │          │  │  (cross-workspace)     │  │
  │  │ Hive Metastore │  │          │  └────────────────────────┘  │
  │  │ (separate)     │  │          │         │           │        │
  │  │ (separate ACLs)│  │          │    Workspace A   Workspace B │
  │  └────────────────┘  │          │    (same metastore assigned) │
  └──────────────────────┘          └──────────────────────────────┘
```

**Key Insight**: Before Unity Catalog, each workspace had its own Hive Metastore with separate users, groups, and access controls. Unity Catalog moves governance to the **account level**, so you define access rules once and they apply across all workspaces.

### Why Unity Catalog?

| Problem (Hive Metastore) | Solution (Unity Catalog) |
|--------------------------|--------------------------|
| Users & groups per workspace | Account-level identity, assigned to workspaces |
| ACLs scoped to one workspace | Cross-workspace, cross-cloud governance |
| No file/volume governance | Volumes for governed file access |
| No data lineage | Automated lineage across tables, notebooks, jobs |
| No built-in data search | Built-in search and discovery |
| Tables and views only | Governs tables, views, volumes, ML models, functions |

---

## The 3-Level Namespace

### Hive Metastore: 2-Level

```sql
SELECT * FROM schema.table
-- e.g., SELECT * FROM hr_db.employees
```

### Unity Catalog: 3-Level

```sql
SELECT * FROM catalog.schema.table
-- e.g., SELECT * FROM prod_catalog.hr_db.employees
```

```
Unity Catalog Hierarchy
━━━━━━━━━━━━━━━━━━━━━━

  Metastore (top-level container, assigned to workspaces)
  │
  ├── Catalog: prod_catalog
  │   ├── Schema: hr_db
  │   │   ├── Table: employees
  │   │   ├── Table: departments
  │   │   ├── View: active_employees_vw
  │   │   └── Function: mask_ssn()
  │   ├── Schema: finance_db
  │   │   ├── Table: transactions
  │   │   └── Table: accounts
  │   └── Schema: information_schema (auto-created)
  │
  ├── Catalog: dev_catalog
  │   └── Schema: sandbox
  │       └── Table: experiments
  │
  └── Catalog: hive_metastore (legacy, always available)
      └── Schema: default
          └── Table: old_table
```

**Catalogs** form the first level and are the top-level container for organizing data. Think of them as logical groupings -- by environment (`dev`, `staging`, `prod`), by business unit (`marketing`, `engineering`), or by project.

**Schemas** (also called databases) sit within catalogs and group related tables, views, and functions.

**Tables, Views, Functions, Volumes** are the data objects that live inside schemas.

---

## Metastore Deep Dive

The **metastore** is the top-level logical container in Unity Catalog. It stores:
- Metadata about all data objects (catalogs, schemas, tables, etc.)
- Access control lists (ACLs) governing who can access what
- Audit logs of all access and changes

```
┌──────────────────────────────────────────────────────┐
│                   UC Metastore                        │
│                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐ │
│  │  Metadata   │  │  ACLs       │  │  Audit Logs  │ │
│  │  (catalogs, │  │  (who can   │  │  (who did    │ │
│  │   schemas,  │  │   access    │  │   what,      │ │
│  │   tables)   │  │   what)     │  │   when)      │ │
│  └─────────────┘  └─────────────┘  └──────────────┘ │
│                                                       │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Managed Storage Location                        │ │
│  │  (S3 bucket / ADLS container / GCS bucket)       │ │
│  │  Where managed table data lives                  │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
         │                     │
    Workspace A           Workspace B
    (assigned)            (assigned)
```

### Key Metastore Facts

- One metastore per **region** (e.g., one for `us-east-1`, one for `eu-west-1`)
- A metastore can be assigned to **multiple workspaces** in the same region
- Multiple workspaces sharing a metastore see the same catalogs, schemas, and tables
- The `hive_metastore` catalog always provides access to the legacy Hive Metastore local to that workspace

### Metastore vs Hive Metastore

| Aspect | Hive Metastore | Unity Catalog Metastore |
|--------|---------------|------------------------|
| Scope | Per workspace | Per region, multi-workspace |
| Identity | Workspace-local users/groups | Account-level identity federation |
| Governance | Basic ACLs on tables/views | Fine-grained on all asset types |
| Lineage | None | Automated |
| Search | None | Built-in discovery |
| File governance | None | Volumes |

---

## Managed Storage

When you create a **managed table** in Unity Catalog, Databricks controls the storage location. The data lives in the metastore's managed storage (an S3 bucket, ADLS container, or GCS bucket that was configured during metastore setup).

```
Managed Storage Hierarchy
━━━━━━━━━━━━━━━━━━━━━━━━

  Metastore Default Storage (s3://uc-metastore-bucket/)
  │
  ├── Catalog-level storage override (optional)
  │   └── s3://prod-catalog-bucket/
  │
  └── Schema-level storage override (optional)
      └── s3://hr-schema-bucket/
```

You can override the default storage at the **catalog** or **schema** level, giving teams their own isolated buckets while maintaining centralized governance.

For **managed tables**, Unity Catalog manages the lifecycle -- dropping the table deletes the data. Users never need to know the physical path.

For **external tables**, you specify a `LOCATION` pointing to existing files. Unity Catalog manages metadata and access control, but dropping the table does not delete the data.

---

## Unity Catalog Volumes

**Volumes** are a Unity Catalog object for governing access to non-tabular files (CSVs, images, PDFs, model artifacts, etc.).

```
Schema
├── Tables      (structured data in Delta/Parquet)
├── Views       (virtual tables)
├── Functions   (UDFs)
└── Volumes     (files -- CSVs, images, JARs, etc.)
    ├── Managed Volume  (UC controls storage)
    └── External Volume (you control storage location)
```

Volumes provide a centralized, governed platform for all your files -- following the same permission model as tables.

```sql
-- Create a managed volume
CREATE VOLUME my_catalog.my_schema.raw_files;

-- List files in a volume
LIST '/Volumes/my_catalog/my_schema/raw_files/';

-- Read files from a volume
SELECT * FROM read_files('/Volumes/my_catalog/my_schema/raw_files/data.csv');
```

---

## Data Lineage

Unity Catalog automatically tracks **lineage** -- the origin of your data and where it flows. This works across:
- Tables and views
- Notebooks and jobs
- Dashboards
- ML models

```
  Source Table A ──┐
                   ├──▶ Silver Table ──▶ Gold Table ──▶ Dashboard
  Source Table B ──┘         │
                             ▼
                        ML Model (Feature Store)
```

Lineage is captured automatically -- no configuration required. You can view it in the Catalog Explorer UI.

---

## Accessing Legacy Hive Metastore

Unity Catalog is **additive** -- enabling it does not break existing Hive Metastore tables. The catalog named `hive_metastore` always provides access to the legacy Hive Metastore local to each workspace.

```sql
-- Access legacy tables (2-level namespace still works via hive_metastore)
SELECT * FROM hive_metastore.default.old_table

-- Access Unity Catalog tables (3-level namespace)
SELECT * FROM prod_catalog.hr_db.employees
```

No hard migration is required. You can gradually move tables from `hive_metastore` to Unity Catalog catalogs.

---

## Key Features Summary

| Feature | Description |
|---------|-------------|
| Centralized governance | One place for all data and AI asset permissions |
| 3-level namespace | Catalog → Schema → Table/View/Volume/Function |
| Cross-workspace | Define once, enforce everywhere |
| Identity federation | Account-level users/groups assigned to workspaces |
| Volumes | Governed file access for non-tabular data |
| Automated lineage | Track data origin and flow automatically |
| Built-in search | Discover tables, columns, and descriptions |
| Audit logging | Full trail of who accessed what, when |
| No hard migration | Legacy hive_metastore remains accessible |

---

## Hands-On Walkthrough

See the companion notebook: [`10-unity-catalog-fundamentals_notebook.py`](10-unity-catalog-fundamentals_notebook.py)

The lab covers:
1. Creating catalogs and schemas
2. Creating managed tables in Unity Catalog
3. Exploring the 3-level namespace
4. Working with Volumes for file governance
5. Exploring table metadata and history
6. Viewing data lineage
7. Accessing legacy hive_metastore tables

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Metastore storage | S3 bucket | ADLS Gen2 container | GCS bucket |
| Account console URL | accounts.cloud.databricks.com | accounts.azuredatabricks.net | accounts.gcp.databricks.com |
| Identity provider | AWS IAM + SCIM | Azure AD (Entra ID) + SCIM | Google Identity + SCIM |
| Storage credential | IAM Role | Managed Identity / Service Principal | Service Account |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- The 3-level namespace: `catalog.schema.table`
- Difference between Unity Catalog metastore and Hive Metastore
- Managed vs external tables in Unity Catalog
- How metastores are assigned to workspaces
- Identity federation: account-level vs workspace-level identities
- Volumes for file governance

---

## Key Takeaways

1. **Unity Catalog** is an account-level governance solution replacing workspace-scoped Hive Metastore
2. The **3-level namespace** (catalog.schema.table) adds catalogs as the top organizational layer
3. A **metastore** is regional and can be assigned to multiple workspaces
4. **Managed tables** have lifecycle controlled by UC; **external tables** reference existing data
5. **Volumes** extend governance to non-tabular files (CSVs, images, JARs)
6. **Lineage** is automatic -- no configuration needed
7. **Legacy Hive Metastore** remains accessible via the `hive_metastore` catalog

---

## Next Steps

- [Day 11: Unity Catalog Security](../day11-unity-catalog-security/) -- RBAC, privileges, row-level security, column masking
- [Day 12: Managed vs External Tables](../day12-managed-vs-external-tables/) -- deep dive into table types
- [Day 13: Volumes in Databricks](../day13-volumes-in-databricks/) -- governed file access
