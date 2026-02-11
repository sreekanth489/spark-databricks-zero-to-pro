# Unity Catalog Deep Dive
> Module 08 — Topic 01 | Level: Intermediate | Time: 45 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Explain the three-level namespace (`catalog.schema.table`) and why it replaced
   the legacy two-level model.
2. Describe the Unity Catalog metastore architecture and how it operates at the
   account level.
3. Create catalogs, schemas, and tables using SQL DDL commands.
4. Differentiate between managed and external tables in Unity Catalog.
5. Configure storage credentials and external locations.
6. Plan a migration from Hive metastore to Unity Catalog.
7. Use Volumes for managing non-tabular data (files, images, models).

---

## Conceptual Overview

### What Is Unity Catalog?

Unity Catalog is Databricks' centralized governance solution for all data and AI
assets. It provides a single control plane that spans every workspace in your
Databricks account. Before Unity Catalog, each workspace had its own Hive
metastore, which created governance silos.

```
  BEFORE: Siloed Hive Metastores          AFTER: Unified Governance
  ================================        ================================

  +-----------+   +-----------+           +---------------------------+
  | Workspace |   | Workspace |           |    Unity Catalog          |
  |     A     |   |     B     |           |    (Account-Level)        |
  | +-------+ |   | +-------+ |           |                           |
  | | Hive  | |   | | Hive  | |           |  +---------+---------+   |
  | | Meta  | |   | | Meta  | |           |  | WS - A  | WS - B  |   |
  | +-------+ |   | +-------+ |           |  +---------+---------+   |
  +-----------+   +-----------+           +---------------------------+
  No cross-workspace visibility           Single source of truth
```

### The Three-Level Namespace

Unity Catalog introduces a three-level namespace that brings structure and
environment isolation to your data:

```
  catalog.schema.table

  Example: prod.sales.transactions
           ^^^^  ^^^^^  ^^^^^^^^^^^^
           |     |      |
           |     |      +-- Table / View / Function / Volume
           |     +--------- Schema (logical grouping, like a database)
           +--------------- Catalog (top-level container, like an environment)
```

**Why three levels?**

| Level | Purpose | Analogy |
|-------|---------|---------|
| Catalog | Environment or domain separation | A filing cabinet |
| Schema | Logical grouping of related objects | A drawer in the cabinet |
| Table | The actual data asset | A file in the drawer |

Common catalog naming patterns:

- **By environment**: `dev`, `staging`, `prod`
- **By domain**: `finance`, `marketing`, `engineering`
- **By team**: `data_science`, `analytics`, `platform`
- **Combined**: `prod_finance`, `dev_marketing`

### Metastore Architecture

The Unity Catalog metastore sits at the **account level**, not the workspace
level. This is a fundamental architectural difference from Hive metastore.

```
  +-----------------------------------------------+
  |            Databricks Account                  |
  |                                                |
  |  +------------------------------------------+  |
  |  |         Unity Catalog Metastore          |  |
  |  |                                          |  |
  |  |  +----------+  +----------+  +--------+  |  |
  |  |  | Catalog  |  | Catalog  |  | Catalog|  |  |
  |  |  |  "dev"   |  | "staging"|  | "prod" |  |  |
  |  |  +----------+  +----------+  +--------+  |  |
  |  |                                          |  |
  |  |  Storage     External    Access          |  |
  |  |  Credentials Locations   Policies        |  |
  |  +------------------------------------------+  |
  |                                                |
  |  +-----------+  +-----------+  +-----------+   |
  |  | Workspace |  | Workspace |  | Workspace |   |
  |  |     A     |  |     B     |  |     C     |   |
  |  +-----------+  +-----------+  +-----------+   |
  +-----------------------------------------------+
```

Key points:
- **One metastore per region** — a metastore is created per cloud region.
- **Multiple workspaces share one metastore** — enabling cross-workspace governance.
- **Account admins** create and manage metastores; workspace admins manage
  workspace-level objects.

### Managed vs External Tables

Unity Catalog supports two table types:

| Feature | Managed Table | External Table |
|---------|---------------|----------------|
| Storage location | Metastore's managed storage | User-specified cloud path |
| Lifecycle | Dropping table deletes data | Dropping table removes metadata only |
| Use case | Standard analytics tables | Data shared with non-Databricks tools |
| Creation | `CREATE TABLE t (...)` | `CREATE TABLE t (...) LOCATION 's3://...'` |

### Storage Credentials and External Locations

Unity Catalog centralizes cloud storage access through two objects:

```
  +---------------------+          +----------------------+
  | Storage Credential  |          | External Location    |
  |                     |          |                      |
  | IAM Role (AWS)      |---used-->| s3://bucket/path/    |
  | Service Principal   |   by     | abfss://container@.. |
  | (Azure)             |          | gs://bucket/path/    |
  | Service Account     |          |                      |
  | (GCP)               |          |                      |
  +---------------------+          +----------------------+
```

- **Storage Credential** — wraps a cloud IAM identity (AWS IAM role, Azure service
  principal, or GCP service account).
- **External Location** — maps a cloud storage path to a storage credential,
  allowing Unity Catalog to govern access to files at that path.

### Volumes: Non-Tabular Data

Volumes are Unity Catalog's solution for governing non-tabular data — files,
images, ML models, JARs, and any other artifacts.

```
  catalog.schema.volume

  Types:
  +-------------------+    +-------------------+
  | Managed Volume    |    | External Volume   |
  | (UC manages path) |    | (user-specified)  |
  +-------------------+    +-------------------+
```

Volumes follow the same three-level namespace and the same permission model as
tables. You can `GRANT READ VOLUME` or `GRANT WRITE VOLUME` on them.

### Hive Metastore to Unity Catalog Migration

For organizations already using Databricks with Hive metastore, migration follows
this general path:

```
  Migration Steps:
  ================
  1. Create Unity Catalog metastore for your region
  2. Assign metastore to workspaces
  3. Create catalogs and schemas in Unity Catalog
  4. Use SYNC command or CTAS to migrate tables
  5. Update notebooks/jobs to use three-level names
  6. Set up access controls (GRANT/REVOKE)
  7. Deprecate hive_metastore references
```

The legacy `hive_metastore` catalog remains accessible during migration, providing
a bridge between old and new:

```sql
-- Old (Hive metastore)
SELECT * FROM my_database.my_table;

-- New (Unity Catalog)
SELECT * FROM prod.my_database.my_table;

-- Accessing legacy catalog explicitly
SELECT * FROM hive_metastore.my_database.my_table;
```

---

## Hands-On Walkthrough

Open the companion notebook `01-unity-catalog-deep-dive_notebook.py` and follow
along. The notebook covers:

1. Creating catalogs, schemas, and tables with SQL DDL
2. Navigating the three-level namespace with SHOW and DESCRIBE commands
3. Working with managed vs external tables
4. Creating and managing Volumes
5. Exploring metastore metadata
6. Community Edition alternatives using Hive metastore

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Storage Credential | IAM Role (cross-account) | Service Principal or Managed Identity | Service Account |
| Managed Storage | S3 bucket | ADLS Gen2 container | GCS bucket |
| External Location | `s3://bucket/path` | `abfss://container@account.dfs.core.windows.net/path` | `gs://bucket/path` |
| Metastore Region | One per AWS region | One per Azure region | One per GCP region |
| Identity Federation | AWS IAM Identity Center | Azure Active Directory (Entra ID) | Google Cloud Identity |

---

## Certification Tip

Unity Catalog is a **major topic** on the Databricks Certified Data Engineer
Associate exam. You should know:

- The three-level namespace and how to reference objects fully qualified
- The difference between managed and external tables in Unity Catalog
- How metastores are assigned to workspaces (account-level, one per region)
- That dropping a managed table deletes data; dropping an external table does not
- Storage credentials and external locations are how UC governs cloud storage
- The `hive_metastore` catalog provides backward compatibility during migration

---

## Key Takeaways

1. **Unity Catalog is the single governance layer** for all data and AI assets in
   Databricks — it replaces per-workspace Hive metastores.
2. **Three-level namespace** (`catalog.schema.table`) enables environment isolation
   and domain-driven organization.
3. **Metastore is account-level** — one per region, shared across workspaces.
4. **Managed tables** have their lifecycle controlled by Unity Catalog; **external
   tables** only register metadata.
5. **Storage credentials** and **external locations** centralize cloud storage
   access governance.
6. **Volumes** extend Unity Catalog governance to non-tabular data.
7. **Migration** from Hive metastore is incremental — the `hive_metastore` catalog
   provides a bridge.

---

## Next Steps

Proceed to [02 — Access Control & Permissions](02-access-control.md) to learn how
to secure your Unity Catalog objects with GRANT/REVOKE and the privilege hierarchy.
