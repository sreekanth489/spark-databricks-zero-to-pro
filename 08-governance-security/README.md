# Module 08 — Governance & Security

> Build enterprise-grade data governance using Unity Catalog, access controls,
> lineage tracking, row/column security, Delta Sharing, and Lakehouse Federation.

---

## Why This Module Matters

Data governance is no longer optional. Regulations like GDPR, CCPA, and HIPAA
demand that organizations know where sensitive data lives, who can access it, and
how it flows through pipelines. Databricks Unity Catalog provides a single control
plane that answers all three questions — and this module teaches you how to use it.

Whether you are preparing for the Databricks Certified Data Engineer Associate
exam or building production governance frameworks, this module gives you the
practical skills and conceptual depth you need.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| **Modules 00-06 completed** | You need working knowledge of Spark SQL, Delta Lake, and the Lakehouse architecture |
| **Databricks workspace (recommended)** | Unity Catalog features require a full Databricks workspace — Community Edition has limited support |
| **Basic SQL knowledge** | GRANT/REVOKE, CREATE, ALTER statements are used throughout |

> **Community Edition users**: Every notebook includes simulated alternatives so
> you can follow along without a full workspace. Look for the "Community Edition
> Alternative" sections in each notebook.

---

## Table of Contents

| # | Topic | Guide | Notebook | Time | Level |
|---|-------|-------|----------|------|-------|
| 01 | Unity Catalog Deep Dive | [Guide](01-unity-catalog-deep-dive.md) | [Notebook](01-unity-catalog-deep-dive_notebook.py) | 45 min | Intermediate |
| 02 | Access Control & Permissions | [Guide](02-access-control.md) | [Notebook](02-access-control_notebook.py) | 40 min | Intermediate |
| 03 | Data Lineage | [Guide](03-data-lineage.md) | [Notebook](03-data-lineage_notebook.py) | 35 min | Intermediate |
| 04 | Row & Column Security | [Guide](04-row-column-security.md) | [Notebook](04-row-column-security_notebook.py) | 45 min | Advanced |
| 05 | Delta Sharing | [Guide](05-delta-sharing.md) | [Notebook](05-delta-sharing_notebook.py) | 40 min | Advanced |
| 06 | Lakehouse Federation | [Guide](06-lakehouse-federation.md) | [Notebook](06-lakehouse-federation_notebook.py) | 35 min | Advanced |

**Total estimated time: ~4 hours**

---

## Learning Path

```
  Module 08 Learning Flow
  ========================

  01-Unity Catalog Deep Dive
    |
    |  Understand the three-level namespace, metastore architecture,
    |  and how Unity Catalog replaces Hive metastore.
    |
    v
  02-Access Control & Permissions
    |
    |  Learn GRANT/REVOKE, privilege hierarchy, ownership,
    |  and identity management.
    |
    v
  03-Data Lineage
    |
    |  See how Unity Catalog automatically captures table-level
    |  and column-level lineage across all workloads.
    |
    v
  04-Row & Column Security
    |
    |  Apply fine-grained access with dynamic views, row filters,
    |  and column masking functions.
    |
    v
  05-Delta Sharing
    |
    |  Share data across organizations and clouds using the open
    |  Delta Sharing protocol.
    |
    v
  06-Lakehouse Federation
    |
    |  Query external databases (PostgreSQL, MySQL, Snowflake,
    |  BigQuery) without copying data.
```

---

## Key Concepts at a Glance

- **Unity Catalog** — Centralized governance for all data and AI assets across
  workspaces, with a single metastore at the account level.
- **Three-Level Namespace** — `catalog.schema.table` replaces the legacy
  `database.table` model, enabling environment separation (dev, staging, prod).
- **Securable Hierarchy** — METASTORE > CATALOG > SCHEMA > TABLE/VIEW/FUNCTION/VOLUME.
  Privileges cascade downward.
- **Row Filters & Column Masks** — Native Unity Catalog features for fine-grained
  security without creating separate views.
- **Delta Sharing** — Open protocol for sharing data across organizations without
  copying, regardless of cloud or platform.
- **Lakehouse Federation** — Query external databases through Unity Catalog without
  ETL, using Spark SQL pushdown optimization.

---

## Important Notes

1. **Unity Catalog requires a full Databricks workspace** — most governance features
   are not available in Databricks Community Edition. Notebooks include simulated
   output and Hive metastore alternatives where possible.

2. **Account-level admin access** is needed to create metastores and manage
   account-level groups. Workspace admins can manage workspace-level objects.

3. **Cloud provider differences** exist for storage credentials and external
   locations. Each guide includes an AWS / Azure / GCP comparison table.

4. **Certification relevance** — Unity Catalog, access control, and lineage are
   heavily tested on the Databricks Certified Data Engineer Associate and
   Professional exams. Look for "Certification Tip" sections in each guide.

---

## Next Steps

After completing this module, proceed to:
- **Module 09** — Performance Tuning & Optimization
- **Module 10** — Production Pipelines & CI/CD
