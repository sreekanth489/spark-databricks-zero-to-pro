# Day 10: Unity Catalog Fundamentals

> Module: Data Governance | Level: Beginner-Intermediate | Time: 120 min

## Learning Objectives

- Understand what Unity Catalog is and WHY it replaces the Hive Metastore (before/after comparison)
- Navigate the 3-level namespace: Catalog → Schema → Table
- Create a Unity Catalog metastore on Azure (step-by-step)
- Understand account-level admin roles: Account Admin, Metastore Admin, Workspace Admin
- Create and manage catalogs, schemas, tables, views, and volumes
- Explore managed storage, data lineage, system tables, and search capabilities
- Access legacy hive_metastore tables alongside Unity Catalog

## Key Concepts

- **Unity Catalog** — Databricks' centralized governance solution for all data and AI assets across workspaces
- **Before UC** — each workspace had its own isolated Hive Metastore (no sharing, no cross-workspace governance)
- **3-Level Namespace** — `catalog.schema.table` replaces the 2-level Hive namespace
- **Metastore** — top-level container; one per region; assigned to multiple workspaces
- **Account Admin** — creates metastores, manages account-level identities
- **Metastore Admin** — manages catalogs, storage credentials, external locations
- **Managed Tables** — UC controls storage lifecycle; DROP deletes data
- **External Tables** — you specify LOCATION; DROP keeps data
- **Volumes** — governed access to non-tabular files (CSVs, images, JARs)
- **Identity Federation** — account-level users/groups (SCIM-synced from Azure AD/Okta)
- **Automated Lineage** — tracks data origin and flow across all asset types
- **System Tables** — queryable audit logs, lineage, usage data in `system.*` catalog

## Topics Covered

- Before/after Unity Catalog comparison
- Unity Catalog architecture and motivation
- Azure metastore creation: ADLS Gen2, Access Connector, Account Console
- Admin roles hierarchy: Account Admin → Metastore Admin → Workspace Admin
- 3-level namespace: Catalog → Schema → Table/View/Volume/Function
- Identity federation with Azure AD (Entra ID) via SCIM
- Managed vs external storage at metastore, catalog, and schema levels
- Volumes for file governance (vs raw ADLS/S3)
- Data lineage and search in Catalog Explorer
- System tables: audit logs, lineage, table storage
- Legacy hive_metastore coexistence

## Hands-On

- **Guide**: [`10-unity-catalog-fundamentals.md`](10-unity-catalog-fundamentals.md) — theory, architecture, Azure metastore setup, and concepts
- **Notebook**: [`10-unity-catalog-fundamentals_notebook.py`](10-unity-catalog-fundamentals_notebook.py) — runnable Databricks lab covering namespace exploration, tables, views, volumes, time travel, system tables, and lineage

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- The 3-level namespace: `catalog.schema.table`
- Difference between Unity Catalog metastore and Hive Metastore
- Managed vs external tables in Unity Catalog (especially DROP behavior)
- How metastores are assigned to workspaces (one per region)
- Identity federation: account-level vs workspace-level identities
- Volumes for file governance
- `hive_metastore` as the legacy catalog name

## Next Steps

- [Day 11: Unity Catalog Security](../day11-unity-catalog-security/) — RBAC, privileges, native row filters, column masks
