# Day 10: Unity Catalog Fundamentals

> Module: Data Governance | Level: Beginner | Time: 90 min

## Learning Objectives

- Understand what Unity Catalog is and why it replaces the Hive Metastore
- Navigate the 3-level namespace: Catalog -> Schema -> Table
- Distinguish between Unity Catalog metastore and Hive Metastore
- Create and manage catalogs, schemas, tables, views, and volumes
- Explore managed storage, data lineage, and search capabilities
- Access legacy hive_metastore tables alongside Unity Catalog

## Key Concepts

- **Unity Catalog** -- Databricks' centralized governance solution for all data and AI assets across workspaces
- **3-Level Namespace** -- `catalog.schema.table` replaces the 2-level Hive namespace
- **Metastore** -- top-level container storing metadata, ACLs, and audit logs; regional, multi-workspace
- **Managed Tables** -- UC controls storage lifecycle; DROP deletes data
- **External Tables** -- you specify LOCATION; DROP keeps data
- **Volumes** -- governed access to non-tabular files (CSVs, images, JARs)
- **Identity Federation** -- account-level users/groups assigned to workspaces
- **Automated Lineage** -- tracks data origin and flow across all asset types

## Topics Covered

- Unity Catalog architecture and motivation
- 3-level namespace: Catalog -> Schema -> Table/View/Volume/Function
- Metastore: regional scope, multi-workspace assignment
- Managed vs external storage at metastore, catalog, and schema levels
- Volumes for file governance
- Data lineage and search
- Legacy hive_metastore coexistence

## Hands-On

- **Guide**: [`10-unity-catalog-fundamentals.md`](10-unity-catalog-fundamentals.md) -- theory, architecture, and concepts
- **Notebook**: [`10-unity-catalog-fundamentals_notebook.py`](10-unity-catalog-fundamentals_notebook.py) -- runnable Databricks lab covering namespace exploration, tables, views, volumes, time travel, and lineage

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- The 3-level namespace: `catalog.schema.table`
- Difference between Unity Catalog metastore and Hive Metastore
- Managed vs external tables in Unity Catalog
- How metastores are assigned to workspaces
- Identity federation: account-level vs workspace-level identities
- Volumes for file governance

## Next Steps

- [Day 11: Unity Catalog Security](../day11-unity-catalog-security/) -- RBAC, privileges, row-level security, column masking
