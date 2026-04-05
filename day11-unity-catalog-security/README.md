# Day 11: Unity Catalog Security

> Module: Data Governance | Level: Intermediate | Time: 120 min

## Two-Part Lab

- **Part 1**: GRANT/REVOKE, dynamic views, regional views pattern (legacy)
- **Part 2**: Native Row Filters and Column Masks (modern, DBR 12.2+)

## Learning Objectives

- Understand the UC security model and how it differs from Hive Metastore
- Manage identities: users, service principals, and groups
- Grant, revoke, and show privileges on data objects
- Implement row-level security using **native Row Filters** (table-level, DBR 12.2+)
- Apply **native Column Masks** to protect PII columns (DBR 12.2+)
- Understand the migration from regional dynamic views to native Row Filters
- Configure storage credentials and external locations (Azure)
- Know when to use dynamic views vs native policies

## Key Concepts

- **Security Model** — GRANT privilege ON securable_object TO principal
- **Prerequisite Chain** — USE CATALOG → USE SCHEMA → SELECT (each step required)
- **Dynamic Views** — traditional row/column security via VIEWs (still valid, legacy pattern)
- **Regional Views** — old pattern: 1 view per region (APAC/EMEA/AMER) — now replaced
- **Native Row Filters** — functions attached to TABLE; filter rows transparently (DBR 12.2+)
- **Native Column Masks** — functions attached to COLUMN; mask values transparently (DBR 12.2+)
- **Storage Credentials** — Azure Managed Identity for UC to access ADLS
- **External Locations** — map storage credential to a specific ADLS path

## Topics Covered

- Before/after UC security comparison
- Privilege prerequisite chain (USE CATALOG → USE SCHEMA → SELECT)
- GRANT/REVOKE/SHOW GRANTS syntax
- Dynamic views for row-level security (legacy/HMS pattern)
- Dynamic views for column masking (legacy/HMS pattern)
- Regional views pattern and its limitations
- **Native Row Filters**: CREATE FUNCTION → ALTER TABLE SET ROW FILTER
- **Native Column Masks**: CREATE FUNCTION → ALTER TABLE ALTER COLUMN SET MASK
- Migration path: regional views → Row Filters
- Storage credentials and external locations (Azure)
- Audit logging via system.access.audit

## Hands-On

- **Guide**: [`11-unity-catalog-security.md`](11-unity-catalog-security.md) — theory, before/after, migration patterns
- **Notebook Part 1**: [`11-unity-catalog-security_notebook.py`](11-unity-catalog-security_notebook.py) — GRANT/REVOKE, dynamic views, reusable masking UDFs
- **Notebook Part 2**: [`11b-row-filters-column-masks_notebook.py`](11b-row-filters-column-masks_notebook.py) — native Row Filters, Column Masks, migration walkthrough

## Certification Tip

The Databricks Certified Data Engineer Associate and Professional exams test:
- GRANT/REVOKE syntax and the full prerequisite chain
- USE CATALOG and USE SCHEMA as prerequisites (not data access by themselves)
- Ownership and who can grant privileges (metastore admin > catalog owner > schema owner > table owner)
- Dynamic views for row-level security and column masking using `is_account_group_member()`
- Native Row Filters and Column Masks (DBR 12.2+)
- Storage credentials and external locations

## Next Steps

- [Day 12: Managed vs External Tables](../day12-managed-vs-external-tables/) — deep dive into table types and storage
