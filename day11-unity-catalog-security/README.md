# Day 11: Unity Catalog Security

> Module: Data Governance | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand the Unity Catalog security model and how it differs from Hive Metastore
- Manage identities: users, service principals, and groups
- Grant, revoke, and deny privileges on data objects
- Implement row-level security using dynamic views
- Apply column masking to protect sensitive data
- Understand identity federation, storage credentials, and external locations

## Key Concepts

- **Security Model** -- GRANT privilege ON securable_object TO principal
- **Principals** -- users (email), service principals (app ID), groups (nested)
- **Privileges** -- SELECT, MODIFY, CREATE, USE CATALOG, USE SCHEMA, READ/WRITE FILES, EXECUTE
- **Prerequisite Chain** -- USE CATALOG + USE SCHEMA required before SELECT on a table
- **Ownership** -- every object has an owner with full control; transfer to groups
- **Dynamic Views** -- implement row-level security and column masking using `is_account_group_member()`
- **Storage Credentials** -- IAM Role/Managed Identity/Service Account for cloud storage auth
- **External Locations** -- map credentials to specific storage paths

## Topics Covered

- UC security model: principals, securable objects, privileges
- Identities and identity federation
- GRANT/REVOKE/SHOW GRANTS syntax
- Privilege hierarchy: metastore admin > catalog owner > schema owner > table owner
- Row-level security with dynamic views
- Column masking with dynamic views
- UDF-based security helpers for reusable masking
- Storage credentials and external locations
- Hive Metastore vs Unity Catalog security comparison
- Best practices for production access control

## Hands-On

- **Guide**: [`11-unity-catalog-security.md`](11-unity-catalog-security.md) -- theory, security model, and patterns
- **Notebook**: [`11-unity-catalog-security_notebook.py`](11-unity-catalog-security_notebook.py) -- runnable Databricks lab covering GRANT/REVOKE, dynamic views, column masking, UDFs, and ownership

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- GRANT/REVOKE syntax and behavior
- USE CATALOG and USE SCHEMA as prerequisites
- Ownership and who can grant privileges
- Dynamic views for row-level security and column masking
- `is_account_group_member()` function
- Storage credentials and external locations

## Next Steps

- [Day 12: Managed vs External Tables](../day12-managed-vs-external-tables/) -- deep dive into table types and storage
