# Access Control & Permissions
> Module 08 — Topic 02 | Level: Intermediate | Time: 40 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Use GRANT and REVOKE SQL commands to manage data access.
2. Explain the privilege hierarchy from METASTORE down to TABLE.
3. Describe the ownership model and how ownership transfers work.
4. List the securable objects in Unity Catalog and the privileges that apply.
5. Manage identities: users, groups, and service principals.
6. Differentiate between account-level and workspace-level groups.
7. Leverage privilege inheritance to simplify access management.

---

## Conceptual Overview

### The GRANT/REVOKE Model

Unity Catalog uses standard SQL GRANT/REVOKE syntax to control access:

```sql
-- Grant syntax
GRANT privilege ON securable_type securable_name TO principal;

-- Revoke syntax
REVOKE privilege ON securable_type securable_name FROM principal;

-- Examples
GRANT SELECT ON TABLE prod.sales.transactions TO `data_analysts`;
GRANT USE SCHEMA ON SCHEMA prod.sales TO `data_analysts`;
GRANT CREATE TABLE ON SCHEMA dev.sandbox TO `data_engineers`;
REVOKE SELECT ON TABLE prod.hr.salaries FROM `interns`;
```

The backtick syntax (`` ` ``) is used for principals that contain special
characters, such as email addresses or group names with spaces.

### Securable Object Hierarchy

Unity Catalog organizes securables in a strict hierarchy. Privileges granted at a
higher level cascade to all objects below.

```
  METASTORE
    |
    +-- CATALOG
    |     |
    |     +-- SCHEMA
    |     |     |
    |     |     +-- TABLE
    |     |     +-- VIEW
    |     |     +-- FUNCTION
    |     |     +-- VOLUME
    |     |     +-- MODEL (ML)
    |     |
    |     +-- (schema-level securables)
    |
    +-- STORAGE CREDENTIAL
    +-- EXTERNAL LOCATION
    +-- CONNECTION (Federation)
    +-- SHARE (Delta Sharing)
```

### Privilege Types

| Privilege | Applies To | What It Allows |
|-----------|------------|----------------|
| `SELECT` | TABLE, VIEW | Read data |
| `MODIFY` | TABLE | INSERT, UPDATE, DELETE, MERGE |
| `CREATE TABLE` | SCHEMA | Create tables within the schema |
| `CREATE SCHEMA` | CATALOG | Create schemas within the catalog |
| `CREATE CATALOG` | METASTORE | Create catalogs |
| `USE CATALOG` | CATALOG | Access the catalog (required to see schemas) |
| `USE SCHEMA` | SCHEMA | Access the schema (required to see tables) |
| `ALL PRIVILEGES` | Any securable | All applicable privileges |
| `CREATE FUNCTION` | SCHEMA | Create UDFs within the schema |
| `CREATE VOLUME` | SCHEMA | Create volumes within the schema |
| `READ VOLUME` | VOLUME | Read files from the volume |
| `WRITE VOLUME` | VOLUME | Write files to the volume |
| `EXECUTE` | FUNCTION | Run the function |
| `READ FILES` | EXTERNAL LOCATION | Read files at the location |
| `WRITE FILES` | EXTERNAL LOCATION | Write files at the location |

### Privilege Inheritance

This is one of the most powerful features of Unity Catalog. When you grant a
privilege at a higher level, it automatically applies to all objects below.

```
  Example: GRANT SELECT ON CATALOG prod TO `analysts`;

  This single command gives SELECT on:
  +-- prod (catalog)
       +-- prod.sales (schema)
       |    +-- prod.sales.transactions  (table)  <-- SELECT granted
       |    +-- prod.sales.customers     (table)  <-- SELECT granted
       |    +-- prod.sales.daily_summary (view)   <-- SELECT granted
       |
       +-- prod.marketing (schema)
            +-- prod.marketing.campaigns (table)  <-- SELECT granted
            +-- prod.marketing.leads     (table)  <-- SELECT granted
```

**Important:** To actually query a table, a user needs:
1. `USE CATALOG` on the catalog
2. `USE SCHEMA` on the schema
3. `SELECT` on the table

A common pattern is to grant `USE CATALOG` and `USE SCHEMA` broadly, then
control data access at the table level with `SELECT`.

### The Ownership Model

Every securable object has an **owner**. The owner has all privileges on that
object, including the ability to grant privileges to others.

```
  Ownership Rules:
  ================
  - The creator of an object is its initial owner
  - Ownership can be transferred: ALTER TABLE t SET OWNER TO `new_owner`
  - The owner of a catalog owns its default schema
  - Metastore admins can transfer ownership of any object
  - Only the owner or a metastore admin can DROP an object
```

### Identity Management

Unity Catalog recognizes three types of principals:

```
  +------------------+    +-----------------+    +---------------------+
  | Users            |    | Groups          |    | Service Principals  |
  |                  |    |                 |    |                     |
  | Individual human |    | Collection of   |    | Non-human identity  |
  | identities       |    | users, service  |    | for automated       |
  | (email-based)    |    | principals, or  |    | workloads (jobs,    |
  |                  |    | nested groups   |    | pipelines, APIs)    |
  +------------------+    +-----------------+    +---------------------+
```

### Account-Level vs Workspace-Level Groups

| Feature | Account-Level Groups | Workspace-Level Groups |
|---------|---------------------|----------------------|
| Scope | All workspaces in the account | Single workspace |
| Created by | Account admins | Workspace admins |
| Unity Catalog | Fully supported | Legacy, limited support |
| Recommendation | Preferred for all new setups | Use only for backward compatibility |
| Managed in | Account console | Workspace admin settings |

Best practice: Always use **account-level groups** for Unity Catalog governance.
Workspace-level groups cannot be used as principals in GRANT statements in Unity
Catalog.

### Common Access Patterns

**Pattern 1: Environment Isolation**
```sql
-- Data engineers get full access to dev, read-only to prod
GRANT ALL PRIVILEGES ON CATALOG dev TO `data_engineers`;
GRANT USE CATALOG ON CATALOG prod TO `data_engineers`;
GRANT USE SCHEMA ON CATALOG prod TO `data_engineers`;
GRANT SELECT ON CATALOG prod TO `data_engineers`;
```

**Pattern 2: Analyst Read-Only Access**
```sql
-- Analysts can read from prod only
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA ON SCHEMA prod.sales TO `analysts`;
GRANT SELECT ON SCHEMA prod.sales TO `analysts`;
```

**Pattern 3: Service Principal for Pipelines**
```sql
-- Pipeline service principal needs write access to specific schemas
GRANT USE CATALOG ON CATALOG prod TO `etl_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA prod.raw TO `etl_pipeline_sp`;
GRANT ALL PRIVILEGES ON SCHEMA prod.raw TO `etl_pipeline_sp`;
```

---

## Hands-On Walkthrough

Open the companion notebook `02-access-control_notebook.py` and follow along.
The notebook covers:

1. GRANT and REVOKE SQL command demonstrations
2. SHOW GRANTS to inspect current privileges
3. Privilege inheritance examples
4. Ownership inspection and transfer syntax
5. Simulated permission checks for Community Edition

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Identity Provider | AWS IAM Identity Center, Okta, Azure AD | Azure Active Directory (Entra ID) | Google Cloud Identity |
| SCIM Provisioning | Supported (Okta, Azure AD, etc.) | Native Azure AD integration | Supported via Google Workspace |
| Service Principals | Created in account console | Azure AD App Registrations | GCP Service Accounts |
| SSO Protocol | SAML 2.0, OIDC | SAML 2.0 (via Azure AD) | SAML 2.0, OIDC |
| Group Sync | SCIM from IdP to Databricks account | Automatic with Azure AD connector | SCIM from Google Workspace |

---

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests your understanding of:

- **GRANT/REVOKE syntax** — know the exact SQL commands
- **Privilege inheritance** — a GRANT on a catalog applies to all schemas and
  tables within it
- **USE CATALOG + USE SCHEMA** — users need these in addition to SELECT to query
- **Ownership** — the creator is the initial owner; only owners or admins can DROP
- **Account-level groups** are required for Unity Catalog (workspace-level groups
  are legacy)
- **Service principals** are used for automated workloads, not personal accounts

---

## Key Takeaways

1. **GRANT/REVOKE** with standard SQL syntax is how you control all access in
   Unity Catalog.
2. **Privilege inheritance** simplifies management — grant at the catalog level to
   cover all schemas and tables.
3. Users need **USE CATALOG + USE SCHEMA + SELECT** to query a table (all three).
4. **Ownership** determines who can manage an object — the creator is the default
   owner.
5. Use **account-level groups** for Unity Catalog principals, not workspace-level.
6. **Service principals** provide non-human identities for pipelines and jobs.
7. Follow the **principle of least privilege** — grant only what is needed.

---

## Next Steps

Proceed to [03 — Data Lineage](03-data-lineage.md) to learn how Unity Catalog
automatically tracks data flow across your Lakehouse.
