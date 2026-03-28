# Unity Catalog Security
> Module: Data Governance | Day 11 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain the Unity Catalog security model and how it differs from Hive Metastore
- Manage identities: users, service principals, and groups
- Grant, revoke, and deny privileges on data objects
- Implement row-level security using dynamic views
- Apply column masking to protect sensitive data
- Understand identity federation and storage credentials
- Configure data access control in production environments

---

## Conceptual Overview

### The Hive Metastore Security Model (Legacy)

In the traditional Hive Metastore, access control was limited:

```
Hive Metastore ACLs
━━━━━━━━━━━━━━━━━━

  GRANT privilege ON object TO user_or_group

  Objects:  CATALOG | SCHEMA | TABLE | VIEW | FUNCTION | ANY FILE
  Privileges: SELECT | MODIFY | CREATE | READ_METADATA | USAGE | ALL PRIVILEGES
```

**Limitations**:
- Users and groups were workspace-local
- No governance for files, ML models, or volumes
- No cross-workspace access control
- No automated lineage or audit logging
- `USAGE` privilege required as a prerequisite for any action on database objects

### The Unity Catalog Security Model

Unity Catalog builds on the same GRANT/REVOKE SQL syntax but adds account-level identity, more securable objects, and finer-grained control.

```
Unity Catalog Security Model
━━━━━━━━━━━━━━━━━━━━━━━━━━━

  GRANT privilege ON securable_object TO principal

  Principals:
  ┌────────────────────────────────────────────────┐
  │  Users           (email-based, individual)     │
  │  Service Principals (application ID, automated)│
  │  Groups          (collections of above)        │
  └────────────────────────────────────────────────┘

  Securable Objects:
  ┌────────────────────────────────────────────────┐
  │  METASTORE                                      │
  │  └── CATALOG                                    │
  │      └── SCHEMA                                 │
  │          ├── TABLE                              │
  │          ├── VIEW                               │
  │          ├── VOLUME                             │
  │          ├── FUNCTION                           │
  │          └── MODEL                              │
  │  STORAGE CREDENTIAL                             │
  │  EXTERNAL LOCATION                              │
  │  SHARE / RECIPIENT (Delta Sharing)              │
  └────────────────────────────────────────────────┘

  Privileges:
  ┌────────────────────────────────────────────────┐
  │  CREATE, SELECT, MODIFY, EXECUTE               │
  │  USE CATALOG, USE SCHEMA                       │
  │  READ FILES, WRITE FILES                       │
  │  READ VOLUME, WRITE VOLUME                     │
  │  CREATE CATALOG, CREATE SCHEMA, CREATE TABLE   │
  │  ALL PRIVILEGES                                │
  └────────────────────────────────────────────────┘
```

---

## Identities (Principals)

Unity Catalog has three types of principals:

### Users

Individual people identified by their **email address**. Users can be assigned admin roles.

### Service Principals

Automated identities for tools and applications, identified by an **Application ID**. Used for CI/CD pipelines, scheduled jobs, and service-to-service access.

### Groups

Collections of users and service principals. Groups can be **nested** -- a parent group can contain other groups.

```
  Group: all_employees
  ├── Group: engineering
  │   ├── User: alice@company.com
  │   ├── User: bob@company.com
  │   └── Service Principal: cicd-bot
  ├── Group: marketing
  │   ├── User: carol@company.com
  │   └── User: frank@company.com
  └── Group: finance
      ├── User: david@company.com
      └── User: grace@company.com
```

---

## Identity Federation

Before Unity Catalog, identities existed only at the workspace level. With **Identity Federation**, identities are created once at the **account level** and then assigned to one or more workspaces.

```
  Account Console (identity source of truth)
  ┌──────────────────────────────────────────┐
  │  Users: alice, bob, carol, david         │
  │  Groups: engineering, marketing          │
  │  Service Principals: cicd-bot            │
  └──────────────────────────────────────────┘
           │                    │
      ┌────┴─────┐       ┌────┴─────┐
      │ Workspace│       │ Workspace│
      │    A     │       │    B     │
      │(assigned)│       │(assigned)│
      └──────────┘       └──────────┘
```

**Benefits**:
- No duplicate identity management across workspaces
- Single source of truth for all users and groups
- Centralized admin via the Account Console

---

## Privilege Hierarchy

Privileges flow through the object hierarchy. Granting `SELECT` on a catalog implicitly grants `SELECT` on all schemas and tables within it.

```
  GRANT SELECT ON CATALOG prod_catalog TO analysts
  │
  └── Applies to ALL schemas in prod_catalog
      └── Applies to ALL tables/views in those schemas
```

However, **USE CATALOG** and **USE SCHEMA** are required prerequisites:

```
  To SELECT from prod_catalog.hr_db.employees:
  ┌──────────────────────────────────────────────────────┐
  │  1. GRANT USE CATALOG ON CATALOG prod_catalog TO ... │
  │  2. GRANT USE SCHEMA ON SCHEMA hr_db TO ...          │
  │  3. GRANT SELECT ON TABLE employees TO ...           │
  └──────────────────────────────────────────────────────┘
```

`USE CATALOG` and `USE SCHEMA` do not grant any data access by themselves -- they only allow navigating to the object.

### Privilege Types

| Privilege | Description |
|-----------|-------------|
| `SELECT` | Read data from tables/views |
| `MODIFY` | Insert, update, delete data |
| `CREATE` | Create objects (tables, views, etc.) |
| `USE CATALOG` | Navigate into a catalog |
| `USE SCHEMA` | Navigate into a schema |
| `READ FILES` | Read from external location |
| `WRITE FILES` | Write to external location |
| `READ VOLUME` | Read files in a volume |
| `WRITE VOLUME` | Write files to a volume |
| `EXECUTE` | Run user-defined functions |
| `ALL PRIVILEGES` | Grant all of the above |

---

## Ownership

Every securable object in Unity Catalog has an **owner**. The owner has full control over the object, including the ability to grant/revoke privileges to others.

```
  Object Owner Can:
  ├── GRANT privileges to other principals
  ├── REVOKE privileges from other principals
  ├── DROP the object
  ├── ALTER the object
  └── TRANSFER ownership to another principal
```

Ownership rules:
- The creator of an object is its default owner
- Ownership can be transferred: `ALTER TABLE t OWNER TO `group_name``
- Metastore admins and catalog owners can override ownership

---

## Granting Privileges by Role

| Role | Can Grant Privileges On |
|------|------------------------|
| Metastore admin | All objects in the metastore |
| Catalog owner | All objects in that catalog |
| Schema owner | All objects in that schema |
| Table/View owner | That specific table/view |

```sql
-- Metastore admin or catalog owner
GRANT USE CATALOG ON CATALOG prod_catalog TO analysts;
GRANT USE SCHEMA ON SCHEMA prod_catalog.hr_db TO analysts;
GRANT SELECT ON TABLE prod_catalog.hr_db.employees TO analysts;

-- Schema owner
GRANT SELECT ON TABLE employees TO `analyst@company.com`;

-- View permissions
SHOW GRANTS ON TABLE employees;
SHOW GRANTS ON SCHEMA hr_db;
SHOW GRANTS `analyst@company.com`;
```

---

## Row-Level Security with Dynamic Views

Unity Catalog does not have built-in row-level security (RLS), but you can implement it using **dynamic views** with the `current_user()` and `is_account_group_member()` functions.

```sql
-- Only show employees from the user's own department
CREATE OR REPLACE VIEW secure_employees_vw AS
SELECT *
FROM employees
WHERE department = CASE
    WHEN is_account_group_member('engineering') THEN 'Engineering'
    WHEN is_account_group_member('marketing') THEN 'Marketing'
    WHEN is_account_group_member('finance') THEN 'Finance'
    WHEN is_account_group_member('hr') THEN 'HR'
    WHEN is_account_group_member('admins') THEN department  -- admins see all
    ELSE NULL
END;
```

Then grant `SELECT` on the view (not the underlying table) to users:

```sql
GRANT SELECT ON VIEW secure_employees_vw TO analysts;
-- Do NOT grant SELECT on the employees table directly
```

---

## Column Masking with Dynamic Views

Protect sensitive columns (PII, salary, SSN) by creating views that mask data based on group membership.

```sql
CREATE OR REPLACE VIEW masked_employees_vw AS
SELECT
    employee_id,
    first_name,
    last_name,
    -- Mask email for non-HR users
    CASE
        WHEN is_account_group_member('hr') THEN email
        ELSE concat(left(email, 2), '***@***')
    END AS email,
    department,
    -- Mask salary for non-finance users
    CASE
        WHEN is_account_group_member('finance') OR is_account_group_member('admins')
        THEN salary
        ELSE NULL
    END AS salary
FROM employees;
```

```
  User in 'hr' group sees:     alice@company.com, NULL salary
  User in 'finance' group:     al***@***, 120000.0
  User in 'admins' group:      alice@company.com, 120000.0
  Regular user:                 al***@***, NULL
```

---

## Storage Credentials and External Locations

To access data in your cloud storage, Unity Catalog uses two objects:

### Storage Credentials

A storage credential stores authentication info for a cloud storage container (IAM Role for AWS, Managed Identity for Azure, Service Account for GCP).

```sql
-- Created by metastore admin (typically via UI or Terraform)
CREATE STORAGE CREDENTIAL my_s3_cred
WITH (
    AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/uc-access-role'
);
```

### External Locations

An external location maps a storage credential to a specific path in cloud storage.

```sql
CREATE EXTERNAL LOCATION my_raw_data
URL 's3://my-bucket/raw-data/'
WITH (STORAGE CREDENTIAL my_s3_cred);
```

```
  Storage Credential (IAM Role)
  └── External Location 1: s3://bucket/raw/
  └── External Location 2: s3://bucket/curated/
  └── External Location 3: s3://bucket/archive/
```

You then grant `READ FILES` or `WRITE FILES` on external locations to control who can access cloud storage.

```sql
GRANT READ FILES ON EXTERNAL LOCATION my_raw_data TO data_engineers;
```

---

## Security Model Comparison

| Aspect | Hive Metastore | Unity Catalog |
|--------|---------------|---------------|
| Identity scope | Workspace | Account |
| Identity types | Users, groups | Users, service principals, groups |
| Securable objects | Catalog, schema, table, view, function | All above + volumes, models, credentials, locations, shares |
| Prerequisite privilege | `USAGE` | `USE CATALOG` + `USE SCHEMA` |
| File governance | `ANY FILE` (all or nothing) | `READ FILES` / `WRITE FILES` per external location |
| Row-level security | Dynamic views | Dynamic views + `is_account_group_member()` |
| Column masking | Dynamic views | Dynamic views (+ built-in masking policies in preview) |
| Audit logging | Limited | Full audit trail |
| Cross-workspace | Not possible | Built-in |

---

## Best Practices

1. **Use groups, not individual users** -- assign privileges to groups for maintainability
2. **Least privilege** -- grant only the minimum privileges needed
3. **Use views for row/column security** -- don't give direct table access when masking is needed
4. **Transfer ownership** -- assign table ownership to groups, not individual users
5. **Separate catalogs by environment** -- `dev_catalog`, `staging_catalog`, `prod_catalog`
6. **Use external locations** -- govern cloud storage access through UC, not IAM alone
7. **Audit regularly** -- review `SHOW GRANTS` and system audit logs

---

## Hands-On Walkthrough

See the companion notebook: [`11-unity-catalog-security_notebook.py`](11-unity-catalog-security_notebook.py)

The lab covers:
1. Granting and revoking privileges on catalogs, schemas, and tables
2. Implementing row-level security with dynamic views
3. Implementing column masking with dynamic views
4. Exploring the `is_account_group_member()` function
5. Querying privilege grants with `SHOW GRANTS`
6. Working with table ownership

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Storage Credential type | IAM Role | Managed Identity / Service Principal | Service Account |
| Identity provider | SCIM + AWS SSO | Azure AD (Entra ID) + SCIM | Google Identity + SCIM |
| Account console | accounts.cloud.databricks.com | accounts.azuredatabricks.net | accounts.gcp.databricks.com |
| External location storage | S3 paths | ABFSS paths | GCS paths |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- GRANT/REVOKE/DENY syntax and behavior
- `USE CATALOG` and `USE SCHEMA` as prerequisites
- Difference between Hive Metastore ACLs and Unity Catalog privileges
- Ownership and who can grant privileges (metastore admin, catalog owner, schema owner)
- Dynamic views for row-level security and column masking
- `is_account_group_member()` function
- Storage credentials and external locations

---

## Key Takeaways

1. Unity Catalog uses **account-level identities** (users, service principals, groups) with identity federation
2. The security model uses **GRANT/REVOKE on securable objects** with a hierarchy from metastore down to table
3. **USE CATALOG** and **USE SCHEMA** are required prerequisites to access any object within
4. **Dynamic views** with `is_account_group_member()` implement row-level security and column masking
5. **Storage credentials** and **external locations** govern cloud storage access through Unity Catalog
6. **Ownership** determines who can manage and grant access to objects
7. Always prefer **groups over individual users** for privilege management

---

## Next Steps

- [Day 12: Managed vs External Tables](../day12-managed-vs-external-tables/) -- deep dive into table types and storage
- [Day 13: Volumes in Databricks](../day13-volumes-in-databricks/) -- governed file access patterns
