# Delta Sharing
> Module 08 — Topic 05 | Level: Advanced | Time: 40 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Explain the Delta Sharing open protocol and its architecture.
2. Describe the provider/recipient/share model for data sharing.
3. Configure shares, recipients, and providers using SQL DDL.
4. Set up cross-organization and cross-cloud sharing.
5. Differentiate between open-source Delta Sharing and Databricks-managed sharing.
6. Implement recipient authentication and audit logging.
7. Design a production Delta Sharing strategy.

---

## Conceptual Overview

### What Is Delta Sharing?

Delta Sharing is the world's first **open protocol** for secure data sharing.
It allows organizations to share live data with external partners without copying
data, regardless of what platform or cloud the recipient uses.

```
  Traditional Data Sharing          Delta Sharing
  =========================         ==========================

  Provider                          Provider
  +----------+                      +----------+
  | Data     |  Copy via            | Data     |  Share live
  | Warehouse|  S3/FTP/API          | Lakehouse|  via protocol
  +----------+  -------->           +----------+  -------->
                                         |
  Recipient                              |   No data copying
  +----------+                           |   No vendor lock-in
  | Separate |  Stale copy               |
  | Storage  |  Data drift          Recipient can use:
  +----------+                      - Databricks
                                    - Apache Spark
                                    - pandas
                                    - Power BI
                                    - Tableau
                                    - Any Delta Sharing client
```

### The Delta Sharing Protocol

Delta Sharing uses a REST-based protocol with three core concepts:

```
  +------------+          +------------+          +------------+
  |  PROVIDER  |          |   SHARE    |          | RECIPIENT  |
  |            |          |            |          |            |
  | Owns the   |--creates-->| Container |--granted-->| Accesses   |
  | data       |          | of tables  |   to     | shared data|
  +------------+          +------------+          +------------+
```

1. **Provider** — the organization that owns and shares the data
2. **Share** — a named container of tables/views that are being shared
3. **Recipient** — the external entity that receives access to the shared data

### Architecture: Open Source vs Databricks-Managed

```
  OPEN-SOURCE DELTA SHARING SERVER
  =================================

  +-------------------+       REST API       +------------------+
  | Delta Sharing     |<-------------------->| Recipients       |
  | Server (OSS)      |                      | (any platform)   |
  |                   |                      |                  |
  | - Self-hosted     |                      | - pandas         |
  | - S3/ADLS/GCS     |                      | - Spark          |
  | - Config file     |                      | - Power BI       |
  +-------------------+                      +------------------+


  DATABRICKS-MANAGED DELTA SHARING
  =================================

  +-------------------+       REST API       +------------------+
  | Unity Catalog     |<-------------------->| Recipients       |
  | (built-in)        |                      |                  |
  |                   |                      | - Databricks     |
  | - No server mgmt  |                      | - pandas         |
  | - UI management   |                      | - Spark          |
  | - Audit logging   |                      | - Power BI       |
  | - Access control   |                      | - Tableau        |
  +-------------------+                      +------------------+
```

| Feature | Open-Source | Databricks-Managed |
|---------|------------|-------------------|
| Hosting | Self-managed server | Built into Unity Catalog |
| Setup | Deploy and configure server | Enable in workspace settings |
| Authentication | Bearer token (manual) | Token or Databricks-to-Databricks |
| Audit logging | Custom implementation | Built-in audit logs |
| UI management | None (API/config only) | Full UI in Unity Catalog |
| Access control | Server config | Unity Catalog GRANT model |
| Table types | Delta tables only | Delta tables + views |
| Change data feed | Supported | Supported |

### Recipient Authentication

Recipients authenticate using one of two methods:

**Method 1: Token-Based (Open Sharing)**
```
  Provider creates recipient -> generates activation link
  Recipient downloads credentials file (.share)
  Credentials file contains:
    - endpoint URL
    - bearer token
    - token expiration
  Recipient uses credentials in their client (pandas, Spark, etc.)
```

**Method 2: Databricks-to-Databricks (Managed Sharing)**
```
  Provider shares with recipient's Databricks workspace
  Authentication uses Databricks metastore identity
  No credentials file needed
  Automatic, secure, seamless
```

### Share Content: What Can Be Shared?

```
  Shareable Objects:
  ==================

  +-- SHARE
       |
       +-- TABLE (Delta tables)
       |     |
       |     +-- Full table
       |     +-- Partitions (subset of data)
       |     +-- With history (time travel)
       |     +-- With change data feed
       |
       +-- VIEW (Unity Catalog views)
       |     |
       |     +-- Dynamic views (for row/column security)
       |
       +-- VOLUME (non-tabular data)
       |     |
       |     +-- Files, models, artifacts
       |
       +-- SCHEMA (all objects in a schema)
       |
       +-- NOTEBOOK (shared notebooks)
```

### Cross-Cloud Sharing

One of Delta Sharing's most powerful features is cross-cloud sharing. A provider
on AWS can share data with a recipient on Azure or GCP without any data copying.

```
  Cross-Cloud Sharing:
  ====================

  AWS Provider                 Azure Recipient
  +-----------+                +-----------+
  | Databricks|  Delta Sharing | Databricks|
  | (us-east) |  Protocol      | (westeu)  |
  | Data on S3|--------------->| Reads via |
  +-----------+                | protocol  |
                               +-----------+

  The recipient's Spark cluster reads data directly from the
  provider's cloud storage via pre-signed URLs. No data is copied
  to the recipient's cloud account.
```

### Audit Logging

Every access to shared data is logged, providing complete visibility:

```
  Audit Events Captured:
  ======================
  - Share created/modified/deleted
  - Recipient created/modified/deleted
  - Tables added to/removed from share
  - Recipient accessed shared data (table, time, rows read)
  - Recipient activated credentials
  - Share permissions changed
```

On Databricks-managed sharing, audit logs are available via:
- Unity Catalog system tables (`system.access.audit`)
- Cloud provider audit logs (CloudTrail, Azure Monitor, GCP Logging)

---

## Hands-On Walkthrough

Open the companion notebook `05-delta-sharing_notebook.py` and follow along.
The notebook covers:

1. SQL commands for creating shares, recipients, and providers
2. Adding tables to shares with various options
3. Recipient access patterns and credential management
4. Configuration templates for open-source and managed sharing
5. Audit logging queries

> **Note:** This topic was introduced at a basic level in Module 03. This deep
> dive covers production setup, cross-org sharing, and advanced configurations.

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Provider storage | S3 buckets | ADLS Gen2 | GCS buckets |
| Pre-signed URL | S3 pre-signed URLs | Azure SAS tokens | GCS signed URLs |
| Cross-cloud | AWS to Azure/GCP | Azure to AWS/GCP | GCP to AWS/Azure |
| Network security | VPC endpoints (optional) | Private Link (optional) | VPC Service Controls (optional) |
| Managed sharing | Unity Catalog | Unity Catalog | Unity Catalog |
| OSS server hosting | EC2 / ECS / EKS | AKS / App Service | GKE / Cloud Run |

---

## Certification Tip

For the Databricks Certified Data Engineer exams:

- Delta Sharing is an **open protocol** — no vendor lock-in
- The three core objects: **SHARE**, **PROVIDER**, **RECIPIENT**
- Recipients can use **any client** (pandas, Spark, Power BI, Tableau)
- Databricks-to-Databricks sharing uses **metastore identity** (no tokens)
- Open sharing uses **bearer tokens** distributed via activation links
- Shared data is read **directly from the provider's storage** — no copying
- **Audit logging** captures all access to shared data
- You can share **tables, views, volumes, and entire schemas**

---

## Key Takeaways

1. **Delta Sharing** is the first open protocol for secure, live data sharing
   across organizations and platforms.
2. The **provider/share/recipient** model organizes who shares what with whom.
3. **No data copying** — recipients read directly from the provider's storage
   via pre-signed URLs.
4. **Open-source** Delta Sharing requires a self-hosted server;
   **Databricks-managed** sharing is built into Unity Catalog.
5. **Cross-cloud sharing** works natively — AWS to Azure, Azure to GCP, etc.
6. **Token-based** authentication for open sharing; **metastore identity** for
   Databricks-to-Databricks sharing.
7. **Comprehensive audit logging** tracks every access to shared data.
8. You can share **tables, views, schemas, and volumes** with fine-grained control.

---

## Next Steps

Proceed to [06 — Lakehouse Federation](06-lakehouse-federation.md) to learn how
to query external databases (PostgreSQL, MySQL, Snowflake, BigQuery) without
copying data into the Lakehouse.
