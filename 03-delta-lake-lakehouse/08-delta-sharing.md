# Delta Sharing

> Module 03 -- Topic 08 | Level: Intermediate | Time: 30 min

## Learning Objectives

- Explain the Delta Sharing protocol and its purpose
- Differentiate between Databricks-to-Databricks and open sharing
- Create shares and manage recipients
- Access shared data from external tools and platforms
- Understand the security model and audit capabilities
- Know the limitations and requirements of Delta Sharing

## Conceptual Overview

### What Is Delta Sharing?

Delta Sharing is an **open protocol** for secure data sharing across
organizations, platforms, and clouds. It allows a data provider to share
live Delta tables with recipients -- **without copying data**.

```
  Traditional Data Sharing:          Delta Sharing:
  =========================          ==============

  Provider --> Export CSV/Parquet     Provider --> Create Share
           --> Upload to S3/SFTP                --> Add Recipients
           --> Notify recipient                 --> Grant Access
                                                    |
  Recipient --> Download files       Recipient --> Read live data
            --> Load into system                (no copy needed)
            --> Data is stale                   (always up-to-date)

  Problems:                          Benefits:
  - Data goes stale instantly        - Always current data
  - Multiple copies = cost           - No data duplication
  - No access control after share    - Audited, revocable access
  - No audit trail                   - Open protocol (any client)
```

### How Delta Sharing Works

```
  +------------------+                    +-------------------+
  |  DATA PROVIDER   |                    |  DATA RECIPIENT   |
  |  (Databricks)    |                    |  (Any platform)   |
  +------------------+                    +-------------------+
  |                  |                    |                   |
  | Delta Tables     |    Delta Sharing   | Pandas            |
  | in Unity Catalog |<-- Protocol ------>| Spark             |
  |                  |    (REST API)      | Power BI          |
  | CREATE SHARE     |                    | Tableau           |
  | ADD RECIPIENT    |    Bearer Token    | Databricks        |
  |                  |    + HTTPS         | Any tool with     |
  +------------------+                    | delta-sharing lib |
                                          +-------------------+
```

**Protocol flow**:

1. Provider creates a **share** containing specific tables/views
2. Provider creates a **recipient** and generates an activation link
3. Recipient activates the link, receiving a credential file
4. Recipient uses the credential file to query shared data via REST API
5. Data is read directly from the provider's storage -- no copying

### Databricks-to-Databricks vs Open Sharing

| Feature | Databricks-to-Databricks | Open Sharing |
|---------|------------------------|--------------|
| Recipient platform | Must be Databricks | Any platform |
| Authentication | Unity Catalog federated | Bearer token (credential file) |
| Setup complexity | Lower (automatic) | Higher (manual token exchange) |
| Recipient capabilities | Full SQL, DataFrame | Depends on client library |
| Catalog integration | Automatic in recipient's UC | Manual registration |
| Performance | Optimized (cloud-to-cloud) | Standard REST protocol |
| Use case | Internal org sharing | Cross-org, cross-platform |

### Creating a Share

Shares are created in Unity Catalog:

```sql
-- Create a share
CREATE SHARE customer_analytics_share;

-- Add tables to the share
ALTER SHARE customer_analytics_share
ADD TABLE gold.customer_360;

ALTER SHARE customer_analytics_share
ADD TABLE gold.daily_revenue_summary;

-- Add a table with partition filtering (recipients see only their data)
ALTER SHARE customer_analytics_share
ADD TABLE silver.orders
  PARTITION (region = 'US-West');

-- View share contents
SHOW ALL IN SHARE customer_analytics_share;

-- Describe the share
DESCRIBE SHARE customer_analytics_share;
```

**Share properties**:

| Property | Description |
|----------|-------------|
| Name | Unique identifier for the share |
| Tables | List of tables/views included |
| Partitions | Optional partition filters per table |
| Comment | Description of the share's purpose |
| Owner | User or group that manages the share |

### Managing Recipients

```sql
-- Create a recipient (open sharing)
CREATE RECIPIENT partner_company
COMMENT 'Analytics partner for Q1 2025 project';

-- The above command generates an activation link
-- Share the activation link with the recipient securely

-- Grant the share to the recipient
GRANT SELECT ON SHARE customer_analytics_share
TO RECIPIENT partner_company;

-- List all recipients
SHOW RECIPIENTS;

-- See what a recipient can access
SHOW GRANTS TO RECIPIENT partner_company;

-- Revoke access
REVOKE SELECT ON SHARE customer_analytics_share
FROM RECIPIENT partner_company;

-- Remove a recipient
DROP RECIPIENT partner_company;
```

### Accessing Shared Data (Recipient Side)

**From Databricks (Databricks-to-Databricks)**:

```sql
-- Create a catalog from the share (automatic)
CREATE CATALOG partner_data
USING SHARE provider_workspace.customer_analytics_share;

-- Query shared data
SELECT * FROM partner_data.gold.customer_360;
```

**From Python (any platform)**:

```python
import delta_sharing

# Path to the credential file (received during activation)
profile_file = "/path/to/config.share"

# Create a sharing client
client = delta_sharing.SharingClient(profile_file)

# List available shares and tables
shares = client.list_shares()
tables = client.list_all_tables()

# Read a shared table into a Pandas DataFrame
df = delta_sharing.load_as_pandas(
    f"{profile_file}#share_name.schema_name.table_name"
)

# Read into a Spark DataFrame
spark_df = delta_sharing.load_as_spark(
    f"{profile_file}#share_name.schema_name.table_name"
)
```

**From Power BI, Tableau, and other tools**:

- Use the Delta Sharing connector (ODBC/JDBC)
- Or use the REST API directly with the bearer token

### Security Model

```
  Delta Sharing Security Layers:
  ==============================

  1. SHARE-level:     What tables are included?
  2. RECIPIENT-level: Who can access the share?
  3. PARTITION-level: What subset of data is visible?
  4. TOKEN-level:     Bearer tokens with expiration
  5. AUDIT-level:     Every access is logged
  6. NETWORK-level:   IP whitelisting (optional)

  Provider retains FULL CONTROL:
  - Revoke access at any time
  - Change what's shared without recipient involvement
  - Monitor all access via audit logs
  - Data never leaves provider's storage account
```

**Key security properties**:

| Property | Details |
|----------|---------|
| Data location | Data stays in provider's cloud storage |
| Encryption | TLS in transit; storage encryption at rest |
| Token expiration | Configurable (default varies by setup) |
| Revocation | Immediate -- drop recipient or revoke grant |
| IP restrictions | Optional allowlist for recipient IPs |
| Audit logging | All reads logged in Unity Catalog audit trail |

### Audit Logging

Every access to shared data is recorded:

```sql
-- View sharing audit events (requires admin access)
SELECT
  event_time,
  event_type,
  recipient_name,
  share_name,
  table_name,
  action
FROM system.access.audit
WHERE service_name = 'deltaSharingService'
ORDER BY event_time DESC;
```

### Limitations

| Limitation | Details |
|-----------|---------|
| Requires Unity Catalog | Shares are managed through UC |
| Read-only for recipients | Recipients cannot write to shared tables |
| No streaming reads | CDF streaming not supported (batch only) |
| Table types | Only Delta tables and views (no volumes, models) |
| Network bandwidth | Large tables may be slow over public internet |
| Feature support | Some advanced Delta features (e.g., DVs) may not transfer |
| Partition filters | Must use explicit partition predicates, not dynamic |

### Delta Sharing vs Alternatives

| Approach | Data Copy? | Real-time? | Cross-platform? | Governance? |
|----------|-----------|-----------|----------------|------------|
| Delta Sharing | No | Near real-time | Yes (open protocol) | Full |
| File export (CSV/Parquet) | Yes | No (stale) | Yes | None |
| Database replication | Yes | Configurable | Limited | Partial |
| API data endpoints | No | Yes | Yes | Custom |
| Snowflake Data Sharing | No | Yes | Snowflake only | Full |

## Hands-On Walkthrough

Open the companion notebook `08-delta-sharing_notebook.py` in your Databricks
workspace. The notebook demonstrates:

1. Creating a Delta table suitable for sharing
2. Share creation and table addition (SQL syntax)
3. Recipient management commands
4. Querying shared data (simulated recipient side)

**Note**: Full Delta Sharing functionality requires a Unity Catalog-enabled
workspace. The notebook demonstrates the SQL syntax and concepts; actual
cross-organization sharing requires a proper UC setup.

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Delta Sharing | Supported | Supported | Supported |
| Unity Catalog required | Yes | Yes | Yes |
| Cross-cloud sharing | Yes (provider AWS -> recipient Azure) | Yes | Yes |
| Network path | S3 presigned URLs | ADLS SAS tokens | GCS signed URLs |
| Marketplace integration | Databricks Marketplace | Same | Same |

Delta Sharing is cloud-agnostic -- a provider on AWS can share with a recipient
on Azure or GCP. The protocol uses cloud-native presigned URLs for efficient
data transfer.

## Certification Tip

Delta Sharing questions on the exams tend to be conceptual:

- "What is Delta Sharing?" -- An open protocol for sharing live data without
  copying
- "Does data get copied to the recipient?" -- No, data stays in the provider's
  storage
- "What is required to set up Delta Sharing?" -- Unity Catalog
- "Can recipients write to shared data?" -- No, read-only
- "How is access revoked?" -- DROP RECIPIENT or REVOKE GRANT (immediate)
- "What is the difference between D2D and open sharing?" -- D2D is between
  Databricks workspaces with automatic UC integration; open sharing works with
  any platform via bearer tokens

This topic is more common on the Professional exam than the Associate.

## Key Takeaways

1. Delta Sharing is an open protocol for sharing live Delta Lake data without
   copying.
2. Shares contain tables and optional partition filters; recipients are granted
   access to specific shares.
3. Two modes: Databricks-to-Databricks (automatic UC integration) and open
   sharing (bearer token for any platform).
4. The provider retains full control -- access can be revoked instantly, and all
   reads are audited.
5. Unity Catalog is required; shared data is read-only for recipients.

## Next Steps

Congratulations -- you have completed Module 03! Proceed to
**[Module 04: Data Engineering Pipelines](../04-data-engineering-pipelines/)**
to learn how to build production ETL pipelines using Delta Live Tables and
Structured Streaming.
