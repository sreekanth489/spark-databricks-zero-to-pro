# Day 29: Lakehouse Federation

> Module: Data Governance | Level: Intermediate | Time: 90 min

## Learning Objectives

- Explain what Lakehouse Federation is and why it eliminates data copies
- Connect Databricks to external databases (Snowflake, MySQL, PostgreSQL, Redshift, SQL Server, Synapse)
- Query federated sources using standard SQL with Unity Catalog governance
- Understand pushdown optimizations and when federation outperforms ETL
- Apply UC access controls (GRANT/REVOKE) to federated foreign tables
- Decide when to federate vs when to ingest

## Key Concepts

- **Lakehouse Federation** — query external databases live from Databricks, no data copy, no ETL
- **Connection** — credential object at the metastore level; points to an external system
- **Foreign Catalog** — maps a remote database into the UC 3-level namespace
- **Foreign Table** — auto-discovered table in a foreign catalog; reads live from source
- **Pushdown Optimization** — WHERE filters, GROUP BY, LIMIT pushed to source system
- **Single Governance Point** — same GRANT/REVOKE for Delta tables AND external databases

## Supported Sources

| Source | Status |
|--------|--------|
| MySQL, PostgreSQL, Redshift, Snowflake, SQL Server, Synapse, Databricks | GA |
| BigQuery, Hive, AWS Glue | Preview |
| Teradata, Oracle, Salesforce | Roadmap |

## Topics Covered

- The copy problem: why having data in multiple systems is painful
- Federation architecture: Connection → Foreign Catalog → Foreign Table
- Creating connections with secure credential references (Databricks Secrets)
- Auto-discovery of schemas and tables from external sources
- Querying foreign tables with standard SQL
- Cross-source joins: Snowflake + PostgreSQL + local Delta in one query
- Pushdown optimization and how to verify it with EXPLAIN
- Governing foreign catalogs with GRANT/REVOKE
- Federation vs Ingestion decision guide

## Hands-On

- **Guide**: [`29-lakehouse-federation.md`](29-lakehouse-federation.md) — theory, architecture, connection examples for all GA sources
- **Notebook**: [`29-lakehouse-federation_notebook.py`](29-lakehouse-federation_notebook.py) — CREATE CONNECTION, FOREIGN CATALOG, cross-source joins, governance

## Certification Tip

The **Databricks Certified Data Engineer Professional** exam tests:
- What Lakehouse Federation is (query without copying)
- The three objects: Connection, Foreign Catalog, Foreign Table
- Governance model for foreign catalogs (same as regular UC)
- Pushdown optimization concept
- When to federate vs when to ingest

## Next Steps

- [Day 30: Delta Sharing](../day30-delta-sharing/) — share data externally across orgs and clouds
