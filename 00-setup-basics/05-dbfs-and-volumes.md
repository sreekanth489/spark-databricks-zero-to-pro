# DBFS and Volumes
> Module 00 — Topic 05 | Level: Beginner | Time: 30 min

## Learning Objectives
- Describe the DBFS architecture and how it abstracts cloud object storage
- Use dbutils.fs commands to list, read, write, copy, and remove files
- Understand DBFS root, DBFS mounts, and their security limitations
- Explain Unity Catalog Volumes (managed and external) and their advantages
- Plan a migration path from DBFS mounts to Unity Catalog Volumes
- Apply best practices for file storage in modern Databricks environments

## Conceptual Overview

### What Is DBFS?

DBFS (Databricks File System) is a distributed file system abstraction that sits
on top of cloud object storage. It allows you to interact with files using
familiar path syntax (`/mnt/data/file.csv`) regardless of the underlying cloud
provider.

```
  YOUR CODE
  +-- dbutils.fs.ls("/mnt/data/")
  +-- spark.read.csv("/mnt/data/sales.csv")
  +-- %sh ls /dbfs/mnt/data/
      |
      v
  DBFS LAYER (abstraction)
  +-- Translates paths to cloud-native URIs
  +-- Handles authentication transparently
  +-- Provides a POSIX-like interface
      |
      v
  CLOUD OBJECT STORAGE
  +-- AWS: s3://my-bucket/data/sales.csv
  +-- Azure: abfss://container@account.dfs.core.windows.net/data/sales.csv
  +-- GCP: gs://my-bucket/data/sales.csv
```

### DBFS Path Schemes

| Path Format | Description | Example |
|-------------|-------------|---------|
| `dbfs:/` | Default DBFS prefix | `dbfs:/FileStore/data.csv` |
| `/` (in dbutils.fs) | Shorthand for `dbfs:/` | `dbutils.fs.ls("/")` |
| `/dbfs/` (in shell/Python) | Local filesystem mount point | `open("/dbfs/FileStore/data.csv")` |
| `file:/` | Driver local filesystem | `file:/tmp/local_file.txt` |

Important distinctions:
- In `dbutils.fs` and Spark, paths like `/data/file.csv` refer to `dbfs:/data/file.csv`
- In `%sh` cells and native Python (`open()`), use `/dbfs/data/file.csv` to access DBFS
- `file:/` paths refer to the driver node's local disk (not distributed, not persistent)

### DBFS Root

Every workspace comes with a DBFS root — a small cloud storage location managed
by Databricks:

```
  DBFS ROOT (dbfs:/)
  +-- /FileStore/          <-- Used by display(), file uploads, plots
  |   +-- tables/          <-- Files uploaded via UI
  |   +-- plots/           <-- Matplotlib/plot images
  |
  +-- /databricks-datasets/ <-- Sample datasets provided by Databricks
  |   +-- COVID/
  |   +-- flights/
  |   +-- wine-quality/
  |
  +-- /user/               <-- Per-user storage
  +-- /tmp/                <-- Temporary files
  +-- /mnt/                <-- Mount points (external storage)
```

**Security warning**: DBFS root is accessible to all users in the workspace.
Do not store sensitive data in DBFS root. Use Unity Catalog Volumes instead.

### DBFS Mounts

Mounts create a mapping between a DBFS path and a cloud storage location:

```
  MOUNT SETUP

  dbutils.fs.mount(
      source = "s3://my-company-datalake/production/",
      mount_point = "/mnt/datalake",
      extra_configs = {"key": "value"}
  )

  RESULT:
  /mnt/datalake/        -->  s3://my-company-datalake/production/
  /mnt/datalake/raw/    -->  s3://my-company-datalake/production/raw/
  /mnt/datalake/curated -->  s3://my-company-datalake/production/curated/
```

Mount example (AWS):
```python
dbutils.fs.mount(
    source="s3://my-bucket/data",
    mount_point="/mnt/data",
    extra_configs={
        "fs.s3a.access.key": dbutils.secrets.get("aws", "access_key"),
        "fs.s3a.secret.key": dbutils.secrets.get("aws", "secret_key")
    }
)
```

Mount example (Azure):
```python
dbutils.fs.mount(
    source="abfss://container@storageaccount.dfs.core.windows.net/",
    mount_point="/mnt/data",
    extra_configs={
        "fs.azure.account.key.storageaccount.dfs.core.windows.net":
            dbutils.secrets.get("azure", "storage_key")
    }
)
```

**Problems with mounts** (why we are moving away from them):
- All users share the same credentials
- No fine-grained access control (a user can read everything under the mount)
- Credential rotation requires remounting
- No audit logging at the file level
- Workspace-level scope — not portable across workspaces

### dbutils.fs Commands Reference

| Command | Purpose | Example |
|---------|---------|---------|
| `ls(path)` | List directory contents | `dbutils.fs.ls("/data/")` |
| `head(path, maxBytes)` | Read first bytes of a file | `dbutils.fs.head("/data/file.csv", 1000)` |
| `put(path, content, overwrite)` | Write string to a file | `dbutils.fs.put("/tmp/test.txt", "hello", True)` |
| `cp(src, dst, recurse)` | Copy files | `dbutils.fs.cp("/src/", "/dst/", True)` |
| `mv(src, dst, recurse)` | Move files | `dbutils.fs.mv("/old/", "/new/", True)` |
| `rm(path, recurse)` | Delete files | `dbutils.fs.rm("/tmp/test.txt")` |
| `mkdirs(path)` | Create directories | `dbutils.fs.mkdirs("/data/output/")` |
| `mount(source, mount_point)` | Mount external storage | See examples above |
| `unmount(mount_point)` | Remove a mount | `dbutils.fs.unmount("/mnt/data")` |
| `mounts()` | List all mount points | `dbutils.fs.mounts()` |

### Unity Catalog Volumes

Unity Catalog Volumes are the modern replacement for DBFS root and DBFS mounts.
They provide governed, fine-grained access to files.

```
  UNITY CATALOG HIERARCHY

  Catalog
  +-- Schema (Database)
      +-- Tables          <-- structured data (Delta, Parquet)
      +-- Views           <-- virtual tables
      +-- Functions        <-- UDFs
      +-- Volumes          <-- unstructured / semi-structured FILES
          +-- Managed Volume  (Databricks manages the storage)
          +-- External Volume (you provide the storage location)
```

#### Managed Volumes

Databricks manages the underlying storage location:

```sql
CREATE VOLUME my_catalog.my_schema.raw_files;
```

- Files stored in a Databricks-managed cloud location
- Automatically cleaned up when the volume is dropped
- Path: `/Volumes/my_catalog/my_schema/raw_files/`

#### External Volumes

You provide the cloud storage location:

```sql
CREATE EXTERNAL VOLUME my_catalog.my_schema.landing_zone
LOCATION 's3://my-bucket/landing/';
```

- Databricks does not manage the storage lifecycle
- Dropping the volume removes the metadata, not the files
- Path: `/Volumes/my_catalog/my_schema/landing_zone/`

### Volumes vs. DBFS — Side-by-Side

```
  DBFS (Legacy)                        VOLUMES (Modern)
  +-----------------------------+      +-----------------------------+
  | Path: dbfs:/mnt/data/       |      | Path: /Volumes/cat/schema/  |
  | Auth: mount credentials     |      | Auth: Unity Catalog ACLs    |
  | Scope: workspace            |      | Scope: metastore (cross-WS) |
  | Audit: limited              |      | Audit: full audit logging   |
  | Governance: none            |      | Governance: UC permissions  |
  | Lifecycle: manual           |      | Lifecycle: managed (if mgd) |
  +-----------------------------+      +-----------------------------+
```

| Feature | DBFS Root | DBFS Mount | Managed Volume | External Volume |
|---------|-----------|------------|----------------|-----------------|
| Access control | Workspace-wide | Workspace-wide | UC GRANT/REVOKE | UC GRANT/REVOKE |
| Audit logging | Basic | Basic | Full | Full |
| Cross-workspace | No | No | Yes (shared metastore) | Yes (shared metastore) |
| Data lifecycle | Manual | Manual | Auto (drop = delete) | Manual |
| Credential mgmt | N/A | Mount config | Automatic | Storage credential |
| File path | `dbfs:/FileStore/` | `dbfs:/mnt/name/` | `/Volumes/c/s/v/` | `/Volumes/c/s/v/` |

### Migration from DBFS to Volumes

If you have existing DBFS mounts, here is the migration approach:

```
  MIGRATION PLAN

  Phase 1: Inventory
  +-- List all mounts: dbutils.fs.mounts()
  +-- Catalog files under each mount
  +-- Identify owners and downstream consumers

  Phase 2: Create Volumes
  +-- Create external volumes pointing to the SAME storage
  +-- Test reads from the new /Volumes/ paths

  Phase 3: Update Code
  +-- Search for dbfs:/mnt/ and /mnt/ references
  +-- Replace with /Volumes/catalog/schema/volume/ paths
  +-- Update Spark read/write paths in notebooks and jobs

  Phase 4: Validate
  +-- Run pipelines with new paths
  +-- Compare outputs to verify correctness

  Phase 5: Decommission Mounts
  +-- Remove mount points: dbutils.fs.unmount("/mnt/old")
  +-- Delete mount credential secrets
```

**Key insight**: If you create an external volume pointing to the same cloud
storage location as an existing mount, the data does not need to move. Only
the access path and governance layer change.

### Best Practices

1. **Use Unity Catalog Volumes for all new projects** — DBFS mounts are
   considered legacy.

2. **Never store sensitive data in DBFS root** — it is accessible to all
   workspace users.

3. **Use managed volumes for temporary or intermediate files** — Databricks
   handles storage lifecycle.

4. **Use external volumes for data shared across systems** — when external
   tools also need access to the storage location.

5. **Set up storage credentials and external locations in Unity Catalog** —
   this is the secure, auditable way to connect cloud storage.

6. **Avoid writing to `/dbfs/` via `%sh` or `open()`** — use `dbutils.fs` or
   Spark instead for proper distributed access.

7. **Use `/Volumes/` paths in production code** — they are portable across
   workspaces that share a metastore.

## Hands-On Walkthrough

Import the companion notebook `05-dbfs-and-volumes_notebook.py` into your
workspace. The notebook demonstrates:

1. Listing DBFS root contents
2. Creating and reading files with `dbutils.fs`
3. Exploring the `/databricks-datasets/` sample data
4. Understanding the difference between `dbfs:/` and `/dbfs/` paths
5. Checking Volume paths (if Unity Catalog is enabled)

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| DBFS root storage | S3 (Databricks-managed) | ADLS Gen2 (Databricks-managed) | GCS (Databricks-managed) |
| Mount source | `s3://bucket` | `abfss://container@account` | `gs://bucket` |
| External volume storage | S3 | ADLS Gen2 | GCS |
| Storage credential type | IAM Role | Service Principal / Managed Identity | Service Account |
| Cross-region volumes | Not recommended | Not recommended | Not recommended |
| FUSE mount path | `/dbfs/` | `/dbfs/` | `/dbfs/` |

## Certification Tip

The **Data Engineer Associate** exam covers:
- Understanding DBFS and how to use `dbutils.fs` commands
- Knowing that `dbfs:/` is the default scheme in Spark/dbutils
- Recognizing the `/Volumes/` path format for Unity Catalog Volumes
- Understanding the difference between managed and external tables/volumes

The **Professional** exam adds:
- Designing storage architectures with external locations and storage credentials
- Planning migrations from DBFS mounts to Unity Catalog Volumes
- Implementing fine-grained access control with GRANT/REVOKE on volumes
- Understanding how volumes interact with cluster access modes

Key exam pattern: "Which path should you use to access governed file storage?"
Answer: `/Volumes/<catalog>/<schema>/<volume>/` (not `dbfs:/mnt/`).

## Key Takeaways

- DBFS is a file system abstraction layer over cloud object storage
- `dbutils.fs` provides commands for listing, reading, writing, copying, and deleting files
- DBFS root (`dbfs:/`) is workspace-wide and not secure for sensitive data
- DBFS mounts map cloud storage to simple paths but lack fine-grained governance
- Unity Catalog Volumes are the modern, governed replacement for DBFS mounts
- Managed volumes have Databricks-managed storage; external volumes point to your storage
- The `/Volumes/catalog/schema/volume/` path format is portable across workspaces
- Migration from mounts to volumes can be done without moving data (use external volumes)

## Next Steps

Congratulations — you have completed Module 00! You now have a solid foundation
in the Databricks platform. Proceed to **Module 01: DataFrames and
Transformations** to start working with Spark DataFrames.
