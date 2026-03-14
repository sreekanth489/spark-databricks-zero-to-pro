# Databricks on AWS Setup

> Complete setup for running production labs with Unity Catalog, S3 storage, and external locations.

---

## Prerequisites

- An AWS account (free tier works for S3)
- A Databricks account (free trial or paid)
- Admin permissions in both AWS and Databricks

---

## Part 1: Create an AWS Account

If you don't have an AWS account:

1. Go to [aws.amazon.com](https://aws.amazon.com/) and click **Create an AWS Account**
2. Enter your email, password, and account name
3. Choose **Personal** account type
4. Enter payment information (required, but S3 free tier covers 5 GB for 12 months)
5. Verify your phone number
6. Select the **Basic Support (Free)** plan
7. Sign in to the [AWS Management Console](https://console.aws.amazon.com)

### Create an S3 Bucket

1. In the AWS Console, search for **S3** and open the S3 service
2. Click **Create bucket**
3. Enter a bucket name (e.g., `databricks-zero-to-pro`)
4. Select your preferred region (e.g., `us-east-1`)
5. Leave all other settings as default (Block all public access = ON)
6. Click **Create bucket**

**Remember your bucket name and region** -- you'll need them later.

---

## Part 2: Create a Databricks Workspace on AWS

### Option A: Databricks Free Trial (14 days Premium)

1. Go to [databricks.com/try-databricks](https://www.databricks.com/try-databricks)
2. Click **Start Free Trial**
3. Select **AWS** as the cloud provider
4. Follow the guided setup to link your AWS account
5. The trial gives you 14 days of Premium features (Unity Catalog, external locations, Auto Loader)

### Option B: Databricks Free Edition + Separate AWS Account

If you already have the Free Edition (see [00-databricks-free-setup.md](00-databricks-free-setup.md)), you can upgrade to a trial to unlock Unity Catalog and AWS integration. Click **"Get started for free"** on the Free Edition page for the full Data Intelligence Platform.

---

## Part 3: Create a Compute Cluster

1. In your Databricks workspace, click **Compute** in the left sidebar
2. Click **Create Compute**
3. Configure:
   - **Name**: `learning-cluster`
   - **Databricks Runtime**: Latest LTS (e.g., 15.x LTS)
   - **Node Type**: `i3.xlarge` or smallest available
   - **Workers**: 0 (single-node mode for labs)
   - **Auto-terminate**: 60 minutes
4. Click **Create Compute**
5. Wait 3-5 minutes for the cluster to start

---

## Part 4: Generate a Personal Access Token (PAT)

You need a PAT to authenticate Databricks with AWS CloudFormation when creating external locations.

1. In Databricks, click your **profile icon** (top-right corner)
2. Click **Settings**
3. In the left sidebar, click **Developer** > **Access Tokens**
4. Click **Manage** > **Generate new token**
5. Enter a comment (e.g., `AWS External Location Setup`)
6. Set lifetime (e.g., 90 days)
7. Click **Generate**
8. **Copy the token immediately** -- you won't be able to see it again

> **Security**: Never share your PAT token. Never commit it to version control. Regenerate it if you suspect it has been exposed.

---

## Part 5: Create an External Location (AWS Quickstart)

An external location connects your Databricks workspace to your S3 bucket, allowing Unity Catalog to read/write data in S3.

### Step 1: Navigate to External Locations

1. In Databricks, click **Catalog** in the left sidebar
2. Click **External Data** at the top
3. Click the **External Locations** tab
4. Click **Create external location**

![External Locations page in Databricks Catalog Explorer](images/external_location.png)

### Step 2: Choose Quickstart

You'll see two options:

| Option | When to Use |
|--------|-------------|
| **AWS Quickstart (Recommended)** | Creates external location + IAM role automatically via CloudFormation |
| **Manual** | For advanced users who already have a storage credential or need custom IAM |

Select **AWS Quickstart**.

### Step 3: Enter Bucket Name and PAT Token

1. Enter your **S3 bucket name** in the Bucket Name field (e.g., `s3://databricks-zero-to-pro`)
2. Paste the **Personal Access Token** you generated in Part 4

![Quickstart form: enter S3 bucket name and PAT token](images/quickstart-bucket-pat.png)

3. Click **Launch in Quickstart**

### Step 4: Create the CloudFormation Stack in AWS

Clicking "Launch in Quickstart" redirects you to the **AWS CloudFormation Console** with a pre-configured template. This template automatically creates:

| AWS Resource | Purpose |
|-------------|---------|
| **IAM Role** | Allows Databricks to assume role and access your S3 bucket |
| **IAM Policy** | Grants S3 permissions: GetObject, PutObject, DeleteObject, ListBucket, GetBucketNotification, PutBucketNotification |
| **Trust Policy** | Allows the Databricks control plane to assume the IAM role |

**Steps in the AWS Console:**

1. You'll be redirected to the AWS CloudFormation console
2. The template is pre-filled with:
   - Your Databricks workspace details
   - S3 bucket path
   - PAT token (for authentication during setup)
3. **Review the parameters** -- ensure the bucket name is correct
4. Scroll to the bottom
5. **Check the acknowledgment box**: *"I acknowledge that AWS CloudFormation might create IAM resources with custom names"*
6. Click **Create stack**
7. Wait 2-3 minutes for the stack status to change to `CREATE_COMPLETE`

### Step 5: Verify External Location

Once the CloudFormation stack completes, go back to Databricks:

1. Navigate to **Catalog** > **External Data** > **External Locations**
2. You should see your new external location with:
   - **URL**: `s3://your-bucket-name/`
   - **Credential**: Auto-created IAM role credential
3. Click on the location to verify connectivity

### Step 6: Verify Storage Credentials

1. Click the **Credentials** tab in External Data
2. You should see the auto-created credential with:
   - **Credential Type**: IAM Role
   - **IAM Role ARN**: `arn:aws:iam::YOUR_ACCOUNT:role/databricks-...`
   - **Purpose**: STORAGE

![Storage credentials showing IAM Role](images/Credentials.png)

---

## Part 6: Create Unity Catalog Schemas

The Medallion Architecture labs use separate schemas per layer:

```sql
USE CATALOG databricks_pro;  -- replace with your catalog name

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw, unfiltered data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed, validated data';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business-ready aggregations';
```

---

## Part 7: Test Your Setup

Run this in a Databricks notebook to verify everything works:

```python
# Test S3 write access
test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
test_df.write.format("delta").mode("overwrite").save("s3://databricks-zero-to-pro/test_setup")
print("S3 WRITE: OK")

# Test S3 read access
result = spark.read.format("delta").load("s3://databricks-zero-to-pro/test_setup")
result.display()
print("S3 READ: OK")

# Cleanup
dbutils.fs.rm("s3://databricks-zero-to-pro/test_setup", recurse=True)
print("CLEANUP: OK")
```

If all three succeed, your setup is complete.

---

## Part 8: Enable File Events (Optional -- for Auto Loader Day 20)

If you plan to use Auto Loader with managed file events:

```sql
-- Enable file events on your external location
ALTER EXTERNAL LOCATION your_location_name ENABLE FILE EVENTS;
```

This allows Auto Loader to use `cloudFiles.useManagedFileEvents = true` for near-real-time file detection. See [Day 20: Auto Loader](../day20-auto-loader/) for details.

---

## Troubleshooting

### CloudFormation stack fails

| Issue | Fix |
|-------|-----|
| Timeout / stuck in CREATING | Check your AWS region matches S3 bucket region |
| Permission denied | You need AWS admin permissions to create IAM roles |
| Invalid PAT token | Generate a new token in Databricks and retry |

### "Access Denied" when writing to S3

- Verify the external location points to the correct S3 path
- Check that the CloudFormation-created IAM role has S3 permissions
- Ensure the S3 bucket exists and is in the expected region

### External location not showing up

- The CloudFormation stack may still be creating -- wait 2-3 minutes and refresh
- Check the CloudFormation stack status in the AWS Console for errors

### Auto Loader notification errors

| Error | Fix |
|-------|-----|
| `PermanentRedirect` | Set `cloudFiles.region` to match S3 bucket region |
| `GetBucketNotification AccessDenied` | IAM role needs `s3:GetBucketNotification` permission |
| "no matching external location" | Create external location and enable file events |

---

## Quick Reference

| Item | Value |
|------|-------|
| Databricks Console | Your workspace URL |
| AWS Console | [console.aws.amazon.com](https://console.aws.amazon.com) |
| S3 Bucket | `s3://databricks-zero-to-pro` (replace with yours) |
| Unity Catalog | `databricks_pro` (your catalog name) |
| Schemas | `bronze`, `silver`, `gold` |

---

## What's Next?

- [Day 18: Medallion Architecture](../day18-medallion-architecture/) -- Bronze/Silver/Gold pipeline on S3
- [Day 19: Structured Streaming](../day19-structured-streaming/) -- streaming engine fundamentals
- [Day 20: Auto Loader](../day20-auto-loader/) -- optimized file ingestion from S3
