# Day 00: Environment Setup

> Prerequisites | Level: Beginner | Time: 30-45 min

## Overview

Complete setup guide for running the labs in this repository. Two options:

### Option 1: Databricks Free Edition (Start Here)

Sign up with Gmail/Microsoft/email. No cloud account needed. No credit card.
Databricks now provides **serverless compute** -- no cluster creation required.

See [`00-databricks-free-setup.md`](00-databricks-free-setup.md)

### Option 2: Databricks Premium on AWS/Azure (Production Labs)

Create a Databricks workspace on your own cloud account (AWS or Azure).
Required for Unity Catalog, external locations, S3/ADLS storage, and Auto Loader.

See [`00-databricks-cloud-setup.md`](00-databricks-cloud-setup.md)

## Which Option Do I Need?

| Lab | Free Edition | Premium (AWS/Azure) |
|-----|:------------:|:-------------------:|
| Day 01-17 (Spark, SQL, Delta basics) | Yes | Yes |
| Day 18 (Medallion Architecture with S3) | Limited | **Required** |
| Day 19 (Structured Streaming) | Yes | Yes |
| Day 20 (Auto Loader with S3) | Limited | **Required** |
| Day 21+ (DLT, Feature Store, etc.) | Limited | **Required** |

**Recommendation**: Start with **Free Edition** -- it covers everything through Day 17 and gives you serverless compute. Set up a cloud account when you reach Day 18.
