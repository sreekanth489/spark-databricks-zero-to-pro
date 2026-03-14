# Day 00: Environment Setup

> Prerequisites | Level: Beginner | Time: 30-45 min

## Overview

Complete setup guide for running the labs in this repository. Covers two tracks:

- **Track A**: Databricks Free Edition -- sign up with Google/Microsoft/email, no cloud account needed
- **Track B**: Databricks on AWS with S3 -- full Unity Catalog, external locations, and S3 storage for production labs

## Guides

- [`00-databricks-free-setup.md`](00-databricks-free-setup.md) -- Databricks Free Edition signup and cluster creation
- [`00-databricks-aws-setup.md`](00-databricks-aws-setup.md) -- AWS account, S3 bucket, external locations, PAT token, Unity Catalog schemas

## Which Track Do I Need?

| Lab | Free Edition | AWS + S3 |
|-----|:------------:|:--------:|
| Day 01-17 (Spark, SQL, Delta basics) | Yes | Yes |
| Day 18 (Medallion Architecture) | Limited | **Required** |
| Day 19 (Structured Streaming) | Yes | Yes |
| Day 20 (Auto Loader) | Limited | **Required** |
| Day 21+ (DLT, Feature Store, etc.) | Limited | **Required** |

**Recommendation**: Start with Free Edition for basic labs, then set up AWS when you reach Day 18.
