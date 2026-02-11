# Setting Up Databricks Community Edition

> Free. No credit card. No cloud account required.

Databricks Community Edition is a free, limited version of the Databricks platform — perfect for learning Spark, Delta Lake, and the notebooks in this repository.

---

## Step 1: Create an Account

1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com/)
2. Click **Sign Up**
3. Fill in your name, email, and company (use "Student" or "Self-study" if applicable)
4. **Important**: On the cloud provider selection page, click **"Get started with Community Edition"** at the bottom — do NOT select AWS/Azure/GCP (those require paid cloud accounts)
5. Verify your email and log in

## Step 2: Create a Cluster

A cluster is the compute engine that runs your notebooks.

1. In the left sidebar, click **Compute**
2. Click **Create Cluster**
3. Give it a name (e.g., `learning-cluster`)
4. Leave all defaults — Community Edition provides a single-node cluster with:
   - 1 driver (no separate workers)
   - 15.3 GB memory
   - Databricks Runtime (latest LTS version)
5. Click **Create Cluster**
6. Wait 3–5 minutes for it to start (status changes to **Running**)

> **Note**: Community Edition clusters auto-terminate after 2 hours of inactivity. Just restart when needed.

## Step 3: Import Notebooks

See [importing-notebooks.md](importing-notebooks.md) for detailed instructions.

Quick version:
1. Click **Workspace** in the sidebar
2. Navigate to your user folder
3. Right-click → **Import**
4. Upload any `_notebook.py` file from this repository

## Step 4: Attach and Run

1. Open an imported notebook
2. Click the cluster dropdown at the top and select your running cluster
3. Run cells with **Shift+Enter** or click **Run All**

---

## Community Edition Limitations

| Feature | Community Edition | Full Databricks |
|---------|:-:|:-:|
| Single-node cluster | Yes | Multi-node |
| Max cluster runtime | 2 hrs idle timeout | Configurable |
| Unity Catalog | No | Yes |
| Workflows / Jobs | No | Yes |
| Auto Loader | Limited | Full |
| Delta Sharing | No | Yes |
| Repos / Git integration | Limited | Full |
| Cluster types | Single option | Multiple |

### What This Means for This Course

- **Modules 00–05**: Fully compatible with Community Edition
- **Modules 06–09**: Some features (Workflows, Unity Catalog) need descriptions but can't be run live — notebooks include simulated alternatives
- **Modules 10–11**: Projects work; certification prep is all compatible
- **Modules 20–22**: ML/AI features require full Databricks (notebooks explain alternatives)

---

## Troubleshooting

### Cluster won't start
- Community Edition has limited resources. Wait a few minutes and try again.
- Clear browser cache if the UI seems stuck.

### Cluster terminated unexpectedly
- Community Edition auto-terminates after 2 hours of inactivity.
- Click **Restart** on the Compute page.

### Import fails
- Ensure the file ends in `.py` and starts with `# Databricks notebook source`.
- Try the **URL** import option with the raw GitHub file URL.

### "Table or view not found" errors
- Run cells from top to bottom — earlier cells create the temp views.
- If you restarted the cluster, re-run the data generation cells.

---

## Next Steps

- [Import your first notebook](importing-notebooks.md)
- [Start with Module 00: Setup and Basics](../00-setup-basics/)
