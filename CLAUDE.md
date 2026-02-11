# CLAUDE.md — Spark Databricks Zero-to-Pro

## Project Conventions

### File Naming
- Module directories: `NN-kebab-case-name/` (e.g., `03-delta-lake-lakehouse/`)
- Conceptual guides: `NN-topic-name.md` (e.g., `01-delta-lake-fundamentals.md`)
- Notebooks: `NN-topic-name_notebook.py` (e.g., `01-delta-lake-fundamentals_notebook.py`)
- Every module has a `README.md` with table of contents, prerequisites, and time estimates

### Notebook Format
All `.py` notebooks use **Databricks source format**:
```python
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Title

# COMMAND ----------

# Code cell
spark.sql("SELECT 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Section
```

Key rules:
- First line must be `# Databricks notebook source`
- Cells separated by `# COMMAND ----------`
- Markdown cells use `# MAGIC %md` prefix on each line
- SQL cells use `# MAGIC %sql` prefix
- Shell cells use `# MAGIC %sh` prefix

### Content Structure
Each topic produces two files:
1. **Guide (`.md`)**: Theory, architecture diagrams (ASCII), cloud-specific notes, certification mapping, key takeaways
2. **Notebook (`_notebook.py`)**: Runnable code with inline explanations, generates its own sample data

### Self-Contained Notebooks
- Every notebook creates its own sample data — no dependency on prior notebooks
- Use `resources/data-generators/` utilities where applicable
- Only use libraries available in Databricks Runtime (pyspark, pandas, numpy)
- Clean up created tables/temp views at the end of each notebook

### Guide Template
```markdown
# Topic Title
> Module NN — Topic NN | Level: Beginner/Intermediate/Advanced | Time: X min

## Learning Objectives
## Conceptual Overview
## Hands-On Walkthrough (reference to notebook)
## Cloud Provider Notes (AWS / Azure / GCP table)
## Certification Tip
## Key Takeaways
## Next Steps
```

### Data Generators
- Located in `resources/data-generators/`
- `generator_utils.py` — shared utilities (timestamp ranges, ID generators, random helpers)
- `generate_ecommerce.py` — e-commerce domain (customers, orders, products, clickstream)
- All generators produce deterministic output when given a seed

## Build & Test
- No build step required — notebooks are imported directly into Databricks
- Verify notebook format: first line is `# Databricks notebook source`
- Verify markdown renders correctly on GitHub
- All internal links should be relative and valid
