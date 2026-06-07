# Bundle CLI Quick Reference

A concise cheat sheet for the most common Databricks Asset Bundle commands.

## Authentication

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks auth describe
```

## Full Lifecycle

```bash
# 1. Validate YAML — no API calls, instant
databricks bundle validate
databricks bundle validate --target staging

# 2. Deploy resources to workspace
databricks bundle deploy
databricks bundle deploy --target staging
databricks bundle deploy --target prod

# 3. Run a job or pipeline
databricks bundle run ecommerce_medallion_pipeline
databricks bundle run ecommerce_medallion_pipeline --target staging
databricks bundle run ecommerce_medallion_pipeline --task ingest_bronze

# 4. Check deployed resources and their workspace URLs
databricks bundle summary
databricks bundle summary --target prod

# 5. Tear down all bundle-managed resources
databricks bundle destroy --target dev
```

## Override Variables at Deploy Time

```bash
databricks bundle deploy --target dev --var="catalog=my_catalog"
databricks bundle deploy --target dev --var="schedule_pause_status=UNPAUSED"
```

## Useful Flags

| Flag | Purpose |
|------|---------|
| `--target <name>` | Select environment (dev / staging / prod) |
| `--var="key=value"` | Override a variable for this run only |
| `--auto-approve` | Skip confirmation prompt (for CI use) |
| `--output json` | Output resolved bundle as JSON |
