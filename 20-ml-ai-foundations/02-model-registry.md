# Model Registry
> Module 20 -- Topic 02 | Level: Intermediate | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain the model lifecycle: development, staging, production, archived
2. Register a trained model from an MLflow run into the Model Registry
3. Manage model versions and promote models through lifecycle stages
4. Use aliases (Champion/Challenger) for deployment-friendly references
5. Compare the workspace-level registry with the Unity Catalog model registry
6. Implement approval workflows for model promotion
7. Load models from the registry by name, version, or alias

---

## Conceptual Overview

### Why a Model Registry?

Training a good model is only half the battle. In production, you need answers to
questions like:
- Which model version is currently serving traffic?
- Who approved the promotion from staging to production?
- Can we roll back to the previous version if the new one degrades?
- What experiment and run produced this model?

The Model Registry provides a centralized catalog that answers all of these. It
sits between experiment tracking (Topic 01) and model serving (Topic 05).

```
  ML Lifecycle with Model Registry
  =================================

  Training                    Registry                      Serving
  +-----------------+      +------------------+      +------------------+
  | MLflow Tracking |      | Model Registry   |      | Model Serving    |
  | Experiment Runs | ---> | Versions/Aliases | ---> | REST Endpoints   |
  +-----------------+      +------------------+      +------------------+
       |                         |                         |
  parameters              version 1: v1                real-time
  metrics                 version 2: Champion           predictions
  artifacts               version 3: Challenger         A/B testing
```

### Model Lifecycle Stages

The workspace-level registry uses four stages:

```
  Model Version Lifecycle (Workspace Registry)
  =============================================

  +--------+     +---------+     +------------+     +----------+
  | None   | --> | Staging | --> | Production | --> | Archived |
  +--------+     +---------+     +------------+     +----------+
       |              |                |                  |
   Just registered   Testing &      Serving live       Retired,
   Not yet reviewed  validation     traffic            kept for
                                                       audit trail
```

The Unity Catalog registry replaces stages with **aliases** -- a more flexible
approach:

```
  Model Aliases (Unity Catalog Registry)
  =======================================

  Model: catalog.schema.churn_predictor
  |
  +-- Version 1 ........................ (no alias)
  +-- Version 2 .... alias: "Challenger"
  +-- Version 3 .... alias: "Champion"   <-- serving traffic
  +-- Version 4 ........................ (no alias, in development)

  Aliases are mutable labels. Move "Champion" to a new version
  to promote it, without changing any serving endpoint configuration.
```

### Workspace Registry vs Unity Catalog Registry

| Feature | Workspace Registry | Unity Catalog Registry |
|---------|-------------------|----------------------|
| Namespace | `model_name` | `catalog.schema.model_name` |
| Lifecycle | Stages (None/Staging/Production/Archived) | Aliases (Champion, Challenger, custom) |
| Governance | Workspace-level ACLs | Unity Catalog permissions |
| Cross-workspace | Not supported | Supported via catalog sharing |
| Model lineage | Basic (run link) | Full (catalog-level lineage) |
| Status | Legacy (still supported) | Recommended |
| API | `mlflow.register_model()` | `mlflow.register_model()` with UC URI |

### Registering a Model

From an MLflow run:

```python
# Option 1: Register during logging
mlflow.sklearn.log_model(
    model, "model",
    registered_model_name="churn_predictor"
)

# Option 2: Register after the run
result = mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name="churn_predictor"
)
print(f"Registered version: {result.version}")
```

With Unity Catalog:

```python
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model(
    model_uri=f"runs:/{run_id}/model",
    name="catalog.schema.churn_predictor"
)
```

### Version Management

Each registration creates a new version automatically. You never overwrite an
existing version -- the registry is append-only for audit purposes.

```python
client = mlflow.tracking.MlflowClient()

# List all versions
versions = client.search_model_versions("name='churn_predictor'")
for v in versions:
    print(f"Version {v.version}: stage={v.current_stage}, run={v.run_id}")

# Transition a version to production (workspace registry)
client.transition_model_version_stage(
    name="churn_predictor",
    version=3,
    stage="Production"
)
```

### Loading Models from the Registry

```python
# By version number
model = mlflow.sklearn.load_model("models:/churn_predictor/3")

# By stage (workspace registry)
model = mlflow.sklearn.load_model("models:/churn_predictor/Production")

# By alias (Unity Catalog registry)
model = mlflow.sklearn.load_model("models:/catalog.schema.churn_predictor@Champion")
```

### Approval Patterns

In production environments, model promotion should not be ad-hoc. Common patterns:

```
  Approval Workflow
  ==================

  Data Scientist         ML Engineer           Platform Team
       |                      |                      |
  Train model            Review metrics         Monitor endpoint
  Register v4            Validate in staging    Check latency/errors
       |                      |                      |
       +----> Request ------->+                      |
              promotion       |                      |
                         Run validation        Approve / reject
                         tests in staging            |
                              |                      |
                         Promote to ------> Champion alias
                         production          (automatic serving)
```

Databricks supports webhooks on model registry events so you can trigger
validation pipelines when a new version is registered.

---

## Hands-On Walkthrough

Open `02-model-registry_notebook.py` to practice:
- Registering models from MLflow runs
- Creating multiple model versions
- Transitioning versions through lifecycle stages
- Setting and using aliases
- Loading models by name, version, and alias
- Comparing model versions side-by-side

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Workspace registry | Supported | Supported | Supported |
| Unity Catalog registry | Supported | Supported | Supported |
| Cross-workspace sharing | Via UC catalog sharing | Via UC catalog sharing | Via UC catalog sharing |
| Webhooks | REST callbacks | REST callbacks | REST callbacks |
| Model registry UI | Sidebar in workspace | Sidebar in workspace | Sidebar in workspace |

---

## Certification Tip

> **Databricks ML Professional**: Know the difference between workspace and Unity
> Catalog model registries. Understand model version transitions and how aliases
> replace stages in the UC registry. Expect questions about loading models with
> `models:/name/version` vs `models:/name@alias` URI syntax.
>
> **Key concept**: In Unity Catalog, models are governed objects with full lineage.
> The three-level namespace (`catalog.schema.model`) integrates with the same
> permission system used for tables and volumes.

---

## Key Takeaways

1. **The Model Registry** is the bridge between experimentation and production. It
   provides versioning, lifecycle management, and an audit trail.
2. **Unity Catalog registry** is the recommended approach. It uses aliases instead
   of stages and integrates with Databricks governance.
3. **Aliases** (Champion, Challenger) are mutable labels that decouple model identity
   from deployment configuration. Move the alias to promote a model.
4. **Every registration creates a new version**. The registry is append-only -- you
   never lose a previous version.
5. **Load models by alias**, not version number, in production code. This lets you
   promote models without changing serving configurations.
6. **Approval workflows** combine webhooks, validation pipelines, and manual review
   to ensure only vetted models reach production.

---

## Next Steps

- Proceed to **Topic 03: Feature Store** to learn how features are managed and
  served consistently between training and inference.
- Register your best model from Topic 01 and practice version management.
