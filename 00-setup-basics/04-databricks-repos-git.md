# Databricks Repos & Git Integration
> Module 00 — Topic 04 | Level: Beginner | Time: 25 min

## Learning Objectives
- Understand how Databricks Repos integrates with Git providers
- Connect a Databricks workspace to a remote Git repository
- Perform branching, committing, and pulling from the Databricks UI
- Configure .gitignore and understand what gets tracked
- Use the Repos API for programmatic Git operations
- Apply best practices for notebook version control

## Conceptual Overview

### Why Git Integration Matters

Without version control, notebook development suffers from:
- No audit trail of changes
- Difficulty collaborating across team members
- No code review process
- No reliable rollback mechanism
- Environment drift between development and production

Databricks Repos solves these problems by embedding Git directly into the
workspace.

### How Databricks Repos Works

```
  GIT PROVIDER (GitHub / Azure DevOps / GitLab / Bitbucket)
  +-----------------------------------------------------------+
  |  Remote Repository: https://github.com/org/data-project   |
  |  Branches: main, feature/new-pipeline, bugfix/null-check  |
  +----------------------------+------------------------------+
                               |
                    clone / pull / push
                               |
  +----------------------------v------------------------------+
  |  DATABRICKS WORKSPACE — Repos Section                     |
  |                                                           |
  |  /Repos/user@company.com/data-project/                    |
  |  +-----------------------------------------------------+  |
  |  |  notebooks/                                          |  |
  |  |    etl_pipeline.py          <-- notebook (source fmt)|  |
  |  |    data_quality_checks.sql  <-- SQL notebook         |  |
  |  |  src/                                                |  |
  |  |    utils.py                 <-- plain Python module  |  |
  |  |    config.yaml              <-- config file          |  |
  |  |  tests/                                              |  |
  |  |    test_utils.py            <-- test file            |  |
  |  |  .gitignore                                          |  |
  |  |  README.md                                           |  |
  |  +-----------------------------------------------------+  |
  +-----------------------------------------------------------+
```

### Supported Git Providers

| Provider | Supported | Authentication |
|----------|-----------|----------------|
| GitHub | Yes | Personal access token (PAT) or GitHub App |
| Azure DevOps | Yes | Azure AD token or PAT |
| GitLab | Yes | Personal access token |
| Bitbucket | Yes | App password |
| AWS CodeCommit | Yes | Git credentials |
| Self-hosted Git | Yes (Enterprise) | PAT or SSH |

### Setting Up Git Integration

#### Step 1: Configure Git Credentials

Navigate to **User Settings > Git Integration** (or **Linked Accounts**):

```
  User Settings
  +-- Git Integration
      +-- Git provider:  [GitHub        v]
      +-- Username:      [your-username  ]
      +-- Token:         [ghp_xxxxxxxxxx ]
      [Save]
```

#### Step 2: Clone a Repository

```
  Workspace sidebar
  +-- Repos
      +-- [Add Repo]
          +-- URL: https://github.com/org/project.git
          +-- Provider: GitHub (auto-detected)
          +-- Name: project
          [Create Repo]
```

#### Step 3: Work with Branches

```
  BRANCHING WORKFLOW IN DATABRICKS REPOS

  main ─────────────────────────────────────────────
       \                                     /
        \  (create branch)                  / (merge via PR)
         \                                 /
          feature/add-etl ────────────────
                 |         |          |
              (commit 1) (commit 2) (push)
```

From the Repos UI, you can:
1. **Create a branch** — click the branch name, type a new name, select "Create branch"
2. **Switch branches** — click the branch name and select an existing branch
3. **Commit changes** — click the Git status icon, stage files, write a message, commit
4. **Push** — push committed changes to the remote
5. **Pull** — pull remote changes into your local copy

### What Gets Tracked in Git

Databricks Repos stores notebooks in **source format** (not `.ipynb`):

| File Type in Workspace | Stored in Git As | Format |
|------------------------|------------------|--------|
| Python notebook | `name.py` | Databricks source (# Databricks notebook source) |
| SQL notebook | `name.sql` | SQL statements with `-- Databricks notebook source` |
| Scala notebook | `name.scala` | Scala source with `// Databricks notebook source` |
| R notebook | `name.r` | R source with `# Databricks notebook source` |
| Plain Python file | `name.py` | Standard Python (no magic header) |
| Any other file | `name.ext` | As-is (YAML, JSON, TXT, etc.) |

Databricks distinguishes between notebooks and plain files by the presence of
the `# Databricks notebook source` header.

### .gitignore in Databricks Repos

Create a `.gitignore` file in the root of your repo to exclude files:

```gitignore
# Databricks artifacts
.databricks/
*.egg-info/
dist/
build/

# Python artifacts
__pycache__/
*.pyc
.pytest_cache/

# Data files (do not commit data to Git)
*.csv
*.parquet
*.json
data/

# Environment files
.env
*.pem
```

**Important**: Databricks Repos respects `.gitignore` when showing the commit
dialog. Files matching ignore patterns will not appear as candidates for staging.

### Repos Folder Structure Best Practices

```
  my-project/
  +-- notebooks/               <-- Databricks notebooks (source format)
  |   +-- 01_ingest.py
  |   +-- 02_transform.py
  |   +-- 03_publish.py
  |
  +-- src/                     <-- Reusable Python modules
  |   +-- __init__.py
  |   +-- utils.py
  |   +-- validators.py
  |
  +-- tests/                   <-- Unit tests
  |   +-- test_utils.py
  |   +-- test_validators.py
  |
  +-- configs/                 <-- Configuration files
  |   +-- dev.yaml
  |   +-- prd.yaml
  |
  +-- .gitignore
  +-- README.md
  +-- requirements.txt
```

Benefits of this structure:
- Notebooks can import from `src/` using `import src.utils`
- Tests can run on the cluster or in CI/CD
- Configs separate environment-specific values from code

### Importing Python Modules from Repos

When a notebook is inside a Repo, it can import plain Python files:

```python
# Notebook at: /Repos/user/project/notebooks/etl.py
# Module at:   /Repos/user/project/src/utils.py

# Option 1: Add the repo root to sys.path
import sys
sys.path.append("/Workspace/Repos/user/project")
from src.utils import clean_data

# Option 2: Use relative import with %run (not recommended for large projects)
# %run ../src/utils
```

### The Repos API

For automation (CI/CD pipelines, workspace setup), use the Repos REST API:

```bash
# List repos for the current user
curl -X GET \
  "https://<workspace-url>/api/2.0/repos" \
  -H "Authorization: Bearer <token>"

# Create (clone) a repo
curl -X POST \
  "https://<workspace-url>/api/2.0/repos" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "url": "https://github.com/org/project.git",
    "provider": "github",
    "path": "/Repos/user@company.com/project"
  }'

# Update (pull or switch branch)
curl -X PATCH \
  "https://<workspace-url>/api/2.0/repos/<repo-id>" \
  -H "Authorization: Bearer <token>" \
  -d '{"branch": "main"}'
```

### Limitations

| Limitation | Detail |
|------------|--------|
| **Repo size** | Max 10 GB per repo (recommended < 500 MB) |
| **File count** | Max 10,000 files per repo |
| **File size** | Single file max 10 MB |
| **Merge conflicts** | Must be resolved externally (Git CLI or provider UI) |
| **Submodules** | Not supported |
| **Git LFS** | Not supported |
| **Interactive rebase** | Not available in Databricks UI |
| **Webhooks** | Use CI/CD pipelines to trigger sync |

### Best Practices for Notebook Version Control

1. **Use source format** — Databricks Repos stores notebooks in source format
   by default, which diffs well in Git.

2. **Keep notebooks focused** — one notebook per logical step (ingest, transform,
   publish) rather than monolithic notebooks.

3. **Extract reusable code** — move utility functions into plain `.py` files in
   `src/` and import them.

4. **Use feature branches** — never commit directly to `main`. Create a feature
   branch, make changes, push, and open a pull request.

5. **Code review notebooks** — since notebooks are stored as text files, standard
   pull request reviews work well.

6. **Do not store data in Git** — use `.gitignore` to exclude CSV, Parquet, and
   other data files. Store data in cloud storage.

7. **Pin library versions** — use `requirements.txt` or `%pip install lib==x.y.z`
   to ensure reproducibility.

8. **Use CI/CD** — set up GitHub Actions or Azure Pipelines to run tests,
   linting, and automated deployment when PRs are merged.

## Hands-On Walkthrough

Import the companion notebook `04-databricks-repos-git_notebook.py` into your
workspace. The notebook demonstrates:

1. Exploring the Repos API with dbutils
2. Running a child notebook with `dbutils.notebook`
3. Understanding the workspace file system layout
4. Checking the current notebook context

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Preferred Git provider | GitHub | Azure DevOps | GitHub or GitLab |
| SSO for Git | SAML via Okta/OneLogin | Azure AD | Google Identity |
| CI/CD integration | GitHub Actions | Azure Pipelines | Cloud Build |
| Repos API | Same across all | Same across all | Same across all |
| Secret storage for PATs | AWS Secrets Manager | Azure Key Vault | GCP Secret Manager |

## Certification Tip

The **Data Engineer Associate** exam covers:
- Understanding that Repos enables Git-based version control for notebooks
- Knowing that notebooks are stored in source format (not `.ipynb`)
- Recognizing the role of Repos in CI/CD workflows

The **Professional** exam adds:
- Automating Repos operations via the REST API
- Designing multi-environment promotion workflows (dev -> stg -> prd)
- Understanding how Repos interacts with Unity Catalog permissions

## Key Takeaways

- Databricks Repos provides native Git integration directly in the workspace
- Supported providers include GitHub, Azure DevOps, GitLab, and Bitbucket
- Notebooks are stored in source format, which produces clean Git diffs
- Use feature branches and pull requests — never commit directly to main
- The Repos REST API enables CI/CD automation
- Keep repos under 500 MB; do not store data files in Git
- Extract reusable code into plain Python modules and import them from notebooks
- Merge conflicts must be resolved outside Databricks (in Git CLI or provider UI)

## Next Steps

Proceed to [05 — DBFS and Volumes](05-dbfs-and-volumes.md) to learn how
Databricks manages file storage and how Unity Catalog Volumes modernize the
approach.
