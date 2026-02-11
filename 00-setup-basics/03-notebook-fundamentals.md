# Notebook Fundamentals
> Module 00 — Topic 03 | Level: Beginner | Time: 30 min

## Learning Objectives
- Use all cell types in a Databricks notebook (Python, SQL, Markdown, Shell, R, Scala)
- Master magic commands including %run, %pip, %sh, and language switches
- Create interactive widgets for parameterized notebooks
- Use display(), displayHTML(), and Markdown rendering for rich output
- Navigate dbutils and understand its major modules
- Apply essential keyboard shortcuts for productive notebook editing

## Conceptual Overview

### What Is a Databricks Notebook?

A Databricks notebook is a collaborative, cell-based document that supports
multiple programming languages in the same file. It is the primary interface
for interactive development on Databricks.

```
  NOTEBOOK ARCHITECTURE
  +===========================================================+
  |  Notebook: "my_analysis"          Default language: Python  |
  +===========================================================+
  |  [ Markdown Cell ]  ## Introduction                        |
  |  Description of the analysis...                            |
  +-----------------------------------------------------------+
  |  [ Python Cell ]    df = spark.read.table("sales")         |
  |                     df.show()                              |
  +-----------------------------------------------------------+
  |  [ SQL Cell ]       %sql SELECT * FROM sales LIMIT 10     |
  +-----------------------------------------------------------+
  |  [ Shell Cell ]     %sh ls /dbfs/mnt/                     |
  +-----------------------------------------------------------+
  |  [ Python Cell ]    display(df.groupBy("region").count())  |
  +===========================================================+
         |
         v  (attached to)
  +-------------------+
  |    CLUSTER        |
  |  (runs the code)  |
  +-------------------+
```

### Cell Types and Language Magic

Every notebook has a **default language** (Python, SQL, Scala, or R). To use
a different language in a specific cell, prefix it with a magic command:

| Magic Command | Language | Example |
|---------------|----------|---------|
| `%python` | Python | `%python print("hello")` |
| `%sql` | SQL | `%sql SELECT 1` |
| `%scala` | Scala | `%scala println("hello")` |
| `%r` | R | `%r print("hello")` |
| `%md` | Markdown | `%md ## Heading` |
| `%sh` | Shell (Bash) | `%sh ls -la` |

The magic command must be on the **first line** of the cell.

### Special Magic Commands

#### %run — Execute Another Notebook
```python
%run ./helper_functions
```
- Runs the target notebook **in the same context** as the calling notebook
- All variables, functions, and imports from the target become available
- Use it to share utility functions across notebooks
- The path is relative to the current notebook (or absolute from the workspace root)

#### %pip — Install Python Packages
```python
%pip install pandas==2.1.0 requests
```
- Installs packages on the current cluster
- The notebook **restarts the Python interpreter** after `%pip` — put it in the first cell
- Packages are available for the lifetime of the cluster
- Use `%pip` instead of `!pip` for proper Databricks integration

#### %sh — Run Shell Commands
```bash
%sh wget https://example.com/data.csv -O /tmp/data.csv
```
- Runs on the **driver node only** (not on workers)
- Use for downloading files, checking system info, running OS commands
- Files written to local disk (`/tmp/`) are only visible to the driver

### Widgets

Widgets add interactive inputs to your notebooks. They are essential for
creating parameterized notebooks that can be reused with different inputs.

```
  WIDGET TYPES
  +------------------+--------------------------------------+
  | Type             | Description                          |
  +------------------+--------------------------------------+
  | text             | Free-form text input                 |
  | dropdown         | Single selection from a list         |
  | combobox         | Dropdown with free-form typing       |
  | multiselect      | Multiple selections from a list      |
  +------------------+--------------------------------------+
```

Creating widgets:
```python
# Text widget
dbutils.widgets.text("name", "default_value", "Label")

# Dropdown widget
dbutils.widgets.dropdown("env", "dev", ["dev", "stg", "prd"], "Environment")

# Get widget value
env = dbutils.widgets.get("env")

# Remove a widget
dbutils.widgets.remove("name")

# Remove all widgets
dbutils.widgets.removeAll()
```

When a notebook with widgets runs as a job, widget values can be passed as
parameters — making the same notebook reusable across environments.

### Display Functions

#### display()
The `display()` function renders DataFrames as formatted, interactive HTML tables
with built-in charting:

```python
display(df)  # Interactive table with sort, filter, chart options
```

Features of `display()`:
- Paginated table view (up to 10,000 rows)
- Click column headers to sort
- Built-in visualization (bar, line, scatter, pie, map)
- Download results as CSV
- Works with Spark DataFrames, Pandas DataFrames, and images

#### displayHTML()
For custom HTML rendering:
```python
displayHTML("<h1>Custom Output</h1><p>Any valid HTML</p>")
```

#### DataFrame.show() vs display()
| Method | Output | Interactivity | Rows |
|--------|--------|---------------|------|
| `df.show()` | Plain text | None | 20 (default) |
| `display(df)` | HTML table | Sort, filter, chart | 1,000 (default) |

**Best practice**: Use `display()` for interactive exploration. Use `show()` in
production code where you only need to verify output.

### dbutils — The Databricks Utility Library

`dbutils` provides notebook-level utilities organized into modules:

```
  dbutils
  +-- fs          File system operations (DBFS)
  |   +-- ls()       List files
  |   +-- cp()       Copy files
  |   +-- mv()       Move files
  |   +-- rm()       Remove files
  |   +-- put()      Write text to a file
  |   +-- head()     Read first bytes of a file
  |   +-- mkdirs()   Create directories
  |
  +-- widgets     Input widgets (text, dropdown, etc.)
  |   +-- text()
  |   +-- dropdown()
  |   +-- combobox()
  |   +-- multiselect()
  |   +-- get()
  |   +-- remove()
  |   +-- removeAll()
  |
  +-- notebook    Notebook workflow control
  |   +-- run()      Run another notebook and return a value
  |   +-- exit()     Exit the current notebook with a value
  |
  +-- secrets     Access secret scopes (key vaults)
  |   +-- get()      Get a secret value
  |   +-- list()     List secrets in a scope
  |   +-- listScopes()  List all scopes
  |
  +-- library     (Deprecated — use %pip instead)
  +-- jobs        Access job context information
  +-- credentials Access cloud credentials
```

### Notebook Workflows (dbutils.notebook)

You can chain notebooks together:

```
  PARENT NOTEBOOK
  +------------------------------------------------+
  |  result = dbutils.notebook.run(                |
  |      path="./child_notebook",                  |
  |      timeout_seconds=300,                      |
  |      arguments={"env": "prd", "date": "2024"} |
  |  )                                             |
  |  print(f"Child returned: {result}")            |
  +------------------------------------------------+
           |
           v
  CHILD NOTEBOOK
  +------------------------------------------------+
  |  env = dbutils.widgets.get("env")              |
  |  # ... processing ...                          |
  |  dbutils.notebook.exit("SUCCESS")              |
  +------------------------------------------------+
```

Key differences from `%run`:
| Feature | `%run` | `dbutils.notebook.run()` |
|---------|--------|--------------------------|
| Execution context | Shared (same scope) | Isolated (separate scope) |
| Return value | No (shares variables) | Yes (string from `exit()`) |
| Timeout | No | Yes (timeout_seconds) |
| Parameters | No | Yes (arguments dict) |
| Error handling | Fails the parent | Can be caught with try/except |

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Shift + Enter` | Run cell and move to next |
| `Ctrl + Enter` | Run cell and stay |
| `Ctrl + Shift + -` | Split cell at cursor |
| `Esc` then `A` | Insert cell above |
| `Esc` then `B` | Insert cell below |
| `Esc` then `D, D` | Delete cell |
| `Esc` then `M` | Convert to Markdown |
| `Esc` then `Y` | Convert to Code |
| `Ctrl + /` | Toggle comment |
| `Tab` | Code completion |
| `Shift + Tab` | Parameter info |

## Hands-On Walkthrough

Import the companion notebook `03-notebook-fundamentals_notebook.py` into your
workspace. The notebook demonstrates:

1. All cell types (Python, SQL, Markdown, Shell)
2. Creating and using widgets
3. The `display()` and `displayHTML()` functions
4. Core dbutils modules (fs, widgets, notebook)
5. Building parameterized output

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Secret scopes backend | AWS Secrets Manager or Databricks | Azure Key Vault or Databricks | GCP Secret Manager or Databricks |
| Shell commands (`%sh`) | Amazon Linux 2 | Ubuntu | Ubuntu |
| `%pip` behavior | Same across all clouds | Same across all clouds | Same across all clouds |
| displayHTML restrictions | Same across all clouds | Same across all clouds | Same across all clouds |
| Real-time coauthoring | Yes | Yes | Yes |

## Certification Tip

The **Data Engineer Associate** exam expects you to:
- Know how `%run` differs from `dbutils.notebook.run()`
- Understand that `%run` shares execution context while `dbutils.notebook.run()` is isolated
- Know that `%pip install` restarts the Python interpreter
- Recognize that `display()` provides built-in visualization

The **Professional** exam adds:
- Widget parameterization for multi-environment deployments
- Error handling with `dbutils.notebook.run()` and timeout management
- Using `dbutils.notebook.exit()` to return status values

## Key Takeaways

- Databricks notebooks support Python, SQL, Scala, R, Markdown, and Shell in a single document
- Magic commands (`%sql`, `%sh`, `%md`, etc.) switch the cell language
- `%run` includes another notebook in the same scope; `dbutils.notebook.run()` executes in isolation
- Widgets turn notebooks into parameterized, reusable templates
- `display()` provides interactive tables with charting; prefer it over `show()` for exploration
- `dbutils` is your toolkit for file operations, secrets, widgets, and notebook workflows
- Put `%pip install` commands in the first cell since they restart the Python interpreter

## Next Steps

Proceed to [04 — Databricks Repos & Git](04-databricks-repos-git.md) to learn
how to version-control your notebooks and integrate with Git.
