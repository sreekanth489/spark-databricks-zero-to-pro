# Spark SQL

> Module 01 -- Topic 06 | Level: Beginner | Time: 55 min

## Learning Objectives

- Create and manage temporary views for SQL access to DataFrames
- Write SQL queries in Spark using SELECT, WHERE, JOIN, GROUP BY, and ORDER BY
- Use Common Table Expressions (CTEs) and subqueries
- Apply built-in functions for strings, dates, math, and conditional logic
- Mix the DataFrame API and SQL within the same pipeline
- Explore the Spark catalog API

## Conceptual Overview

### Why Spark SQL?

Spark SQL lets you query distributed data using standard SQL syntax. This is powerful
because:

1. **Familiar**: SQL is the universal language for data analysis
2. **Optimized**: Spark SQL goes through the same Catalyst optimizer as DataFrame operations
3. **Interoperable**: You can switch between SQL and DataFrame API freely
4. **Shared**: Analysts, engineers, and scientists all understand SQL

```
  ┌─────────────────────────────────────────────────────────┐
  │                     User Code                           │
  │                                                         │
  │  DataFrame API         SQL                              │
  │  df.filter(...)        spark.sql("SELECT ...")          │
  │  df.groupBy(...)       spark.sql("SELECT ... GROUP BY") │
  │                                                         │
  │           │                      │                      │
  │           └──────────┬───────────┘                      │
  │                      ▼                                  │
  │              Catalyst Optimizer                         │
  │              (same execution plan)                      │
  │                      │                                  │
  │                      ▼                                  │
  │              Spark Engine                               │
  └─────────────────────────────────────────────────────────┘
```

### Creating Temporary Views

Before you can query a DataFrame with SQL, you must register it as a view.

**Temporary view** (session-scoped):
```python
df.createOrReplaceTempView("employees")
spark.sql("SELECT * FROM employees WHERE age > 30")
```

**Global temporary view** (application-scoped, accessible from all sessions):
```python
df.createOrReplaceGlobalTempView("employees")
spark.sql("SELECT * FROM global_temp.employees WHERE age > 30")
```

| View Type | Scope | Prefix | Survives Session Restart |
|-----------|-------|--------|------------------------|
| Temp view | Current SparkSession | None | No |
| Global temp view | All SparkSessions in the application | `global_temp.` | No |
| Permanent view | Catalog (Hive metastore) | Database name | Yes |

### Core SQL Syntax in Spark

Spark SQL supports ANSI SQL with some extensions. The most common statements:

**SELECT and WHERE:**
```sql
SELECT name, age, salary
FROM employees
WHERE department = 'Engineering' AND age > 30
```

**Aggregations:**
```sql
SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 1
ORDER BY avg_salary DESC
```

**Joins:**
```sql
SELECT e.name, e.department, d.budget
FROM employees e
JOIN departments d ON e.department = d.dept_name
```

**DISTINCT and LIMIT:**
```sql
SELECT DISTINCT department FROM employees
LIMIT 10
```

### CREATE TABLE AS SELECT (CTAS)

CTAS creates a new table from a query result. In Databricks, this often creates a
Delta table.

```sql
CREATE TABLE sales_summary AS
SELECT region, SUM(amount) AS total_sales
FROM transactions
GROUP BY region
```

### Common Table Expressions (CTEs)

CTEs make complex queries readable by breaking them into named subqueries.

```sql
WITH department_stats AS (
    SELECT department,
           AVG(salary) AS avg_salary,
           COUNT(*) AS headcount
    FROM employees
    GROUP BY department
),
high_paying AS (
    SELECT *
    FROM department_stats
    WHERE avg_salary > 90000
)
SELECT * FROM high_paying ORDER BY avg_salary DESC
```

CTEs are temporary and exist only for the duration of the query. They do not create
tables or views.

### Subqueries

**Scalar subquery** (returns a single value):
```sql
SELECT name, salary,
       salary - (SELECT AVG(salary) FROM employees) AS diff_from_avg
FROM employees
```

**IN subquery** (returns a set):
```sql
SELECT * FROM employees
WHERE department IN (
    SELECT dept_name FROM departments WHERE budget > 1000000
)
```

**Correlated subquery** (references the outer query):
```sql
SELECT name, salary, department
FROM employees e
WHERE salary > (
    SELECT AVG(salary) FROM employees WHERE department = e.department
)
```

### Set Operations

```sql
-- Union (removes duplicates)
SELECT name FROM table_a
UNION
SELECT name FROM table_b

-- Union All (keeps duplicates)
SELECT name FROM table_a
UNION ALL
SELECT name FROM table_b

-- Intersect
SELECT name FROM table_a
INTERSECT
SELECT name FROM table_b

-- Except (in table_a but not in table_b)
SELECT name FROM table_a
EXCEPT
SELECT name FROM table_b
```

### Built-In Functions

Spark SQL provides hundreds of built-in functions. Here are the most common categories:

**String functions:**

| Function | Description | Example |
|----------|-------------|---------|
| `upper(s)` | Uppercase | `upper('hello')` -> `HELLO` |
| `lower(s)` | Lowercase | `lower('HELLO')` -> `hello` |
| `trim(s)` | Remove whitespace | `trim('  hi  ')` -> `hi` |
| `length(s)` | String length | `length('spark')` -> `5` |
| `substring(s, pos, len)` | Extract substring | `substring('spark', 1, 3)` -> `spa` |
| `concat(a, b)` | Concatenate | `concat('a', 'b')` -> `ab` |
| `replace(s, old, new)` | Replace | `replace('foo', 'o', 'a')` -> `faa` |
| `regexp_extract(s, p, g)` | Regex extract | See docs |
| `split(s, delim)` | Split to array | `split('a,b,c', ',')` -> `['a','b','c']` |

**Date functions:**

| Function | Description |
|----------|-------------|
| `current_date()` | Today's date |
| `current_timestamp()` | Current timestamp |
| `date_add(d, n)` | Add `n` days |
| `date_sub(d, n)` | Subtract `n` days |
| `datediff(end, start)` | Days between dates |
| `year(d)`, `month(d)`, `day(d)` | Extract components |
| `date_format(d, fmt)` | Format as string |
| `to_date(s, fmt)` | Parse string to date |

**Math functions:**

| Function | Description |
|----------|-------------|
| `round(x, d)` | Round to `d` decimal places |
| `ceil(x)` | Round up |
| `floor(x)` | Round down |
| `abs(x)` | Absolute value |
| `pow(x, n)` | Power |
| `sqrt(x)` | Square root |
| `log(x)` | Natural logarithm |

**Conditional functions:**

| Function | Description |
|----------|-------------|
| `CASE WHEN ... THEN ... ELSE ... END` | Conditional expression |
| `IF(cond, true_val, false_val)` | Inline conditional |
| `COALESCE(a, b, c)` | First non-null value |
| `NULLIF(a, b)` | Returns null if `a == b` |
| `NVL(a, b)` | If `a` is null, return `b` |

### Mixing DataFrame API and SQL

One of Spark's strengths is seamless switching between the two APIs:

```python
# Start with DataFrame API
filtered_df = df.filter(col("salary") > 80000)

# Switch to SQL for a complex aggregation
filtered_df.createOrReplaceTempView("high_earners")
result = spark.sql("""
    SELECT department,
           COUNT(*) AS headcount,
           PERCENTILE_APPROX(salary, 0.5) AS median_salary
    FROM high_earners
    GROUP BY department
""")

# Back to DataFrame API for final processing
final = result.orderBy(desc("median_salary"))
```

### The Catalog API

The catalog provides programmatic access to Spark's metadata: databases, tables, views,
functions, and columns.

```python
# List databases
spark.catalog.listDatabases()

# List tables in the current database
spark.catalog.listTables()

# List columns in a table
spark.catalog.listColumns("employees")

# Check if a table exists
spark.catalog.tableExists("employees")

# Set the current database
spark.catalog.setCurrentDatabase("my_database")

# Refresh table metadata (after external changes)
spark.catalog.refreshTable("employees")
```

## Hands-On Walkthrough

Open the companion notebook `06-spark-sql_notebook.py` in Databricks. You will:

- Register DataFrames as temp views
- Run SQL queries with SELECT, WHERE, GROUP BY, HAVING, ORDER BY
- Write CTEs and subqueries
- Use string, date, math, and conditional functions
- Mix SQL and DataFrame API in a single pipeline
- Explore the catalog

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|-------------------|----------------|
| Default metastore | AWS Glue Catalog or Hive | Unity Catalog or Hive | Hive Metastore |
| Permanent tables | Stored in Glue/Hive | Stored in Unity Catalog | Stored in Hive |
| SQL endpoint | N/A | Databricks SQL Warehouses | N/A |
| External tables | S3 paths | ADLS paths | GCS paths |

In Databricks, Spark SQL integrates with Unity Catalog for governance, access control,
and data discovery.

## Certification Tip

Spark SQL is one of the most heavily tested areas on the Databricks certification:

- Know the syntax for `CREATE OR REPLACE TEMP VIEW`
- Understand the difference between temp views and global temp views
- Be comfortable with CTEs (`WITH ... AS`)
- Know common built-in functions (especially date and string functions)
- Understand that SQL and DataFrame operations produce the same execution plan

Practice writing queries without running them -- the exam tests reading comprehension
of SQL code.

## Key Takeaways

- Register DataFrames as temp views to query them with SQL
- Spark SQL uses the same Catalyst optimizer as the DataFrame API -- no performance penalty
- CTEs (`WITH ... AS`) make complex queries readable and maintainable
- Spark provides hundreds of built-in functions across strings, dates, math, and conditionals
- You can freely mix SQL and DataFrame API in the same pipeline
- The catalog API gives programmatic access to metadata (tables, columns, databases)
- Prefer built-in SQL functions over Python UDFs for better performance

## Next Steps

Continue to [07 - Catalyst Optimizer](07-catalyst-optimizer.md) to understand how Spark
optimizes both SQL and DataFrame operations behind the scenes.
