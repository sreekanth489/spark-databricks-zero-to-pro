# Row & Column Security
> Module 08 — Topic 04 | Level: Advanced | Time: 45 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Implement row-level security using dynamic views with `current_user()` and
   `is_member()`.
2. Apply column masking using the MASK clause in Unity Catalog.
3. Create row filters using the ROW FILTER clause in Unity Catalog.
4. Compare dynamic views vs native row filters for different use cases.
5. Implement common data masking patterns: full mask, partial mask, and hash.
6. Design a fine-grained security strategy for sensitive data.

---

## Conceptual Overview

### Why Fine-Grained Security?

Table-level access control (GRANT SELECT) is either all-or-nothing: a user can
see every row and every column, or none at all. Real-world requirements demand
finer control:

- **HR data**: Managers should see only their direct reports' salaries
- **Healthcare**: Doctors should see only their own patients' records
- **Multi-tenant SaaS**: Each tenant should see only their own data
- **PII protection**: Analysts need data for analytics but should not see SSNs

Fine-grained security solves this by controlling access at the **row** and
**column** level.

### Three Approaches to Fine-Grained Security

```
  +-----------------------------------+
  | Approach 1: DYNAMIC VIEWS         |
  | (Legacy but widely used)          |
  |                                   |
  | CREATE VIEW secure_view AS        |
  | SELECT * FROM table               |
  | WHERE region = current_user()     |
  |   OR is_member('admin_group')     |
  +-----------------------------------+
           |
           v
  +-----------------------------------+
  | Approach 2: ROW FILTER (UC)       |
  | (Native Unity Catalog feature)    |
  |                                   |
  | ALTER TABLE t                     |
  | SET ROW FILTER my_filter ON (col) |
  +-----------------------------------+
           |
           v
  +-----------------------------------+
  | Approach 3: COLUMN MASK (UC)      |
  | (Native Unity Catalog feature)    |
  |                                   |
  | ALTER TABLE t                     |
  | ALTER COLUMN ssn SET MASK my_mask |
  +-----------------------------------+
```

### Approach 1: Dynamic Views

Dynamic views use SQL functions like `current_user()` and `is_member()` to
filter rows and mask columns based on the querying user's identity.

```sql
-- Row-level security with a dynamic view
CREATE OR REPLACE VIEW secure_employees AS
SELECT *
FROM employees
WHERE department = current_user()
   OR is_member('hr_admin');

-- Column masking with a dynamic view
CREATE OR REPLACE VIEW masked_employees AS
SELECT
    employee_id,
    first_name,
    last_name,
    CASE
        WHEN is_member('hr_admin') THEN ssn
        ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))
    END AS ssn,
    CASE
        WHEN is_member('finance_team') THEN salary
        ELSE NULL
    END AS salary
FROM employees;
```

**Pros of dynamic views:**
- Work on all Databricks editions (including Community Edition with limitations)
- Familiar SQL syntax
- Combine row filtering and column masking in one object

**Cons of dynamic views:**
- Users must query the view, not the underlying table
- Maintenance overhead (one view per security policy)
- No enforcement at the table level (direct table access bypasses the view)

### Approach 2: ROW FILTER (Unity Catalog Native)

Row filters are SQL functions applied directly to a table. Every query against
the table automatically runs through the filter — there is no way to bypass it.

```sql
-- Step 1: Create a row filter function
CREATE OR REPLACE FUNCTION region_filter(region_val STRING)
RETURNS BOOLEAN
RETURN (region_val = current_user()) OR is_member('admin_group');

-- Step 2: Apply the filter to a table
ALTER TABLE sales_data
SET ROW FILTER region_filter ON (region);

-- Step 3: Now every SELECT is automatically filtered
SELECT * FROM sales_data;
-- Users only see rows where their identity matches the region
-- or they are in the admin_group
```

**How it works:**

```
  User query: SELECT * FROM sales_data
                    |
                    v
  Spark injects:  WHERE region_filter(region) = TRUE
                    |
                    v
  Results: Only rows the user is allowed to see
```

### Approach 3: COLUMN MASK (Unity Catalog Native)

Column masks are SQL functions applied to specific columns. The function
transforms the column value based on the querying user's identity.

```sql
-- Step 1: Create a masking function
CREATE OR REPLACE FUNCTION mask_ssn(ssn_val STRING)
RETURNS STRING
RETURN CASE
    WHEN is_member('hr_admin') THEN ssn_val
    ELSE CONCAT('XXX-XX-', RIGHT(ssn_val, 4))
END;

-- Step 2: Apply the mask to a column
ALTER TABLE employees
ALTER COLUMN ssn SET MASK mask_ssn;

-- Step 3: Now SSN is automatically masked for non-HR users
-- HR admins see: 123-45-6789
-- Everyone else sees: XXX-XX-6789
```

### Comparison: Dynamic Views vs Native Row/Column Security

| Feature | Dynamic Views | ROW FILTER / COLUMN MASK |
|---------|--------------|--------------------------|
| Enforcement level | View only | Table level (cannot be bypassed) |
| Requires Unity Catalog | No | Yes |
| User queries | Must use the view | Can query the table directly |
| Maintenance | One view per policy | Functions reusable across tables |
| Column masking | CASE expressions in SELECT | MASK clause on columns |
| Row filtering | WHERE clause in view | ROW FILTER clause on table |
| Performance | View pushdown | Native Spark optimization |
| Audit trail | View access logged | Table access with filter logged |

### Common Masking Patterns

```
  Pattern 1: Full Mask
  ====================
  Input:  "John Smith"
  Output: "**********"
  Use:    When the column value must be completely hidden.

  Pattern 2: Partial Mask (last N characters visible)
  ====================================================
  Input:  "123-45-6789"
  Output: "XXX-XX-6789"
  Use:    SSNs, phone numbers — enough to verify but not identify.

  Pattern 3: Hash Mask
  ====================
  Input:  "john@example.com"
  Output: "a1b2c3d4e5f6..."  (SHA-256 hash)
  Use:    When you need consistent mapping for joins but not the value.

  Pattern 4: Null Mask
  ====================
  Input:  125000.00
  Output: NULL
  Use:    When unauthorized users should see no value at all.

  Pattern 5: Range/Bucket Mask
  ============================
  Input:  32 (age)
  Output: "30-39"
  Use:    When aggregations need approximate values but not exact ones.

  Pattern 6: Date Truncation
  ==========================
  Input:  "1990-03-15"
  Output: "1990-01-01"
  Use:    Keep the year for analytics but hide the exact date.
```

---

## Hands-On Walkthrough

Open the companion notebook `04-row-column-security_notebook.py` and follow
along. The notebook covers:

1. Creating sample data with sensitive columns
2. Implementing dynamic views for row-level security
3. Implementing dynamic views for column masking
4. Demonstrating ROW FILTER DDL syntax
5. Demonstrating COLUMN MASK DDL syntax
6. Testing masking patterns (full, partial, hash)

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Dynamic views | All editions | All editions | All editions |
| ROW FILTER | Unity Catalog (Premium+) | Unity Catalog (Premium+) | Unity Catalog (Premium+) |
| COLUMN MASK | Unity Catalog (Premium+) | Unity Catalog (Premium+) | Unity Catalog (Premium+) |
| `current_user()` | Returns email/username | Returns Azure AD UPN | Returns Google identity |
| `is_member()` | Checks account-level groups | Checks Azure AD groups | Checks GCP groups |
| Audit logging | AWS CloudTrail + UC logs | Azure Monitor + UC logs | GCP Logging + UC logs |

---

## Certification Tip

The Databricks Certified Data Engineer exams test:

- **Dynamic views** with `current_user()` and `is_member()` — know the exact
  syntax and when to use each function
- **ROW FILTER** — know that it is a SQL function that returns BOOLEAN, applied
  with ALTER TABLE ... SET ROW FILTER
- **COLUMN MASK** — know that it is a SQL function that transforms the column
  value, applied with ALTER TABLE ... ALTER COLUMN ... SET MASK
- The key difference: dynamic views are **view-level** enforcement; row filters
  and column masks are **table-level** enforcement
- Row filters and column masks **cannot be bypassed** — even direct table access
  goes through the filter/mask

---

## Key Takeaways

1. **Dynamic views** provide row and column security using `current_user()` and
   `is_member()` — works on all editions but only at the view level.
2. **ROW FILTER** is a native Unity Catalog feature that applies a boolean filter
   function directly on the table — cannot be bypassed.
3. **COLUMN MASK** is a native Unity Catalog feature that transforms column values
   based on the querying user — cannot be bypassed.
4. **Common masking patterns**: full mask, partial mask, hash, null, range/bucket,
   and date truncation.
5. Native row filters and column masks are **preferred** for new implementations
   because they enforce security at the table level.
6. Dynamic views remain useful for **complex security logic** or when Unity Catalog
   native features are not available.

---

## Next Steps

Proceed to [05 — Delta Sharing](05-delta-sharing.md) to learn how to share data
securely across organizations and cloud platforms.
