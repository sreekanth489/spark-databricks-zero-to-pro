# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Lineage
# MAGIC > Module 08 — Topic 03 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - Building a multi-step transformation pipeline that creates lineage
# MAGIC - Understanding how Unity Catalog tracks each step automatically
# MAGIC - Querying INFORMATION_SCHEMA for metadata exploration
# MAGIC - Simulating lineage output for impact analysis
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog (for actual lineage tracking)
# MAGIC - Community Edition users: lineage is simulated but the pipeline is real
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup — Create Source Data
# MAGIC
# MAGIC We will build a realistic multi-step pipeline:
# MAGIC
# MAGIC ```
# MAGIC [raw_orders] + [raw_customers] --> [enriched_orders] --> [daily_summary]
# MAGIC                                                      --> [customer_metrics]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m08_lineage_demo
# MAGIC COMMENT 'Data lineage demonstration schema';
# MAGIC USE m08_lineage_demo;

# COMMAND ----------

# Create raw_orders (source table 1)
from pyspark.sql import Row
from datetime import date, timedelta
import random

random.seed(42)

orders = []
for i in range(1, 51):
    orders.append(Row(
        order_id=i,
        customer_id=random.randint(1, 10),
        product_id=random.randint(100, 110),
        quantity=random.randint(1, 5),
        unit_price=round(random.uniform(10, 200), 2),
        order_date=date(2024, 1, 1) + timedelta(days=random.randint(0, 30))
    ))

orders_df = spark.createDataFrame(orders,
    "order_id INT, customer_id INT, product_id INT, quantity INT, unit_price DECIMAL(10,2), order_date DATE")
orders_df.write.mode("overwrite").saveAsTable("m08_lineage_demo.raw_orders")

print(f"Created raw_orders with {orders_df.count()} rows.")

# COMMAND ----------

# Create raw_customers (source table 2)
customers = [
    Row(customer_id=1,  customer_name="Alice Johnson",   region="North", email="alice@example.com"),
    Row(customer_id=2,  customer_name="Bob Smith",        region="South", email="bob@example.com"),
    Row(customer_id=3,  customer_name="Carol Williams",   region="East",  email="carol@example.com"),
    Row(customer_id=4,  customer_name="David Brown",      region="West",  email="david@example.com"),
    Row(customer_id=5,  customer_name="Eve Davis",        region="North", email="eve@example.com"),
    Row(customer_id=6,  customer_name="Frank Miller",     region="South", email="frank@example.com"),
    Row(customer_id=7,  customer_name="Grace Wilson",     region="East",  email="grace@example.com"),
    Row(customer_id=8,  customer_name="Henry Moore",      region="West",  email="henry@example.com"),
    Row(customer_id=9,  customer_name="Irene Taylor",     region="North", email="irene@example.com"),
    Row(customer_id=10, customer_name="Jack Anderson",    region="South", email="jack@example.com"),
]

cust_df = spark.createDataFrame(customers,
    "customer_id INT, customer_name STRING, region STRING, email STRING")
cust_df.write.mode("overwrite").saveAsTable("m08_lineage_demo.raw_customers")

print(f"Created raw_customers with {cust_df.count()} rows.")

# COMMAND ----------

# Create raw_products (source table 3)
products = [
    Row(product_id=100, product_name="Widget A",    category="Electronics"),
    Row(product_id=101, product_name="Widget B",    category="Electronics"),
    Row(product_id=102, product_name="Gadget X",    category="Accessories"),
    Row(product_id=103, product_name="Tool Pro",    category="Hardware"),
    Row(product_id=104, product_name="Sensor Kit",  category="Electronics"),
    Row(product_id=105, product_name="Cable Pack",  category="Accessories"),
    Row(product_id=106, product_name="Drill Set",   category="Hardware"),
    Row(product_id=107, product_name="Monitor",     category="Electronics"),
    Row(product_id=108, product_name="Keyboard",    category="Accessories"),
    Row(product_id=109, product_name="Mouse",       category="Accessories"),
    Row(product_id=110, product_name="Headphones",  category="Electronics"),
]

prod_df = spark.createDataFrame(products,
    "product_id INT, product_name STRING, category STRING")
prod_df.write.mode("overwrite").saveAsTable("m08_lineage_demo.raw_products")

print(f"Created raw_products with {prod_df.count()} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Build the Transformation Pipeline
# MAGIC
# MAGIC Each step here creates lineage that Unity Catalog tracks automatically.
# MAGIC On a full Databricks workspace, you would see the lineage graph populate
# MAGIC in real time after each step.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create enriched_orders (JOIN orders + customers + products)
# MAGIC
# MAGIC **Lineage created:**
# MAGIC - raw_orders --> enriched_orders
# MAGIC - raw_customers --> enriched_orders
# MAGIC - raw_products --> enriched_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This JOIN creates table-level lineage from three sources to one target.
# MAGIC -- Unity Catalog also tracks column-level lineage:
# MAGIC --   enriched_orders.customer_name  <-- raw_customers.customer_name
# MAGIC --   enriched_orders.region         <-- raw_customers.region
# MAGIC --   enriched_orders.product_name   <-- raw_products.product_name
# MAGIC --   enriched_orders.line_total     <-- raw_orders.quantity * raw_orders.unit_price
# MAGIC
# MAGIC CREATE OR REPLACE TABLE m08_lineage_demo.enriched_orders AS
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     o.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.region,
# MAGIC     o.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     o.quantity,
# MAGIC     o.unit_price,
# MAGIC     (o.quantity * o.unit_price) AS line_total,
# MAGIC     o.order_date
# MAGIC FROM m08_lineage_demo.raw_orders o
# MAGIC JOIN m08_lineage_demo.raw_customers c ON o.customer_id = c.customer_id
# MAGIC JOIN m08_lineage_demo.raw_products p ON o.product_id = p.product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m08_lineage_demo.enriched_orders LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create daily_summary (aggregate from enriched_orders)
# MAGIC
# MAGIC **Lineage created:**
# MAGIC - enriched_orders --> daily_summary
# MAGIC
# MAGIC **Column-level lineage:**
# MAGIC - daily_summary.total_revenue <-- enriched_orders.line_total (via SUM)
# MAGIC - daily_summary.order_count <-- enriched_orders.order_id (via COUNT)
# MAGIC - daily_summary.order_date <-- enriched_orders.order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE m08_lineage_demo.daily_summary AS
# MAGIC SELECT
# MAGIC     order_date,
# MAGIC     region,
# MAGIC     COUNT(DISTINCT order_id)  AS order_count,
# MAGIC     SUM(line_total)           AS total_revenue,
# MAGIC     AVG(line_total)           AS avg_order_value,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM m08_lineage_demo.enriched_orders
# MAGIC GROUP BY order_date, region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m08_lineage_demo.daily_summary ORDER BY order_date, region LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create customer_metrics (aggregate from enriched_orders)
# MAGIC
# MAGIC **Lineage created:**
# MAGIC - enriched_orders --> customer_metrics
# MAGIC
# MAGIC **Column-level lineage:**
# MAGIC - customer_metrics.lifetime_value <-- enriched_orders.line_total (via SUM)
# MAGIC - customer_metrics.customer_name <-- enriched_orders.customer_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE m08_lineage_demo.customer_metrics AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     region,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     SUM(line_total)          AS lifetime_value,
# MAGIC     AVG(line_total)          AS avg_order_value,
# MAGIC     MIN(order_date)          AS first_order_date,
# MAGIC     MAX(order_date)          AS last_order_date
# MAGIC FROM m08_lineage_demo.enriched_orders
# MAGIC GROUP BY customer_id, customer_name, region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m08_lineage_demo.customer_metrics ORDER BY lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create a downstream view (from daily_summary)
# MAGIC
# MAGIC **Lineage created:**
# MAGIC - daily_summary --> regional_performance (view)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW m08_lineage_demo.regional_performance AS
# MAGIC SELECT
# MAGIC     region,
# MAGIC     SUM(total_revenue)       AS total_revenue,
# MAGIC     SUM(order_count)         AS total_orders,
# MAGIC     AVG(avg_order_value)     AS avg_order_value,
# MAGIC     SUM(unique_customers)    AS total_customer_visits
# MAGIC FROM m08_lineage_demo.daily_summary
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM m08_lineage_demo.regional_performance ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Visualize the Lineage Graph
# MAGIC
# MAGIC On a full Databricks workspace with Unity Catalog, you would navigate to:
# MAGIC
# MAGIC 1. **Data Explorer** (left sidebar)
# MAGIC 2. Select any table (e.g., `enriched_orders`)
# MAGIC 3. Click the **Lineage** tab
# MAGIC 4. See upstream sources and downstream dependents
# MAGIC
# MAGIC Since Community Edition does not have the lineage UI, we simulate it below.

# COMMAND ----------

# Simulate the lineage graph
def print_lineage_graph():
    """Simulates the lineage graph that Unity Catalog would display."""

    print("=" * 70)
    print("SIMULATED LINEAGE GRAPH")
    print("(On full Databricks, this is an interactive visual in the UI)")
    print("=" * 70)
    print()
    print("  [raw_orders]  [raw_customers]  [raw_products]")
    print("       |              |                |")
    print("       +--------------+----------------+")
    print("                      |")
    print("                      v")
    print("              [enriched_orders]")
    print("                /           \\")
    print("               v             v")
    print("       [daily_summary]  [customer_metrics]")
    print("            |")
    print("            v")
    print("    [regional_performance]  (VIEW)")
    print()

    # Table-level lineage details
    lineage = {
        "raw_orders":            {"upstream": [],                     "downstream": ["enriched_orders"]},
        "raw_customers":         {"upstream": [],                     "downstream": ["enriched_orders"]},
        "raw_products":          {"upstream": [],                     "downstream": ["enriched_orders"]},
        "enriched_orders":       {"upstream": ["raw_orders", "raw_customers", "raw_products"],
                                  "downstream": ["daily_summary", "customer_metrics"]},
        "daily_summary":         {"upstream": ["enriched_orders"],    "downstream": ["regional_performance"]},
        "customer_metrics":      {"upstream": ["enriched_orders"],    "downstream": []},
        "regional_performance":  {"upstream": ["daily_summary"],      "downstream": []},
    }

    print("-" * 70)
    print(f"{'Table':<25s} {'Upstream':<30s} {'Downstream':<30s}")
    print("-" * 70)
    for table, deps in lineage.items():
        up = ", ".join(deps["upstream"]) if deps["upstream"] else "(source)"
        down = ", ".join(deps["downstream"]) if deps["downstream"] else "(terminal)"
        print(f"{table:<25s} {up:<30s} {down:<30s}")

print_lineage_graph()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Column-Level Lineage Detail

# COMMAND ----------

# Simulate column-level lineage for enriched_orders
def print_column_lineage():
    """Simulates column-level lineage for the enriched_orders table."""

    print("=" * 70)
    print("COLUMN-LEVEL LINEAGE: enriched_orders")
    print("=" * 70)
    print()

    column_lineage = [
        ("order_id",       "raw_orders.order_id",       "Direct mapping"),
        ("customer_id",    "raw_orders.customer_id",    "Direct mapping (join key)"),
        ("customer_name",  "raw_customers.customer_name", "Joined via customer_id"),
        ("region",         "raw_customers.region",       "Joined via customer_id"),
        ("product_id",     "raw_orders.product_id",      "Direct mapping (join key)"),
        ("product_name",   "raw_products.product_name",  "Joined via product_id"),
        ("category",       "raw_products.category",      "Joined via product_id"),
        ("quantity",       "raw_orders.quantity",         "Direct mapping"),
        ("unit_price",     "raw_orders.unit_price",       "Direct mapping"),
        ("line_total",     "raw_orders.quantity, raw_orders.unit_price", "Computed: quantity * unit_price"),
        ("order_date",     "raw_orders.order_date",       "Direct mapping"),
    ]

    print(f"{'Target Column':<18s} {'Source Column(s)':<40s} {'Transformation':<25s}")
    print("-" * 83)
    for target, source, transform in column_lineage:
        print(f"{target:<18s} {source:<40s} {transform:<25s}")

print_column_lineage()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Impact Analysis
# MAGIC
# MAGIC One of the most valuable uses of lineage: before making changes to a
# MAGIC table, check what downstream objects would be affected.

# COMMAND ----------

def impact_analysis(table_name, lineage_graph):
    """
    Given a table name, find all direct and transitive downstream dependents.
    This simulates the impact analysis feature in Unity Catalog.
    """
    visited = set()
    queue = [table_name]
    impacted = []

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        if current != table_name:
            impacted.append(current)
        for downstream in lineage_graph.get(current, {}).get("downstream", []):
            queue.append(downstream)

    return impacted

# Our lineage graph
lineage_graph = {
    "raw_orders":            {"downstream": ["enriched_orders"]},
    "raw_customers":         {"downstream": ["enriched_orders"]},
    "raw_products":          {"downstream": ["enriched_orders"]},
    "enriched_orders":       {"downstream": ["daily_summary", "customer_metrics"]},
    "daily_summary":         {"downstream": ["regional_performance"]},
    "customer_metrics":      {"downstream": []},
    "regional_performance":  {"downstream": []},
}

# Scenario 1: What happens if we change raw_customers?
print("=" * 60)
print("IMPACT ANALYSIS: What if we modify 'raw_customers'?")
print("=" * 60)
impacted = impact_analysis("raw_customers", lineage_graph)
print(f"\nDirectly and transitively impacted objects ({len(impacted)}):")
for t in impacted:
    print(f"  - {t}")

print()

# Scenario 2: What happens if we change enriched_orders?
print("=" * 60)
print("IMPACT ANALYSIS: What if we modify 'enriched_orders'?")
print("=" * 60)
impacted = impact_analysis("enriched_orders", lineage_graph)
print(f"\nDirectly and transitively impacted objects ({len(impacted)}):")
for t in impacted:
    print(f"  - {t}")

print()

# Scenario 3: What if we drop raw_products?
print("=" * 60)
print("IMPACT ANALYSIS: What if we DROP 'raw_products'?")
print("=" * 60)
impacted = impact_analysis("raw_products", lineage_graph)
print(f"\nDirectly and transitively impacted objects ({len(impacted)}):")
for t in impacted:
    print(f"  - {t}")
print("\nConclusion: Dropping raw_products would break the ENTIRE pipeline!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Querying Metadata via INFORMATION_SCHEMA
# MAGIC
# MAGIC While INFORMATION_SCHEMA does not directly expose lineage, it provides
# MAGIC metadata about tables, columns, and views that supports governance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- UNITY CATALOG: System tables for lineage (requires UC)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Table-level lineage
# MAGIC -- SELECT * FROM system.access.table_lineage
# MAGIC -- WHERE target_table_full_name LIKE '%enriched_orders%'
# MAGIC -- AND event_date >= current_date() - INTERVAL 7 DAYS;
# MAGIC --
# MAGIC -- Column-level lineage
# MAGIC -- SELECT * FROM system.access.column_lineage
# MAGIC -- WHERE target_table_full_name LIKE '%enriched_orders%'
# MAGIC -- AND event_date >= current_date() - INTERVAL 7 DAYS;
# MAGIC --
# MAGIC -- ============================================================
# MAGIC -- COMMUNITY EDITION: Use DESCRIBE and SHOW for metadata
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- List all tables in our demo schema
# MAGIC SHOW TABLES IN m08_lineage_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect column metadata for enriched_orders
# MAGIC DESCRIBE TABLE m08_lineage_demo.enriched_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check view definition (this shows the SQL, which implies lineage)
# MAGIC SHOW CREATE TABLE m08_lineage_demo.regional_performance;

# COMMAND ----------

# Build a metadata summary programmatically
tables_to_inspect = [
    "raw_orders", "raw_customers", "raw_products",
    "enriched_orders", "daily_summary", "customer_metrics"
]

print("=" * 70)
print("TABLE METADATA SUMMARY")
print("=" * 70)
print(f"\n{'Table':<25s} {'Rows':>8s} {'Columns':>8s}")
print("-" * 45)

for table_name in tables_to_inspect:
    fqn = f"m08_lineage_demo.{table_name}"
    row_count = spark.sql(f"SELECT COUNT(*) FROM {fqn}").collect()[0][0]
    col_count = len(spark.sql(f"DESCRIBE TABLE {fqn}").collect())
    print(f"{table_name:<25s} {row_count:>8d} {col_count:>8d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Lineage for GDPR Compliance
# MAGIC
# MAGIC Let us trace PII through our pipeline to support a GDPR deletion request.

# COMMAND ----------

# Trace PII columns through the lineage
print("=" * 70)
print("GDPR PII TRACE: Where does personal data flow?")
print("=" * 70)
print()

pii_trace = {
    "raw_customers.customer_name": [
        "enriched_orders.customer_name",
        "customer_metrics.customer_name",
    ],
    "raw_customers.email": [
        "(not propagated beyond raw_customers)",
    ],
    "raw_customers.region": [
        "enriched_orders.region",
        "daily_summary.region",
        "customer_metrics.region",
        "regional_performance.region",
    ],
}

for source_col, downstream_cols in pii_trace.items():
    print(f"Source: {source_col}")
    for dc in downstream_cols:
        print(f"  --> {dc}")
    print()

print("Action items for GDPR 'right to erasure' request:")
print("  1. DELETE from raw_customers WHERE customer_id = X")
print("  2. DELETE from enriched_orders WHERE customer_id = X")
print("  3. Re-aggregate daily_summary (or accept aggregate data stays)")
print("  4. DELETE from customer_metrics WHERE customer_id = X")
print("  5. regional_performance is aggregated (no individual PII)")
print()
print("Note: Aggregated data (daily_summary, regional_performance) typically")
print("does not need deletion under GDPR, as individual identity is lost.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS m08_lineage_demo.regional_performance;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.customer_metrics;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.daily_summary;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.enriched_orders;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.raw_products;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.raw_customers;
# MAGIC DROP TABLE IF EXISTS m08_lineage_demo.raw_orders;
# MAGIC DROP DATABASE IF EXISTS m08_lineage_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. Unity Catalog captures lineage automatically from Spark query plans")
print("  2. Table-level lineage: which tables feed into which tables")
print("  3. Column-level lineage: which columns contribute to which columns")
print("  4. Impact analysis: use lineage to assess change consequences")
print("  5. GDPR compliance: trace PII through the entire pipeline")
print("  6. System tables (system.access.table_lineage) provide programmatic access")
print()
print("Next: 04-row-column-security_notebook.py")
