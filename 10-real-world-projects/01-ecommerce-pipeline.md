# Project 01: E-Commerce Data Pipeline

> Module 10 -- Capstone Project | Level: Intermediate | Time: 3-4 hours

## Project Overview

Build a complete Bronze-Silver-Gold medallion architecture pipeline for a
fictional e-commerce company called "ShopStream." The pipeline ingests raw
transactional data (customers, products, orders, clickstream events), cleans and
enriches it through Silver, and produces Gold-layer business analytics including
revenue dashboards, customer lifetime value, product performance, and RFM-based
customer segmentation.

This project integrates concepts from Modules 01-05 and 09: PySpark
transformations, Delta Lake CRUD, medallion architecture, window functions,
aggregations, joins, data quality checks, and table optimization.

---

## Architecture

```
  DATA SOURCES (Generated In-Notebook)
  =====================================
  Customers (500)     Products (200)     Orders (5000)     Clickstream (10000)
       |                   |                  |                    |
       +-------------------+------------------+--------------------+
                                    |
                                    v
  +================================================================+
  |                        BRONZE LAYER                             |
  |  Raw ingestion -- append-only, schema-on-read                  |
  |                                                                 |
  |  bronze_customers    bronze_products    bronze_orders           |
  |  bronze_clickstream                                             |
  |                                                                 |
  |  + _ingest_timestamp, _source, _batch_id metadata columns      |
  +================================================================+
                                    |
                      Clean / Deduplicate / Type-Cast
                      Join / Enrich / Quality Check
                                    |
                                    v
  +================================================================+
  |                        SILVER LAYER                             |
  |  Cleaned, deduplicated, schema-enforced                        |
  |                                                                 |
  |  silver_customers     silver_products     silver_orders         |
  |  silver_clickstream   silver_orders_enriched                    |
  |  silver_quarantine    (orders joined with customers & products) |
  |                                                                 |
  |  Data quality checks: completeness, validity, uniqueness        |
  +================================================================+
                                    |
                         Aggregate / Segment / Score
                                    |
                                    v
  +================================================================+
  |                         GOLD LAYER                              |
  |  Business aggregates -- BI-ready                                |
  |                                                                 |
  |  gold_revenue_by_category_month    Revenue by category & month  |
  |  gold_customer_lifetime_value      CLV with recency & frequency |
  |  gold_product_performance          Units, revenue, return rate  |
  |  gold_customer_segments            RFM segmentation quartiles   |
  |  gold_daily_dashboard              Executive daily summary      |
  +================================================================+
                                    |
                                    v
                          BI Tools / SQL Analytics
```

---

## Requirements

### Data Generation

| Dataset | Count | Key Fields |
|---------|-------|------------|
| Customers | 500 | customer_id, name, email, city, state, signup_date, tier |
| Products | 200 | product_id, name, category, subcategory, price, weight_kg |
| Orders | 5000 | order_id, customer_id, product_id, quantity, total_amount, order_date, status, payment_method |
| Clickstream | 10000 | event_id, customer_id, product_id, event_type, page, timestamp, session_id, device |

Data should intentionally include quality issues:
- Duplicate order IDs (overlapping ID ranges)
- Missing customer_ids (null foreign keys)
- Prices stored as strings with `$` prefixes
- Inconsistent date formats
- 2-5% null values across key columns

### Bronze Layer

1. Ingest all four raw datasets into Delta tables.
2. Add metadata columns: `_ingest_timestamp`, `_source`, `_batch_id`.
3. No data cleansing -- store data exactly as generated.
4. All tables are append-only.

### Silver Layer

1. **Type casting**: Convert string prices to doubles, string dates to date type,
   quantities to integers.
2. **Deduplication**: Remove duplicate order_ids (keep latest by ingest timestamp).
3. **Null handling**: Quarantine records with null primary keys or invalid values.
4. **Enriched orders table**: Join orders with customers and products to create
   `silver_orders_enriched` with customer name, tier, product category, and price.
5. **Data quality report**: Count records per layer, measure completeness, report
   quarantine rates.

### Gold Layer

1. **Revenue by category/month**: Monthly revenue, order count, and average order
   value broken down by product category.
2. **Customer lifetime value (CLV)**: Total spend, order count, average order
   value, first and last order dates, customer tenure in days.
3. **Product performance**: Total units sold, total revenue, average rating proxy,
   return/cancellation rate, revenue rank within category.
4. **Customer segmentation (RFM)**: Recency (days since last order), Frequency
   (total orders), Monetary (total spend). Assign quartile scores (1-4 per
   dimension) and segment labels (Champions, Loyal, At Risk, Lost, etc.).
5. **Daily dashboard summary**: A single-row-per-day table with total revenue,
   order count, new customers, top product, and top category.

### Optimization

1. Run `OPTIMIZE` on Silver and Gold tables.
2. Apply `ZORDER` on frequently queried columns (order_date, customer_id).
3. Demonstrate `VACUUM` with a retention period.

---

## RFM Segmentation Reference

RFM analysis scores each customer on three dimensions:

| Dimension | Calculation | Quartile 4 (Best) | Quartile 1 (Worst) |
|-----------|-------------|--------------------|--------------------|
| **Recency** | Days since last order | Most recent (low days) | Longest ago (high days) |
| **Frequency** | Total number of orders | Most orders | Fewest orders |
| **Monetary** | Total spend | Highest spend | Lowest spend |

Combined RFM score (sum of three quartiles, range 3-12) maps to segments:

| Score Range | Segment Label | Description |
|-------------|---------------|-------------|
| 10-12 | Champions | Best customers -- recent, frequent, high spend |
| 7-9 | Loyal Customers | Regular buyers with solid spend |
| 5-6 | At Risk | Were good customers but activity is declining |
| 3-4 | Lost | Have not purchased recently, low engagement |

---

## Implementation Tips

1. **Generate data first, pipeline second.** Get all four DataFrames created and
   validated before building any Delta tables.

2. **Build layer by layer.** Complete Bronze fully before starting Silver.
   Complete Silver fully before starting Gold. This mirrors how real pipelines
   are developed and tested.

3. **Use `spark.sql` for Gold aggregations.** SQL is often more readable than
   DataFrame API for complex aggregations with GROUP BY and window functions.

4. **Window functions for RFM quartiles.** Use `F.ntile(4).over(window)` to
   assign quartile scores. Remember that ntile assigns roughly equal-sized
   groups.

5. **Data quality as a first-class concern.** After each layer, print a summary
   of row counts, null rates, and duplicate counts. This catches bugs early and
   mirrors production monitoring.

---

## Extension Ideas

Once you complete the base project, try these enhancements:

1. **Cohort analysis**: Group customers by signup month and track retention rates
   (what percentage place orders in months 1, 2, 3, etc. after signup).

2. **Market basket analysis**: Find which products are frequently ordered together
   using clickstream session data.

3. **Incremental pipeline**: Convert the pipeline to use Auto Loader for Bronze
   ingestion and MERGE for Silver upserts, so it can process new data
   incrementally rather than rebuilding from scratch.

4. **Data quality framework**: Build a reusable quality check function that
   accepts a DataFrame, a list of rules (non-null, range, uniqueness), and returns
   a quality report DataFrame.

5. **Time travel audit**: Use Delta Lake time travel to compare Gold table states
   across pipeline runs and detect unexpected changes in business metrics.

---

## Companion Notebook

The reference implementation is in
[01-ecommerce-pipeline_notebook.py](01-ecommerce-pipeline_notebook.py). Import
it into Databricks via Workspace > Import > File.

The notebook is self-contained: it generates all sample data, builds the full
pipeline, and cleans up all tables at the end.

---

## Concepts Practiced

| Concept | Module Source | How It Is Used |
|---------|--------------|----------------|
| DataFrame API | Module 01 | All transformations |
| Delta Lake tables | Module 03 | Bronze, Silver, Gold storage |
| MERGE / upsert | Module 03 | Silver deduplication |
| Time travel | Module 03 | Pipeline auditing |
| Window functions | Module 04 | RFM quartiles, ranking, deduplication |
| Joins | Module 04 | Enriched orders (3-way join) |
| Aggregations | Module 04 | All Gold tables |
| OPTIMIZE / ZORDER | Module 05 | Table optimization |
| VACUUM | Module 05 | Storage management |
| Data quality checks | Module 09 | Cross-layer validation |
