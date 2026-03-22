-- task_daily_report.sql -- Generate daily revenue summary from the Gold layer.
--
-- This SQL script is designed to run as a SQL task within a Lakeflow Job.
-- It reads from the Gold fact_orders table and produces a daily revenue
-- summary aggregated by store and date.
--
-- Prerequisites:
--   - ecommerce.gold.fact_orders must exist (populated by the SDP pipeline)
--   - ecommerce.gold.daily_revenue_report will be created/replaced
--
-- Usage in a Lakeflow Job:
--   Task type: SQL
--   SQL warehouse: your_warehouse_id

-- Create or replace the daily revenue report table
CREATE OR REPLACE TABLE ecommerce.gold.daily_revenue_report AS
SELECT
    order_date,
    store_id,
    store_name,
    COUNT(DISTINCT order_id)            AS total_orders,
    COUNT(DISTINCT customer_id)         AS unique_customers,
    SUM(order_total)                    AS gross_revenue,
    SUM(discount_amount)                AS total_discounts,
    SUM(order_total - discount_amount)  AS net_revenue,
    AVG(order_total)                    AS avg_order_value,
    MIN(order_total)                    AS min_order_value,
    MAX(order_total)                    AS max_order_value
FROM ecommerce.gold.fact_orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 1)
  AND order_date < CURRENT_DATE()
GROUP BY
    order_date,
    store_id,
    store_name
ORDER BY
    order_date DESC,
    net_revenue DESC;

-- Display the report summary
SELECT
    order_date,
    COUNT(DISTINCT store_id)  AS stores_with_orders,
    SUM(total_orders)         AS total_orders,
    SUM(unique_customers)     AS unique_customers,
    ROUND(SUM(net_revenue), 2) AS total_net_revenue,
    ROUND(AVG(avg_order_value), 2) AS overall_avg_order_value
FROM ecommerce.gold.daily_revenue_report
GROUP BY order_date
ORDER BY order_date DESC;
