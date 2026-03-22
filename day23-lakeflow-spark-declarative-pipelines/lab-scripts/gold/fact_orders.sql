-- ---------------------------------------------------------------------------
-- Gold Layer: Fact Orders (Denormalized Materialized View)
-- ---------------------------------------------------------------------------
-- Joins silver.orders with silver.stores and silver.calendar to produce
-- a single denormalized materialized view optimized for analytics and reporting.
--
-- Target: ecommerce.gold.fact_orders
-- Sources: ecommerce.silver.orders
--          ecommerce.silver.stores
--          ecommerce.silver.calendar
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW ecommerce.gold.fact_orders
COMMENT 'Denormalized fact table joining orders, stores, and calendar (Gold layer)'
AS
SELECT
    -- Order attributes
    o.order_id,
    o.order_date,
    o.customer_type,
    o.order_amount,
    o.items_count,
    o.customer_rating,

    -- Store attributes
    s.store_id,
    s.store_name,
    s.store_city,
    s.store_region,

    -- Calendar attributes
    c.year                  AS order_year,
    c.quarter               AS order_quarter,
    c.month                 AS order_month,
    c.month_name            AS order_month_name,
    c.week_of_year          AS order_week_of_year,
    c.day_name              AS order_day_name,
    c.is_weekend            AS order_is_weekend,
    c.is_weekday            AS order_is_weekday,
    c.is_us_holiday         AS order_is_us_holiday,
    c.holiday_name          AS order_holiday_name,

    -- Processing metadata
    o.silver_processed_timestamp

FROM ecommerce.silver.orders    AS o
JOIN ecommerce.silver.stores    AS s ON o.store_id = s.store_id
JOIN ecommerce.silver.calendar  AS c ON o.order_date = c.calendar_date;
