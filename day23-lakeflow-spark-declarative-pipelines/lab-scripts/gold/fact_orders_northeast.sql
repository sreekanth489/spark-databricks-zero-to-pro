-- ---------------------------------------------------------------------------
-- Gold Layer: Regional Materialized View -- Northeast
-- ---------------------------------------------------------------------------
-- Filters the denormalized fact_orders view to the Northeast region only.
-- Provides a pre-filtered view for regional analysts and dashboards.
--
-- Target: ecommerce.gold.fact_orders_northeast
-- Source:  ecommerce.gold.fact_orders
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW ecommerce.gold.fact_orders_northeast
COMMENT 'Northeast regional orders materialized view (Gold layer)'
AS
SELECT *
FROM ecommerce.gold.fact_orders
WHERE store_region = 'Northeast';
