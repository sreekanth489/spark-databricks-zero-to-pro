-- ---------------------------------------------------------------------------
-- Gold Layer: Regional Materialized View -- West
-- ---------------------------------------------------------------------------
-- Filters the denormalized fact_orders view to the West region only.
-- Provides a pre-filtered view for regional analysts and dashboards.
--
-- Target: ecommerce.gold.fact_orders_west
-- Source:  ecommerce.gold.fact_orders
-- ---------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW ecommerce.gold.fact_orders_west
COMMENT 'West regional orders materialized view (Gold layer)'
AS
SELECT *
FROM ecommerce.gold.fact_orders
WHERE store_region = 'West';
