-- ---------------------------------------------------------------------------
-- Gold Layer: Regional View -- Midwest
-- ---------------------------------------------------------------------------
-- Filters the denormalized fact_orders view to the Midwest region only.
-- Provides a pre-filtered view for regional analysts and dashboards.
--
-- Target: ecommerce.gold.fact_orders_midwest
-- Source:  ecommerce.gold.fact_orders
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW ecommerce.gold.fact_orders_midwest
COMMENT 'Midwest regional orders view (Gold layer)'
AS
SELECT *
FROM ecommerce.gold.fact_orders
WHERE store_region = 'Midwest';
