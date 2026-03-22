-- ---------------------------------------------------------------------------
-- Gold Layer: Regional View -- Southeast
-- ---------------------------------------------------------------------------
-- Filters the denormalized fact_orders view to the Southeast region only.
-- Provides a pre-filtered view for regional analysts and dashboards.
--
-- Target: ecommerce.gold.fact_orders_southeast
-- Source:  ecommerce.gold.fact_orders
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW ecommerce.gold.fact_orders_southeast
COMMENT 'Southeast regional orders view (Gold layer)'
AS
SELECT *
FROM ecommerce.gold.fact_orders
WHERE store_region = 'Southeast';
