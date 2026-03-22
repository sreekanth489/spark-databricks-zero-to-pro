# ---------------------------------------------------------------------------
# Silver Layer: Orders Fact (with Expectations and Auto CDC)
# ---------------------------------------------------------------------------
# Creates a staging view with data quality expectations, then applies
# Auto CDC flow to upsert into the target streaming table using SCD Type 1.
#
# Expectations:
#   - valid_date:   year(order_date) >= 2020
#   - valid_rating: customer_rating BETWEEN 1 AND 5
#   - valid_amount: order_amount > 0
#
# Target: ecommerce.silver.orders
# Source:  ecommerce.bronze.orders (via staging view)
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
import pyspark.sql.functions as F


# -- Step 1: Staging view with expectations ----------------------------------
# This temporary view applies data quality checks and renames columns
# for business context. Bad records are dropped (expect_or_drop).

@dp.view(
    name="orders_staging",
    comment="Staging view: bronze orders with quality checks and business column names",
)
@dp.expect("valid_date", "year(order_date) >= 2020")
@dp.expect_or_drop("valid_rating", "customer_rating BETWEEN 1 AND 5")
@dp.expect_or_drop("valid_amount", "order_amount > 0")
def orders_staging():
    return (
        spark.readStream.table("ecommerce.bronze.orders")
        .select(
            F.col("order_id"),
            F.col("order_date").cast("date").alias("order_date"),
            F.col("store_id"),
            F.col("customer_type"),
            F.col("order_amount").cast("double").alias("order_amount"),
            F.col("items_count").cast("int").alias("items_count"),
            F.col("customer_rating").cast("int").alias("customer_rating"),
            F.col("ingest_datetime").alias("bronze_ingest_datetime"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


# -- Step 2: Target streaming table -----------------------------------------
dp.create_streaming_table(
    name="ecommerce.silver.orders",
    comment="Cleansed orders with CDC applied via SCD Type 1 (Silver layer)",
    # schema="ecommerce.silver",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)


# -- Step 3: Auto CDC flow --------------------------------------------------
# Uses order_id as the primary key. SEQUENCE BY ingest_datetime ensures
# the latest version of each order wins in case of duplicates.
dp.create_auto_cdc_flow(
    name="orders_cdc_flow",
    target="ecommerce.silver.orders",
    source="orders_staging",
    keys=["order_id"],
    sequence_by="bronze_ingest_datetime",
    stored_as_scd_type=1,
)
