# ---------------------------------------------------------------------------
# Silver Layer: Stores Dimension
# ---------------------------------------------------------------------------
# Reads from bronze.stores, selects and renames columns for business use,
# and drops any corrupt records that were captured in the Bronze layer.
#
# Target: ecommerce.silver.stores
# Source:  ecommerce.bronze.stores
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="ecommerce.silver.stores",
    comment="Cleansed store dimension with business-friendly column names (Silver layer)",
    # schema="ecommerce.silver",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def stores():
    return (
        spark.read.table("ecommerce.bronze.stores")
        .filter(F.col("_corrupt_record").isNull())
        .select(
            F.col("store_id"),
            F.col("store_name"),
            F.col("city").alias("store_city"),
            F.col("region").alias("store_region"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )
