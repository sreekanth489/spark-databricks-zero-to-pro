# ---------------------------------------------------------------------------
# Bronze Layer: Stores Dimension
# ---------------------------------------------------------------------------
# Reads the stores CSV from S3 and lands it in the bronze layer as a
# materialized view. Uses PERMISSIVE mode to capture corrupt records
# rather than silently dropping them.
#
# Target: ecommerce.bronze.stores
# Source:  s3://ecommerce-lakehouse/data-store/stores/stores.csv
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


STORES_SCHEMA = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


@dp.materialized_view(
    name="stores",
    comment="Raw stores dimension ingested from S3 CSV (Bronze layer)",
    schema="ecommerce.bronze",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def stores():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(STORES_SCHEMA)
        .load("s3://ecommerce-lakehouse/data-store/stores")
        .withColumn("file_name", F.input_file_name())
        .withColumn("ingest_datetime", F.current_timestamp())
    )
