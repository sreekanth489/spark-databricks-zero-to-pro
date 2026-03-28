# ---------------------------------------------------------------------------
# Bronze Layer: Orders Fact (Streaming)
# ---------------------------------------------------------------------------
# Reads daily order CSV files from S3 using Auto Loader (cloudFiles).
# New files are automatically detected and appended to the streaming table.
#
# Target: ecommerce.bronze.orders
# Source:  s3://ecommerce-lakehouse/data-store/orders/
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import col


@dp.table(
    name="ecommerce.bronze.orders",
    comment="Raw orders ingested from S3 via Auto Loader (Bronze layer)",
    # schema="ecommerce.bronze",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("maxFilesPerTrigger", "100")
        .load("s3://ecommerce-lakehouse/data-store/orders")
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("ingest_datetime", F.current_timestamp())
    )
