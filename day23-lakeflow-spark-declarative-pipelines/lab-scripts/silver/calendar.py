# ---------------------------------------------------------------------------
# Silver Layer: Calendar Dimension
# ---------------------------------------------------------------------------
# Generates a date dimension table programmatically using the
# explode(sequence(...)) pattern. Includes US holiday flags, weekend
# indicators, and standard calendar attributes.
#
# Pipeline configuration parameters:
#   - start_date: first date in the calendar (e.g., 2024-01-01)
#   - end_date:   last date in the calendar  (e.g., 2024-12-31)
#
# Target: ecommerce.silver.calendar
# Source:  generated programmatically (no upstream table)
# ---------------------------------------------------------------------------

from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="ecommerce.silver.calendar",
    comment="Date dimension with US holidays and calendar attributes (Silver layer)",
    # schema="ecommerce.silver",
    table_properties={
        "quality": "silver",
    },
)
def calendar():
    start_date = spark.conf.get("start_date", "2024-01-01")
    end_date = spark.conf.get("end_date", "2024-12-31")

    # Generate one row per date in the range
    dates_df = (
        spark.sql(f"""
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 day
                )
            ) AS calendar_date
        """)
    )

    # Add calendar attributes
    calendar_df = (
        dates_df
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("day_of_month", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("calendar_date").isin(1, 7), True)
            .otherwise(False),
        )
        .withColumn(
            "is_weekday",
            F.when(F.dayofweek("calendar_date").isin(1, 7), False)
            .otherwise(True),
        )
    )

    # US federal holidays (fixed dates and common approximations)
    # Note: Thanksgiving is 4th Thursday of November -- computed dynamically
    calendar_df = (
        calendar_df
        .withColumn(
            "is_us_holiday",
            F.when(
                # New Year's Day -- January 1
                (F.month("calendar_date") == 1) & (F.dayofmonth("calendar_date") == 1),
                True,
            )
            .when(
                # Independence Day -- July 4
                (F.month("calendar_date") == 7) & (F.dayofmonth("calendar_date") == 4),
                True,
            )
            .when(
                # Christmas Day -- December 25
                (F.month("calendar_date") == 12) & (F.dayofmonth("calendar_date") == 25),
                True,
            )
            .when(
                # Thanksgiving -- 4th Thursday of November
                (F.month("calendar_date") == 11)
                & (F.date_format("calendar_date", "EEEE") == "Thursday")
                & (F.dayofmonth("calendar_date").between(22, 28)),
                True,
            )
            .otherwise(False),
        )
        .withColumn(
            "holiday_name",
            F.when(
                (F.month("calendar_date") == 1) & (F.dayofmonth("calendar_date") == 1),
                F.lit("New Year's Day"),
            )
            .when(
                (F.month("calendar_date") == 7) & (F.dayofmonth("calendar_date") == 4),
                F.lit("Independence Day"),
            )
            .when(
                (F.month("calendar_date") == 12) & (F.dayofmonth("calendar_date") == 25),
                F.lit("Christmas Day"),
            )
            .when(
                (F.month("calendar_date") == 11)
                & (F.date_format("calendar_date", "EEEE") == "Thursday")
                & (F.dayofmonth("calendar_date").between(22, 28)),
                F.lit("Thanksgiving"),
            )
            .otherwise(F.lit(None)),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )

    return calendar_df
