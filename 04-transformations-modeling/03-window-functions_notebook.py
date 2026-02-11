# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 -- Window Functions
# MAGIC
# MAGIC **Module 04 | Topic 03 | Level: Intermediate | Time: 45 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Use ROW_NUMBER, RANK, and DENSE_RANK to order movies within each studio
# MAGIC - Extract Top-N per group
# MAGIC - Compute LAG/LEAD for year-over-year comparisons
# MAGIC - Build running totals and moving averages with frame specifications
# MAGIC - Use NTILE for percentile bucketing

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Movies & Studios Dataset

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, ntile,
    lag, lead, sum as _sum, avg, count, round as _round
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

movies_data = [
    (1, "Avengers: Endgame",     "Marvel",  2797.0, 2019),
    (2, "Iron Man",              "Marvel",   585.0, 2008),
    (3, "Black Panther",         "Marvel",  1347.0, 2018),
    (4, "Captain America: CW",   "Marvel",  1153.0, 2016),
    (5, "Spider-Man: NWH",       "Marvel",  1916.0, 2021),
    (6, "The Dark Knight",       "Warner",  1005.0, 2008),
    (7, "Inception",             "Warner",   836.0, 2010),
    (8, "Tenet",                 "Warner",   363.0, 2020),
    (9, "Dune",                  "Warner",   401.0, 2021),
    (10, "Frozen",               "Disney",  1280.0, 2013),
    (11, "Frozen II",            "Disney",  1450.0, 2019),
    (12, "Moana",                "Disney",   643.0, 2016),
    (13, "Encanto",              "Disney",   256.0, 2021),
    (14, "Coco",                 "Pixar",    807.0, 2017),
    (15, "Inside Out",           "Pixar",    857.0, 2015),
    (16, "Toy Story 4",          "Pixar",   1073.0, 2019),
    (17, "Soul",                 "Pixar",    121.0, 2020),
]

movies_df = spark.createDataFrame(
    data=movies_data,
    schema=["movie_id", "title", "studio", "revenue_millions", "release_year"]
)
movies_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- ROW_NUMBER, RANK, DENSE_RANK
# MAGIC
# MAGIC Order movies within each studio based on revenue (descending).
# MAGIC
# MAGIC - `ROW_NUMBER`: always unique, arbitrary order for ties
# MAGIC - `RANK`: same rank for ties, gaps after
# MAGIC - `DENSE_RANK`: same rank for ties, no gaps

# COMMAND ----------

# Define the window: partition by studio, order by revenue descending
w_revenue = Window.partitionBy("studio").orderBy(col("revenue_millions").desc())

ranked_df = movies_df.select(
    "studio",
    "title",
    "revenue_millions",
    "release_year",
    row_number().over(w_revenue).alias("row_num"),
    rank().over(w_revenue).alias("rank"),
    dense_rank().over(w_revenue).alias("dense_rank"),
)
ranked_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- Top-N Per Group
# MAGIC
# MAGIC Find the top 2 highest-grossing movies per studio.

# COMMAND ----------

top2_per_studio = ranked_df.filter(col("row_num") <= 2)
print("Top 2 movies per studio by revenue:")
top2_per_studio.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- LAG and LEAD
# MAGIC
# MAGIC Compare each movie's revenue with the previous and next movie
# MAGIC (ordered by release year) within the same studio.

# COMMAND ----------

w_year = Window.partitionBy("studio").orderBy("release_year")

lag_lead_df = movies_df.select(
    "studio",
    "title",
    "release_year",
    "revenue_millions",
    lag("revenue_millions", 1).over(w_year).alias("prev_movie_revenue"),
    lead("revenue_millions", 1).over(w_year).alias("next_movie_revenue"),
)
lag_lead_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- Year-over-Year Revenue Change
# MAGIC
# MAGIC Use LAG to calculate the revenue difference from the previous release.

# COMMAND ----------

yoy_df = movies_df.select(
    "studio",
    "title",
    "release_year",
    "revenue_millions",
    lag("revenue_millions", 1).over(w_year).alias("prev_revenue"),
).withColumn(
    "revenue_change",
    _round(col("revenue_millions") - col("prev_revenue"), 2)
).withColumn(
    "pct_change",
    _round((col("revenue_millions") - col("prev_revenue")) / col("prev_revenue") * 100, 1)
)

print("Year-over-year revenue change per studio:")
yoy_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Running Total
# MAGIC
# MAGIC Cumulative revenue per studio ordered by release year.
# MAGIC Frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`

# COMMAND ----------

w_running = Window.partitionBy("studio").orderBy("release_year") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_total_df = movies_df.select(
    "studio",
    "title",
    "release_year",
    "revenue_millions",
    _round(_sum("revenue_millions").over(w_running), 2).alias("cumulative_revenue"),
    count("movie_id").over(w_running).alias("cumulative_movie_count"),
)
running_total_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Moving Average (3-Movie Window)
# MAGIC
# MAGIC Average revenue over the current movie and the 2 preceding movies.
# MAGIC Frame: `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`

# COMMAND ----------

w_moving = Window.partitionBy("studio").orderBy("release_year") \
    .rowsBetween(-2, Window.currentRow)

moving_avg_df = movies_df.select(
    "studio",
    "title",
    "release_year",
    "revenue_millions",
    _round(avg("revenue_millions").over(w_moving), 2).alias("moving_avg_3"),
)
print("3-movie moving average per studio:")
moving_avg_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- NTILE: Percentile Bucketing
# MAGIC
# MAGIC Distribute all movies into 4 quartiles based on revenue.

# COMMAND ----------

w_all = Window.orderBy(col("revenue_millions").desc())

ntile_df = movies_df.select(
    "title",
    "studio",
    "revenue_millions",
    ntile(4).over(w_all).alias("quartile"),
)
print("Revenue quartiles (1 = top 25%):")
ntile_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- Partition-Level Aggregates Without Collapsing
# MAGIC
# MAGIC Add studio-level totals to every row without groupBy.

# COMMAND ----------

w_studio = Window.partitionBy("studio")

enriched_df = movies_df.select(
    "studio",
    "title",
    "revenue_millions",
    _round(_sum("revenue_millions").over(w_studio), 2).alias("studio_total"),
    _round(avg("revenue_millions").over(w_studio), 2).alias("studio_avg"),
    count("movie_id").over(w_studio).alias("studio_movie_count"),
).withColumn(
    "pct_of_studio",
    _round(col("revenue_millions") / col("studio_total") * 100, 1)
)

enriched_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- SQL Window Functions
# MAGIC
# MAGIC Temporary View for SQL-based window queries.

# COMMAND ----------

movies_df.createOrReplaceTempView("movies")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 2 movies per studio using SQL window function
# MAGIC SELECT studio, title, revenue_millions, rn
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         studio,
# MAGIC         title,
# MAGIC         revenue_millions,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY studio
# MAGIC             ORDER BY revenue_millions DESC
# MAGIC         ) AS rn
# MAGIC     FROM movies
# MAGIC )
# MAGIC WHERE rn <= 2
# MAGIC ORDER BY studio, rn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running total and moving average in SQL
# MAGIC SELECT
# MAGIC     studio,
# MAGIC     title,
# MAGIC     release_year,
# MAGIC     revenue_millions,
# MAGIC     SUM(revenue_millions) OVER (
# MAGIC         PARTITION BY studio
# MAGIC         ORDER BY release_year
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS cumulative_revenue,
# MAGIC     ROUND(AVG(revenue_millions) OVER (
# MAGIC         PARTITION BY studio
# MAGIC         ORDER BY release_year
# MAGIC         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS moving_avg_3
# MAGIC FROM movies
# MAGIC ORDER BY studio, release_year;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Cleanup

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS movies")
print("Cleanup complete.")
