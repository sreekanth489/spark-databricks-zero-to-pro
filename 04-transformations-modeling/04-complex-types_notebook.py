# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 -- Complex Types
# MAGIC
# MAGIC **Module 04 | Topic 04 | Level: Intermediate | Time: 40 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Create data with arrays, maps, and structs
# MAGIC - Access struct fields with dot notation
# MAGIC - Use explode/posexplode to flatten arrays
# MAGIC - Apply array and map functions
# MAGIC - Parse and produce JSON with from_json/to_json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Create Nested Event Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, posexplode, explode_outer,
    array, array_contains, array_distinct, array_union, array_intersect,
    array_except, flatten, sort_array, size, slice,
    map_keys, map_values, map_from_entries, element_at, map_concat,
    struct, from_json, to_json, schema_of_json, get_json_object,
    lit, create_map, zip_with, concat
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType,
    DoubleType
)

spark = SparkSession.builder.getOrCreate()

# Nested event data simulating web analytics
events_data = [
    ("U001", "page_view",  "2024-01-15 10:30:00", "products",  "google.com",    ["electronics", "sale"]),
    ("U001", "add_to_cart","2024-01-15 10:35:00", "products",  "direct",        ["electronics"]),
    ("U002", "page_view",  "2024-01-15 11:00:00", "home",      "facebook.com",  ["social"]),
    ("U002", "page_view",  "2024-01-15 11:05:00", "books",     "facebook.com",  ["books", "education"]),
    ("U003", "purchase",   "2024-01-15 12:00:00", "checkout",  "google.com",    ["electronics", "books", "sale"]),
    ("U003", "page_view",  "2024-01-15 12:30:00", "home",      None,            []),
]

events_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("event", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("page", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
])

events_df = spark.createDataFrame(data=events_data, schema=events_schema)
events_df.show(truncate=False)
events_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- Working with Structs
# MAGIC
# MAGIC Create a struct column and access its fields with dot notation.

# COMMAND ----------

# Create a struct column from existing columns
struct_df = events_df.select(
    "user_id",
    struct(
        col("page").alias("page"),
        col("referrer").alias("referrer"),
        col("tags").alias("tags"),
    ).alias("properties"),
    "event",
    "timestamp",
)

# Access struct fields with dot notation
print("Accessing struct fields with dot notation:")
struct_df.select(
    "user_id",
    "properties.page",
    "properties.referrer",
    "properties.tags",
).show(truncate=False)

struct_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- Explode: Arrays to Rows
# MAGIC
# MAGIC `explode` creates one row per array element.
# MAGIC `posexplode` also includes the position index.
# MAGIC
# MAGIC **Note**: `explode` drops rows with null or empty arrays.
# MAGIC Use `explode_outer` to preserve them.

# COMMAND ----------

# Standard explode -- rows with empty/null arrays are dropped
print("explode (drops empty arrays):")
exploded_df = events_df.select("user_id", "event", explode("tags").alias("tag"))
exploded_df.show(truncate=False)
print(f"Row count: {exploded_df.count()}")

# posexplode -- includes position index
print("\nposexplode (with position):")
events_df.select("user_id", posexplode("tags").alias("pos", "tag")).show(truncate=False)

# explode_outer -- preserves rows with null/empty arrays
print("\nexplode_outer (preserves empty):")
events_df.select("user_id", "event", explode_outer("tags").alias("tag")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- Array Functions

# COMMAND ----------

# array_contains: check if a tag is present
print("Events tagged 'sale':")
events_df.filter(array_contains(col("tags"), "sale")).select(
    "user_id", "event", "tags"
).show(truncate=False)

# size: number of elements
print("Number of tags per event:")
events_df.select("user_id", "event", "tags", size("tags").alias("tag_count")).show(truncate=False)

# array_distinct: remove duplicate elements
dup_array_df = spark.createDataFrame(
    [("U001", ["a", "b", "a", "c", "b"])],
    schema=["user_id", "items"]
)
print("array_distinct:")
dup_array_df.select("items", array_distinct("items").alias("unique_items")).show(truncate=False)

# sort_array: sort elements
print("sort_array:")
events_df.select("user_id", "tags", sort_array("tags").alias("sorted_tags")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- Array Set Operations

# COMMAND ----------

# Compare tags between two events for the same user
set_df = spark.createDataFrame([
    ("U001", ["electronics", "sale", "new"],     ["electronics", "premium"]),
    ("U002", ["books", "education"],              ["books", "science"]),
], schema=["user_id", "tags_visit1", "tags_visit2"])

print("Array union (all unique tags):")
set_df.select("user_id", array_union("tags_visit1", "tags_visit2").alias("all_tags")).show(truncate=False)

print("Array intersect (common tags):")
set_df.select("user_id", array_intersect("tags_visit1", "tags_visit2").alias("common_tags")).show(truncate=False)

print("Array except (tags in visit1 but not visit2):")
set_df.select("user_id", array_except("tags_visit1", "tags_visit2").alias("only_visit1")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Flatten Nested Arrays

# COMMAND ----------

# Flatten an array of arrays into a single array
nested_df = spark.createDataFrame([
    ("U001", [["electronics", "sale"], ["books"]]),
    ("U002", [["social"], ["education", "science"]]),
], schema=StructType([
    StructField("user_id", StringType()),
    StructField("tag_groups", ArrayType(ArrayType(StringType()))),
]))

print("Before flatten:")
nested_df.show(truncate=False)

print("After flatten:")
nested_df.select("user_id", flatten("tag_groups").alias("all_tags")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Working with Maps

# COMMAND ----------

# Create data with map columns
map_data = [
    ("U001", {"browser": "Chrome", "os": "macOS", "device": "desktop"}),
    ("U002", {"browser": "Safari", "os": "iOS",   "device": "mobile"}),
    ("U003", {"browser": "Firefox", "os": "Linux", "device": "desktop"}),
]
map_schema = StructType([
    StructField("user_id", StringType()),
    StructField("user_agent", MapType(StringType(), StringType())),
])
map_df = spark.createDataFrame(data=map_data, schema=map_schema)
map_df.show(truncate=False)

# Access a specific key
print("Get browser from map:")
map_df.select("user_id", element_at("user_agent", "browser").alias("browser")).show()

# Get all keys and values
print("Map keys and values:")
map_df.select(
    "user_id",
    map_keys("user_agent").alias("keys"),
    map_values("user_agent").alias("values"),
).show(truncate=False)

# Explode map into key-value rows
print("Explode map:")
map_df.select("user_id", explode("user_agent").alias("key", "value")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- Parsing JSON Strings

# COMMAND ----------

# Simulate raw JSON strings (as you might receive from a message queue)
json_data = [
    ('{"name": "Alice", "age": 30, "scores": [95, 88, 72]}',),
    ('{"name": "Bob",   "age": 25, "scores": [80, 91]}',),
    ('{"name": "Carol", "age": 35, "scores": [70, 85, 90, 88]}',),
]
json_df = spark.createDataFrame(data=json_data, schema=["raw_json"])
json_df.show(truncate=False)

# Step 1: Infer schema from a sample
sample_json = '{"name": "Alice", "age": 30, "scores": [95, 88, 72]}'
inferred_schema = schema_of_json(lit(sample_json))
print(f"Inferred schema: ")
spark.createDataFrame([("x",)], ["dummy"]).select(inferred_schema).show(truncate=False)

# Step 2: Parse JSON string into struct using from_json
parsed_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("scores", ArrayType(IntegerType())),
])

parsed_df = json_df.select(
    from_json(col("raw_json"), parsed_schema).alias("parsed")
)

# Step 3: Access nested fields
print("Parsed and flattened:")
parsed_df.select("parsed.name", "parsed.age", "parsed.scores").show(truncate=False)

# Step 4: Convert back to JSON
print("Back to JSON string:")
parsed_df.select(to_json("parsed").alias("json_string")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- get_json_object for Quick Extraction
# MAGIC
# MAGIC When you need just one or two fields from JSON, `get_json_object` is
# MAGIC faster than parsing the entire structure.

# COMMAND ----------

json_df.select(
    get_json_object("raw_json", "$.name").alias("name"),
    get_json_object("raw_json", "$.age").alias("age"),
    get_json_object("raw_json", "$.scores[0]").alias("first_score"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- zip_with: Combine Arrays Element-Wise

# COMMAND ----------

zip_df = spark.createDataFrame([
    ("U001", ["math", "science", "english"], [95, 88, 72]),
    ("U002", ["math", "science"],            [80, 91]),
], schema=StructType([
    StructField("student", StringType()),
    StructField("subjects", ArrayType(StringType())),
    StructField("scores", ArrayType(IntegerType())),
]))

# Combine subject and score into "subject:score" strings
result = zip_df.select(
    "student",
    zip_with("subjects", "scores", lambda s, sc: concat(s, lit(":"), sc.cast("string"))).alias("combined"),
)
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Flattening Deeply Nested Structs

# COMMAND ----------

# Simulate a deeply nested structure
deep_data = [
    (1, ("Alice", ("123 Main St", "Springfield", "IL"))),
    (2, ("Bob",   ("456 Oak Ave", "Portland", "OR"))),
]
deep_schema = StructType([
    StructField("id", IntegerType()),
    StructField("person", StructType([
        StructField("name", StringType()),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
        ])),
    ])),
])
deep_df = spark.createDataFrame(data=deep_data, schema=deep_schema)
deep_df.printSchema()

# Flatten using dot notation
flat_df = deep_df.select(
    "id",
    "person.name",
    "person.address.street",
    "person.address.city",
    "person.address.state",
)
flat_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 -- Cleanup

# COMMAND ----------

print("No temporary views to clean up in this notebook.")
print("Complex types complete.")
