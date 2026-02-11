# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # AI Functions on Databricks
# MAGIC
# MAGIC This notebook demonstrates Databricks AI Functions for SQL-based LLM access.
# MAGIC We cover `ai_query()`, `ai_classify()`, `ai_extract()`, `ai_summarize()`,
# MAGIC `ai_translate()`, `ai_sentiment()`, and their use in ETL pipelines.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 14.x+ (full workspace for AI Functions).
# MAGIC
# MAGIC **Note:** AI Function calls require Foundation Model API access. SQL templates
# MAGIC are provided as reference. Simulated outputs demonstrate the expected results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: Create Sample Datasets

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType
)
from pyspark.sql.functions import (
    col, lit, when, concat, length, lower, regexp_extract,
    array, struct, udf, current_timestamp
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Product reviews dataset
reviews_data = [
    (1, "The noise-cancelling headphones are phenomenal. Crystal clear audio and 30-hour battery life. Worth every penny!", "Electronics", 5, "2024-03-15"),
    (2, "Ordered a medium shirt but received an XL. The fabric quality is nice but the sizing is completely wrong.", "Clothing", 2, "2024-03-16"),
    (3, "This blender handles everything from smoothies to soups. A bit loud but very powerful.", "Kitchen", 4, "2024-03-17"),
    (4, "The worst laptop I have ever owned. Overheats within 30 minutes and the keyboard keys stick.", "Electronics", 1, "2024-03-18"),
    (5, "Great value running shoes. Comfortable for 5K runs. Not ideal for marathons though.", "Footwear", 3, "2024-03-19"),
    (6, "Customer support was incredibly helpful when my order arrived damaged. Full refund processed same day.", "General", 5, "2024-03-20"),
    (7, "The standing desk motor is smooth and quiet. Memory presets save time. Assembly was straightforward.", "Furniture", 5, "2024-03-21"),
    (8, "This phone case claims military-grade protection but cracked on a 2-foot drop. Total waste of money.", "Electronics", 1, "2024-03-22"),
    (9, "Decent wireless mouse for the price. Scroll wheel is a bit stiff. Battery lasts about 2 months.", "Electronics", 3, "2024-03-23"),
    (10, "The espresso machine produces cafe-quality shots. Steaming wand needs better pressure though.", "Kitchen", 4, "2024-03-24"),
]

reviews_schema = StructType([
    StructField("review_id", IntegerType(), False),
    StructField("review_text", StringType(), False),
    StructField("category", StringType(), False),
    StructField("star_rating", IntegerType(), False),
    StructField("review_date", StringType(), False),
])

df_reviews = spark.createDataFrame(reviews_data, schema=reviews_schema)
df_reviews.createOrReplaceTempView("product_reviews")
df_reviews.show(truncate=60)

# COMMAND ----------

# Customer messages dataset (multi-language simulation)
messages_data = [
    (1, "I need to cancel my subscription immediately.", "en"),
    (2, "When will my package arrive? Order number 45678.", "en"),
    (3, "The software crashes every time I open a large file.", "en"),
    (4, "Can you recommend a good laptop for video editing?", "en"),
    (5, "I was charged twice for the same order. Please refund.", "en"),
    (6, "How do I export my data from the platform?", "en"),
    (7, "Your product changed my workflow completely. Thank you!", "en"),
    (8, "The delivery driver left my package in the rain.", "en"),
]

messages_schema = StructType([
    StructField("message_id", IntegerType(), False),
    StructField("content", StringType(), False),
    StructField("language", StringType(), False),
])

df_messages = spark.createDataFrame(messages_data, schema=messages_schema)
df_messages.createOrReplaceTempView("customer_messages")
df_messages.show(truncate=60)

# COMMAND ----------

# Invoice text dataset
invoices_data = [
    (1, "Invoice #INV-2024-001 from Acme Corp. Date: March 15, 2024. Total: $5,250.00. Due: April 15, 2024. Services: Cloud hosting for Q1."),
    (2, "Invoice #INV-2024-002 from TechSupply Inc. Date: March 20, 2024. Total: $1,890.50. Due: April 20, 2024. Items: 10x USB-C cables, 5x monitors."),
    (3, "Invoice #INV-2024-003 from DataFlow LLC. Date: March 25, 2024. Total: $12,000.00. Due: April 25, 2024. Services: Annual data platform license."),
    (4, "Invoice #INV-2024-004 from OfficeMax. Date: April 1, 2024. Total: $423.75. Due: May 1, 2024. Items: Paper, toner, desk organizers."),
    (5, "Invoice #INV-2024-005 from SecureNet. Date: April 5, 2024. Total: $8,500.00. Due: May 5, 2024. Services: Penetration testing and security audit."),
]

invoice_schema = StructType([
    StructField("invoice_id", IntegerType(), False),
    StructField("invoice_text", StringType(), False),
])

df_invoices = spark.createDataFrame(invoices_data, schema=invoice_schema)
df_invoices.createOrReplaceTempView("raw_invoices")
df_invoices.show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ai_query() -- Universal LLM Function
# MAGIC
# MAGIC The most flexible AI function. Accepts any prompt and returns model output.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Sentiment classification with ai_query()
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   review_text,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT('Classify the sentiment as Positive, Negative, or Neutral: ', review_text),
# MAGIC --     returnType => 'STRING'
# MAGIC --   ) AS sentiment
# MAGIC -- FROM product_reviews

# COMMAND ----------

# Simulated ai_query() sentiment output
df_sentiment = df_reviews.withColumn(
    "ai_sentiment",
    when(col("star_rating") >= 4, lit("Positive"))
    .when(col("star_rating") <= 2, lit("Negative"))
    .otherwise(lit("Neutral"))
)

print("=== Simulated ai_query() for Sentiment ===")
df_sentiment.select("review_id", "review_text", "star_rating", "ai_sentiment").show(truncate=55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ai_query() with Structured Output

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Structured extraction with ai_query()
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT(
# MAGIC --       'Extract product type, main issue/praise, and sentiment from: ',
# MAGIC --       review_text
# MAGIC --     ),
# MAGIC --     returnType => 'STRUCT<product_type: STRING, main_point: STRING, sentiment: STRING>'
# MAGIC --   ) AS extracted
# MAGIC -- FROM product_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ### ai_query() with Model Parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Controlling generation parameters
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT('Summarize in one sentence: ', review_text),
# MAGIC --     returnType => 'STRING',
# MAGIC --     modelParameters => named_struct(
# MAGIC --       'temperature', CAST(0.0 AS DOUBLE),
# MAGIC --       'max_tokens', CAST(50 AS INT)
# MAGIC --     )
# MAGIC --   ) AS summary
# MAGIC -- FROM product_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ai_classify() -- Text Classification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Classify reviews by topic
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   review_text,
# MAGIC --   ai_classify(
# MAGIC --     review_text,
# MAGIC --     ARRAY('Product Quality', 'Customer Service', 'Shipping', 'Price', 'Sizing')
# MAGIC --   ) AS topic
# MAGIC -- FROM product_reviews

# COMMAND ----------

# Simulated ai_classify() output
topic_map = {
    1: "Product Quality", 2: "Sizing", 3: "Product Quality",
    4: "Product Quality", 5: "Product Quality", 6: "Customer Service",
    7: "Product Quality", 8: "Product Quality", 9: "Product Quality",
    10: "Product Quality"
}

@udf(StringType())
def sim_classify(review_id):
    return topic_map.get(review_id, "General")

df_classified = df_reviews.withColumn("ai_topic", sim_classify(col("review_id")))
print("=== Simulated ai_classify() Output ===")
df_classified.select("review_id", "review_text", "ai_topic").show(truncate=55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ai_extract() -- Entity Extraction

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Extract entities from invoices
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   invoice_id,
# MAGIC --   ai_extract(
# MAGIC --     invoice_text,
# MAGIC --     ARRAY('vendor_name', 'invoice_number', 'total_amount', 'due_date')
# MAGIC --   ) AS extracted
# MAGIC -- FROM raw_invoices

# COMMAND ----------

# Simulated ai_extract() using regex-based extraction
from pyspark.sql.functions import regexp_extract

df_extracted = df_invoices.withColumn(
    "vendor_name",
    regexp_extract(col("invoice_text"), r"from (.+?)\.", 1)
).withColumn(
    "invoice_number",
    regexp_extract(col("invoice_text"), r"(INV-\d{4}-\d{3})", 1)
).withColumn(
    "total_amount",
    regexp_extract(col("invoice_text"), r"Total: (\$[\d,]+\.\d{2})", 1)
).withColumn(
    "due_date",
    regexp_extract(col("invoice_text"), r"Due: (.+?)\.", 1)
)

print("=== Simulated ai_extract() Output ===")
df_extracted.select("invoice_id", "vendor_name", "invoice_number", "total_amount", "due_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ai_summarize() -- Text Summarization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Summarize reviews
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   review_text,
# MAGIC --   ai_summarize(review_text) AS summary
# MAGIC -- FROM product_reviews
# MAGIC -- WHERE LENGTH(review_text) > 50

# COMMAND ----------

# Simulated ai_summarize() -- extract first sentence as summary
from pyspark.sql.functions import split as spark_split, element_at

df_summarized = df_reviews.withColumn(
    "ai_summary",
    element_at(spark_split(col("review_text"), r"\. "), 1)
)

print("=== Simulated ai_summarize() Output ===")
df_summarized.select("review_id", "ai_summary").show(truncate=70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ai_translate() -- Language Translation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Translate messages to Spanish
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   message_id,
# MAGIC --   content,
# MAGIC --   ai_translate(content, 'Spanish') AS content_es,
# MAGIC --   ai_translate(content, 'French')  AS content_fr
# MAGIC -- FROM customer_messages

# COMMAND ----------

# Simulated translations (hard-coded for demonstration)
translations = {
    1: {"es": "Necesito cancelar mi suscripcion inmediatamente.", "fr": "Je dois annuler mon abonnement immediatement."},
    2: {"es": "Cuando llegara mi paquete? Numero de pedido 45678.", "fr": "Quand mon colis arrivera-t-il? Numero de commande 45678."},
    3: {"es": "El software se bloquea cada vez que abro un archivo grande.", "fr": "Le logiciel plante chaque fois que j'ouvre un gros fichier."},
    4: {"es": "Puede recomendar un buen portatil para edicion de video?", "fr": "Pouvez-vous recommander un bon ordinateur portable pour le montage video?"},
    5: {"es": "Me cobraron dos veces por el mismo pedido. Por favor reembolse.", "fr": "J'ai ete facture deux fois pour la meme commande. Veuillez rembourser."},
    6: {"es": "Como exporto mis datos de la plataforma?", "fr": "Comment exporter mes donnees de la plateforme?"},
    7: {"es": "Su producto cambio mi flujo de trabajo completamente. Gracias!", "fr": "Votre produit a completement change mon flux de travail. Merci!"},
    8: {"es": "El repartidor dejo mi paquete bajo la lluvia.", "fr": "Le livreur a laisse mon colis sous la pluie."},
}

@udf(StringType())
def sim_translate_es(msg_id):
    return translations.get(msg_id, {}).get("es", "")

@udf(StringType())
def sim_translate_fr(msg_id):
    return translations.get(msg_id, {}).get("fr", "")

df_translated = df_messages.withColumn(
    "content_es", sim_translate_es(col("message_id"))
).withColumn(
    "content_fr", sim_translate_fr(col("message_id"))
)

print("=== Simulated ai_translate() Output ===")
df_translated.select("message_id", "content", "content_es").show(truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ai_sentiment() -- Sentiment Detection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Detect sentiment in reviews
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   review_text,
# MAGIC --   star_rating,
# MAGIC --   ai_sentiment(review_text) AS detected_sentiment
# MAGIC -- FROM product_reviews

# COMMAND ----------

# Simulated ai_sentiment()
df_with_sentiment = df_reviews.withColumn(
    "detected_sentiment",
    when(col("star_rating") >= 4, lit("POSITIVE"))
    .when(col("star_rating") <= 2, lit("NEGATIVE"))
    .otherwise(lit("MIXED"))
)

print("=== Simulated ai_sentiment() Output ===")
df_with_sentiment.select("review_id", "review_text", "star_rating", "detected_sentiment").show(truncate=50)

# COMMAND ----------

# Sentiment distribution analysis
print("=== Sentiment Distribution ===")
df_with_sentiment.groupBy("detected_sentiment").count().orderBy("count", ascending=False).show()

print("=== Sentiment by Category ===")
df_with_sentiment.groupBy("category", "detected_sentiment").count().orderBy("category", "detected_sentiment").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ETL Pipeline with AI Functions
# MAGIC
# MAGIC Combine multiple AI functions in a single enrichment pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Enrichment Pipeline (SQL Template)
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE enriched_reviews AS
# MAGIC SELECT
# MAGIC   review_id,
# MAGIC   review_text,
# MAGIC   star_rating,
# MAGIC   review_date,
# MAGIC   category,
# MAGIC   ai_sentiment(review_text)                                    AS sentiment,
# MAGIC   ai_classify(review_text,
# MAGIC     ARRAY('Product Quality', 'Service', 'Shipping', 'Price')) AS topic,
# MAGIC   ai_extract(review_text,
# MAGIC     ARRAY('product_name', 'issue'))                           AS entities,
# MAGIC   ai_summarize(review_text)                                    AS summary
# MAGIC FROM product_reviews
# MAGIC ```

# COMMAND ----------

# Simulated full enrichment pipeline
df_enriched = df_reviews.withColumn(
    "sentiment",
    when(col("star_rating") >= 4, lit("POSITIVE"))
    .when(col("star_rating") <= 2, lit("NEGATIVE"))
    .otherwise(lit("MIXED"))
).withColumn(
    "topic", sim_classify(col("review_id"))
).withColumn(
    "summary", element_at(spark_split(col("review_text"), r"\. "), 1)
)

df_enriched.createOrReplaceTempView("enriched_reviews")

print("=== Simulated Full AI Enrichment Pipeline ===")
df_enriched.select("review_id", "sentiment", "topic", "summary").show(truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cost Estimation for AI Functions
# MAGIC
# MAGIC Before running AI functions on large datasets, estimate the cost.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, avg as spark_avg, ceil

# Estimate tokens and cost for the reviews dataset
df_cost = df_reviews.withColumn(
    "input_tokens", ceil(length(col("review_text")) / lit(4))
)

# Each AI function call has different expected output tokens
function_costs = [
    ("ai_sentiment", 3),       # Short response: "POSITIVE"
    ("ai_classify", 5),        # Short response: "Product Quality"
    ("ai_summarize", 30),      # 1-2 sentence summary
    ("ai_extract", 40),        # JSON with extracted entities
    ("ai_translate", None),    # Similar length to input
]

print("=== Cost Estimation per AI Function ===")
print(f"{'Function':<20} {'Avg Input Tkn':<15} {'Output Tkn':<12} {'Total Tkn':<12} {'Est. Cost/Row':<15}")
print("-" * 75)

total_rows = df_reviews.count()
avg_input = df_cost.agg(spark_avg("input_tokens")).collect()[0][0]

for func_name, output_tokens in function_costs:
    if output_tokens is None:
        output_tokens = int(avg_input)
    total_per_row = int(avg_input) + output_tokens
    cost_per_row = total_per_row / 1_000_000 * 0.50  # Illustrative pricing
    print(f"{func_name:<20} {int(avg_input):<15} {output_tokens:<12} {total_per_row:<12} ${cost_per_row:.8f}")

print(f"\nWith {total_rows} rows, running ALL 5 functions costs approximately:")
all_output_tokens = sum(ot for _, ot in function_costs if ot is not None) + int(avg_input)
total_cost = total_rows * (int(avg_input) * 5 + all_output_tokens) / 1_000_000 * 0.50
print(f"  ${total_cost:.6f}")
print(f"\nAt 1 million rows: ${total_cost / total_rows * 1_000_000:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Incremental Processing Pattern
# MAGIC
# MAGIC Only process new rows to avoid re-running AI functions on existing data.

# COMMAND ----------

# Simulate incremental processing

# "Previously enriched" rows (IDs 1-5)
df_existing = df_enriched.filter(col("review_id") <= 5)
df_existing.write.format("delta").mode("overwrite").saveAsTable("enriched_reviews_incremental")

print(f"Existing enriched rows: {df_existing.count()}")

# New rows arrive (IDs 6-10)
df_new = df_reviews.filter(col("review_id") > 5)
print(f"New rows to process: {df_new.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Incremental enrichment (only process new rows)
# MAGIC --
# MAGIC -- INSERT INTO enriched_reviews_incremental
# MAGIC -- SELECT
# MAGIC --   r.review_id,
# MAGIC --   r.review_text,
# MAGIC --   r.star_rating,
# MAGIC --   r.review_date,
# MAGIC --   r.category,
# MAGIC --   ai_sentiment(r.review_text)  AS sentiment,
# MAGIC --   ai_classify(r.review_text,
# MAGIC --     ARRAY('Product Quality', 'Service', 'Shipping', 'Price')) AS topic,
# MAGIC --   ai_summarize(r.review_text)  AS summary
# MAGIC -- FROM product_reviews r
# MAGIC -- LEFT ANTI JOIN enriched_reviews_incremental e
# MAGIC --   ON r.review_id = e.review_id

# COMMAND ----------

# Simulated incremental processing
df_new_enriched = df_new.withColumn(
    "sentiment",
    when(col("star_rating") >= 4, lit("POSITIVE"))
    .when(col("star_rating") <= 2, lit("NEGATIVE"))
    .otherwise(lit("MIXED"))
).withColumn(
    "topic", sim_classify(col("review_id"))
).withColumn(
    "summary", element_at(spark_split(col("review_text"), r"\. "), 1)
)

# Append new enriched rows
df_new_enriched.write.format("delta").mode("append").saveAsTable("enriched_reviews_incremental")

total = spark.table("enriched_reviews_incremental").count()
print(f"Total enriched rows after incremental update: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Error Handling for AI Functions

# COMMAND ----------

# Pattern: Graceful error handling for AI functions
from pyspark.sql.functions import coalesce

# In production SQL:
# SELECT
#   review_id,
#   COALESCE(TRY(ai_sentiment(review_text)), 'UNKNOWN') AS sentiment
# FROM product_reviews

# Simulate error handling
@udf(StringType())
def safe_classify(text):
    """Simulate a function that occasionally fails."""
    if text is None or len(text) < 10:
        return "ERROR: Input too short"
    return "Product Quality"  # Simulated result

df_safe = df_reviews.withColumn(
    "raw_result", safe_classify(col("review_text"))
).withColumn(
    "safe_result",
    when(col("raw_result").startswith("ERROR"), lit("UNKNOWN"))
    .otherwise(col("raw_result"))
)

print("=== Error Handling Pattern ===")
df_safe.select("review_id", "raw_result", "safe_result").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Combining AI Functions with Analytics

# COMMAND ----------

# Analyze enriched data
print("=== Analytics on AI-Enriched Data ===\n")

# Sentiment trend by date
print("Sentiment by Review Date:")
df_enriched.groupBy("review_date", "sentiment").count().orderBy("review_date").show(truncate=False)

# Topic distribution
print("\nTopic Distribution:")
df_enriched.groupBy("topic").count().orderBy(col("count").desc()).show()

# Average rating by sentiment (validation check)
print("Average Star Rating by Detected Sentiment (validation):")
df_enriched.groupBy("sentiment").agg(
    spark_avg("star_rating").alias("avg_stars"),
    spark_sum(lit(1)).alias("count")
).orderBy("sentiment").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS enriched_reviews_incremental")
spark.catalog.dropTempView("product_reviews")
spark.catalog.dropTempView("customer_messages")
spark.catalog.dropTempView("raw_invoices")
spark.catalog.dropTempView("enriched_reviews")

print("Cleanup complete. All temporary tables and views removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **AI Functions** bring LLM power directly into SQL -- no Python required
# MAGIC 2. **ai_query()** is the most flexible; specialized functions are simpler for common tasks
# MAGIC 3. **ai_extract()** is especially powerful for parsing unstructured text into structured columns
# MAGIC 4. **Incremental processing** avoids re-running expensive AI functions on existing data
# MAGIC 5. **Cost estimation** before large runs prevents budget surprises
# MAGIC 6. **Error handling** with TRY/COALESCE ensures pipelines do not fail on individual rows
# MAGIC
# MAGIC **Next:** [05 - Fine-Tuning](05-fine-tuning_notebook.py)
