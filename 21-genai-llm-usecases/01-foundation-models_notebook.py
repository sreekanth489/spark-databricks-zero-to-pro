# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Foundation Models on Databricks
# MAGIC
# MAGIC This notebook demonstrates how to call foundation models through Databricks APIs.
# MAGIC We cover the `ai_query()` SQL function, Python SDK patterns, prompt templates,
# MAGIC and a model comparison framework.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 14.x+ (full workspace for API calls).
# MAGIC
# MAGIC **Note:** Cells that call Foundation Model APIs include simulated outputs so you can
# MAGIC follow along even without API access. Data preparation cells run on any cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Sample Data
# MAGIC
# MAGIC We create sample datasets that represent common LLM use cases:
# MAGIC customer reviews, support tickets, and product descriptions.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)
from pyspark.sql.functions import col, lit, concat, length, round as spark_round
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Customer reviews dataset
reviews_data = [
    (1, "The wireless headphones have amazing sound quality and the battery lasts all day. Best purchase this year!", "Electronics", 5),
    (2, "Terrible experience. The laptop arrived with a cracked screen and customer support took 3 weeks to respond.", "Electronics", 1),
    (3, "The running shoes are comfortable for short runs but the sole wears out quickly. Decent for the price.", "Footwear", 3),
    (4, "Absolutely love this coffee maker! Perfect temperature every time and the auto-brew feature is a game changer.", "Kitchen", 5),
    (5, "The jacket zipper broke after two uses. Very disappointing quality for a premium brand.", "Clothing", 1),
    (6, "Good value for money. The tablet handles basic tasks well but struggles with heavy multitasking.", "Electronics", 3),
    (7, "Worst customer service I have ever dealt with. Product was fine but the return process was a nightmare.", "General", 2),
    (8, "This standing desk changed my work life. Smooth motor, solid build, and the memory presets are perfect.", "Furniture", 5),
    (9, "The blender is loud but powerful. Makes great smoothies. Cleaning could be easier.", "Kitchen", 4),
    (10, "Do not buy this phone case. It cracked on the first drop. False advertising about drop protection.", "Electronics", 1),
]

reviews_schema = StructType([
    StructField("review_id", IntegerType(), False),
    StructField("review_text", StringType(), False),
    StructField("category", StringType(), False),
    StructField("star_rating", IntegerType(), False),
])

df_reviews = spark.createDataFrame(reviews_data, schema=reviews_schema)
df_reviews.createOrReplaceTempView("customer_reviews")
df_reviews.show(truncate=60)

# COMMAND ----------

# Support tickets dataset
tickets_data = [
    (101, "I cannot log into my account. I keep getting an error 403 after entering my password.", "open"),
    (102, "Please cancel my subscription effective immediately. I no longer need the service.", "open"),
    (103, "My invoice shows a charge of $49.99 but my plan is $29.99. Please correct this billing error.", "open"),
    (104, "The API endpoint /v2/users returns a 500 error when the payload exceeds 1MB.", "open"),
    (105, "I would like to request a feature to export reports as PDF directly from the dashboard.", "open"),
    (106, "The mobile app crashes every time I try to upload a photo larger than 5MB.", "open"),
    (107, "Can you help me understand the difference between the Pro and Enterprise plans?", "open"),
    (108, "Our team needs 50 additional seats added to our enterprise account by end of month.", "open"),
]

tickets_schema = StructType([
    StructField("ticket_id", IntegerType(), False),
    StructField("description", StringType(), False),
    StructField("status", StringType(), False),
])

df_tickets = spark.createDataFrame(tickets_data, schema=tickets_schema)
df_tickets.createOrReplaceTempView("support_tickets")
df_tickets.show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calling Models with ai_query() -- SQL Interface
# MAGIC
# MAGIC The `ai_query()` function is the simplest way to call an LLM from SQL.
# MAGIC It takes an endpoint name and a prompt string, and returns the model's response.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC SQL Query --> ai_query() --> Model Serving Endpoint --> LLM --> Response
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Sentiment Classification
# MAGIC
# MAGIC The following SQL calls an LLM to classify each review's sentiment.
# MAGIC
# MAGIC **Note:** This requires Foundation Model API access. The template is shown
# MAGIC as reference, followed by a simulated result.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Sentiment classification with ai_query()
# MAGIC -- Uncomment and run on a workspace with Foundation Model API access
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   review_text,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT(
# MAGIC --       'Classify the sentiment of this review as exactly one word: Positive, Negative, or Neutral.\n\n',
# MAGIC --       'Review: ', review_text
# MAGIC --     ),
# MAGIC --     returnType => 'STRING'
# MAGIC --   ) AS sentiment
# MAGIC -- FROM customer_reviews

# COMMAND ----------

# Simulated sentiment classification output
from pyspark.sql.functions import when

sentiment_map = {1: "Negative", 2: "Negative", 3: "Neutral", 4: "Positive", 5: "Positive"}

df_sentiment = df_reviews.withColumn(
    "predicted_sentiment",
    when(col("star_rating") <= 2, lit("Negative"))
    .when(col("star_rating") == 3, lit("Neutral"))
    .otherwise(lit("Positive"))
)

print("=== Simulated ai_query() sentiment classification output ===")
df_sentiment.select("review_id", "review_text", "star_rating", "predicted_sentiment").show(truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Ticket Classification
# MAGIC
# MAGIC Classify support tickets into categories using an LLM.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Ticket classification with ai_query()
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   ticket_id,
# MAGIC --   description,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT(
# MAGIC --       'Classify this support ticket into exactly one category: ',
# MAGIC --       'Billing, Technical, Account, Feature Request, or General.\n',
# MAGIC --       'Respond with only the category name.\n\n',
# MAGIC --       'Ticket: ', description
# MAGIC --     ),
# MAGIC --     returnType => 'STRING'
# MAGIC --   ) AS ticket_category
# MAGIC -- FROM support_tickets

# COMMAND ----------

# Simulated ticket classification
ticket_categories = {
    101: "Account",
    102: "Account",
    103: "Billing",
    104: "Technical",
    105: "Feature Request",
    106: "Technical",
    107: "General",
    108: "Account",
}

from pyspark.sql.functions import udf

@udf(StringType())
def simulate_classify(ticket_id):
    return ticket_categories.get(ticket_id, "General")

df_classified = df_tickets.withColumn("predicted_category", simulate_classify(col("ticket_id")))
print("=== Simulated ai_query() ticket classification output ===")
df_classified.show(truncate=70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Structured Extraction
# MAGIC
# MAGIC Use `returnType => 'STRUCT<...>'` to get structured output from the LLM.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Structured extraction with ai_query()
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   review_id,
# MAGIC --   ai_query(
# MAGIC --     'databricks-dbrx-instruct',
# MAGIC --     CONCAT(
# MAGIC --       'Extract the product type and the main issue or praise from this review. ',
# MAGIC --       'Return JSON with keys: product_type, main_point\n\n',
# MAGIC --       'Review: ', review_text
# MAGIC --     ),
# MAGIC --     returnType => 'STRUCT<product_type: STRING, main_point: STRING>'
# MAGIC --   ) AS extracted
# MAGIC -- FROM customer_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calling Models with Python SDK
# MAGIC
# MAGIC The Python SDK gives you more control over the conversation,
# MAGIC including system prompts and multi-turn chat.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Python SDK Pattern (Template)
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC response = w.serving_endpoints.query(
# MAGIC     name="databricks-dbrx-instruct",
# MAGIC     messages=[
# MAGIC         {"role": "system", "content": "You are a data quality analyst."},
# MAGIC         {"role": "user", "content": "Review this schema for issues: ..."}
# MAGIC     ],
# MAGIC     max_tokens=256,
# MAGIC     temperature=0.0
# MAGIC )
# MAGIC
# MAGIC print(response.choices[0].message.content)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. OpenAI-Compatible Interface (Template)
# MAGIC
# MAGIC Databricks model serving endpoints are OpenAI-compatible:
# MAGIC
# MAGIC ```python
# MAGIC import openai
# MAGIC
# MAGIC client = openai.OpenAI(
# MAGIC     api_key=dbutils.secrets.get("scope", "token"),
# MAGIC     base_url="https://<workspace>.databricks.com/serving-endpoints"
# MAGIC )
# MAGIC
# MAGIC response = client.chat.completions.create(
# MAGIC     model="databricks-dbrx-instruct",
# MAGIC     messages=[{"role": "user", "content": "Summarize: ..."}],
# MAGIC     max_tokens=100,
# MAGIC     temperature=0.0
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prompt Engineering Templates
# MAGIC
# MAGIC Well-crafted prompts are critical for consistent, high-quality LLM outputs.
# MAGIC Below are reusable templates for common data-engineering tasks.

# COMMAND ----------

# Prompt template library
prompt_templates = {
    "sentiment": {
        "system": "You are a sentiment analysis expert. Respond with exactly one word.",
        "user": "Classify the sentiment of this text as Positive, Negative, or Neutral.\n\nText: {text}"
    },
    "summarize": {
        "system": "You are a concise summarizer. Never exceed two sentences.",
        "user": "Summarize the following text in one to two sentences.\n\nText: {text}"
    },
    "classify": {
        "system": "You are a support ticket classifier. Respond with exactly one category.",
        "user": "Classify this ticket into one category: {categories}\n\nTicket: {text}"
    },
    "extract": {
        "system": "You are a data extraction specialist. Return valid JSON only.",
        "user": "Extract {fields} from the following text. Return JSON.\n\nText: {text}"
    },
    "translate": {
        "system": "You are a professional translator. Translate accurately.",
        "user": "Translate the following text to {target_language}.\n\nText: {text}"
    },
}

# Display templates
for name, template in prompt_templates.items():
    print(f"--- {name.upper()} ---")
    print(f"  System: {template['system']}")
    print(f"  User:   {template['user'][:80]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Templates to DataFrames
# MAGIC
# MAGIC Here we show how to build prompts from templates and DataFrame columns.

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

# Build prompts for sentiment analysis
df_with_prompts = df_reviews.withColumn(
    "prompt",
    concat(
        lit("Classify the sentiment of this review as Positive, Negative, or Neutral.\n\nReview: "),
        col("review_text")
    )
)

# Show the generated prompts
df_with_prompts.select("review_id", "prompt").show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Token Estimation and Cost Analysis
# MAGIC
# MAGIC Understanding token counts helps you estimate costs before running large jobs.
# MAGIC A rough rule: 1 token ~ 4 characters in English.

# COMMAND ----------

from pyspark.sql.functions import length, ceil

# Estimate tokens for each review prompt
df_token_estimate = df_with_prompts.withColumn(
    "estimated_input_tokens",
    ceil(length(col("prompt")) / lit(4))
).withColumn(
    "estimated_output_tokens",
    lit(5)  # Short classification response
).withColumn(
    "estimated_total_tokens",
    col("estimated_input_tokens") + col("estimated_output_tokens")
)

df_token_estimate.select(
    "review_id", "estimated_input_tokens", "estimated_output_tokens", "estimated_total_tokens"
).show()

# COMMAND ----------

# Total token and cost estimation
from pyspark.sql.functions import sum as spark_sum

totals = df_token_estimate.agg(
    spark_sum("estimated_total_tokens").alias("total_tokens")
).collect()[0]

total_tokens = totals["total_tokens"]

# Example pricing: $0.50 per 1M input tokens, $1.50 per 1M output tokens (illustrative)
input_cost_per_million = 0.50
output_cost_per_million = 1.50

input_tokens_total = df_token_estimate.agg(spark_sum("estimated_input_tokens")).collect()[0][0]
output_tokens_total = df_token_estimate.agg(spark_sum("estimated_output_tokens")).collect()[0][0]

estimated_cost = (input_tokens_total / 1_000_000 * input_cost_per_million +
                  output_tokens_total / 1_000_000 * output_cost_per_million)

print(f"Total input tokens:  {input_tokens_total}")
print(f"Total output tokens: {output_tokens_total}")
print(f"Total tokens:        {total_tokens}")
print(f"Estimated cost:      ${estimated_cost:.6f}")
print(f"\nAt 1M rows, estimated cost: ${estimated_cost * 100_000:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Model Comparison Framework
# MAGIC
# MAGIC When choosing a model, compare quality, latency, and cost across candidates.
# MAGIC The framework below structures this evaluation.

# COMMAND ----------

# Model comparison evaluation structure
model_configs = [
    {"model": "databricks-dbrx-instruct", "size": "132B MoE", "cost_tier": "$$", "best_for": "General enterprise tasks"},
    {"model": "databricks-meta-llama-3-1-8b-instruct", "size": "8B", "cost_tier": "$", "best_for": "Simple classification, extraction"},
    {"model": "databricks-meta-llama-3-1-70b-instruct", "size": "70B", "cost_tier": "$$$", "best_for": "Complex reasoning, summarization"},
    {"model": "databricks-meta-llama-3-1-405b-instruct", "size": "405B", "cost_tier": "$$$$$", "best_for": "Highest quality, complex tasks"},
    {"model": "databricks-mixtral-8x7b-instruct", "size": "47B MoE", "cost_tier": "$$", "best_for": "Balanced quality and speed"},
]

df_models = spark.createDataFrame(model_configs)
df_models.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Selection Decision Tree
# MAGIC
# MAGIC ```
# MAGIC Is the task simple (classification, sentiment)?
# MAGIC   YES --> Try Llama 3.1 8B first ($)
# MAGIC   NO  --> Does it require complex reasoning?
# MAGIC            YES --> Llama 3.1 70B ($$$) or 405B ($$$$$)
# MAGIC            NO  --> DBRX ($$) or Mixtral ($$)
# MAGIC
# MAGIC Always validate with a sample before processing full dataset!
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Batch Processing Pattern
# MAGIC
# MAGIC For large-scale LLM processing, use Spark to parallelize API calls.

# COMMAND ----------

# Pattern: Using mapInPandas for batch LLM calls (template)
#
# def call_llm_batch(pdf_iterator):
#     """Process batches of rows through the LLM API."""
#     import openai
#     client = openai.OpenAI(
#         api_key="<token>",
#         base_url="https://<workspace>.databricks.com/serving-endpoints"
#     )
#     for pdf in pdf_iterator:
#         results = []
#         for _, row in pdf.iterrows():
#             response = client.chat.completions.create(
#                 model="databricks-dbrx-instruct",
#                 messages=[{"role": "user", "content": row["prompt"]}],
#                 max_tokens=50,
#                 temperature=0.0
#             )
#             results.append(response.choices[0].message.content)
#         pdf["llm_response"] = results
#         yield pdf
#
# result_df = df_with_prompts.mapInPandas(call_llm_batch, schema=output_schema)

print("Batch processing pattern shown as template above.")
print("Key considerations:")
print("  - Use mapInPandas for row-level LLM calls at scale")
print("  - Control concurrency with spark.sql.execution.arrow.maxRecordsPerBatch")
print("  - Implement retry logic for transient API errors")
print("  - Cache results in Delta tables to avoid reprocessing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Caching LLM Results in Delta
# MAGIC
# MAGIC LLM calls are expensive. Always cache results to avoid reprocessing.

# COMMAND ----------

# Save LLM results to Delta for caching
output_table = "llm_sentiment_cache"

df_sentiment.write.format("delta").mode("overwrite").saveAsTable(output_table)

print(f"Cached {df_sentiment.count()} LLM results to table: {output_table}")

# Read back from cache
df_cached = spark.table(output_table)
df_cached.show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Monitoring Model Serving Usage
# MAGIC
# MAGIC Track token usage and costs through system tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPLATE: Query model serving usage (requires system table access)
# MAGIC --
# MAGIC -- SELECT
# MAGIC --   date_trunc('hour', usage_date) AS hour,
# MAGIC --   endpoint_name,
# MAGIC --   SUM(input_tokens)  AS total_input_tokens,
# MAGIC --   SUM(output_tokens) AS total_output_tokens,
# MAGIC --   SUM(total_tokens)  AS total_tokens,
# MAGIC --   COUNT(*)           AS request_count
# MAGIC -- FROM system.serving.endpoint_usage
# MAGIC -- WHERE usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC -- GROUP BY 1, 2
# MAGIC -- ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS llm_sentiment_cache")
spark.catalog.dropTempView("customer_reviews")
spark.catalog.dropTempView("support_tickets")

print("Cleanup complete. Temporary tables and views removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **ai_query()** is the fastest path from SQL to LLM -- one function call enriches your data
# MAGIC 2. **Python SDK** gives more control for complex multi-turn conversations
# MAGIC 3. **Prompt templates** standardize LLM interactions across your team
# MAGIC 4. **Token estimation** prevents cost surprises before large batch jobs
# MAGIC 5. **Cache results in Delta** -- never pay twice for the same LLM call
# MAGIC 6. **Start small** -- use the cheapest model that meets your quality bar
# MAGIC
# MAGIC **Next:** [02 - Vector Search](02-vector-search_notebook.py)
