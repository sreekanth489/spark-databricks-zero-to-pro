# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Fine-Tuning LLMs on Databricks
# MAGIC
# MAGIC This notebook demonstrates how to prepare training data, configure fine-tuning jobs,
# MAGIC and evaluate fine-tuned models. Data preparation steps run on any cluster; fine-tuning
# MAGIC API calls are shown as templates.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 14.x+ (full workspace with GPU for fine-tuning).
# MAGIC
# MAGIC **Note:** Fine-tuning API calls require appropriate entitlements. Templates and
# MAGIC simulated outputs are provided for learning purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. When to Fine-Tune: Decision Framework
# MAGIC
# MAGIC ```
# MAGIC Can prompt engineering solve it?
# MAGIC   YES --> Stop. Use prompts.
# MAGIC   NO  --> Does the model need your data at inference time?
# MAGIC            YES --> Use RAG
# MAGIC            NO  --> Does it need a new style, format, or behavior?
# MAGIC                     YES --> Fine-tune!
# MAGIC                     NO  --> Try a larger base model first
# MAGIC ```

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, ArrayType
)
from pyspark.sql.functions import (
    col, lit, when, concat, length, struct, to_json, from_json,
    monotonically_increasing_id, rand, expr, count, avg as spark_avg
)
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Training Data Preparation
# MAGIC
# MAGIC Fine-tuning requires labeled examples in instruction/chat format.
# MAGIC We create a dataset for a customer support ticket classifier.

# COMMAND ----------

# Raw labeled data for fine-tuning a ticket classifier
labeled_data = [
    # Billing tickets
    ("I was charged twice for my subscription this month.", "Billing"),
    ("Can you update my credit card on file?", "Billing"),
    ("I need a refund for order #12345.", "Billing"),
    ("My invoice shows charges I do not recognize.", "Billing"),
    ("When will my refund be processed?", "Billing"),
    ("I want to downgrade to a cheaper plan.", "Billing"),
    ("The promotional discount was not applied to my account.", "Billing"),
    ("How do I cancel my auto-renewal?", "Billing"),
    ("I need a receipt for my recent purchase.", "Billing"),
    ("Why did my subscription price increase?", "Billing"),

    # Technical tickets
    ("The app crashes when I try to upload a file larger than 10MB.", "Technical"),
    ("I am getting a 500 error on the API endpoint /v2/users.", "Technical"),
    ("The search function returns no results even for known items.", "Technical"),
    ("Performance is extremely slow during peak hours.", "Technical"),
    ("The export to CSV feature generates corrupted files.", "Technical"),
    ("Push notifications stopped working after the latest update.", "Technical"),
    ("The dashboard charts are not loading on Firefox.", "Technical"),
    ("I cannot connect to the database through the web console.", "Technical"),
    ("The webhook integration is dropping events randomly.", "Technical"),
    ("Images are not rendering in the mobile app.", "Technical"),

    # Account tickets
    ("I forgot my password and the reset email is not arriving.", "Account"),
    ("I need to change the email address on my account.", "Account"),
    ("My account was locked after too many failed login attempts.", "Account"),
    ("How do I enable two-factor authentication?", "Account"),
    ("I want to delete my account and all associated data.", "Account"),
    ("Can I merge two accounts into one?", "Account"),
    ("I need to add 5 new users to our team account.", "Account"),
    ("My SSO login is not working with our company identity provider.", "Account"),
    ("How do I change my username?", "Account"),
    ("I need to transfer account ownership to a colleague.", "Account"),

    # Feature Request tickets
    ("It would be great to have dark mode in the desktop app.", "Feature Request"),
    ("Please add the ability to export reports as PDF.", "Feature Request"),
    ("Can you add keyboard shortcuts for common actions?", "Feature Request"),
    ("I would like to see a calendar view for tasks.", "Feature Request"),
    ("Please support bulk import from Excel files.", "Feature Request"),
    ("Can you add an API endpoint for batch operations?", "Feature Request"),
    ("It would help to have email notifications for task deadlines.", "Feature Request"),
    ("Please add support for custom fields in the contact form.", "Feature Request"),
    ("I wish there was a way to set recurring tasks.", "Feature Request"),
    ("Can you add integration with Slack for notifications?", "Feature Request"),
]

# Create DataFrame
raw_schema = StructType([
    StructField("ticket_text", StringType(), False),
    StructField("category", StringType(), False),
])

df_labeled = spark.createDataFrame(labeled_data, schema=raw_schema)
df_labeled = df_labeled.withColumn("id", monotonically_increasing_id())

print(f"Total labeled examples: {df_labeled.count()}")
print("\nCategory distribution:")
df_labeled.groupBy("category").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convert to Chat Format (JSONL)
# MAGIC
# MAGIC Databricks fine-tuning expects data in the chat messages format.

# COMMAND ----------

def create_chat_message(ticket_text, category):
    """Convert a labeled example into the chat message format."""
    return json.dumps({
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a support ticket classifier. Classify each ticket into "
                    "exactly one category: Billing, Technical, Account, or Feature Request. "
                    "Respond with only the category name."
                )
            },
            {
                "role": "user",
                "content": f"Classify this support ticket:\n\n{ticket_text}"
            },
            {
                "role": "assistant",
                "content": category
            }
        ]
    })

# Generate JSONL lines
jsonl_lines = []
for row in df_labeled.collect():
    jsonl_lines.append(create_chat_message(row["ticket_text"], row["category"]))

# Display sample
print("=== Sample Training Examples (Chat Format) ===\n")
for i in range(3):
    parsed = json.loads(jsonl_lines[i])
    print(f"Example {i+1}:")
    for msg in parsed["messages"]:
        print(f"  [{msg['role']}]: {msg['content'][:80]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Training Data Quality

# COMMAND ----------

# Data quality checks
print("=== Training Data Quality Report ===\n")

# Check 1: Format consistency
valid_count = 0
for line in jsonl_lines:
    parsed = json.loads(line)
    msgs = parsed.get("messages", [])
    roles = [m["role"] for m in msgs]
    if roles == ["system", "user", "assistant"]:
        valid_count += 1

print(f"1. Format validation: {valid_count}/{len(jsonl_lines)} examples are correctly formatted")

# Check 2: Token length estimation
token_lengths = []
for line in jsonl_lines:
    tokens = len(line) // 4  # Rough estimate
    token_lengths.append(tokens)

avg_tokens = sum(token_lengths) / len(token_lengths)
max_tokens = max(token_lengths)
min_tokens = min(token_lengths)

print(f"2. Token length: avg={avg_tokens:.0f}, min={min_tokens}, max={max_tokens}")

# Check 3: Category balance
print(f"3. Categories: {df_labeled.select('category').distinct().count()} distinct categories")

# Check 4: Duplicates
total = df_labeled.count()
distinct = df_labeled.select("ticket_text").distinct().count()
print(f"4. Duplicates: {total - distinct} duplicate texts found")

# Check 5: Minimum examples per category
min_per_cat = df_labeled.groupBy("category").count().agg({"count": "min"}).collect()[0][0]
print(f"5. Minimum examples per category: {min_per_cat}")

print(f"\nOverall: {'PASS' if valid_count == len(jsonl_lines) and min_per_cat >= 5 else 'NEEDS REVIEW'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train/Validation Split

# COMMAND ----------

# Split data 80/20 for training and validation
df_train, df_eval = df_labeled.randomSplit([0.8, 0.2], seed=42)

print(f"Training examples: {df_train.count()}")
print(f"Validation examples: {df_eval.count()}")

print("\nTraining set category distribution:")
df_train.groupBy("category").count().orderBy("category").show()

print("Validation set category distribution:")
df_eval.groupBy("category").count().orderBy("category").show()

# COMMAND ----------

# Save as JSONL files
train_jsonl = []
for row in df_train.collect():
    train_jsonl.append(create_chat_message(row["ticket_text"], row["category"]))

eval_jsonl = []
for row in df_eval.collect():
    eval_jsonl.append(create_chat_message(row["ticket_text"], row["category"]))

print(f"Training JSONL lines: {len(train_jsonl)}")
print(f"Validation JSONL lines: {len(eval_jsonl)}")
print(f"\nSample training line:\n{train_jsonl[0][:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Training Data to Delta Table
# MAGIC
# MAGIC Store the training data in a Delta table for versioning and lineage.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Save training data with metadata
df_train_with_meta = df_train.withColumn(
    "jsonl_content",
    lit("")  # Placeholder -- in production, store the actual JSONL
).withColumn(
    "split", lit("train")
).withColumn(
    "created_at", current_timestamp()
)

df_eval_with_meta = df_eval.withColumn(
    "jsonl_content", lit("")
).withColumn(
    "split", lit("eval")
).withColumn(
    "created_at", current_timestamp()
)

df_all = df_train_with_meta.union(df_eval_with_meta)
df_all.write.format("delta").mode("overwrite").saveAsTable("fine_tuning_data_v1")

print(f"Saved {df_all.count()} examples to Delta table 'fine_tuning_data_v1'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fine-Tuning Job Configuration (Template)
# MAGIC
# MAGIC The following shows the API calls to launch a fine-tuning job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Launch Fine-Tuning Run
# MAGIC
# MAGIC ```python
# MAGIC from databricks.model_training import foundation_model as fm
# MAGIC
# MAGIC # Create a fine-tuning run
# MAGIC run = fm.create(
# MAGIC     model="meta-llama/Meta-Llama-3.1-8B-Instruct",
# MAGIC     train_data_path="dbfs:/Volumes/catalog/schema/ft_data/train.jsonl",
# MAGIC     eval_data_path="dbfs:/Volumes/catalog/schema/ft_data/eval.jsonl",
# MAGIC     register_to="catalog.schema.ticket_classifier_v1",
# MAGIC     training_duration="5ep",       # 5 epochs
# MAGIC     learning_rate=1e-5,
# MAGIC     context_length=2048,
# MAGIC     task_type="CHAT_COMPLETION",
# MAGIC )
# MAGIC
# MAGIC print(f"Run name: {run.name}")
# MAGIC print(f"Status: {run.status}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Training Progress
# MAGIC
# MAGIC ```python
# MAGIC # Poll training status
# MAGIC import time
# MAGIC
# MAGIC while True:
# MAGIC     run = fm.get(run.name)
# MAGIC     print(f"Status: {run.status}")
# MAGIC     if run.status in ["COMPLETED", "FAILED"]:
# MAGIC         break
# MAGIC     time.sleep(60)
# MAGIC
# MAGIC # View training metrics
# MAGIC import mlflow
# MAGIC run_data = mlflow.get_run(run.mlflow_run_id)
# MAGIC metrics = run_data.data.metrics
# MAGIC print(f"Final training loss: {metrics.get('train_loss', 'N/A')}")
# MAGIC print(f"Final validation loss: {metrics.get('eval_loss', 'N/A')}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulated Training Metrics
# MAGIC
# MAGIC We simulate what training metrics look like during fine-tuning.

# COMMAND ----------

import random

# Simulate training metrics over epochs
epochs = 5
steps_per_epoch = 8  # 40 examples / batch_size ~5

simulated_metrics = []
train_loss = 2.5  # Starting loss
eval_loss = 2.8

for epoch in range(1, epochs + 1):
    for step in range(1, steps_per_epoch + 1):
        global_step = (epoch - 1) * steps_per_epoch + step
        train_loss *= 0.92 + random.uniform(-0.02, 0.02)
        eval_loss *= 0.94 + random.uniform(-0.02, 0.03)
        train_loss = max(train_loss, 0.15)
        eval_loss = max(eval_loss, 0.25)

        simulated_metrics.append((
            epoch, step, global_step,
            round(train_loss, 4), round(eval_loss, 4)
        ))

metrics_schema = StructType([
    StructField("epoch", IntegerType()),
    StructField("step", IntegerType()),
    StructField("global_step", IntegerType()),
    StructField("train_loss", FloatType()),
    StructField("eval_loss", FloatType()),
])

df_metrics = spark.createDataFrame(simulated_metrics, schema=metrics_schema)

# Show epoch-end metrics
print("=== Simulated Training Metrics (Epoch End) ===")
from pyspark.sql.functions import max as spark_max, min as spark_min

df_metrics.groupBy("epoch").agg(
    spark_min("train_loss").alias("min_train_loss"),
    spark_min("eval_loss").alias("min_eval_loss"),
    spark_max("global_step").alias("steps")
).orderBy("epoch").show()

# COMMAND ----------

# Check for overfitting
print("=== Overfitting Check ===\n")

epoch_losses = df_metrics.groupBy("epoch").agg(
    spark_min("train_loss").alias("train_loss"),
    spark_min("eval_loss").alias("eval_loss"),
).orderBy("epoch").collect()

for row in epoch_losses:
    gap = row["eval_loss"] - row["train_loss"]
    status = "OK" if gap < 0.5 else "WARNING: possible overfitting"
    print(f"  Epoch {row['epoch']}: train={row['train_loss']:.4f}, eval={row['eval_loss']:.4f}, gap={gap:.4f} [{status}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Model Evaluation
# MAGIC
# MAGIC Compare the fine-tuned model against the base model on a held-out test set.

# COMMAND ----------

# Simulated evaluation: base model vs fine-tuned model
test_tickets = [
    ("I need a refund for a duplicate charge.", "Billing"),
    ("The API returns a 403 error for valid tokens.", "Technical"),
    ("How do I reset my two-factor authentication?", "Account"),
    ("Please add support for dark mode.", "Feature Request"),
    ("My credit card was charged the wrong amount.", "Billing"),
    ("The mobile app freezes when switching tabs.", "Technical"),
    ("I want to close my account permanently.", "Account"),
    ("Can you add a bulk delete feature?", "Feature Request"),
]

# Simulate model predictions
import random
random.seed(42)

categories = ["Billing", "Technical", "Account", "Feature Request"]

results = []
for ticket, true_label in test_tickets:
    # Base model: 60% accuracy (sometimes confused)
    base_pred = true_label if random.random() < 0.6 else random.choice(categories)
    # Fine-tuned model: 90% accuracy
    ft_pred = true_label if random.random() < 0.9 else random.choice(categories)

    results.append((ticket[:60], true_label, base_pred, ft_pred))

eval_schema = StructType([
    StructField("ticket", StringType()),
    StructField("true_label", StringType()),
    StructField("base_model_pred", StringType()),
    StructField("fine_tuned_pred", StringType()),
])

df_eval_results = spark.createDataFrame(results, schema=eval_schema)
df_eval_results.show(truncate=50)

# COMMAND ----------

# Calculate accuracy comparison
from pyspark.sql.functions import sum as spark_sum

df_accuracy = df_eval_results.withColumn(
    "base_correct", when(col("base_model_pred") == col("true_label"), 1).otherwise(0)
).withColumn(
    "ft_correct", when(col("fine_tuned_pred") == col("true_label"), 1).otherwise(0)
)

totals = df_accuracy.agg(
    spark_sum("base_correct").alias("base_correct"),
    spark_sum("ft_correct").alias("ft_correct"),
    count("*").alias("total")
).collect()[0]

base_acc = totals["base_correct"] / totals["total"]
ft_acc = totals["ft_correct"] / totals["total"]
improvement = ft_acc - base_acc

print(f"=== Model Comparison ===")
print(f"  Base model accuracy:       {base_acc:.1%}")
print(f"  Fine-tuned model accuracy: {ft_acc:.1%}")
print(f"  Improvement:               {improvement:+.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Deployment Configuration (Template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Model in Unity Catalog
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC
# MAGIC mlflow.set_registry_uri("databricks-uc")
# MAGIC
# MAGIC # Register the fine-tuned model
# MAGIC mlflow.register_model(
# MAGIC     model_uri=f"runs:/{run.mlflow_run_id}/model",
# MAGIC     name="catalog.schema.ticket_classifier_v1"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Serving Endpoint
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC endpoint = w.serving_endpoints.create(
# MAGIC     name="ticket-classifier-ft",
# MAGIC     config={
# MAGIC         "served_entities": [{
# MAGIC             "entity_name": "catalog.schema.ticket_classifier_v1",
# MAGIC             "entity_version": "1",
# MAGIC             "workload_type": "GPU_SMALL",
# MAGIC             "workload_size": "Small",
# MAGIC             "scale_to_zero_enabled": True
# MAGIC         }]
# MAGIC     }
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cost Estimation

# COMMAND ----------

# Estimate fine-tuning costs
model_configs = [
    {"model": "Llama 3.1 8B", "gpu": "A10G x 1", "time_1k": "1-2 hrs", "cost_1k": "$5-15"},
    {"model": "Llama 3.1 13B", "gpu": "A100 x 1", "time_1k": "2-4 hrs", "cost_1k": "$15-40"},
    {"model": "Llama 3.1 70B", "gpu": "A100 x 8", "time_1k": "8-16 hrs", "cost_1k": "$100-300"},
]

df_costs = spark.createDataFrame(model_configs)
print("=== Estimated Fine-Tuning Costs (1,000 training examples, 5 epochs) ===")
df_costs.show(truncate=False)

# ROI calculation
print("\n=== ROI Analysis ===")
print("Scenario: Classify 100,000 tickets/month")
print()
print("Option A: Large base model with prompting")
print(f"  Tokens per request: ~200 input + ~5 output = 205 tokens")
print(f"  Monthly token cost: 100,000 * 205 / 1,000,000 * $2.00 = $41.00/month")
print()
print("Option B: Fine-tuned small model")
print(f"  One-time training cost: ~$10")
print(f"  Tokens per request: ~100 input + ~3 output = 103 tokens")
print(f"  Monthly token cost: 100,000 * 103 / 1,000,000 * $0.50 = $5.15/month")
print(f"  Monthly savings: $35.85 (87% reduction)")
print(f"  Break-even: Less than 1 month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Advanced: Data Augmentation for Fine-Tuning
# MAGIC
# MAGIC When you have limited training data, augmentation can help.

# COMMAND ----------

# Simple augmentation: rephrase templates
augmentation_templates = [
    "I need help with: {text}",
    "Hi, {text}",
    "Urgent: {text}",
    "{text} Please help.",
    "Hello, {text} Thanks.",
]

# Augment a few examples
sample_data = labeled_data[:4]
augmented = []

for ticket, category in sample_data:
    augmented.append((ticket, category))  # Original
    for template in augmentation_templates[:2]:  # 2 augmentations per example
        new_text = template.format(text=ticket)
        augmented.append((new_text, category))

print(f"Original examples: {len(sample_data)}")
print(f"After augmentation: {len(augmented)}")
print(f"\nSample augmented examples:")
for text, cat in augmented[:6]:
    print(f"  [{cat}] {text[:70]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Completion Format (Alternative)
# MAGIC
# MAGIC Some fine-tuning tasks use the simpler completion format.

# COMMAND ----------

# Completion format example
completion_examples = []
for ticket, category in labeled_data[:5]:
    example = {
        "prompt": f"Classify this support ticket into one category (Billing, Technical, Account, Feature Request):\n\n{ticket}\n\nCategory:",
        "completion": f" {category}"
    }
    completion_examples.append(example)
    print(json.dumps(example, indent=2)[:200])
    print("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS fine_tuning_data_v1")

print("Cleanup complete. All temporary tables removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Fine-tune** only when prompt engineering and RAG are not enough
# MAGIC 2. **Data quality** matters more than quantity -- 100 great examples beat 10,000 noisy ones
# MAGIC 3. **Chat format** (system/user/assistant messages) is the recommended training format
# MAGIC 4. **Validate** data format, balance, and token lengths before submitting a training job
# MAGIC 5. **Compare** fine-tuned vs base model on a held-out test set with multiple metrics
# MAGIC 6. **Start small** -- fine-tune an 8B model first, then scale up if needed
# MAGIC 7. **Calculate ROI** -- fine-tuning a small model can be much cheaper than using a large one
# MAGIC
# MAGIC **Next:** [06 - LLMOps](06-llmops_notebook.py)
