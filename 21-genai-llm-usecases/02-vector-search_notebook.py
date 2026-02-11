# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Vector Search on Databricks
# MAGIC
# MAGIC This notebook demonstrates embeddings concepts, vector search configuration,
# MAGIC and query patterns. We create sample text data, simulate embedding generation,
# MAGIC and show how vector search finds semantically similar documents.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 14.x+ (full workspace for Vector Search APIs).
# MAGIC
# MAGIC **Note:** Vector Search API calls are shown as templates. Data preparation and
# MAGIC similarity search simulation run on any cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understanding Embeddings
# MAGIC
# MAGIC An embedding converts text into a fixed-length numerical vector.
# MAGIC Semantically similar texts have vectors that are close together.
# MAGIC
# MAGIC ```
# MAGIC "reset my password"        --> [0.12, 0.85, -0.34, ...]
# MAGIC "change account password"  --> [0.11, 0.83, -0.31, ...]   <-- close
# MAGIC "quarterly sales report"   --> [0.78, -0.22, 0.65, ...]   <-- far
# MAGIC ```

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, FloatType
)
from pyspark.sql.functions import col, lit, udf, array, size
import random
import math

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Sample Document Collection
# MAGIC
# MAGIC We create a knowledge base of IT support articles that we will use
# MAGIC to demonstrate vector search patterns.

# COMMAND ----------

documents_data = [
    (1, "How to Reset Your Password", "Go to Settings > Security > Change Password. Enter your current password, then type your new password twice. Click Save.", "account", "2024-01-15"),
    (2, "Setting Up Two-Factor Authentication", "Navigate to Settings > Security > 2FA. Choose between SMS or authenticator app. Scan the QR code with your authenticator app and enter the verification code.", "security", "2024-01-20"),
    (3, "Connecting to VPN", "Download the VPN client from the IT portal. Enter server address vpn.company.com. Use your corporate credentials to log in. Select the nearest server.", "network", "2024-02-01"),
    (4, "Printer Setup Guide", "Go to Settings > Printers > Add Printer. Select your floor printer from the list. Click Install Driver. Print a test page to verify.", "hardware", "2024-02-10"),
    (5, "Email Configuration for Mobile", "Open your mail app. Add a new Exchange account. Server: mail.company.com. Enter your corporate email and password. Enable sync for Calendar and Contacts.", "email", "2024-02-15"),
    (6, "Requesting Software Installation", "Submit a ticket through the IT portal under Software Requests. Include the software name, version, and business justification. Approval typically takes 2 business days.", "software", "2024-03-01"),
    (7, "Troubleshooting Slow Computer", "First restart your computer. Clear temporary files. Check for Windows updates. If still slow, run the disk cleanup utility. Contact IT if issues persist.", "hardware", "2024-03-05"),
    (8, "File Sharing and Permissions", "Right-click the folder in SharePoint. Select Share. Enter colleague emails. Choose view or edit permissions. Set an expiration date for external shares.", "collaboration", "2024-03-10"),
    (9, "Remote Desktop Connection", "Open Remote Desktop app. Enter the hostname of your office computer. Use your corporate credentials. Ensure VPN is connected first for off-network access.", "network", "2024-03-15"),
    (10, "Data Backup Procedures", "All files on your corporate drive are backed up automatically every night. Personal files on the desktop are NOT backed up. Move important files to the corporate drive.", "data", "2024-03-20"),
    (11, "Account Lockout Recovery", "If your account is locked after failed login attempts, wait 30 minutes for automatic unlock. For immediate access, call the IT helpdesk at ext. 5555.", "account", "2024-04-01"),
    (12, "Meeting Room Booking System", "Open the booking portal at rooms.company.com. Select your building and floor. Choose available time slots. Add required equipment like projector or whiteboard.", "collaboration", "2024-04-05"),
    (13, "Updating Your Security Certificate", "Certificates expire annually. When prompted, click Renew Certificate. If expired, visit the IT portal to request a new certificate. Restart your browser after installation.", "security", "2024-04-10"),
    (14, "Network Drive Mapping", "Open File Explorer. Click Map Network Drive. Enter the path provided by IT. Check Reconnect at sign-in. Use your corporate credentials when prompted.", "network", "2024-04-15"),
    (15, "Handling Phishing Emails", "Do not click any links or download attachments. Forward the suspicious email to security@company.com. Delete the email. Report it via the phishing button in Outlook.", "security", "2024-04-20"),
]

doc_schema = StructType([
    StructField("doc_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("content", StringType(), False),
    StructField("category", StringType(), False),
    StructField("last_updated", StringType(), False),
])

df_docs = spark.createDataFrame(documents_data, schema=doc_schema)
df_docs.createOrReplaceTempView("knowledge_base")
df_docs.show(truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulating Embedding Generation
# MAGIC
# MAGIC In production, embeddings are generated by models like BGE or GTE.
# MAGIC Here we simulate embeddings using a deterministic hash-based approach
# MAGIC to demonstrate the mechanics of vector search.

# COMMAND ----------

import hashlib
import struct

def generate_simulated_embedding(text, dimensions=64):
    """
    Generate a deterministic pseudo-embedding from text.
    Similar texts will produce somewhat similar vectors because
    overlapping words contribute the same hash components.

    NOTE: This is for demonstration only. Real embeddings from
    transformer models capture deep semantic relationships.
    """
    words = text.lower().split()
    embedding = [0.0] * dimensions

    for word in words:
        word_hash = hashlib.md5(word.encode()).digest()
        for i in range(min(dimensions, len(word_hash))):
            embedding[i % dimensions] += (word_hash[i] - 128) / 128.0

    # Normalize to unit vector (cosine similarity works on unit vectors)
    magnitude = math.sqrt(sum(x * x for x in embedding))
    if magnitude > 0:
        embedding = [x / magnitude for x in embedding]

    return embedding

# Test with similar texts
emb1 = generate_simulated_embedding("reset my password")
emb2 = generate_simulated_embedding("change account password")
emb3 = generate_simulated_embedding("quarterly sales report")

# Compute cosine similarity
def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    return dot / (mag_a * mag_b) if mag_a * mag_b > 0 else 0

sim_12 = cosine_similarity(emb1, emb2)
sim_13 = cosine_similarity(emb1, emb3)

print(f"Similarity('reset my password', 'change account password'): {sim_12:.4f}")
print(f"Similarity('reset my password', 'quarterly sales report'):  {sim_13:.4f}")
print(f"\nAs expected, the password-related texts are more similar to each other.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Computing Embeddings for All Documents
# MAGIC
# MAGIC In production, you would use a Databricks embedding model endpoint.
# MAGIC Here we use our simulation function.

# COMMAND ----------

from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import ArrayType, FloatType

# Register UDF for simulated embedding generation
@udf(ArrayType(FloatType()))
def compute_embedding_udf(text):
    if text is None:
        return [0.0] * 64
    return [float(x) for x in generate_simulated_embedding(text, dimensions=64)]

# Combine title and content for embedding
df_with_embeddings = df_docs.withColumn(
    "full_text",
    concat_ws(" ", col("title"), col("content"))
).withColumn(
    "embedding",
    compute_embedding_udf(col("full_text"))
)

df_with_embeddings.select("doc_id", "title", "embedding").show(5, truncate=50)

# COMMAND ----------

# Show embedding dimensions
sample_embedding = df_with_embeddings.select("embedding").first()[0]
print(f"Embedding dimensions: {len(sample_embedding)}")
print(f"First 10 values: {[round(v, 4) for v in sample_embedding[:10]]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vector Search API Configuration Templates
# MAGIC
# MAGIC Below are the API calls to set up Vector Search in a full Databricks workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Create a Vector Search Endpoint
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Create a managed vector search endpoint
# MAGIC endpoint = w.vector_search_endpoints.create_endpoint(
# MAGIC     name="it_support_vs_endpoint",
# MAGIC     endpoint_type="STANDARD"
# MAGIC )
# MAGIC print(f"Endpoint created: {endpoint.name}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Create a Delta Sync Index (Auto-Embedding)
# MAGIC
# MAGIC The Delta Sync Index automatically generates embeddings when data changes.
# MAGIC
# MAGIC ```python
# MAGIC # Create index with auto-embedding from a Delta table
# MAGIC index = w.vector_search_indexes.create_index(
# MAGIC     name="catalog.schema.it_support_index",
# MAGIC     endpoint_name="it_support_vs_endpoint",
# MAGIC     primary_key="doc_id",
# MAGIC     index_type="DELTA_SYNC",
# MAGIC     delta_sync_index_spec={
# MAGIC         "source_table": "catalog.schema.knowledge_base",
# MAGIC         "pipeline_type": "TRIGGERED",  # or "CONTINUOUS"
# MAGIC         "embedding_source_columns": [
# MAGIC             {
# MAGIC                 "name": "content",
# MAGIC                 "embedding_model_endpoint_name": "databricks-bge-large-en"
# MAGIC             }
# MAGIC         ]
# MAGIC     }
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Create a Direct Vector Access Index (Pre-Computed Embeddings)
# MAGIC
# MAGIC ```python
# MAGIC # Create index for pre-computed embeddings
# MAGIC index = w.vector_search_indexes.create_index(
# MAGIC     name="catalog.schema.it_support_direct_index",
# MAGIC     endpoint_name="it_support_vs_endpoint",
# MAGIC     primary_key="doc_id",
# MAGIC     index_type="DIRECT_ACCESS",
# MAGIC     direct_access_index_spec={
# MAGIC         "embedding_dimension": 1024,
# MAGIC         "embedding_vector_column": "embedding",
# MAGIC         "schema": {
# MAGIC             "doc_id": "int",
# MAGIC             "title": "string",
# MAGIC             "content": "string",
# MAGIC             "category": "string",
# MAGIC             "embedding": "array<float>"
# MAGIC         }
# MAGIC     }
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulated Vector Search
# MAGIC
# MAGIC We implement similarity search locally to demonstrate how
# MAGIC Vector Search works under the hood.

# COMMAND ----------

def vector_search(query_text, df_embeddings, top_k=5):
    """
    Simulate vector search by computing cosine similarity
    between the query embedding and all document embeddings.
    """
    query_embedding = generate_simulated_embedding(query_text, dimensions=64)

    # Collect all documents with their embeddings
    docs = df_embeddings.select("doc_id", "title", "content", "category", "embedding").collect()

    # Compute similarity scores
    results = []
    for doc in docs:
        doc_embedding = doc["embedding"]
        similarity = cosine_similarity(query_embedding, doc_embedding)
        results.append({
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "content": doc["content"][:100],
            "category": doc["category"],
            "similarity_score": round(similarity, 4)
        })

    # Sort by similarity (descending) and return top_k
    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    return results[:top_k]

# COMMAND ----------

# Search for password-related documents
query = "I forgot my password and cannot log in"
print(f"Query: '{query}'")
print(f"{'='*80}")

results = vector_search(query, df_with_embeddings, top_k=5)
for i, r in enumerate(results, 1):
    print(f"\n  #{i} | Score: {r['similarity_score']} | [{r['category']}]")
    print(f"      Title: {r['title']}")
    print(f"      Content: {r['content']}...")

# COMMAND ----------

# Search for network-related documents
query = "How do I connect to the company network from home?"
print(f"Query: '{query}'")
print(f"{'='*80}")

results = vector_search(query, df_with_embeddings, top_k=5)
for i, r in enumerate(results, 1):
    print(f"\n  #{i} | Score: {r['similarity_score']} | [{r['category']}]")
    print(f"      Title: {r['title']}")
    print(f"      Content: {r['content']}...")

# COMMAND ----------

# Search for security-related documents
query = "I received a suspicious email with a strange link"
print(f"Query: '{query}'")
print(f"{'='*80}")

results = vector_search(query, df_with_embeddings, top_k=5)
for i, r in enumerate(results, 1):
    print(f"\n  #{i} | Score: {r['similarity_score']} | [{r['category']}]")
    print(f"      Title: {r['title']}")
    print(f"      Content: {r['content']}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filtered (Hybrid) Vector Search
# MAGIC
# MAGIC Combine semantic similarity with metadata filters to narrow results.

# COMMAND ----------

def filtered_vector_search(query_text, df_embeddings, filters=None, top_k=5):
    """
    Vector search with metadata filtering.
    filters: dict like {"category": "security"} or {"category": ["security", "account"]}
    """
    query_embedding = generate_simulated_embedding(query_text, dimensions=64)
    docs = df_embeddings.select("doc_id", "title", "content", "category", "embedding").collect()

    results = []
    for doc in docs:
        # Apply metadata filters
        if filters:
            skip = False
            for key, value in filters.items():
                doc_val = doc[key]
                if isinstance(value, list):
                    if doc_val not in value:
                        skip = True
                else:
                    if doc_val != value:
                        skip = True
            if skip:
                continue

        doc_embedding = doc["embedding"]
        similarity = cosine_similarity(query_embedding, doc_embedding)
        results.append({
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "category": doc["category"],
            "similarity_score": round(similarity, 4)
        })

    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    return results[:top_k]

# COMMAND ----------

# Search only within security category
query = "How do I stay safe online?"
print(f"Query: '{query}'")
print(f"Filter: category = 'security'")
print(f"{'='*80}")

results = filtered_vector_search(
    query, df_with_embeddings,
    filters={"category": "security"},
    top_k=3
)
for i, r in enumerate(results, 1):
    print(f"  #{i} | Score: {r['similarity_score']} | [{r['category']}] {r['title']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### API Template for Filtered Search
# MAGIC
# MAGIC ```python
# MAGIC results = w.vector_search_indexes.query_index(
# MAGIC     index_name="catalog.schema.it_support_index",
# MAGIC     columns=["doc_id", "title", "content", "category"],
# MAGIC     query_text="How do I stay safe online?",
# MAGIC     num_results=5,
# MAGIC     filters={"category": "security"}
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Embedding Quality Analysis
# MAGIC
# MAGIC Analyze the distribution of similarity scores to understand embedding quality.

# COMMAND ----------

from pyspark.sql.functions import avg, min as spark_min, max as spark_max, stddev

# Compute pairwise similarities for a sample
all_docs = df_with_embeddings.select("doc_id", "title", "category", "embedding").collect()

pairwise_data = []
for i in range(len(all_docs)):
    for j in range(i + 1, len(all_docs)):
        sim = cosine_similarity(all_docs[i]["embedding"], all_docs[j]["embedding"])
        same_category = all_docs[i]["category"] == all_docs[j]["category"]
        pairwise_data.append((
            all_docs[i]["doc_id"],
            all_docs[j]["doc_id"],
            all_docs[i]["category"],
            all_docs[j]["category"],
            same_category,
            round(sim, 4)
        ))

pairwise_schema = StructType([
    StructField("doc_a", IntegerType()),
    StructField("doc_b", IntegerType()),
    StructField("cat_a", StringType()),
    StructField("cat_b", StringType()),
    StructField("same_category", StringType()),
    StructField("similarity", FloatType()),
])

df_pairwise = spark.createDataFrame(
    [(a, b, ca, cb, str(sc), s) for a, b, ca, cb, sc, s in pairwise_data],
    schema=pairwise_schema
)

# COMMAND ----------

# Compare same-category vs cross-category similarity
print("=== Embedding Quality Analysis ===\n")
print("Average similarity by category relationship:")
df_pairwise.groupBy("same_category").agg(
    avg("similarity").alias("avg_similarity"),
    spark_min("similarity").alias("min_similarity"),
    spark_max("similarity").alias("max_similarity"),
    stddev("similarity").alias("stddev_similarity")
).show()

print("Good embeddings show HIGHER similarity for same-category pairs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Index Management Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync and Monitor Index
# MAGIC
# MAGIC ```python
# MAGIC # Trigger a sync
# MAGIC w.vector_search_indexes.sync_index(
# MAGIC     index_name="catalog.schema.it_support_index"
# MAGIC )
# MAGIC
# MAGIC # Check index status
# MAGIC index = w.vector_search_indexes.get_index(
# MAGIC     index_name="catalog.schema.it_support_index"
# MAGIC )
# MAGIC print(f"Status: {index.status.ready}")
# MAGIC print(f"Row count: {index.status.num_rows}")
# MAGIC print(f"Index type: {index.index_type}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete Index and Endpoint
# MAGIC
# MAGIC ```python
# MAGIC # Clean up (order matters: delete index before endpoint)
# MAGIC w.vector_search_indexes.delete_index(
# MAGIC     index_name="catalog.schema.it_support_index"
# MAGIC )
# MAGIC w.vector_search_endpoints.delete_endpoint(
# MAGIC     endpoint_name="it_support_vs_endpoint"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Preparing Data for Vector Search
# MAGIC
# MAGIC Best practices for structuring your source Delta table.

# COMMAND ----------

# Optimal Delta table structure for Vector Search
optimal_schema = StructType([
    StructField("doc_id", IntegerType(), False),        # Primary key (required)
    StructField("content", StringType(), False),         # Text to embed (required)
    StructField("title", StringType(), True),            # Metadata for display
    StructField("category", StringType(), True),         # Filter column
    StructField("source", StringType(), True),           # Provenance tracking
    StructField("last_updated", StringType(), True),     # Freshness tracking
    StructField("chunk_index", IntegerType(), True),     # For chunked documents
])

print("Recommended Delta table schema for Vector Search:")
for field in optimal_schema.fields:
    nullable = "nullable" if field.nullable else "NOT NULL"
    print(f"  {field.name:20s}  {str(field.dataType):15s}  {nullable}")

# COMMAND ----------

# Save the knowledge base as a Delta table (ready for Vector Search)
df_docs.write.format("delta").mode("overwrite").saveAsTable("knowledge_base_delta")

print("Knowledge base saved as Delta table.")
print(f"Row count: {spark.table('knowledge_base_delta').count()}")
spark.table("knowledge_base_delta").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS knowledge_base_delta")
spark.catalog.dropTempView("knowledge_base")

print("Cleanup complete. Temporary tables and views removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Embeddings** capture semantic meaning -- similar texts have similar vectors
# MAGIC 2. **Delta Sync Index** auto-updates embeddings as your Delta table changes
# MAGIC 3. **Direct Vector Access Index** gives full control with pre-computed embeddings
# MAGIC 4. **Cosine similarity** is the standard metric for comparing embeddings
# MAGIC 5. **Metadata filters** combine semantic search with business rules (hybrid search)
# MAGIC 6. **Good embeddings** show higher similarity for related documents
# MAGIC
# MAGIC **Next:** [03 - RAG Pipelines](03-rag-pipelines_notebook.py)
