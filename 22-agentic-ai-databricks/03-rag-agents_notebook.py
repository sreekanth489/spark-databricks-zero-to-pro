# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - RAG Agents
# MAGIC > Module 22 -- Topic 03 | Build a complete RAG agent with document processing, retrieval, and generation
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Build a document ingestion and chunking pipeline
# MAGIC 2. Create a simulated vector search index with embeddings
# MAGIC 3. Implement a retriever tool that searches the document index
# MAGIC 4. Build a RAG agent with conversation memory
# MAGIC 5. Demonstrate multi-turn conversations with context awareness
# MAGIC 6. Show production deployment configuration templates
# MAGIC
# MAGIC **Note:** This notebook simulates vector search and LLM behavior.
# MAGIC Production RAG agents use Databricks Vector Search and Model Serving endpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Document Generation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, ArrayType, FloatType
)
from datetime import datetime, timedelta
import json
import random
import hashlib
import math

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Knowledge Base of Company Documents
# MAGIC We create realistic documents covering company policies, product information,
# MAGIC and procedures that our RAG agent will search.

# COMMAND ----------

documents = [
    {
        "doc_id": "DOC-001",
        "title": "Return Policy",
        "category": "Policy",
        "content": (
            "Our standard return policy allows customers to return products within 30 days "
            "of purchase with a valid receipt. Products must be in original packaging and "
            "unused condition. Electronics have a 15-day return window. International orders "
            "may be returned within 45 days but the customer is responsible for return shipping. "
            "Refunds are processed within 5-7 business days. Gift cards and downloadable "
            "software are non-refundable."
        ),
    },
    {
        "doc_id": "DOC-002",
        "title": "Shipping Information",
        "category": "Policy",
        "content": (
            "Standard shipping takes 5-7 business days and is free for orders over $50. "
            "Express shipping is 2-3 business days at a flat rate of $12.99. "
            "Next-day delivery is available for $24.99 for orders placed before 2 PM EST. "
            "International shipping takes 10-15 business days. All orders include tracking. "
            "Oversized items may incur additional shipping charges."
        ),
    },
    {
        "doc_id": "DOC-003",
        "title": "Laptop Pro X1 Specifications",
        "category": "Product",
        "content": (
            "The Laptop Pro X1 features a 14-inch 2K display, Intel Core i7-13700H "
            "processor, 16GB DDR5 RAM, and 512GB NVMe SSD. Battery life is up to 12 hours. "
            "Weight is 3.2 lbs. Ports include 2x USB-C Thunderbolt 4, 1x USB-A 3.2, HDMI 2.1, "
            "and a 3.5mm headphone jack. Available in Space Gray and Silver. "
            "Starting price is $1,299. Business models include 32GB RAM option for $1,599."
        ),
    },
    {
        "doc_id": "DOC-004",
        "title": "Warranty Coverage",
        "category": "Policy",
        "content": (
            "All products come with a standard 1-year manufacturer warranty covering defects "
            "in materials and workmanship. Extended warranty options are available: 2-year for "
            "$99 and 3-year for $149. Warranty does not cover accidental damage, water damage, "
            "or unauthorized modifications. To file a warranty claim, contact support with your "
            "order number and description of the issue. Warranty replacements are shipped "
            "within 3-5 business days."
        ),
    },
    {
        "doc_id": "DOC-005",
        "title": "Employee Onboarding Guide",
        "category": "Internal",
        "content": (
            "New employees complete a 2-week onboarding program. Week 1 covers company culture, "
            "IT setup, security training, and benefits enrollment. Week 2 includes team "
            "introductions, role-specific training, and a buddy system assignment. All new hires "
            "receive a company laptop, badge, and parking pass on day one. The HR portal at "
            "hr.company.com has forms, policies, and the employee handbook."
        ),
    },
    {
        "doc_id": "DOC-006",
        "title": "Data Security Policy",
        "category": "Internal",
        "content": (
            "All employees must complete annual security awareness training. Passwords must be "
            "at least 12 characters with mixed case, numbers, and symbols. Multi-factor "
            "authentication is required for all systems. Customer data must be encrypted at "
            "rest and in transit. Data classification levels: Public, Internal, Confidential, "
            "Restricted. Incidents must be reported within 1 hour to security@company.com."
        ),
    },
    {
        "doc_id": "DOC-007",
        "title": "Tablet Ultra S8 Specifications",
        "category": "Product",
        "content": (
            "The Tablet Ultra S8 has a 10.5-inch AMOLED display with 120Hz refresh rate. "
            "Powered by Snapdragon 8 Gen 2 processor with 8GB RAM and 128GB or 256GB storage. "
            "Battery capacity is 8000mAh with fast charging. Includes S Pen stylus. "
            "Camera: 13MP rear, 8MP front. Weight: 1.1 lbs. Available in Graphite and Cream. "
            "Starting price is $699. Bundle with keyboard case for $799."
        ),
    },
    {
        "doc_id": "DOC-008",
        "title": "Customer Loyalty Program",
        "category": "Policy",
        "content": (
            "The rewards program has four tiers: Bronze (0-999 points), Silver (1000-4999), "
            "Gold (5000-9999), and Platinum (10000+). Earn 1 point per dollar spent. "
            "Silver members get free standard shipping. Gold members get 10% off all orders. "
            "Platinum members get 15% off, free express shipping, and early access to sales. "
            "Points expire after 12 months of account inactivity."
        ),
    },
    {
        "doc_id": "DOC-009",
        "title": "Headset Pro Max Features",
        "category": "Product",
        "content": (
            "The Headset Pro Max offers active noise cancellation with transparency mode. "
            "40mm custom drivers deliver high-fidelity audio. Bluetooth 5.3 with multipoint "
            "connection supports two devices simultaneously. Battery life is 30 hours with ANC. "
            "Quick charge: 10 minutes for 3 hours of playback. Available in Black, White, "
            "and Navy. Price: $249. Includes carrying case and USB-C charging cable."
        ),
    },
    {
        "doc_id": "DOC-010",
        "title": "Technical Support Escalation Process",
        "category": "Internal",
        "content": (
            "Support tickets are classified into four severity levels. Severity 1 (Critical): "
            "System down, escalate immediately to engineering. Severity 2 (High): Major feature "
            "broken, 4-hour response SLA. Severity 3 (Medium): Minor issue, 24-hour response. "
            "Severity 4 (Low): Feature request or cosmetic issue, 72-hour response. "
            "Escalation path: Tier 1 Support -> Tier 2 Specialist -> Engineering -> VP Engineering."
        ),
    },
]

# Create documents DataFrame
doc_rows = [(d["doc_id"], d["title"], d["category"], d["content"]) for d in documents]
doc_schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("category", StringType(), False),
    StructField("content", StringType(), False),
])
docs_df = spark.createDataFrame(doc_rows, schema=doc_schema)
docs_df.createOrReplaceTempView("raw_documents")
print(f"Knowledge base: {docs_df.count()} documents")
docs_df.select("doc_id", "title", "category", F.length("content").alias("content_length")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Document Chunking Pipeline
# MAGIC
# MAGIC Documents are split into smaller chunks for more precise retrieval.

# COMMAND ----------

def chunk_document(doc_id, title, category, content, chunk_size=200, overlap=50):
    """
    Split a document into overlapping chunks.
    In production, use a library like LangChain's RecursiveCharacterTextSplitter.
    """
    words = content.split()
    chunks = []
    start = 0
    chunk_num = 0

    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk_text = " ".join(words[start:end])
        chunk_id = f"{doc_id}_chunk_{chunk_num:03d}"
        chunks.append({
            "chunk_id": chunk_id,
            "doc_id": doc_id,
            "title": title,
            "category": category,
            "chunk_text": chunk_text,
            "chunk_index": chunk_num,
            "word_count": end - start,
        })
        chunk_num += 1
        start += chunk_size - overlap
        if start >= len(words):
            break

    return chunks


# Chunk all documents
all_chunks = []
for doc in documents:
    chunks = chunk_document(
        doc_id=doc["doc_id"],
        title=doc["title"],
        category=doc["category"],
        content=doc["content"],
        chunk_size=80,
        overlap=20,
    )
    all_chunks.extend(chunks)

chunk_rows = [(c["chunk_id"], c["doc_id"], c["title"], c["category"],
                c["chunk_text"], c["chunk_index"], c["word_count"]) for c in all_chunks]

chunk_schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("doc_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("category", StringType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("chunk_index", IntegerType(), False),
    StructField("word_count", IntegerType(), False),
])

chunks_df = spark.createDataFrame(chunk_rows, schema=chunk_schema)
chunks_df.createOrReplaceTempView("document_chunks")
print(f"Total chunks created: {chunks_df.count()} from {docs_df.count()} documents")
chunks_df.groupBy("doc_id", "title").count().orderBy("doc_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Simulated Vector Embeddings
# MAGIC
# MAGIC In production, embeddings are computed using models like BGE or E5.
# MAGIC Here we simulate embeddings using a hash-based approach.

# COMMAND ----------

def simulate_embedding(text, dimension=64):
    """
    Generate a simulated embedding vector for demonstration.
    Production systems use real embedding models (BGE, E5, OpenAI ada-002).
    """
    # Create a deterministic pseudo-embedding based on text content
    hash_bytes = hashlib.sha256(text.encode()).digest()
    # Extend hash to fill the embedding dimension
    extended = hash_bytes * ((dimension // len(hash_bytes)) + 1)
    raw_values = [b / 255.0 for b in extended[:dimension]]
    # Normalize to unit vector
    magnitude = math.sqrt(sum(v * v for v in raw_values))
    if magnitude == 0:
        return raw_values
    return [round(v / magnitude, 6) for v in raw_values]


def cosine_similarity(vec_a, vec_b):
    """Compute cosine similarity between two vectors."""
    dot_product = sum(a * b for a, b in zip(vec_a, vec_b))
    mag_a = math.sqrt(sum(a * a for a in vec_a))
    mag_b = math.sqrt(sum(b * b for b in vec_b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot_product / (mag_a * mag_b)


# Compute embeddings for all chunks
chunk_embeddings = {}
for chunk in all_chunks:
    embedding = simulate_embedding(chunk["chunk_text"])
    chunk_embeddings[chunk["chunk_id"]] = {
        "chunk_id": chunk["chunk_id"],
        "doc_id": chunk["doc_id"],
        "title": chunk["title"],
        "category": chunk["category"],
        "chunk_text": chunk["chunk_text"],
        "embedding": embedding,
    }

print(f"Embeddings computed for {len(chunk_embeddings)} chunks")
sample_id = list(chunk_embeddings.keys())[0]
sample = chunk_embeddings[sample_id]
print(f"\nSample embedding for '{sample_id}':")
print(f"  Dimension: {len(sample['embedding'])}")
print(f"  First 8 values: {sample['embedding'][:8]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Simulated Vector Search Index
# MAGIC
# MAGIC This class simulates a Databricks Vector Search index for demonstration.

# COMMAND ----------

class SimulatedVectorSearchIndex:
    """
    Simulates a Databricks Vector Search index.
    In production, use the VectorSearchClient API.
    """

    def __init__(self, index_name, embeddings_store):
        self.index_name = index_name
        self.store = embeddings_store
        self.query_count = 0

    def search(self, query_text, num_results=5, filters=None):
        """
        Search the index for the most similar chunks to the query.
        Returns ranked results with similarity scores.
        """
        self.query_count += 1
        query_embedding = simulate_embedding(query_text)

        # Compute similarity with all chunks
        scored_results = []
        for chunk_id, chunk_data in self.store.items():
            # Apply category filter if specified
            if filters and "category" in filters:
                if chunk_data["category"] != filters["category"]:
                    continue

            similarity = cosine_similarity(query_embedding, chunk_data["embedding"])
            scored_results.append({
                "chunk_id": chunk_data["chunk_id"],
                "doc_id": chunk_data["doc_id"],
                "title": chunk_data["title"],
                "category": chunk_data["category"],
                "chunk_text": chunk_data["chunk_text"],
                "similarity_score": round(similarity, 4),
            })

        # Sort by similarity and return top results
        scored_results.sort(key=lambda x: x["similarity_score"], reverse=True)
        return scored_results[:num_results]


# Create the simulated index
vector_index = SimulatedVectorSearchIndex(
    index_name="catalog.schema.document_index",
    embeddings_store=chunk_embeddings,
)

# Test the search
print("Vector Search Test: 'return policy for electronics'")
results = vector_index.search("return policy for electronics", num_results=3)
for i, result in enumerate(results):
    print(f"\n  Result {i+1} (score: {result['similarity_score']}):")
    print(f"    Title: {result['title']}")
    print(f"    Text: {result['chunk_text'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Building the RAG Agent
# MAGIC
# MAGIC Now we combine the retriever, LLM (simulated), and memory into a
# MAGIC complete RAG agent.

# COMMAND ----------

class ConversationMemory:
    """Manages multi-turn conversation history."""

    def __init__(self, max_turns=10):
        self.messages = []
        self.max_turns = max_turns

    def add(self, role, content):
        self.messages.append({"role": role, "content": content})
        # Keep only recent messages
        non_system = [m for m in self.messages if m["role"] != "system"]
        system = [m for m in self.messages if m["role"] == "system"]
        if len(non_system) > self.max_turns * 2:
            self.messages = system + non_system[-(self.max_turns * 2):]

    def get_context(self):
        """Return conversation context as a formatted string."""
        context_parts = []
        for msg in self.messages:
            context_parts.append(f"{msg['role'].upper()}: {msg['content']}")
        return "\n".join(context_parts)

    def get_messages(self):
        return self.messages.copy()

    def clear(self):
        self.messages = []

# COMMAND ----------

class RAGAgent:
    """
    A RAG agent that retrieves documents and generates grounded responses.
    Simulates the behavior of a Databricks Mosaic AI RAG agent.
    """

    def __init__(self, vector_index, system_prompt, config=None):
        self.vector_index = vector_index
        self.system_prompt = system_prompt
        self.config = config or {}
        self.memory = ConversationMemory(max_turns=self.config.get("max_turns", 10))
        self.memory.add("system", system_prompt)
        self.execution_log = []

    def predict(self, user_message):
        """
        Process a user message:
        1. Decide if retrieval is needed
        2. Retrieve relevant documents
        3. Generate a grounded response
        4. Return the response with citations
        """
        self.memory.add("user", user_message)
        log_entry = {"query": user_message, "steps": []}

        # Step 1: Decide if retrieval is needed
        needs_retrieval = self._needs_retrieval(user_message)
        log_entry["steps"].append({
            "action": "retrieval_decision",
            "result": needs_retrieval,
        })

        retrieved_context = ""
        citations = []

        if needs_retrieval:
            # Step 2: Formulate search query (may use conversation context)
            search_query = self._formulate_search_query(user_message)
            log_entry["steps"].append({
                "action": "search_query_formulation",
                "query": search_query,
            })

            # Step 3: Retrieve documents
            results = self.vector_index.search(
                query_text=search_query,
                num_results=self.config.get("num_results", 3),
            )
            log_entry["steps"].append({
                "action": "retrieval",
                "num_results": len(results),
                "top_score": results[0]["similarity_score"] if results else 0,
            })

            # Step 4: Build context from retrieved documents
            for i, result in enumerate(results):
                retrieved_context += f"\n[Source {i+1}: {result['title']}]\n{result['chunk_text']}\n"
                citations.append({
                    "source_num": i + 1,
                    "title": result["title"],
                    "doc_id": result["doc_id"],
                    "score": result["similarity_score"],
                })

        # Step 5: Generate response (simulated LLM call)
        response = self._generate_response(user_message, retrieved_context, citations)
        log_entry["steps"].append({
            "action": "response_generation",
            "has_citations": len(citations) > 0,
            "response_length": len(response),
        })

        self.memory.add("assistant", response)
        self.execution_log.append(log_entry)
        return response

    def _needs_retrieval(self, message):
        """Determine if the query needs document retrieval."""
        greetings = ["hello", "hi", "hey", "thanks", "thank you", "bye", "goodbye"]
        msg_lower = message.lower().strip()
        if any(msg_lower.startswith(g) for g in greetings):
            return False
        if msg_lower in ["yes", "no", "ok", "sure"]:
            return False
        return True

    def _formulate_search_query(self, message):
        """
        Formulate the search query using conversation context.
        Resolves pronouns and references from previous turns.
        """
        msg_lower = message.lower()
        # Check for references to previous context (pronouns, "that", "it")
        referential_words = ["that", "it", "this", "those", "them"]
        if any(word in msg_lower.split() for word in referential_words):
            # Include context from the last assistant message
            prev_messages = self.memory.get_messages()
            for msg in reversed(prev_messages):
                if msg["role"] == "assistant":
                    # Combine current question with previous context
                    return f"{message} {msg['content'][:100]}"
            return message
        return message

    def _generate_response(self, question, context, citations):
        """
        Simulate LLM response generation.
        In production, this calls the Model Serving endpoint.
        """
        if not context:
            return (
                "I can help you with questions about our products, policies, and procedures. "
                "What would you like to know?"
            )

        # Simulate grounded response using retrieved context
        # Extract key information from context
        response_parts = []
        response_parts.append(f"Based on our documentation, here is what I found:\n")

        # Use the retrieved chunks to build a response
        context_lines = context.strip().split("\n")
        content_lines = [line for line in context_lines if line.strip() and not line.startswith("[Source")]
        if content_lines:
            response_parts.append(content_lines[0])
            if len(content_lines) > 1:
                response_parts.append(content_lines[1])

        # Add citations
        if citations:
            response_parts.append("\n\nSources:")
            for cite in citations[:3]:
                response_parts.append(f"  [{cite['source_num']}] {cite['title']} (relevance: {cite['score']:.2f})")

        return "\n".join(response_parts)

    def get_log(self):
        return self.execution_log

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create and Test the RAG Agent

# COMMAND ----------

rag_agent = RAGAgent(
    vector_index=vector_index,
    system_prompt=(
        "You are a helpful assistant for our company. Answer questions using the "
        "knowledge base. Always cite your sources. If you cannot find relevant "
        "information, say so honestly."
    ),
    config={
        "num_results": 3,
        "max_turns": 10,
        "score_threshold": 0.5,
    },
)

print("RAG Agent created. Testing with sample questions...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Multi-Turn RAG Conversation Demo

# COMMAND ----------

# Conversation turn 1
print("=" * 70)
print("TURN 1")
print("=" * 70)
q1 = "What is your return policy?"
print(f"USER: {q1}")
r1 = rag_agent.predict(q1)
print(f"\nAGENT:\n{r1}")

# COMMAND ----------

# Conversation turn 2 (references previous context)
print("=" * 70)
print("TURN 2")
print("=" * 70)
q2 = "Does that apply to electronics specifically?"
print(f"USER: {q2}")
r2 = rag_agent.predict(q2)
print(f"\nAGENT:\n{r2}")

# COMMAND ----------

# Conversation turn 3 (different topic)
print("=" * 70)
print("TURN 3")
print("=" * 70)
q3 = "Tell me about the Laptop Pro X1 specifications"
print(f"USER: {q3}")
r3 = rag_agent.predict(q3)
print(f"\nAGENT:\n{r3}")

# COMMAND ----------

# Conversation turn 4 (greeting -- no retrieval needed)
print("=" * 70)
print("TURN 4")
print("=" * 70)
q4 = "Thanks for the help!"
print(f"USER: {q4}")
r4 = rag_agent.predict(q4)
print(f"\nAGENT:\n{r4}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Analyzing Agent Execution Logs

# COMMAND ----------

# Display execution logs as a DataFrame
log_data = []
for i, entry in enumerate(rag_agent.get_log()):
    for step in entry["steps"]:
        log_data.append((
            i + 1,
            entry["query"][:50],
            step["action"],
            str(step.get("result", step.get("query", step.get("num_results", step.get("has_citations", ""))))),
        ))

log_schema = StructType([
    StructField("turn", IntegerType()),
    StructField("query", StringType()),
    StructField("action", StringType()),
    StructField("detail", StringType()),
])

log_df = spark.createDataFrame(log_data, schema=log_schema)
print("RAG Agent Execution Log:")
log_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Production Deployment Templates

# COMMAND ----------

# Template: Vector Search Index Creation (requires full workspace)
print("=" * 70)
print("TEMPLATE: Vector Search Index Creation")
print("=" * 70)
print("""
# Production vector search index creation
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create a vector search endpoint (compute resources for serving)
vsc.create_endpoint(
    name="rag-agent-vs-endpoint",
    endpoint_type="STANDARD",
)

# Create a Delta Sync index (auto-updates when source table changes)
index = vsc.create_delta_sync_index(
    endpoint_name="rag-agent-vs-endpoint",
    index_name="catalog.schema.knowledge_base_index",
    source_table_name="catalog.schema.chunked_documents",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="chunk_text",
    embedding_model_endpoint_name="databricks-bge-large-en",
)
""")

# COMMAND ----------

# Template: RAG Agent Deployment
print("=" * 70)
print("TEMPLATE: RAG Agent Deployment with MLflow")
print("=" * 70)
print("""
import mlflow
from databricks.agents import ChatAgent, ChatAgentMessage, ChatAgentResponse
from databricks.agents.tools import VectorSearchRetrieverTool

class ProductionRAGAgent(ChatAgent):
    def __init__(self):
        super().__init__()
        self.retriever = VectorSearchRetrieverTool(
            index_name="catalog.schema.knowledge_base_index",
            num_results=5,
            columns=["chunk_text", "title", "doc_id"],
        )

    def predict(self, messages):
        # Framework handles ReAct loop, tool calling, and memory
        response = self._run_agent_loop(messages)
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=response)]
        )

# Log and register the agent
with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="rag_agent",
        python_model=ProductionRAGAgent(),
        registered_model_name="catalog.schema.rag_knowledge_assistant",
    )

# Deploy to serving endpoint
# Use the Databricks UI or REST API to create a serving endpoint
# pointing to the registered model version
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: RAG Quality Metrics

# COMMAND ----------

# Simulate RAG quality metrics
quality_data = []
questions = [
    ("What is the return policy?", True, True, True, "DOC-001"),
    ("Laptop Pro X1 specs?", True, True, True, "DOC-003"),
    ("What is the capital of France?", True, False, False, "N/A"),
    ("Warranty coverage details?", True, True, True, "DOC-004"),
    ("Headset Pro Max price?", True, True, True, "DOC-009"),
    ("How do I onboard?", True, True, True, "DOC-005"),
    ("Loyalty program tiers?", True, True, True, "DOC-008"),
    ("What is quantum computing?", True, False, False, "N/A"),
    ("Shipping options?", True, True, True, "DOC-002"),
    ("Escalation process?", True, True, True, "DOC-010"),
]

for i, (question, retrieved, relevant, grounded, expected_doc) in enumerate(questions):
    quality_data.append((
        i + 1,
        question,
        retrieved,
        relevant,
        grounded,
        expected_doc,
        round(random.uniform(0.5, 1.0) if relevant else random.uniform(0.1, 0.4), 3),
    ))

quality_schema = StructType([
    StructField("question_id", IntegerType()),
    StructField("question", StringType()),
    StructField("docs_retrieved", BooleanType()),
    StructField("is_relevant", BooleanType()),
    StructField("is_grounded", BooleanType()),
    StructField("expected_source", StringType()),
    StructField("confidence_score", DoubleType()),
])

quality_df = spark.createDataFrame(quality_data, schema=quality_schema)
quality_df.createOrReplaceTempView("rag_quality")

print("RAG Quality Assessment:")
quality_df.show(truncate=False)

# COMMAND ----------

# Compute aggregate metrics
print("Aggregate RAG Quality Metrics:")
spark.sql("""
    SELECT
        COUNT(*) as total_questions,
        ROUND(SUM(CASE WHEN is_relevant THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as relevance_rate_pct,
        ROUND(SUM(CASE WHEN is_grounded THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as groundedness_rate_pct,
        ROUND(AVG(confidence_score), 3) as avg_confidence,
        ROUND(AVG(CASE WHEN is_relevant THEN confidence_score END), 3) as avg_relevant_confidence,
        ROUND(AVG(CASE WHEN NOT is_relevant THEN confidence_score END), 3) as avg_irrelevant_confidence
    FROM rag_quality
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Cleanup

# COMMAND ----------

spark.catalog.dropTempView("raw_documents")
spark.catalog.dropTempView("document_chunks")
spark.catalog.dropTempView("rag_quality")
print("Temporary views cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **RAG agents** combine retrieval, generation, and autonomous reasoning
# MAGIC 2. The **document pipeline** has four stages: ingest, chunk, embed, index
# MAGIC 3. **Chunking strategy** affects retrieval quality -- balance size vs. context preservation
# MAGIC 4. **Vector search indexes** are configured as retriever tools for the agent
# MAGIC 5. **Conversation memory** enables multi-turn RAG with pronoun resolution
# MAGIC 6. RAG agents decide **when** to retrieve, not just **what** -- this is key over basic RAG
# MAGIC 7. **Citations** are essential for user trust and response verification
# MAGIC 8. Monitor **retrieval quality** and **generation quality** as separate dimensions
