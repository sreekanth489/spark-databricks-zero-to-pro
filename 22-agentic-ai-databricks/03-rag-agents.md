# RAG Agents
> Module 22 -- Topic 03 | Level: Advanced | Time: 65 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain how a RAG agent combines retrieval, generation, and tool use
2. Describe the document preparation pipeline for RAG on Databricks
3. Create a vector search index and configure it as a retriever tool
4. Build a RAG agent with the ChatAgent interface
5. Implement conversation memory for multi-turn RAG interactions
6. Configure and deploy a production RAG agent

---

## Conceptual Overview

### What Is a RAG Agent?

A RAG (Retrieval-Augmented Generation) agent combines three capabilities:

1. **Retrieval** -- Searching a knowledge base (vector search index) to find
   documents relevant to the user's question
2. **Generation** -- Using an LLM to synthesize a response grounded in the
   retrieved documents
3. **Agent reasoning** -- Deciding WHEN to retrieve, WHAT to search for, and
   WHETHER the retrieved documents are sufficient to answer the question

The key difference between a basic RAG pipeline and a RAG agent is autonomy.
A basic RAG pipeline always retrieves, then generates. A RAG agent can decide
to skip retrieval if the question does not need it, reformulate the search
query if the first results are not relevant, or call additional tools (SQL
queries, calculators) to supplement the retrieved information.

```
  Basic RAG vs. RAG Agent
  ========================

  Basic RAG Pipeline (fixed):
  +-------+     +----------+     +-------+     +----------+
  | Query | --> | Retrieve | --> |  LLM  | --> | Response |
  +-------+     +----------+     +-------+     +----------+
  (always retrieves, always generates, no flexibility)

  RAG Agent (dynamic):
  +-------+     +-------+     +----------+
  | Query | --> |  LLM  | --> | Decision |
  +-------+     +-------+     +----------+
                    ^              |
                    |    +---------+---------+----------+
                    |    |         |         |          |
                    |    v         v         v          v
                    | Retrieve  SQL Tool  Calculator  Answer
                    | (vector   (query    (compute    (no tool
                    |  search)   tables)   values)    needed)
                    |    |         |         |
                    +----+---------+---------+
                         (results feed back to LLM)
```

### Why RAG Agents on Databricks?

Databricks provides an integrated stack for building RAG agents:

```
  RAG Agent Stack on Databricks
  ==============================

  +-----------------------------------------------------------+
  |  Agent Layer (Mosaic AI Agent Framework)                   |
  |  - ChatAgent interface                                    |
  |  - Tool orchestration                                     |
  |  - Conversation memory                                    |
  +-----------------------------------------------------------+
  |  LLM Layer (Model Serving)                                |
  |  - Foundation models (DBRX, Llama, Mixtral)               |
  |  - External models (GPT-4, Claude via gateway)            |
  |  - Fine-tuned models                                      |
  +-----------------------------------------------------------+
  |  Retrieval Layer (Vector Search)                          |
  |  - Managed vector search indexes                          |
  |  - Delta Sync for automatic index updates                 |
  |  - Embedding models (BGE, E5, OpenAI)                     |
  +-----------------------------------------------------------+
  |  Data Layer (Unity Catalog + Delta Lake)                  |
  |  - Source documents in Delta tables                       |
  |  - Chunked and embedded documents                         |
  |  - Governance and access control                          |
  +-----------------------------------------------------------+
```

---

## Document Preparation Pipeline

Before an agent can retrieve documents, those documents must be prepared:
chunked, embedded, and indexed.

### Step 1: Document Ingestion

Raw documents (PDFs, HTML, text files, Markdown) are loaded into a Delta table:

```python
# Ingest documents into a Delta table
documents_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load("/Volumes/catalog/schema/documents/")
)

# Parse and extract text (using libraries like pypdf, unstructured)
parsed_df = documents_df.select(
    F.col("path").alias("source"),
    parse_document_udf(F.col("content")).alias("text"),
    F.current_timestamp().alias("ingested_at"),
)
parsed_df.write.mode("append").saveAsTable("catalog.schema.raw_documents")
```

### Step 2: Chunking

Documents are split into smaller chunks that fit within the embedding model's
token limit and provide focused context for retrieval:

```
  Chunking Strategies
  ====================

  Fixed-size chunks:     Semantic chunks:        Recursive splitting:
  +------+------+       +----------+             +------------------+
  | 500  | 500  |       | Paragraph|             | Split on headings|
  |tokens|tokens|       +----------+             +------------------+
  +------+------+       | Paragraph|             | Then paragraphs  |
  | 500  | 500  |       +----------+             +------------------+
  |tokens|tokens|       | Paragraph|             | Then sentences   |
  +------+------+       +----------+             +------------------+
  (simple, may          (preserves meaning,      (hierarchical,
   split mid-sentence)   variable size)           best balance)
```

### Step 3: Embedding

Each chunk is converted to a vector (embedding) using an embedding model:

```python
# Compute embeddings using a Databricks-hosted model
from databricks.vector_search.client import VectorSearchClient

# The embedding is computed automatically when using managed embeddings
# OR you can compute embeddings explicitly:
embedded_df = chunks_df.withColumn(
    "embedding",
    ai_query("databricks-bge-large-en", F.col("chunk_text")),
)
```

### Step 4: Vector Search Index

The embeddings are stored in a vector search index for fast similarity search:

```python
vsc = VectorSearchClient()

# Create a Delta Sync index (automatically syncs with source table)
index = vsc.create_delta_sync_index(
    endpoint_name="my-vector-search-endpoint",
    index_name="catalog.schema.document_index",
    source_table_name="catalog.schema.chunked_documents",
    pipeline_type="TRIGGERED",      # or CONTINUOUS
    primary_key="chunk_id",
    embedding_dimension=1024,
    embedding_vector_column="embedding",
    embedding_source_column="chunk_text",  # For managed embeddings
    embedding_model_endpoint_name="databricks-bge-large-en",
)
```

---

## Building a RAG Agent

### Retriever Tool Configuration

The vector search index becomes a tool that the agent can invoke:

```python
from databricks.agents.tools import VectorSearchRetrieverTool

retriever_tool = VectorSearchRetrieverTool(
    index_name="catalog.schema.document_index",
    description=(
        "Search the company knowledge base for relevant documents. "
        "Use this tool when the user asks about company policies, "
        "product documentation, or internal procedures. "
        "Returns the most relevant document chunks with similarity scores."
    ),
    num_results=5,
    columns=["chunk_text", "source", "title"],
    filters={"status": "published"},
)
```

### RAG Agent Definition

```python
from databricks.agents import ChatAgent, ChatAgentMessage, ChatAgentResponse

class RAGAgent(ChatAgent):
    """
    A RAG agent that retrieves relevant documents before generating responses.
    Uses vector search as a retriever tool alongside other tools.
    """

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.tools = self._initialize_tools()

    def _initialize_tools(self):
        return [
            VectorSearchRetrieverTool(
                index_name=self.config["index_name"],
                num_results=self.config.get("num_results", 5),
            ),
            # Additional tools as needed
        ]

    def predict(self, messages: list[ChatAgentMessage]) -> ChatAgentResponse:
        """
        Process messages using retrieval-augmented generation.
        The agent decides whether to retrieve, what to search for,
        and how to synthesize the response.
        """
        # The framework handles the ReAct loop:
        # 1. LLM reads the message and tool schemas
        # 2. LLM decides to call the retriever tool
        # 3. Retriever returns relevant chunks
        # 4. LLM generates a grounded response
        response = self._run_agent_loop(messages)
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=response)]
        )
```

---

## Conversation Memory for RAG

RAG agents often handle multi-turn conversations where context from previous
turns is essential for understanding the current question:

```
  Multi-Turn RAG Conversation
  =============================

  Turn 1:
    User: "What is our return policy?"
    Agent: [retrieves return policy doc] "Our return policy allows..."

  Turn 2:
    User: "Does that apply to electronics?"
    Agent: [uses conversation memory to understand "that" = return policy]
           [retrieves electronics-specific return policy]
           "For electronics specifically, the return window is..."

  Turn 3:
    User: "What about international orders?"
    Agent: [memory: topic = return policy + electronics]
           [retrieves international electronics return policy]
           "For international electronics orders..."
```

Memory strategies for RAG agents:

| Strategy | How It Works | Best For |
|----------|-------------|----------|
| Full history | Send all previous messages to the LLM | Short conversations (<10 turns) |
| Sliding window | Keep last N turns only | Medium conversations |
| Summary memory | Periodically summarize older turns | Long conversations |
| Hybrid | Sliding window + summary of older turns | Production agents |

---

## Production Deployment

### Deployment Configuration

```python
# RAG agent deployment configuration
rag_agent_config = {
    "agent_name": "knowledge_base_assistant",
    "llm_endpoint": "databricks-dbrx-instruct",
    "retriever": {
        "index_name": "catalog.schema.document_index",
        "num_results": 5,
        "score_threshold": 0.7,   # Minimum similarity score
    },
    "memory": {
        "type": "sliding_window",
        "window_size": 10,
    },
    "guardrails": {
        "max_tokens": 4096,
        "blocked_topics": ["competitor_pricing", "internal_salary"],
        "require_citations": True,  # Agent must cite source documents
    },
    "serving": {
        "endpoint_name": "rag-agent-endpoint",
        "min_replicas": 1,
        "max_replicas": 10,
        "scale_to_zero": False,    # Keep warm for low latency
    },
}
```

### Monitoring RAG Agent Quality

Key metrics to track for RAG agents:

```
  RAG Agent Metrics
  ==================

  Retrieval Quality:
  - Retrieval precision: % of retrieved docs that are relevant
  - Retrieval recall: % of relevant docs that are retrieved
  - Average similarity score of retrieved documents

  Generation Quality:
  - Groundedness: Is the response supported by retrieved docs?
  - Relevance: Does the response answer the user's question?
  - Correctness: Is the factual content accurate?
  - Citation accuracy: Are sources cited correctly?

  Operational Metrics:
  - End-to-end latency (retrieval + generation)
  - Token usage per request
  - Tool call patterns (retrieval frequency)
  - Error rates and failure modes
```

---

## Key Takeaways

1. RAG agents combine retrieval, generation, and agent reasoning for dynamic behavior
2. Document preparation requires: ingestion, chunking, embedding, and indexing
3. Vector search indexes are configured as retriever tools for the agent
4. Conversation memory is essential for multi-turn RAG interactions
5. Production RAG agents need guardrails (citations, blocked topics, score thresholds)
6. Monitor both retrieval quality and generation quality separately

---

## Practice Exercises

1. Design a chunking strategy for a set of technical documentation that includes
   code examples, tables, and narrative text. How would you handle each type?
2. Write the configuration for a RAG agent that answers HR policy questions.
   What guardrails would you put in place?
3. Describe how you would handle a conversation where the RAG agent cannot find
   relevant documents. What should the agent do?
