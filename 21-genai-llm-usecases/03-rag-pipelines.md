# RAG Pipelines on Databricks

> Module 21 -- Topic 03 | Level: Intermediate | Time: 60 min

## Learning Objectives

- Explain the RAG architecture and why it outperforms standalone LLM inference for enterprise data
- Design a document processing pipeline: chunking, embedding, and indexing
- Compare retrieval strategies: similarity search, hybrid search, and re-ranking
- Construct augmented prompts that ground LLM responses in retrieved context
- Evaluate RAG quality using relevance, faithfulness, and answer correctness metrics
- Apply production RAG patterns including guardrails and fallback strategies

## Conceptual Overview

### What Is RAG?

Retrieval-Augmented Generation (RAG) is a pattern that combines information retrieval
with text generation. Instead of relying solely on what the LLM "knows" from training,
RAG retrieves relevant documents from your data and includes them in the prompt.

```
Without RAG:
  User Question --> LLM --> Answer (may hallucinate if data is not in training)

With RAG:
  User Question --> Retriever --> Relevant Documents
                                        |
                                        v
  User Question + Documents --> LLM --> Grounded Answer
```

### Why RAG?

| Problem | RAG Solution |
|---------|-------------|
| LLM does not know your private data | Retrieves from your knowledge base |
| Training data has a cutoff date | Retrieves from up-to-date documents |
| LLM may hallucinate facts | Grounds answers in actual documents |
| Fine-tuning is expensive | No model training needed |
| Data changes frequently | Vector index stays synced with Delta |

### RAG Architecture on Databricks

```
+----------------------------------------------------------------+
|                     RAG Pipeline                                |
|                                                                 |
|  INGESTION PHASE                                                |
|  +----------+    +---------+    +----------+    +------------+  |
|  | Raw Docs |    | Chunk   |    | Embed    |    | Vector     |  |
|  | (PDF,    |--->| (split  |--->| (BGE /   |--->| Search     |  |
|  |  HTML,   |    |  into   |    |  GTE)    |    | Index      |  |
|  |  TXT)    |    |  parts) |    +----------+    +------------+  |
|  +----------+    +---------+          ^               |         |
|                                       |               |         |
|  QUERY PHASE                          |               v         |
|  +----------+    +----------+    +---------+    +----------+    |
|  | User     |--->| Embed    |--->| Search  |--->| Top-K    |    |
|  | Question |    | Query    |    | Index   |    | Results  |    |
|  +----------+    +----------+    +---------+    +----------+    |
|                                                      |          |
|                                                      v          |
|  +----------+    +----------+    +-------------------+          |
|  | LLM      |<---| Augmented|<---| Build Prompt with|          |
|  | Response  |    | Prompt   |    | Context + Query  |          |
|  +----------+    +----------+    +-------------------+          |
+----------------------------------------------------------------+
```

## Document Processing Pipeline

### Step 1: Document Loading

Documents come from various sources. Databricks supports loading from:
- Unity Catalog volumes (files stored in cloud storage)
- Delta tables (structured or semi-structured text)
- External storage (S3, ADLS, GCS) via Spark

### Step 2: Chunking

Long documents must be split into smaller chunks that fit within the LLM's context
window and provide focused, retrievable units of information.

#### Chunking Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| Fixed-size | Split every N characters | Simple, fast |
| Sentence-based | Split on sentence boundaries | Readable chunks |
| Paragraph-based | Split on paragraph breaks | Structured documents |
| Recursive | Split hierarchically (section > paragraph > sentence) | Complex documents |
| Semantic | Split where topic changes | Varied-length documents |

#### Key Parameters

- **Chunk size**: 500-1000 tokens is typical. Too small = lost context. Too large = diluted relevance.
- **Overlap**: 50-200 tokens of overlap between chunks prevents cutting off important context.

```
Document:  [AAAA BBBB CCCC DDDD EEEE FFFF GGGG HHHH]

Fixed-size (chunk=4, overlap=1):
  Chunk 1: [AAAA BBBB CCCC DDDD]
  Chunk 2: [DDDD EEEE FFFF GGGG]
  Chunk 3: [GGGG HHHH]
```

### Step 3: Embedding

Each chunk is converted to a vector using an embedding model. On Databricks,
this is handled automatically by the Delta Sync Index or manually via the
embedding model endpoint.

### Step 4: Indexing

Embeddings are stored in a Vector Search Index for fast similarity lookup.
Delta Sync Index keeps the index synchronized as your source data changes.

## Retrieval Strategies

### Similarity Search

The baseline approach: embed the query and find the most similar chunks.

```python
results = index.query(
    query_text="What is the refund policy?",
    num_results=5
)
```

### Hybrid Search

Combines semantic similarity with keyword matching (BM25). Useful when exact
terms matter (product IDs, error codes, proper nouns).

```
Final Score = alpha * semantic_score + (1 - alpha) * keyword_score
```

### Re-Ranking

A two-stage approach:
1. Retrieve a larger set (e.g., top 20) using fast vector search
2. Re-rank using a more powerful model that scores query-document relevance

```
Query --> Vector Search (top 20) --> Re-Ranker Model --> Top 5
```

### Multi-Query Retrieval

Generate multiple query variations to improve recall:
1. Original: "What is the return policy?"
2. Variation: "How do I return a product?"
3. Variation: "What are the rules for refunds?"
4. Union the results and de-duplicate

## Prompt Engineering for RAG

### The Augmented Prompt Template

```
System: You are a helpful assistant. Answer questions based ONLY on the
provided context. If the context does not contain the answer, say
"I don't have enough information to answer this question."

Context:
---
{retrieved_chunk_1}
---
{retrieved_chunk_2}
---
{retrieved_chunk_3}

Question: {user_question}

Answer:
```

### Prompt Design Principles

1. **Instruct grounding** -- tell the LLM to use only the provided context
2. **Include source metadata** -- add document titles and dates for attribution
3. **Set boundaries** -- explicitly say what to do when context is insufficient
4. **Control format** -- specify output format (bullet points, JSON, etc.)

## Evaluation Metrics

### Component-Level Metrics

| Metric | Measures | How |
|--------|----------|-----|
| Context Relevance | Are retrieved chunks relevant to the query? | Score each chunk against the query |
| Context Recall | Does the retrieved context contain the answer? | Compare context to ground truth |
| Faithfulness | Does the answer stick to the retrieved context? | Check for unsupported claims |
| Answer Correctness | Is the answer factually correct? | Compare to ground truth answer |

### Evaluation Framework

```
                     +---> Context Relevance (retrieval quality)
                     |
Query + Context ---> +---> Faithfulness (no hallucination)
                     |
                     +---> Answer Correctness (factual accuracy)
```

### Automated Evaluation with LLM-as-Judge

Use an LLM to evaluate RAG output quality:

```python
evaluation_prompt = """
Rate the following answer on a scale of 1-5 for:
1. Relevance: Does it address the question?
2. Faithfulness: Does it only use information from the context?
3. Completeness: Does it fully answer the question?

Context: {context}
Question: {question}
Answer: {answer}

Provide ratings as JSON: {"relevance": N, "faithfulness": N, "completeness": N}
"""
```

## Production RAG Patterns

### Pattern 1: Guardrails

```
User Query --> Input Filter --> Retriever --> LLM --> Output Filter --> Response
                (block PII,      |                     (block harmful
                 off-topic)      |                      content)
                                 v
                          Fallback if no
                          relevant docs found
```

### Pattern 2: Citation and Attribution

Include source references in the LLM output so users can verify answers.

### Pattern 3: Caching

Cache frequent queries and their answers in a Delta table to reduce LLM calls.

### Pattern 4: Feedback Loop

Collect user feedback (thumbs up/down) and use it to improve retrieval
and prompt quality over time.

## Key Takeaways

1. RAG grounds LLM responses in your actual data, reducing hallucination
2. Chunking strategy significantly impacts retrieval quality -- experiment with sizes
3. Overlap between chunks prevents losing context at boundaries
4. Hybrid search outperforms pure semantic search when exact terms matter
5. Always include grounding instructions in the system prompt
6. Evaluate RAG pipelines on relevance, faithfulness, and correctness
7. Production RAG needs guardrails, caching, and feedback loops

## Practice Exercises

1. Design a chunking strategy for a collection of 50-page PDF policy documents
2. Write a RAG prompt template that includes citation instructions
3. Create an evaluation rubric for a customer-support RAG system

## Next Topic

[Topic 04: AI Functions](04-ai-functions.md)
