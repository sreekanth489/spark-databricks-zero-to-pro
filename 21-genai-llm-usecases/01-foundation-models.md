# Foundation Models on Databricks

> Module 21 -- Topic 01 | Level: Intermediate | Time: 50 min

## Learning Objectives

- Explain what Foundation Model APIs provide and how they differ from self-hosted models
- Choose between pay-per-token and provisioned throughput serving modes
- Call models using both the `ai_query()` SQL function and the Python SDK
- Design prompt templates for common data-engineering tasks
- Estimate token usage and manage costs for production workloads

## Conceptual Overview

### What Are Foundation Models?

Foundation models are large neural networks pre-trained on massive text corpora. They can
generate text, answer questions, summarize documents, classify content, and extract structured
data from unstructured text -- all without task-specific training.

Databricks provides access to foundation models through its **Foundation Model APIs**, which
let you call open-source and proprietary models as a managed service. You do not need to
provision GPUs, manage model weights, or handle inference infrastructure.

### Supported Models

```
+-----------------------------------------------+
|          Foundation Model APIs                 |
|                                                |
|   Open Source          Proprietary             |
|   +-----------+        +-----------+           |
|   | DBRX      |        | GPT-4o    |           |
|   | Llama 3.1 |        | Claude    |           |
|   | Mixtral   |        | (via      |           |
|   | MPT       |        |  external |           |
|   | BGE (emb) |        |  models)  |           |
|   +-----------+        +-----------+           |
|                                                |
|   Serving Modes:                               |
|   [Pay-per-token]  [Provisioned Throughput]    |
+-----------------------------------------------+
```

**DBRX** -- Databricks' own open model, optimized for enterprise tasks.
**Llama 3.1** -- Meta's open-weight model family (8B, 70B, 405B parameters).
**Mixtral** -- Mistral AI's mixture-of-experts architecture.
**MPT** -- MosaicML (Databricks) pre-trained transformer family.
**BGE** -- BAAI embedding models for vector search and RAG.

### Serving Modes

#### Pay-per-Token

Best for development, experimentation, and bursty workloads.

- No upfront cost -- you pay only for tokens processed
- Shared infrastructure managed by Databricks
- Higher latency variability under load
- Rate limits apply

#### Provisioned Throughput

Best for production workloads with predictable traffic.

- Dedicated compute reserved for your endpoint
- Guaranteed throughput (tokens per second)
- Lower and more consistent latency
- Fixed cost regardless of usage volume

### Architecture: How a Model Call Flows

```
Your Code (SQL / Python / REST)
        |
        v
+------------------+
| Model Serving    |    <-- Managed by Databricks
| Endpoint         |
+------------------+
        |
        v
+------------------+
| Foundation Model |    <-- DBRX, Llama, Mixtral, etc.
| (GPU Cluster)    |
+------------------+
        |
        v
  Response (JSON)
        |
        v
  Your Pipeline
```

## Calling Models with SQL: ai_query()

The `ai_query()` function lets you call LLMs directly from SQL queries. This is the
simplest way to integrate LLMs into data pipelines.

### Syntax

```sql
SELECT ai_query(
  'databricks-dbrx-instruct',           -- model endpoint name
  'Summarize this text: ' || content,    -- prompt (can reference columns)
  returnType => 'STRING'                 -- expected return type
) AS summary
FROM articles
```

### Key Parameters

| Parameter | Description |
|-----------|-------------|
| `endpoint` | Name of the serving endpoint (string) |
| `request` | The prompt text or structured input |
| `returnType` | Expected output type: `STRING`, `STRUCT`, `ARRAY` |
| `modelParameters` | JSON with `temperature`, `max_tokens`, `top_p` |

### Structured Output

```sql
SELECT ai_query(
  'databricks-dbrx-instruct',
  'Extract the person name and city from: ' || text,
  returnType => 'STRUCT<name: STRING, city: STRING>'
) AS extracted
FROM documents
```

## Calling Models with Python SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="databricks-dbrx-instruct",
    messages=[
        {"role": "system", "content": "You are a helpful data analyst."},
        {"role": "user", "content": "Explain partitioning in Delta Lake."}
    ],
    max_tokens=256,
    temperature=0.1
)

print(response.choices[0].message.content)
```

### OpenAI-Compatible Interface

Databricks endpoints are OpenAI-compatible, so you can also use the `openai` library:

```python
import openai

client = openai.OpenAI(
    api_key=dbutils.secrets.get("scope", "token"),
    base_url="https://<workspace>.databricks.com/serving-endpoints"
)

response = client.chat.completions.create(
    model="databricks-dbrx-instruct",
    messages=[{"role": "user", "content": "Hello!"}],
    max_tokens=100
)
```

## Prompt Templates for Data Engineering

### Summarization

```
Summarize the following customer review in one sentence.
Focus on the main complaint or praise.

Review: {review_text}
```

### Classification

```
Classify the following support ticket into exactly one category:
[Billing, Technical, Account, Feature Request, Other]

Respond with only the category name.

Ticket: {ticket_text}
```

### Entity Extraction

```
Extract all product names and their prices from the text below.
Return the result as JSON: [{"product": "...", "price": "..."}]

Text: {document_text}
```

## Token Management and Cost Considerations

### Understanding Tokens

A token is roughly 4 characters or 0.75 words in English. Both input (prompt) and
output (completion) tokens are billed.

```
Approximate token counts:
  "Hello, world!"          -->  4 tokens
  A 500-word article       -->  ~670 tokens
  A full SQL CREATE TABLE  -->  ~200 tokens
```

### Cost Optimization Strategies

1. **Keep prompts concise** -- remove unnecessary instructions and examples
2. **Use smaller models first** -- try DBRX or Llama-8B before Llama-405B
3. **Cache repeated queries** -- store LLM results in Delta tables
4. **Batch requests** -- process multiple rows per API call when possible
5. **Set max_tokens** -- always cap output length to prevent runaway costs
6. **Use temperature=0** -- for deterministic tasks, lower temperature reduces retries

### Monitoring Usage

```sql
-- Query the system billing table for model serving costs
SELECT
  date,
  endpoint_name,
  SUM(total_tokens) AS tokens_used,
  SUM(cost)         AS total_cost
FROM system.billing.usage
WHERE usage_type = 'MODEL_SERVING'
GROUP BY date, endpoint_name
ORDER BY date DESC
```

## Model Comparison Framework

When choosing a model for your use case, evaluate across these dimensions:

| Dimension | Small (8B) | Medium (70B) | Large (405B) |
|-----------|-----------|-------------|-------------|
| Latency | Low | Medium | High |
| Cost per 1M tokens | $ | $$$ | $$$$$ |
| Simple classification | Good | Great | Great |
| Complex reasoning | Fair | Good | Great |
| Code generation | Fair | Good | Great |
| Summarization | Good | Great | Great |

**Rule of thumb:** Start with the smallest model that meets your quality bar, then scale
up only if accuracy is insufficient.

## Key Takeaways

1. Foundation Model APIs remove the need to manage GPU infrastructure for LLM inference
2. `ai_query()` in SQL is the fastest path to LLM-powered data enrichment
3. Pay-per-token suits development; provisioned throughput suits production
4. Always set `max_tokens` and `temperature` to control cost and determinism
5. Start with smaller models and scale up based on measured quality
6. Cache LLM outputs in Delta tables to avoid repeated computation

## Practice Exercises

1. Write an `ai_query()` call that classifies customer reviews as Positive, Negative, or Neutral
2. Design a prompt template that extracts dates and amounts from invoice text
3. Calculate the token cost for processing a Delta table with 1 million rows where
   each row's prompt is ~200 tokens and expected output is ~50 tokens

## Next Topic

[Topic 02: Vector Search](02-vector-search.md)
