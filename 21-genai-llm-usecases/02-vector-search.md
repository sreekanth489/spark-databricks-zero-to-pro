# Vector Search on Databricks

> Module 21 -- Topic 02 | Level: Intermediate | Time: 55 min

## Learning Objectives

- Explain how text embeddings represent semantic meaning as numerical vectors
- Compare managed vs self-managed vector search endpoints on Databricks
- Distinguish Delta Sync Index from Direct Vector Access Index
- Configure a vector search index backed by a Delta table
- Write similarity search queries and interpret relevance scores

## Conceptual Overview

### What Are Embeddings?

An embedding is a fixed-length numerical vector that captures the semantic meaning of a
piece of text. Similar texts produce vectors that are close together in vector space;
dissimilar texts produce vectors that are far apart.

```
"The cat sat on the mat"    --> [0.12, -0.45, 0.78, 0.33, ...]  (384+ dimensions)
"A kitten rested on a rug"  --> [0.11, -0.43, 0.76, 0.35, ...]  <-- very close
"Stock prices fell today"   --> [0.89, 0.22, -0.56, 0.01, ...]  <-- far away
```

### Why Vector Search?

Traditional keyword search fails when users phrase queries differently from the stored text.
Vector search matches by meaning, not by exact words.

```
Query: "How do I fix a broken screen?"

Keyword search matches:
  - "Broken screen repair guide"         <-- matches "broken screen"
  - Misses: "Display replacement steps"  <-- same meaning, different words

Vector search matches:
  - "Broken screen repair guide"         <-- semantically similar
  - "Display replacement steps"          <-- also semantically similar
  - "LCD panel troubleshooting"          <-- also relevant
```

### Embedding Models on Databricks

| Model | Dimensions | Use Case |
|-------|-----------|----------|
| BGE-large-en | 1024 | English text, high quality |
| BGE-base-en | 768 | English text, balanced |
| GTE-large | 1024 | Multilingual text |
| E5-large-v2 | 1024 | General purpose |
| Instructor-XL | 768 | Instruction-tuned embeddings |

Databricks hosts these through Foundation Model APIs, so you can generate embeddings
without managing GPU infrastructure.

## Databricks Vector Search Architecture

```
+-----------------------------------------------------+
|              Databricks Workspace                    |
|                                                      |
|  +------------------+    +----------------------+    |
|  | Source Delta      |    | Vector Search        |    |
|  | Table             |--->| Endpoint             |    |
|  | (text + metadata) |    | (managed / self-mgd) |    |
|  +------------------+    +----------------------+    |
|          |                        |                  |
|          | Auto-sync              | Query            |
|          v                        v                  |
|  +------------------+    +----------------------+    |
|  | Delta Sync Index |    | Similarity Results   |    |
|  | (embeddings      |    | (ranked by distance) |    |
|  |  auto-computed)   |    +----------------------+    |
|  +------------------+                                |
+-----------------------------------------------------+
```

### Endpoint Types

#### Managed Endpoint

- Databricks provisions and manages the compute
- Automatic scaling based on query load
- Simplest setup -- recommended for most use cases
- Higher-level abstraction

#### Self-Managed Endpoint

- You control the compute resources (cluster size, GPU type)
- More control over performance tuning
- Better for very large indexes or strict latency requirements
- Requires more operational overhead

### Index Types

#### Delta Sync Index

The recommended approach. Databricks automatically:
1. Monitors your source Delta table for changes
2. Generates embeddings for new or updated rows
3. Updates the vector index incrementally

```
Source Delta Table  --[auto-sync]-->  Embedding Model  --[index]-->  Vector Index
     (text)                          (BGE/GTE/E5)                   (searchable)
```

**Key benefit:** Your vector index stays fresh as your data changes. No manual
re-indexing needed.

Configuration options:
- **Triggered sync** -- updates on demand or on a schedule
- **Continuous sync** -- updates in near real-time as the Delta table changes

#### Direct Vector Access Index

For when you compute embeddings yourself:
1. You generate embeddings using your own model or pipeline
2. You write the embedding vectors directly to the index
3. Full control over the embedding process

```
Your Pipeline  --[compute embeddings]-->  Write to Index  -->  Vector Index
                                          (via API)            (searchable)
```

**When to use:** Custom embedding models, pre-computed embeddings, or embeddings
from external systems.

## Creating a Vector Search Index

### Step 1: Create a Vector Search Endpoint

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create a managed endpoint
w.vector_search_endpoints.create_endpoint(
    name="my_vs_endpoint",
    endpoint_type="STANDARD"  # STANDARD = managed
)
```

### Step 2: Create a Delta Sync Index

```python
# Create an index that auto-syncs from a Delta table
w.vector_search_indexes.create_index(
    name="catalog.schema.my_index",
    endpoint_name="my_vs_endpoint",
    primary_key="doc_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec={
        "source_table": "catalog.schema.documents",
        "pipeline_type": "TRIGGERED",
        "embedding_source_columns": [
            {
                "name": "content",
                "embedding_model_endpoint_name": "databricks-bge-large-en"
            }
        ]
    }
)
```

### Step 3: Query the Index

```python
results = w.vector_search_indexes.query_index(
    index_name="catalog.schema.my_index",
    columns=["doc_id", "content", "source"],
    query_text="How do I reset my password?",
    num_results=5
)

for doc in results.result.data_array:
    print(f"Score: {doc[-1]:.4f}  |  {doc[1][:80]}...")
```

## Similarity Metrics

Vector search uses distance metrics to rank results.

### Cosine Similarity

Measures the angle between two vectors. Range: -1 to 1 (higher = more similar).
This is the default and most common metric.

```
cos(A, B) = (A . B) / (|A| * |B|)
```

### Euclidean Distance (L2)

Measures straight-line distance between vectors. Range: 0 to infinity (lower = more similar).

### Dot Product

Measures the projection of one vector onto another. Useful when vector magnitudes matter.

### Choosing a Metric

| Metric | When to Use |
|--------|-------------|
| Cosine | Default choice; works well for normalized embeddings |
| L2 | When magnitude matters (e.g., frequency-weighted embeddings) |
| Dot Product | When embeddings are already normalized and you need speed |

## Filtering and Metadata

Vector search supports hybrid queries that combine semantic similarity with metadata filters.

```python
results = w.vector_search_indexes.query_index(
    index_name="catalog.schema.my_index",
    columns=["doc_id", "content", "department", "date"],
    query_text="quarterly revenue performance",
    num_results=10,
    filters={"department": "Finance", "date >=": "2024-01-01"}
)
```

This returns the 10 most semantically similar documents that are also from the
Finance department and dated after January 2024.

## Index Management

### Syncing

```python
# Trigger a manual sync for a Delta Sync Index
w.vector_search_indexes.sync_index(
    index_name="catalog.schema.my_index"
)
```

### Monitoring Index Status

```python
index = w.vector_search_indexes.get_index(
    index_name="catalog.schema.my_index"
)
print(f"Status: {index.status.ready}")
print(f"Row count: {index.status.num_rows}")
```

### Deleting

```python
w.vector_search_indexes.delete_index(index_name="catalog.schema.my_index")
w.vector_search_endpoints.delete_endpoint(endpoint_name="my_vs_endpoint")
```

## Performance Considerations

1. **Embedding dimensions** -- Fewer dimensions = faster search, more dimensions = better quality
2. **Index size** -- Millions of vectors are fine; billions may need sharding
3. **Batch queries** -- Query in batches rather than one-at-a-time for throughput
4. **Filters** -- Add metadata filters to reduce the search space before vector comparison
5. **Refresh frequency** -- Continuous sync adds overhead; use triggered sync when near-real-time is not required

## Key Takeaways

1. Embeddings convert text into numerical vectors that capture semantic meaning
2. Delta Sync Index is the recommended approach -- it auto-updates as your data changes
3. Direct Vector Access Index gives full control when you compute your own embeddings
4. Cosine similarity is the default and best starting metric for most use cases
5. Metadata filters enable hybrid search combining semantic relevance with business rules
6. Vector Search is a foundational building block for RAG pipelines (next topic)

## Practice Exercises

1. Design a Delta table schema suitable for a vector search index over product descriptions
2. Write the API call to create a Delta Sync Index with continuous sync enabled
3. Explain when you would choose Direct Vector Access over Delta Sync

## Next Topic

[Topic 03: RAG Pipelines](03-rag-pipelines.md)
