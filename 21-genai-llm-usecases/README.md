# Module 21: GenAI & LLM Use Cases on Databricks

> Leverage large language models and generative AI directly within your Databricks Lakehouse
> for data enrichment, retrieval-augmented generation, and production LLM deployments.

## Prerequisites

- Completed Module 20 (ML & AI Foundations) or equivalent understanding of MLflow and model serving
- A Databricks workspace (many features require a full workspace -- not Community Edition)
- Familiarity with Delta Lake, Spark SQL, and Unity Catalog basics

## Important Note

GenAI features on Databricks (Foundation Model APIs, Vector Search, AI Functions) require a
full Databricks workspace with appropriate entitlements. The notebooks in this module provide:

1. **Architecture explanations and diagrams** that work anywhere
2. **SQL and Python API templates** you can copy into a real workspace
3. **Simulated outputs** where actual LLM APIs are unavailable
4. **Working data-preparation code** that runs on any Spark cluster

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 01 | Foundation Models on Databricks | [Guide](01-foundation-models.md) | [Notebook](01-foundation-models_notebook.py) | 50 min |
| 02 | Vector Search | [Guide](02-vector-search.md) | [Notebook](02-vector-search_notebook.py) | 55 min |
| 03 | RAG Pipelines | [Guide](03-rag-pipelines.md) | [Notebook](03-rag-pipelines_notebook.py) | 60 min |
| 04 | AI Functions | [Guide](04-ai-functions.md) | [Notebook](04-ai-functions_notebook.py) | 50 min |
| 05 | Fine-Tuning | [Guide](05-fine-tuning.md) | [Notebook](05-fine-tuning_notebook.py) | 55 min |
| 06 | LLMOps | [Guide](06-llmops.md) | [Notebook](06-llmops_notebook.py) | 60 min |

**Total estimated time: ~5.5 hours**

## Learning Path

```
01 Foundation Models ──> 02 Vector Search ──> 03 RAG Pipelines
                                                     |
                                                     v
           06 LLMOps <── 05 Fine-Tuning <── 04 AI Functions
```

Work through the topics in order. Foundation Models introduces the serving layer that
every subsequent topic builds upon. Vector Search and RAG form a natural pair. AI Functions
show how to embed LLM calls directly into SQL pipelines. Fine-Tuning covers model
customization, and LLMOps ties everything together with production lifecycle management.

## How to Use This Module

1. **Read the guide** (.md file) for each topic to understand the concepts and architecture.
2. **Import the notebook** (.py file) into your Databricks workspace.
3. **Run each cell** in order -- data-preparation cells work on any cluster; API cells
   include templates and simulated outputs for environments without Foundation Model access.
4. **Adapt the templates** to your own data when you have access to a full workspace.

## What You Will Be Able to Do After This Module

- Call foundation models through Databricks APIs using both SQL and Python
- Create and manage vector search indexes backed by Delta tables
- Build end-to-end RAG pipelines that ground LLM responses in your enterprise data
- Use AI Functions in SQL for sentiment analysis, classification, extraction, and translation
- Prepare training data and configure fine-tuning jobs for domain-specific models
- Implement LLMOps practices: prompt versioning, A/B testing, cost tracking, and monitoring

## Next Module

[Module 22: Agentic AI on Databricks](../22-agentic-ai-databricks/README.md)
