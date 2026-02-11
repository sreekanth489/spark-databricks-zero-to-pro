# Module 22 -- Agentic AI on Databricks

> Build, deploy, and evaluate AI agents using the Databricks Mosaic AI Agent
> Framework -- from single-agent tool calling to multi-agent orchestration.

---

## Why This Module Matters

AI agents represent the next evolution beyond simple prompt-response LLM
interactions. An agent does not just generate text -- it reasons about a goal,
selects tools to accomplish it, executes actions, observes results, and iterates
until the task is complete. Databricks provides a first-class platform for
building agents through the Mosaic AI Agent Framework, which integrates with
Unity Catalog for secure tool access, MLflow for experiment tracking, and Model
Serving for production deployment.

Whether you are a data engineer building agents that query your lakehouse, a
data scientist creating RAG-powered assistants, or an ML engineer designing
multi-agent workflows, this module gives you the architectural foundations and
practical patterns you need.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| **Modules 00-09 completed** | Core Spark, Delta Lake, and Databricks knowledge |
| **Module 20 completed** | MLflow tracking, model registry, and model serving foundations |
| **Module 21 completed** | GenAI and LLM fundamentals (embeddings, prompt engineering, RAG basics) |
| **Full Databricks workspace with AI/ML capabilities** | Agent Framework, Vector Search, and Model Serving require a full workspace -- Community Edition does not support these features |
| **Familiarity with LLM concepts** | Understanding of tokens, context windows, system prompts, and tool/function calling |

> **Important**: Agentic AI features are cutting-edge and require a Databricks
> workspace with Mosaic AI capabilities enabled. The notebooks in this module
> provide architectural understanding and configuration patterns. Where actual
> API calls require a full workspace, simulated demonstrations show the expected
> behavior and output formats so you can follow along regardless of your
> environment.

---

## Table of Contents

| # | Topic | Guide | Notebook | Time | Level |
|---|-------|-------|----------|------|-------|
| 01 | Agent Framework | [Guide](01-agent-framework.md) | [Notebook](01-agent-framework_notebook.py) | 60 min | Advanced |
| 02 | Tool Calling | [Guide](02-tool-calling.md) | [Notebook](02-tool-calling_notebook.py) | 55 min | Advanced |
| 03 | RAG Agents | [Guide](03-rag-agents.md) | [Notebook](03-rag-agents_notebook.py) | 65 min | Advanced |
| 04 | Agent Evaluation | [Guide](04-agent-evaluation.md) | [Notebook](04-agent-evaluation_notebook.py) | 55 min | Advanced |
| 05 | Multi-Agent Systems | [Guide](05-multi-agent-systems.md) | [Notebook](05-multi-agent-systems_notebook.py) | 60 min | Advanced |

**Total estimated time: ~5 hours**

---

## Learning Path

```
  Module 22 Learning Flow
  ========================

  01-Agent Framework
    |
    |  Understand what AI agents are and how they differ from
    |  simple LLM calls. Learn the ChatAgent interface, agent
    |  types, and deployment patterns on Databricks.
    |
    v
  02-Tool Calling
    |
    |  Master how agents interact with external systems through
    |  tools. Define UC functions, SQL tools, Python tools, and
    |  retriever tools. Understand the tool execution lifecycle.
    |
    v
  03-RAG Agents
    |
    |  Combine retrieval-augmented generation with the agent
    |  framework. Build document pipelines, vector search
    |  indexes, and deploy RAG agents with conversation memory.
    |
    v
  04-Agent Evaluation
    |
    |  Evaluate agent quality with Mosaic AI Agent Evaluation.
    |  Build evaluation datasets, measure correctness, relevance,
    |  groundedness, and safety. Track results with MLflow.
    |
    v
  05-Multi-Agent Systems
    |
    |  Design systems where multiple specialized agents collaborate.
    |  Learn supervisor, debate, and collaboration architectures.
    |  Orchestrate complex workflows across agent teams.
```

---

## Key Concepts at a Glance

- **AI Agents** -- Autonomous systems that use LLMs as a reasoning engine to
  decide which actions to take, execute those actions through tools, observe
  results, and iterate until a goal is achieved. Unlike a simple chatbot, an
  agent maintains state, plans multi-step workflows, and adapts its strategy
  based on intermediate results.

- **Mosaic AI Agent Framework** -- Databricks' integrated platform for building,
  deploying, and monitoring AI agents. Provides the `ChatAgent` interface, tool
  integration via Unity Catalog, conversation memory, and production serving
  endpoints with built-in observability.

- **Tool Calling** -- The mechanism by which an agent invokes external functions
  (SQL queries, Python code, API calls, retriever lookups). Tools are defined
  with schemas that describe their inputs/outputs so the LLM can decide which
  tool to call and how to invoke it.

- **RAG Agents** -- Agents that combine retrieval-augmented generation with tool
  use. A vector search index becomes a retriever tool that the agent can query
  to ground its responses in your organization's documents and data.

- **Agent Evaluation** -- Systematic assessment of agent behavior across
  dimensions like correctness, relevance, groundedness, and safety. Unlike
  traditional ML metrics, agent evaluation must account for non-deterministic
  behavior and multi-step reasoning chains.

- **Multi-Agent Systems** -- Architectures where multiple specialized agents
  collaborate to solve complex problems. A supervisor agent delegates subtasks,
  specialist agents execute them, and results are aggregated into a final
  response.

---

## Important Notes

1. **Cutting-edge features** -- Agentic AI on Databricks is a rapidly evolving
   space. The notebooks focus on architectural patterns and configuration
   templates that remain durable even as specific APIs evolve.

2. **Self-contained notebooks** -- Every notebook generates its own sample data
   and simulates agent behavior where actual API calls require a full workspace.
   No external datasets or API keys are required to run the demonstrations.

3. **Simulated vs. live** -- Each notebook clearly marks sections that simulate
   agent behavior (runnable anywhere) versus sections that show actual Databricks
   API patterns (require a full workspace). Both are valuable for learning.

4. **Unity Catalog integration** -- The Agent Framework leverages Unity Catalog
   for tool governance, ensuring agents can only access tools and data they are
   authorized to use. This is a key differentiator from open-source agent
   frameworks.

5. **Production readiness** -- Agents deployed through the Agent Framework
   benefit from Databricks Model Serving infrastructure: auto-scaling, latency
   monitoring, token tracking, and A/B testing.

---

## Next Steps

After completing this module:
- Revisit **Module 10** to design real-world projects that incorporate agentic
  AI patterns into data engineering pipelines
- Explore the Databricks documentation for the latest Agent Framework API
  updates and new tool types
- Experiment with building custom agents on your own data using the patterns
  learned in this module
