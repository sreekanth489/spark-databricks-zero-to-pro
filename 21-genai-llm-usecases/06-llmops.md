# LLMOps on Databricks

> Module 21 -- Topic 06 | Level: Advanced | Time: 60 min

## Learning Objectives

- Define LLMOps and explain how it extends traditional MLOps for LLM-specific concerns
- Track prompts, model configurations, and LLM outputs using MLflow
- Implement prompt versioning and management strategies
- Design A/B testing experiments for prompt and model comparison
- Monitor LLM deployments for quality, latency, cost, and hallucination
- Apply guardrails and safety measures for production LLM systems

## Conceptual Overview

### What Is LLMOps?

LLMOps is the set of practices for managing the lifecycle of large language model
applications in production. It extends MLOps with LLM-specific concerns.

```
Traditional MLOps:
  Data --> Train --> Evaluate --> Deploy --> Monitor

LLMOps adds:
  Prompts --> Version --> A/B Test --> Deploy --> Monitor (quality + cost + safety)
                |                        |
                v                        v
          Prompt Registry         Guardrails & Safety
```

### LLMOps vs MLOps

| Concern | MLOps | LLMOps |
|---------|-------|--------|
| Artifacts to track | Model weights, features | Prompts, model configs, chains |
| Evaluation | Accuracy, F1, AUC | Relevance, faithfulness, safety |
| Cost | Compute (training) | Tokens (inference), every call costs |
| Latency | Batch acceptable | Real-time expected |
| Failure modes | Wrong prediction | Hallucination, harmful content |
| Versioning | Model versions | Prompt versions, model versions |
| Testing | Unit tests, integration | Prompt regression, safety checks |

### The LLMOps Lifecycle

```
+-----------------------------------------------------------------------+
|                         LLMOps Lifecycle                              |
|                                                                        |
|  DEVELOP                                                               |
|  +------------------+    +------------------+    +-----------------+   |
|  | Prompt           |    | Model            |    | Chain/Pipeline  |   |
|  | Engineering      |--->| Selection        |--->| Assembly        |   |
|  +------------------+    +------------------+    +-----------------+   |
|          |                                              |              |
|  EVALUATE                                               v              |
|  +------------------+    +------------------+    +-----------------+   |
|  | Automated        |    | Human            |    | A/B Testing     |   |
|  | Metrics          |--->| Evaluation       |--->|                 |   |
|  +------------------+    +------------------+    +-----------------+   |
|          |                                              |              |
|  DEPLOY                                                 v              |
|  +------------------+    +------------------+    +-----------------+   |
|  | Model Serving    |    | Guardrails       |    | Endpoint        |   |
|  | Endpoint         |--->| (input/output)   |--->| Configuration   |   |
|  +------------------+    +------------------+    +-----------------+   |
|          |                                              |              |
|  MONITOR                                                v              |
|  +------------------+    +------------------+    +-----------------+   |
|  | Quality          |    | Cost & Latency   |    | Safety &        |   |
|  | Tracking         |--->| Monitoring       |--->| Compliance      |   |
|  +------------------+    +------------------+    +-----------------+   |
+-----------------------------------------------------------------------+
```

## Prompt Management with MLflow

### Why Version Prompts?

Prompts are the "code" of LLM applications. Like code, they need:
- Version control (what changed and when)
- Reproducibility (same prompt = same behavior)
- Rollback capability (revert to a known-good prompt)
- A/B testing (compare prompt variants)

### Logging Prompts with MLflow

```python
import mlflow

with mlflow.start_run(run_name="sentiment_prompt_v2"):
    # Log the prompt template
    mlflow.log_param("system_prompt",
        "Classify sentiment as Positive, Negative, or Neutral.")
    mlflow.log_param("user_template",
        "Classify: {text}")

    # Log model configuration
    mlflow.log_param("model", "databricks-dbrx-instruct")
    mlflow.log_param("temperature", 0.0)
    mlflow.log_param("max_tokens", 10)

    # Log evaluation metrics
    mlflow.log_metric("accuracy", 0.92)
    mlflow.log_metric("avg_latency_ms", 145)
    mlflow.log_metric("avg_tokens_per_call", 85)
    mlflow.log_metric("cost_per_1000_calls", 0.042)
```

### Prompt Registry Pattern

Store prompts in a Delta table for centralized management:

```sql
CREATE TABLE prompt_registry (
  prompt_id     STRING,
  prompt_name   STRING,
  version       INT,
  system_prompt STRING,
  user_template STRING,
  model         STRING,
  temperature   DOUBLE,
  max_tokens    INT,
  status        STRING,     -- 'active', 'testing', 'retired'
  created_by    STRING,
  created_at    TIMESTAMP,
  metrics       STRING      -- JSON with evaluation metrics
)
```

## A/B Testing for Prompts

### Why A/B Test?

Small changes in prompts can have large effects on output quality. A/B testing
provides statistical evidence for which prompt performs better.

### A/B Testing Architecture

```
Incoming Request
       |
       v
+-------------+
| Traffic     |     50% --> Prompt A (control)
| Splitter    |
|             |     50% --> Prompt B (challenger)
+-------------+
       |
       v
+-------------+
| Log Results |
| to Delta    |
+-------------+
       |
       v
+-------------+
| Statistical |
| Analysis    |
+-------------+
```

### Implementation Pattern

```python
import random

def ab_test_prompt(text, experiment_id):
    """Route requests between prompt variants."""
    variant = "A" if random.random() < 0.5 else "B"

    prompts = {
        "A": f"Classify the sentiment: {text}",
        "B": f"What is the sentiment (Positive/Negative/Neutral)? Text: {text}"
    }

    response = call_llm(prompts[variant])

    # Log the experiment
    log_ab_result(experiment_id, variant, text, response, metrics)

    return response
```

### Statistical Significance

Use the chi-squared test or bootstrap confidence intervals to determine
if the difference between variants is statistically significant.

```python
from scipy import stats

# Compare accuracy of variant A vs B
a_correct, a_total = 450, 500
b_correct, b_total = 470, 500

chi2, p_value = stats.chi2_contingency([
    [a_correct, a_total - a_correct],
    [b_correct, b_total - b_correct]
])[:2]

print(f"P-value: {p_value:.4f}")
print(f"Significant (p < 0.05): {p_value < 0.05}")
```

## Monitoring LLM Deployments

### Key Metrics to Track

| Category | Metric | Alert Threshold |
|----------|--------|----------------|
| Quality | Accuracy / relevance score | < 85% |
| Quality | Hallucination rate | > 5% |
| Quality | Refusal rate (false negatives) | > 10% |
| Performance | P50 latency | > 500ms |
| Performance | P99 latency | > 2000ms |
| Performance | Throughput (req/sec) | < minimum SLA |
| Cost | Tokens per request (avg) | > 2x baseline |
| Cost | Daily spend | > budget cap |
| Safety | Content filter triggers | > 1% |
| Safety | PII detected in output | > 0% |

### Monitoring Architecture

```
LLM Endpoint
     |
     v
+------------------+
| Inference Logger  |  <-- Logs every request/response
+------------------+
     |
     v
+------------------+
| Delta Table       |  <-- inference_logs
+------------------+
     |
     +---> Quality Dashboard (Databricks SQL)
     |
     +---> Cost Tracker (aggregated by hour/day)
     |
     +---> Alert Rules (Databricks Workflows)
```

### Inference Logging Schema

```sql
CREATE TABLE inference_logs (
  request_id      STRING,
  timestamp       TIMESTAMP,
  endpoint_name   STRING,
  prompt_version  STRING,
  input_text      STRING,
  output_text     STRING,
  input_tokens    INT,
  output_tokens   INT,
  latency_ms      INT,
  model           STRING,
  temperature     DOUBLE,
  user_feedback   STRING,    -- thumbs_up, thumbs_down, null
  quality_score   DOUBLE,    -- automated quality check
  flagged         BOOLEAN    -- content filter triggered
)
```

## Hallucination Detection

### Approaches

1. **Factual consistency check** -- compare LLM output against retrieved context
2. **Self-consistency** -- ask the same question multiple times and check agreement
3. **Confidence scoring** -- use log probabilities to detect low-confidence outputs
4. **Knowledge boundary** -- detect when the model is generating beyond its knowledge

### Implementation Pattern

```python
def check_hallucination(answer, context):
    """
    Use an LLM to check if the answer is supported by the context.
    Returns a hallucination score (0 = faithful, 1 = hallucinated).
    """
    check_prompt = f"""
    Context: {context}

    Answer: {answer}

    Is the answer fully supported by the context?
    Rate from 0 (fully supported) to 1 (not supported at all).
    Return only the number.
    """
    score = call_llm(check_prompt)
    return float(score)
```

## Guardrails and Safety

### Input Guardrails

- Block prompt injection attempts
- Filter PII from inputs
- Reject off-topic queries
- Enforce rate limits per user

### Output Guardrails

- Check for PII in generated text
- Filter harmful or offensive content
- Validate output format (JSON, categories, etc.)
- Enforce maximum response length

### Guardrails Architecture

```
User Input
     |
     v
+------------------+
| Input Guardrails  |  --> Block? --> Error Response
+------------------+
     |
     v
+------------------+
| LLM Inference     |
+------------------+
     |
     v
+------------------+
| Output Guardrails |  --> Flag? --> Fallback Response
+------------------+
     |
     v
User Response
```

## Key Takeaways

1. LLMOps extends MLOps with prompt versioning, token cost tracking, and safety monitoring
2. Version prompts like code -- log them in MLflow and store in a prompt registry
3. A/B test prompt changes before rolling them out to production
4. Monitor quality, latency, cost, and safety -- not just accuracy
5. Hallucination detection is an ongoing concern that requires multiple strategies
6. Input and output guardrails protect both the system and users
7. Log every inference for debugging, evaluation, and compliance

## Practice Exercises

1. Design a prompt registry schema for a multi-team organization
2. Write an A/B testing plan for comparing two summarization prompts
3. Create a monitoring dashboard specification for an LLM-powered chatbot

## Module Complete

Congratulations on completing Module 21: GenAI & LLM Use Cases on Databricks!

[Return to Module README](README.md)
