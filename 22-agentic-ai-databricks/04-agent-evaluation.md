# Agent Evaluation
> Module 22 -- Topic 04 | Level: Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain why agent evaluation is fundamentally different from traditional ML evaluation
2. Describe the Mosaic AI Agent Evaluation framework and its components
3. Define evaluation metrics: correctness, relevance, groundedness, and safety
4. Build evaluation datasets with expected answers and tool call annotations
5. Run evaluations using the MLflow evaluation API
6. Interpret evaluation results and identify areas for improvement
7. Set up continuous evaluation for production agents

---

## Conceptual Overview

### Why Agent Evaluation Is Hard

Traditional ML evaluation is straightforward: compare predictions to ground
truth labels using metrics like accuracy, precision, and recall. Agent
evaluation is fundamentally more complex because:

1. **Non-deterministic behavior** -- The same question can produce different
   responses across runs due to LLM temperature, tool result variations, or
   different reasoning paths.

2. **Multi-step processes** -- An agent might take 3 steps to answer one
   question and 7 steps for another. Evaluating intermediate steps matters,
   not just the final answer.

3. **Tool interaction quality** -- Did the agent choose the right tool? Did it
   pass correct arguments? Did it interpret the tool results correctly?

4. **Subjective quality** -- "Good" responses are harder to define. A response
   can be factually correct but poorly written, or well-written but missing
   important context.

5. **Safety and guardrails** -- Agents can take real actions. Evaluation must
   verify the agent stays within safety boundaries.

```
  Traditional ML vs. Agent Evaluation
  =====================================

  Traditional ML:
  +-------+     +-------+     +---------+     +--------+
  | Input | --> | Model | --> | Output  | --> | Compare|
  +-------+     +-------+     +---------+     | to     |
                                              | label  |
                                              +--------+
  Metric: accuracy = correct / total

  Agent Evaluation:
  +-------+     +-------+     +------+     +------+     +------+
  | Input | --> | Agent | --> | Tool | --> | Tool | --> | Final|
  +-------+     +-------+     | Call |     | Call |     | Ans  |
                    |         +------+     +------+     +------+
                    |             |            |            |
                    v             v            v            v
                Reasoning    Tool Choice   Interpretation  Quality
                Quality      Accuracy      Correctness     Metrics

  Multiple dimensions to evaluate at every step.
```

### The Evaluation Framework

Mosaic AI Agent Evaluation provides a structured approach:

```
  Agent Evaluation Pipeline
  ==========================

  +------------------+     +-------------------+     +------------------+
  | Evaluation       | --> | Run Agent on      | --> | Compute Metrics  |
  | Dataset          |     | Each Example      |     | Per Example      |
  | (questions +     |     | (capture traces)  |     | (judge models)   |
  | expected answers)|     |                   |     |                  |
  +------------------+     +-------------------+     +------------------+
                                                            |
  +------------------+     +-------------------+            |
  | Track in MLflow  | <-- | Aggregate Results | <----------+
  | (compare runs)   |     | (pass/fail rates) |
  +------------------+     +-------------------+
```

---

## Evaluation Metrics

### 1. Correctness

Does the agent's response contain the right information?

- **Factual correctness** -- Are stated facts accurate?
- **Completeness** -- Does the response cover all aspects of the question?
- **Comparison to expected answer** -- How closely does it match the ground truth?

```
  Correctness Assessment
  =======================
  Question: "What is the return window for electronics?"
  Expected: "Electronics have a 15-day return window."

  Response A: "Electronics can be returned within 15 days."
  Score: 1.0 (correct and complete)

  Response B: "Our return policy allows returns within 30 days."
  Score: 0.3 (technically true for general products, but wrong for electronics)

  Response C: "Electronics have a 15-day return window with original packaging."
  Score: 0.9 (correct, includes extra accurate detail)
```

### 2. Relevance

Does the response actually answer what was asked?

- **On-topic** -- Is the response about the right subject?
- **Directly addresses the question** -- Does it answer the specific question,
  not a related but different one?
- **Conciseness** -- Does it include unnecessary information?

### 3. Groundedness

Is the response supported by the retrieved context (for RAG agents)?

- **Supported claims** -- Every factual claim should be traceable to a source
- **No hallucination** -- The agent should not invent facts not in the context
- **Citation accuracy** -- If the agent cites sources, are the citations correct?

```
  Groundedness Assessment
  ========================
  Retrieved context: "The Laptop Pro X1 has a 14-inch 2K display and 12-hour battery."

  Grounded response: "The Laptop Pro X1 has a 14-inch 2K display with 12 hours of battery life."
  Score: 1.0 (every claim is in the context)

  Hallucinated response: "The Laptop Pro X1 has a 14-inch 4K OLED display with 15 hours of battery."
  Score: 0.2 (display resolution and battery life are fabricated)
```

### 4. Safety

Does the agent operate within defined boundaries?

- **Toxicity** -- No harmful, offensive, or inappropriate content
- **Topic boundaries** -- Does the agent stay within its intended domain?
- **Action safety** -- For agents that take actions, are actions appropriate?
- **Data privacy** -- Does the agent avoid exposing sensitive information?

---

## Building Evaluation Datasets

### Structure of an Evaluation Dataset

An evaluation dataset contains examples that test different aspects of agent
behavior:

```python
evaluation_dataset = [
    {
        "request": "What is the return policy for electronics?",
        "expected_response": "Electronics have a 15-day return window.",
        "expected_retrieved_context": [
            {"doc_id": "DOC-001", "content": "Electronics have a 15-day return window..."}
        ],
        "expected_tools": ["vector_search_retriever"],
        "category": "policy_question",
        "difficulty": "easy",
    },
    {
        "request": "Compare the Laptop Pro X1 and Tablet Ultra S8 prices.",
        "expected_response": "The Laptop Pro X1 starts at $1,299 and the Tablet Ultra S8 at $699.",
        "expected_retrieved_context": [
            {"doc_id": "DOC-003", "content": "Starting price is $1,299..."},
            {"doc_id": "DOC-007", "content": "Starting price is $699..."},
        ],
        "expected_tools": ["vector_search_retriever"],
        "category": "comparison_question",
        "difficulty": "medium",
    },
]
```

### Dataset Best Practices

| Practice | Why |
|----------|-----|
| Include 50-200+ examples | Statistical significance for metric computation |
| Cover all tool types | Ensure each tool is tested |
| Include edge cases | Questions the agent should NOT answer |
| Include adversarial examples | Attempts to bypass guardrails |
| Vary difficulty levels | Easy, medium, and hard questions |
| Include multi-turn conversations | Test context-dependent questions |
| Update regularly | As the knowledge base changes, so should evaluations |

---

## Running Evaluations

### Using MLflow Evaluate

```python
import mlflow

# Define the evaluation dataset
eval_data = pd.DataFrame({
    "request": [...],
    "expected_response": [...],
})

# Run evaluation
results = mlflow.evaluate(
    model="models:/catalog.schema.my_agent/1",
    data=eval_data,
    model_type="databricks-agent",
    evaluator_config={
        "databricks-agent": {
            "metrics": [
                "correctness",
                "relevance",
                "groundedness",
                "safety",
            ],
        },
    },
)

# Access results
print(results.metrics)       # Aggregate metrics
print(results.tables)        # Per-example results
```

### LLM-as-Judge

Agent evaluation often uses another LLM to judge the quality of responses.
This is called "LLM-as-Judge":

```
  LLM-as-Judge Flow
  ===================

  +------------+     +-----------+     +----------+
  | Question   | --> |           | --> | Quality  |
  | + Response |     | Judge LLM |     | Score    |
  | + Expected |     |           |     | + Reason |
  +------------+     +-----------+     +----------+

  The judge LLM evaluates each response against criteria:
  - "Is this response factually correct given the expected answer?"
  - "Is this response relevant to the question?"
  - "Is this response grounded in the provided context?"

  Returns a score (e.g., 1-5) and a written justification.
```

---

## Interpreting Results

### Aggregate Metrics Dashboard

```
  Evaluation Results Summary
  ===========================

  Metric          Score    Threshold    Status
  ----------      -----    ---------    ------
  Correctness     0.87     0.80         PASS
  Relevance       0.92     0.85         PASS
  Groundedness    0.78     0.80         FAIL
  Safety          0.99     0.95         PASS

  Action items:
  - Groundedness is below threshold. Review hallucination patterns.
  - 12% of responses include unsupported claims.
  - Most common issue: agent extrapolates beyond retrieved context.
```

### Common Failure Patterns

| Pattern | Symptom | Fix |
|---------|---------|-----|
| Retrieval miss | Agent answers from wrong documents | Improve chunking or embeddings |
| Hallucination | Agent invents facts not in context | Add groundedness guardrail in prompt |
| Tool misuse | Agent calls wrong tool or wrong args | Improve tool descriptions |
| Over-retrieval | Agent always retrieves even for greetings | Add retrieval decision logic |
| Context overflow | Too much context confuses the LLM | Reduce num_results or chunk size |
| Safety violation | Agent discusses off-limits topics | Strengthen system prompt guardrails |

---

## Continuous Evaluation in Production

### Monitoring Live Agent Behavior

```
  Production Evaluation Loop
  ============================

  Live Traffic --> Sample Requests --> Evaluate Offline
       |                                    |
       v                                    v
  Serve Responses              Compare to Baselines
       |                                    |
       v                                    v
  Log to MLflow                Alert on Regressions
       |                                    |
       v                                    v
  Human Review Queue           Update Agent / Retrain
```

Key practices for continuous evaluation:
- Sample 5-10% of production traffic for evaluation
- Run daily evaluation against a fixed benchmark dataset
- Track metric trends over time (detect gradual degradation)
- Set up alerts when metrics drop below thresholds
- Maintain a human review queue for edge cases

---

## Key Takeaways

1. Agent evaluation is multi-dimensional: correctness, relevance, groundedness, safety
2. Non-deterministic behavior requires statistical evaluation over many examples
3. Evaluation datasets should be comprehensive: 50-200+ examples covering all scenarios
4. LLM-as-Judge is the primary mechanism for scoring response quality
5. MLflow integration enables tracking and comparing evaluation runs over time
6. Continuous evaluation in production catches regressions before users notice

---

## Practice Exercises

1. Build an evaluation dataset with 20 examples for an HR policy RAG agent.
   Include easy questions, multi-step questions, and adversarial examples.
2. For each evaluation metric (correctness, relevance, groundedness, safety),
   write a rubric that a judge LLM would use to score responses on a 1-5 scale.
3. Design a monitoring dashboard for a production RAG agent. What metrics would
   you track, what thresholds would you set, and what alerts would you create?
