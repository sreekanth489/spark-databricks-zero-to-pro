# Fine-Tuning LLMs on Databricks

> Module 21 -- Topic 05 | Level: Advanced | Time: 55 min

## Learning Objectives

- Decide when fine-tuning is appropriate versus prompt engineering or RAG
- Prepare training data in the instruction format required by Databricks
- Configure a fine-tuning job with appropriate hyperparameters
- Evaluate fine-tuned models against base models using standard metrics
- Deploy and serve fine-tuned models on Databricks
- Estimate compute costs and training time for fine-tuning jobs

## Conceptual Overview

### What Is Fine-Tuning?

Fine-tuning is the process of taking a pre-trained foundation model and training it
further on your own domain-specific data. The model learns patterns, terminology, and
behaviors unique to your use case.

```
Pre-trained Model (general knowledge)
         |
         | + Your domain data
         v
Fine-Tuned Model (general + domain knowledge)
```

### When to Fine-Tune vs Not

```
+------------------------------------------------------------------+
|                    Decision Framework                             |
|                                                                   |
|  START HERE                                                       |
|     |                                                             |
|     v                                                             |
|  Can prompt engineering solve it?                                 |
|     YES --> Use prompt engineering (cheapest, fastest)             |
|     NO  |                                                         |
|         v                                                         |
|  Does the model need access to your data?                         |
|     YES --> Use RAG (no training needed)                          |
|     NO  |                                                         |
|         v                                                         |
|  Does the model need to learn a new style/format/behavior?        |
|     YES --> Fine-tune                                             |
|     NO  |                                                         |
|         v                                                         |
|  Does the model need domain-specific knowledge baked in?          |
|     YES --> Fine-tune + RAG (most powerful, most expensive)       |
+------------------------------------------------------------------+
```

### Comparison

| Approach | Cost | Time to Deploy | Data Needed | Best For |
|----------|------|---------------|-------------|----------|
| Prompt Engineering | $ | Minutes | 0 | Simple tasks, prototyping |
| RAG | $$ | Hours | Documents (unstructured) | Knowledge-grounded answers |
| Fine-Tuning | $$$$ | Days | 100-10,000 labeled examples | Style, format, specialized tasks |
| Fine-Tuning + RAG | $$$$$ | Days | Both | Maximum quality, domain-specific |

### When Fine-Tuning IS Worth It

- The model must follow a strict output format consistently
- Domain-specific jargon or terminology that general models get wrong
- You need lower latency (fine-tuned smaller models can replace larger ones)
- Compliance requires the model to behave in a very specific way
- You have enough high-quality labeled training data (100+ examples minimum)

### When Fine-Tuning Is NOT Worth It

- You just need the model to access your data (use RAG instead)
- The task can be solved with a good prompt (use prompt engineering)
- You have fewer than 100 training examples
- The domain changes frequently (retraining is expensive)
- A larger base model already achieves acceptable quality

## Supervised Fine-Tuning on Databricks

### Architecture

```
+----------------------------------------------------------------+
|                Fine-Tuning Pipeline                              |
|                                                                  |
|  +-------------+    +----------------+    +------------------+   |
|  | Training    |    | Databricks     |    | Fine-Tuned       |   |
|  | Data        |--->| Fine-Tuning    |--->| Model            |   |
|  | (JSONL)     |    | Service        |    | (MLflow logged)  |   |
|  +-------------+    +----------------+    +------------------+   |
|        |                   |                      |              |
|        v                   v                      v              |
|  Unity Catalog       GPU Cluster            Model Serving        |
|  Volume              (managed)              Endpoint             |
+----------------------------------------------------------------+
```

### Data Preparation

Training data must be in the **instruction format** -- each example has a prompt
(instruction) and the desired completion (response).

#### Chat Format (Recommended)

```json
{"messages": [
  {"role": "system", "content": "You are a medical coding assistant."},
  {"role": "user", "content": "Code this diagnosis: chest pain on exertion"},
  {"role": "assistant", "content": "ICD-10: R07.9 - Chest pain, unspecified"}
]}
```

#### Completion Format

```json
{"prompt": "Classify this ticket: My invoice is wrong\n\nCategory:",
 "completion": " Billing"}
```

### Data Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Training examples | 100 | 500-5,000 |
| Validation split | 10% | 10-20% |
| Max tokens per example | Model-dependent | 2,048-4,096 |
| Data format | JSONL | JSONL |

### Quality Guidelines

1. **Consistency** -- all examples should follow the same format
2. **Diversity** -- cover the range of inputs the model will see in production
3. **Accuracy** -- every response should be correct (garbage in = garbage out)
4. **Balance** -- represent all categories proportionally (or use class weights)
5. **Deduplication** -- remove near-duplicate examples

## Configuring a Fine-Tuning Job

### Python API

```python
from databricks.model_training import foundation_model as fm

run = fm.create(
    model="meta-llama/Meta-Llama-3.1-8B-Instruct",
    train_data_path="dbfs:/Volumes/catalog/schema/training/train.jsonl",
    eval_data_path="dbfs:/Volumes/catalog/schema/training/eval.jsonl",
    register_to="catalog.schema.my_fine_tuned_model",
    training_duration="5ep",       # 5 epochs
    learning_rate=1e-5,
    context_length=2048,
    task_type="CHAT_COMPLETION",
)

print(f"Run name: {run.name}")
print(f"Status: {run.status}")
```

### Key Hyperparameters

| Parameter | Description | Default | Tuning Tips |
|-----------|-------------|---------|-------------|
| learning_rate | Step size for weight updates | 1e-5 | Lower for large models, higher for small |
| training_duration | Epochs or steps | 3ep | 3-10 epochs for small datasets |
| context_length | Max tokens per example | 2048 | Match your longest training example |
| batch_size | Examples per gradient update | Auto | Larger = faster but uses more memory |
| warmup_ratio | Fraction of steps for learning rate warmup | 0.1 | Keep at 0.1 unless you see instability |

### Monitoring Training

```python
# Check training status
run = fm.get(run.name)
print(f"Status: {run.status}")
print(f"Events: {run.events}")

# View training metrics in MLflow
import mlflow
mlflow.set_tracking_uri("databricks")
run_data = mlflow.get_run(run.mlflow_run_id)
print(run_data.data.metrics)
```

## Evaluation

### Metrics for Fine-Tuned Models

| Metric | What It Measures | Target |
|--------|-----------------|--------|
| Training Loss | How well the model fits training data | Decreasing |
| Validation Loss | Generalization to unseen data | Decreasing, then stable |
| Accuracy | Exact match for classification tasks | > 85% |
| ROUGE-L | Overlap with reference text (summarization) | > 0.5 |
| Perplexity | Model confidence in predictions | Lower is better |

### Evaluation Strategy

```
+------------------------------------------------------------------+
|               Evaluation Pipeline                                 |
|                                                                   |
|  Test Set (held out)                                              |
|       |                                                           |
|       +--> Base Model --> Predictions --> Compare                 |
|       |                                     |                     |
|       +--> Fine-Tuned Model --> Predictions --+                   |
|                                               |                   |
|                                        +------v------+            |
|                                        | Metrics     |            |
|                                        | - Accuracy  |            |
|                                        | - Latency   |            |
|                                        | - Cost      |            |
|                                        +-------------+            |
+------------------------------------------------------------------+
```

### Human Evaluation

For open-ended generation, automated metrics are insufficient. Use human evaluation:
1. Present base and fine-tuned outputs side by side (blind)
2. Evaluators rate on quality, relevance, and correctness
3. Compute win rate of fine-tuned vs base model

## Deploying Fine-Tuned Models

### Register in Unity Catalog

```python
import mlflow

mlflow.set_registry_uri("databricks-uc")

# The model is auto-registered during fine-tuning if register_to is set
# To register manually:
mlflow.register_model(
    model_uri=f"runs:/{run.mlflow_run_id}/model",
    name="catalog.schema.my_fine_tuned_model"
)
```

### Create a Serving Endpoint

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.serving_endpoints.create(
    name="my-fine-tuned-endpoint",
    config={
        "served_entities": [{
            "entity_name": "catalog.schema.my_fine_tuned_model",
            "entity_version": "1",
            "workload_type": "GPU_MEDIUM",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

## Cost and Compute Considerations

### Estimated Training Costs

| Model Size | GPU Type | Training Time (1K examples) | Approximate Cost |
|-----------|----------|---------------------------|-----------------|
| 7-8B | A10G | 1-2 hours | $5-15 |
| 13B | A100 | 2-4 hours | $15-40 |
| 70B | 8x A100 | 8-16 hours | $100-300 |

### Cost Optimization

1. **Start small** -- fine-tune an 8B model first, scale up only if needed
2. **Fewer epochs** -- 3-5 epochs is usually sufficient
3. **Early stopping** -- monitor validation loss and stop when it plateaus
4. **LoRA/QLoRA** -- parameter-efficient fine-tuning uses much less compute
5. **Clean data** -- higher quality data means fewer examples needed

## Key Takeaways

1. Fine-tune only when prompt engineering and RAG are insufficient
2. Quality of training data matters more than quantity (100 great examples > 10,000 mediocre ones)
3. Use the chat/instruction format for training data
4. Start with the smallest model that might work and scale up if needed
5. Always compare fine-tuned model against the base model on a held-out test set
6. Monitor training and validation loss to detect overfitting early
7. Deploy through Model Serving for production use with auto-scaling

## Practice Exercises

1. Create a training dataset (20 examples) for classifying customer feedback into categories
2. Design an evaluation rubric comparing base vs fine-tuned model quality
3. Calculate the ROI of fine-tuning a small model vs using a larger base model with better prompts

## Next Topic

[Topic 06: LLMOps](06-llmops.md)
