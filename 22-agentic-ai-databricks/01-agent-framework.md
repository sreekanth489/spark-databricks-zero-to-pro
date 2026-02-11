# Agent Framework
> Module 22 -- Topic 01 | Level: Advanced | Time: 60 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain what AI agents are and how they differ from simple LLM interactions
2. Describe the core components of an agent: LLM, tools, memory, and reasoning
3. Compare agent types: ReAct, chain-of-thought, and plan-and-execute
4. Define an agent using the Databricks `ChatAgent` interface
5. Configure agent parameters and model selection
6. Deploy an agent as a serving endpoint on Databricks

---

## Conceptual Overview

### What Are AI Agents?

An AI agent is an autonomous system that uses a large language model as its
reasoning engine. Unlike a simple prompt-response interaction where you send a
question and receive an answer, an agent operates in a loop: it reasons about
what to do, takes an action, observes the result, and decides what to do next.

The key insight is that the LLM does not just generate text -- it generates
**decisions**. Those decisions trigger real actions (querying a database,
calling an API, running a calculation), and the results of those actions feed
back into the LLM for further reasoning.

```
  Simple LLM vs. Agent
  =====================

  Simple LLM:
  +---------+     +-------+     +----------+
  | Prompt  | --> |  LLM  | --> | Response |
  +---------+     +-------+     +----------+
  (one-shot, no actions, no iteration)

  AI Agent:
  +---------+     +-------+     +----------+     +-------+
  |  Goal   | --> |  LLM  | --> | Decision | --> | Tool  |
  +---------+     +-------+     +----------+     +-------+
                      ^                              |
                      |         +-------------+      |
                      +-------- | Observation | <----+
                                +-------------+
  (iterative loop until goal is achieved)
```

### Four Core Components of an Agent

Every agent, regardless of framework or implementation, has four components:

```
  Agent Architecture
  ===================

  +-----------------------------------------------------+
  |                      AGENT                           |
  |                                                      |
  |  +------------+  +---------+  +--------+  +-------+  |
  |  |   LLM      |  |  Tools  |  | Memory |  | Logic |  |
  |  | (Reasoning |  | (Actions|  | (State |  | (How  |  |
  |  |  Engine)   |  |  the    |  |  across|  |  to   |  |
  |  |            |  |  agent  |  |  turns)|  | reason|  |
  |  |            |  |  can    |  |        |  |  and  |  |
  |  |            |  |  take)  |  |        |  | plan) |  |
  |  +------------+  +---------+  +--------+  +-------+  |
  +-----------------------------------------------------+
```

1. **LLM (Reasoning Engine)** -- The foundation model that processes inputs,
   generates reasoning traces, and decides which tools to invoke. On Databricks,
   this can be any model served through a Model Serving endpoint: DBRX, Llama,
   Mixtral, or external models like GPT-4 via External Models.

2. **Tools** -- Functions the agent can call to interact with the outside world.
   Tools are defined with schemas so the LLM knows what inputs they expect and
   what outputs they produce. Examples: SQL query tool, calculator, web search,
   data retrieval.

3. **Memory** -- The agent's ability to maintain context across interactions.
   Short-term memory is the conversation history within a session. Long-term
   memory can be stored in a database or vector store for persistence across
   sessions.

4. **Reasoning Logic** -- The strategy the agent uses to break down problems and
   decide on actions. Different agent types implement different reasoning
   strategies (see below).

### Agent Types

#### ReAct (Reasoning + Acting)

The most common agent pattern. The agent alternates between reasoning (thinking
about what to do) and acting (invoking a tool). Each step produces a thought,
an action, and an observation.

```
  ReAct Loop
  ===========
  Step 1: Thought  -> "I need to find the total revenue for Q4"
          Action   -> sql_query("SELECT SUM(revenue) FROM sales WHERE quarter=4")
          Observe  -> "Result: $2,450,000"

  Step 2: Thought  -> "Now I need to compare this to Q3"
          Action   -> sql_query("SELECT SUM(revenue) FROM sales WHERE quarter=3")
          Observe  -> "Result: $2,100,000"

  Step 3: Thought  -> "Q4 revenue is $350K higher than Q3, a 16.7% increase"
          Action   -> final_answer("Q4 revenue was $2.45M, up 16.7% from Q3")
```

#### Chain-of-Thought (CoT)

The agent reasons through the entire problem before taking any actions. It
produces a detailed reasoning chain first, then executes the plan. Better for
problems that require careful logical reasoning before action.

#### Plan-and-Execute

The agent creates an explicit multi-step plan upfront, then executes each step
sequentially. After each step, it can revise the remaining plan. Best for
complex tasks with many interdependent steps.

```
  Plan-and-Execute
  =================
  Plan:
    1. Query customer database for top 10 customers by revenue
    2. For each customer, calculate year-over-year growth
    3. Identify customers with declining revenue
    4. Generate recommendations for each declining customer
    5. Format final report

  Execute: Step 1 -> ... -> Step 2 -> ... (revise plan if needed)
```

---

## Databricks Mosaic AI Agent Framework

### Overview

The Mosaic AI Agent Framework is Databricks' integrated platform for building,
deploying, and monitoring AI agents. It provides:

- **`ChatAgent` interface** -- A standardized Python class for defining agents
- **Tool integration** -- Register Unity Catalog functions, SQL queries, Python
  functions, and retriever tools as agent tools
- **MLflow integration** -- Log agents as MLflow models for versioning and
  deployment
- **Model Serving deployment** -- Deploy agents as REST endpoints with
  auto-scaling
- **Agent Evaluation** -- Built-in evaluation framework for measuring quality
- **Playground UI** -- Interactive testing interface in the Databricks workspace

### The ChatAgent Interface

The `ChatAgent` class is the foundation for building agents on Databricks. It
defines a standard interface that the platform uses for deployment, evaluation,
and serving.

```python
from databricks.agents import ChatAgent, ChatAgentMessage, ChatAgentResponse

class MyAgent(ChatAgent):
    """
    A custom agent that implements the ChatAgent interface.
    The predict method is called for each user interaction.
    """

    def __init__(self):
        super().__init__()
        # Initialize agent state, load configuration, set up tools

    def predict(self, messages: list[ChatAgentMessage]) -> ChatAgentResponse:
        """
        Process a list of conversation messages and return a response.
        This method implements the agent's core logic:
        1. Parse the user's latest message
        2. Reason about what to do
        3. Call tools as needed
        4. Return the final response
        """
        # Agent logic here
        pass
```

### Agent Configuration

Agents are configured with parameters that control their behavior:

```python
agent_config = {
    "llm_endpoint": "databricks-dbrx-instruct",  # Model serving endpoint
    "llm_parameters": {
        "temperature": 0.1,        # Low for deterministic tool use
        "max_tokens": 4096,        # Maximum response length
    },
    "tools": [
        {"type": "uc_function", "name": "catalog.schema.sql_query"},
        {"type": "uc_function", "name": "catalog.schema.lookup_customer"},
    ],
    "system_prompt": "You are a data analyst agent...",
    "max_iterations": 10,          # Prevent infinite loops
    "memory_type": "conversation", # conversation | summary | none
}
```

### Deploying Agents

Agents are deployed through the standard MLflow and Model Serving workflow:

```
  Agent Deployment Pipeline
  ==========================

  +----------+     +---------+     +----------+     +----------+
  | Define   | --> | Log to  | --> | Register | --> | Deploy   |
  | Agent    |     | MLflow  |     | in UC    |     | Endpoint |
  +----------+     +---------+     +----------+     +----------+
  ChatAgent        mlflow.         catalog.         serving
  subclass         pyfunc.         schema.          endpoint
                   log_model()     agent_name       with scaling
```

```python
import mlflow

# Log the agent as an MLflow model
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=MyAgent(),
        registered_model_name="catalog.schema.my_agent",
    )

# Deploy to serving endpoint (via UI or API)
# The agent is now available as a REST endpoint
```

---

## When to Use Agents vs. Simple LLM Calls

| Scenario | Use Simple LLM | Use Agent |
|----------|---------------|-----------|
| Text summarization | Yes | No |
| Multi-step data analysis | No | Yes |
| Single SQL query generation | Maybe | Maybe |
| Complex question requiring multiple data sources | No | Yes |
| Creative writing | Yes | No |
| Customer service with database lookups | No | Yes |
| Code generation (single function) | Yes | No |
| Automated report generation with live data | No | Yes |

The decision comes down to: **Does the task require multiple steps, external
data access, or dynamic decision-making?** If yes, use an agent.

---

## Key Takeaways

1. Agents are LLMs that can reason, plan, and take actions through tools
2. The four components are: LLM, tools, memory, and reasoning logic
3. ReAct is the most common agent pattern (reason, act, observe, repeat)
4. Databricks provides the `ChatAgent` interface for standardized agent building
5. Agents are logged to MLflow and deployed as Model Serving endpoints
6. Use agents when tasks require multiple steps or external system interaction

---

## Practice Exercises

1. Sketch an agent architecture for a "data quality monitor" that checks tables
   for anomalies. What tools would it need? What reasoning pattern fits best?
2. Compare ReAct vs. plan-and-execute for a task that requires querying 5
   different databases and joining results. Which is more appropriate and why?
3. Design the system prompt for an agent that helps analysts explore a sales
   dataset. What instructions would you include to keep it focused and safe?
