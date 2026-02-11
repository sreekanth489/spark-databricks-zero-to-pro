# Multi-Agent Systems
> Module 22 -- Topic 05 | Level: Advanced | Time: 60 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain multi-agent architectures and when to use them
2. Compare supervisor, debate, and collaboration patterns
3. Design agent roles with clear specialization boundaries
4. Implement inter-agent communication protocols
5. Build orchestration logic for multi-agent workflows
6. Apply multi-agent patterns to real-world use cases

---

## Conceptual Overview

### What Are Multi-Agent Systems?

A multi-agent system is an architecture where multiple specialized agents
collaborate to solve a problem that is too complex for any single agent. Each
agent has a specific role, a focused set of tools, and expertise in a particular
domain. An orchestration layer coordinates their interactions.

Think of it as a team of specialists rather than one generalist. A single
agent might struggle with a task that requires SQL expertise, document analysis,
AND mathematical computation. Three specialized agents -- each excellent at one
thing -- can divide and conquer the problem.

```
  Single Agent vs. Multi-Agent
  ==============================

  Single Agent:
  +-------------------------------------------+
  |            General Purpose Agent           |
  |  SQL + Docs + Math + Analysis + Writing    |
  |  (jack of all trades, master of none)      |
  +-------------------------------------------+

  Multi-Agent System:
  +------------------+
  |   Orchestrator   |
  +------------------+
     |     |      |
     v     v      v
  +-----+ +----+ +-------+
  | SQL | | Doc| | Math  |
  |Agent| |Agent| | Agent|
  +-----+ +----+ +-------+
  (each agent is specialized and excellent at its role)
```

### When to Use Multi-Agent Systems

| Scenario | Single Agent | Multi-Agent | Why |
|----------|-------------|-------------|-----|
| Simple Q&A | Yes | No | Overkill for simple tasks |
| SQL data analysis | Yes | No | One tool type, one domain |
| Cross-domain research | No | Yes | Requires different expertise |
| Complex report generation | No | Yes | Analysis + writing + formatting |
| Customer service escalation | No | Yes | Triage, specialist, supervisor |
| Data quality pipeline | No | Yes | Detection, diagnosis, remediation |

---

## Multi-Agent Architectures

### 1. Supervisor Pattern

One supervisor agent coordinates multiple worker agents. The supervisor
receives the user request, breaks it into subtasks, delegates to specialists,
and aggregates results.

```
  Supervisor Architecture
  ========================

  User Request
       |
       v
  +--------------+
  |  Supervisor   |
  |  Agent        |
  |  - Decompose  |
  |  - Delegate   |
  |  - Aggregate  |
  +--------------+
     |    |    |
     v    v    v
  +----+ +----+ +------+
  |SQL | |RAG | |Report|
  |Work| |Work| |Work  |
  | er | | er | | er   |
  +----+ +----+ +------+
     |    |    |
     v    v    v
  +--------------+
  |  Supervisor   |
  |  (aggregate)  |
  +--------------+
       |
       v
  Final Response
```

**Best for**: Well-defined workflows where the supervisor knows what subtasks
are needed upfront. The supervisor acts as a project manager.

### 2. Debate Pattern

Multiple agents independently analyze the same question and then debate their
answers. A judge agent evaluates the debate and produces the final answer.
This pattern improves accuracy through diverse perspectives.

```
  Debate Architecture
  ====================

  User Question
       |
       +-------+-------+
       |       |       |
       v       v       v
  +------+ +------+ +------+
  |Agent | |Agent | |Agent |
  |  A   | |  B   | |  C   |
  |(view | |(view | |(view |
  |  1)  | |  2)  | |  3)  |
  +------+ +------+ +------+
       |       |       |
       v       v       v
  +--------------------------+
  |     Debate Round(s)      |
  | A challenges B's claims  |
  | B refines its position   |
  | C provides new evidence  |
  +--------------------------+
       |
       v
  +-----------+
  |   Judge   |
  |   Agent   |
  +-----------+
       |
       v
  Final Answer (highest confidence)
```

**Best for**: High-stakes decisions where accuracy matters more than speed.
Reduces individual agent biases through adversarial testing.

### 3. Collaboration Pattern

Agents work together in a pipeline where each agent transforms or enriches
the output of the previous agent. No single agent controls the entire flow --
they are peers that contribute sequentially or in parallel.

```
  Collaboration Architecture
  ============================

  User Request
       |
       v
  +----------+     +----------+     +----------+
  | Research | --> | Analysis | --> | Writing  |
  | Agent    |     | Agent    |     | Agent    |
  | (gather  |     | (process |     | (format  |
  |  data)   |     |  & reason|     |  output) |
  +----------+     +----------+     +----------+
                                         |
                                         v
                                   Final Report
```

**Best for**: Pipeline-style workflows where each stage has distinct expertise.
Similar to a factory assembly line.

---

## Designing Agent Roles

### Principles of Role Design

1. **Clear boundaries** -- Each agent should have a well-defined domain. Overlap
   leads to confusion about which agent should handle a task.

2. **Focused tools** -- Give each agent only the tools relevant to its role.
   The SQL agent does not need document search; the RAG agent does not need
   a calculator.

3. **Explicit handoffs** -- Define how agents pass work to each other. What
   information does the receiving agent need?

4. **Failure isolation** -- If one agent fails, the system should handle it
   gracefully without crashing the entire workflow.

### Example: Data Analysis Team

```
  Data Analysis Multi-Agent Team
  ================================

  +-------------------+
  |   Team Lead Agent |  Role: Orchestrate analysis workflow
  |   Tools: None     |  Delegates to specialists
  +-------------------+
         |
    +----+----+--------+
    |         |        |
    v         v        v
  +--------+ +------+ +--------+
  |Data    | |Stats | |Report  |
  |Engineer| |Agent | |Writer  |
  |Agent   | |      | |Agent   |
  +--------+ +------+ +--------+
  |Role:   | |Role: | |Role:   |
  |Query   | |Stat  | |Format  |
  |data,   | |tests,| |results |
  |clean,  | |trends| |into    |
  |transform| |models| |narrative|
  +--------+ +------+ +--------+
  |Tools:  | |Tools:| |Tools:  |
  |SQL,    | |Calc, | |Template|
  |Schema  | |Python| |Writer, |
  |Inspector| |Stats| |Chart   |
  +--------+ +------+ +--------+
```

---

## Inter-Agent Communication

### Message Protocol

Agents communicate through structured messages:

```python
agent_message = {
    "from_agent": "data_engineer",
    "to_agent": "stats_analyst",
    "message_type": "task_result",  # task_request, task_result, question, error
    "content": {
        "summary": "Queried revenue data for all regions, Q1-Q4 2024",
        "data": {
            "table_name": "tmp_quarterly_revenue",
            "row_count": 16,
            "columns": ["region", "quarter", "revenue"],
        },
        "status": "success",
    },
    "metadata": {
        "timestamp": "2024-07-15T10:30:00Z",
        "task_id": "task_003",
        "parent_task_id": "task_001",
    },
}
```

### Communication Patterns

| Pattern | Description | Use Case |
|---------|-------------|----------|
| Request-Response | Agent A asks Agent B, B responds | Supervisor to worker |
| Broadcast | One agent sends to all others | Debate initiation |
| Pipeline | Output of A becomes input to B | Sequential collaboration |
| Blackboard | Agents read/write to shared state | Complex coordination |

---

## Orchestration Patterns

### Sequential Orchestration

```
  Task 1 -> Agent A -> Result 1 -> Task 2 -> Agent B -> Result 2 -> Final
```

Simple and predictable. Each step depends on the previous one.

### Parallel Orchestration

```
  Task 1 -> Agent A -> Result 1 -+
                                  |
  Task 2 -> Agent B -> Result 2 -+--> Merge -> Final
                                  |
  Task 3 -> Agent C -> Result 3 -+
```

Faster when subtasks are independent. Requires a merge step.

### Conditional Orchestration

```
  Task -> Agent A -> Decision
                       |
           +-----------+-----------+
           |                       |
    (if simple)             (if complex)
           |                       |
      Agent B                 Agent C -> Agent D
           |                       |
           +-----------+-----------+
                       |
                     Final
```

Routes work based on complexity, topic, or intermediate results.

---

## Use Cases

### 1. Customer Service Escalation

```
  Customer Message
       |
       v
  +-----------+
  | Triage    |  Classifies intent: billing, technical, general, complaint
  | Agent     |
  +-----------+
       |
       +---> Billing Agent     (access to billing system tools)
       +---> Technical Agent   (access to troubleshooting tools)
       +---> General Agent     (access to knowledge base)
       +---> Supervisor Agent  (for complaints, handles escalation)
```

### 2. Research Assistant

```
  Research Question
       |
       v
  Planner Agent (decomposes question into sub-questions)
       |
       +---> Literature Agent  (searches document databases)
       +---> Data Agent        (queries structured databases)
       +---> Web Agent         (searches external sources)
       |
  Synthesizer Agent (combines findings into coherent answer)
```

### 3. Data Quality Monitor

```
  Schedule Trigger (daily)
       |
       v
  Scanner Agent (checks all tables for anomalies)
       |
       +---> Schema Agent     (detects schema drift)
       +---> Volume Agent     (detects row count anomalies)
       +---> Freshness Agent  (detects stale data)
       |
  Reporter Agent (generates quality report, files tickets)
```

---

## Building Multi-Agent Systems on Databricks

On Databricks, each agent in a multi-agent system can be:
- A separate `ChatAgent` subclass with its own tools
- Logged independently to MLflow for versioning
- Deployed as separate serving endpoints (for scalability)
- Evaluated independently with its own evaluation dataset

The orchestrator can be another `ChatAgent` that calls the other agents'
endpoints as tools. This creates a clean separation where each agent is
independently deployable, testable, and scalable.

```python
# Each specialist agent is deployed as a serving endpoint
# The orchestrator treats them as tools

orchestrator_tools = [
    EndpointTool(
        endpoint_name="sql-analyst-agent",
        description="Delegates data queries to the SQL specialist agent",
    ),
    EndpointTool(
        endpoint_name="rag-research-agent",
        description="Delegates document research to the RAG specialist agent",
    ),
    EndpointTool(
        endpoint_name="report-writer-agent",
        description="Delegates report formatting to the writing specialist agent",
    ),
]
```

---

## Key Takeaways

1. Multi-agent systems use specialized agents that collaborate on complex tasks
2. Three main architectures: supervisor (delegation), debate (consensus), collaboration (pipeline)
3. Design clear role boundaries with focused tools for each agent
4. Structured message protocols enable reliable inter-agent communication
5. Choose orchestration pattern (sequential, parallel, conditional) based on task structure
6. On Databricks, each agent can be an independent ChatAgent with its own endpoint
7. Multi-agent is powerful but adds complexity -- use only when single-agent is insufficient

---

## Practice Exercises

1. Design a multi-agent system for an e-commerce platform that handles product
   recommendations, order tracking, and returns. Define agent roles, tools, and
   communication flows.
2. For the data quality monitor use case above, write the system prompt for each
   agent (Scanner, Schema, Volume, Freshness, Reporter).
3. Compare supervisor vs. debate patterns for a financial analysis task. When
   would each be more appropriate?
