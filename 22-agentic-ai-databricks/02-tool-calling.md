# Tool Calling
> Module 22 -- Topic 02 | Level: Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain what tool calling (function calling) is and why it matters for agents
2. Define tools using Unity Catalog functions
3. Build SQL warehouse tools, Python tools, and retriever tools
4. Describe tool schemas and how the LLM uses them for tool selection
5. Trace the complete tool execution flow from decision to result
6. Implement tool validation and error handling patterns

---

## Conceptual Overview

### What Is Tool Calling?

Tool calling (also known as function calling) is the mechanism by which an LLM
invokes external functions to perform actions it cannot do on its own. An LLM
can generate text, but it cannot query a database, call an API, perform precise
calculations, or access real-time data. Tools bridge this gap.

When an LLM receives a prompt along with a set of tool definitions, it can
choose to generate a **tool call** instead of (or in addition to) a text
response. The tool call specifies which function to invoke and what arguments
to pass. The agent framework then executes the function and feeds the result
back to the LLM.

```
  Tool Calling Flow
  ==================

  +--------+     +-----------+     +----------+     +---------+
  | User   | --> | LLM       | --> | Tool     | --> | Execute |
  | Query  |     | + Tool    |     | Call     |     | Function|
  |        |     | Schemas   |     | Decision |     |         |
  +--------+     +-----------+     +----------+     +---------+
                                                        |
  +--------+     +-----------+     +----------+         |
  | Final  | <-- | LLM       | <-- | Tool     | <------+
  | Answer |     | Synthesis |     | Result   |
  +--------+     +-----------+     +----------+
```

### Why Tools Matter

Without tools, an LLM is limited to its training data and text generation:

| Capability | LLM Alone | LLM + Tools |
|------------|-----------|-------------|
| Answer factual questions | From training data (may be outdated) | From live databases |
| Perform calculations | Approximate (often wrong) | Exact (calculator tool) |
| Access private data | Cannot | Via authorized SQL/API tools |
| Take real-world actions | Cannot | Create tickets, send emails, etc. |
| Retrieve specific documents | Cannot | Vector search retriever tool |
| Validate data quality | Cannot | Run checks against actual tables |

---

## Tool Types on Databricks

### 1. Unity Catalog Function Tools

Unity Catalog functions are the primary way to define tools on Databricks. They
provide governance (who can execute), lineage tracking, and schema management.

```sql
-- Define a tool as a UC function
CREATE OR REPLACE FUNCTION catalog.schema.get_revenue_by_region(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE(region STRING, total_revenue DOUBLE)
COMMENT 'Returns total revenue by region for the specified date range.
         Use this tool when the user asks about regional sales performance.'
RETURN
    SELECT region, SUM(quantity * unit_price) as total_revenue
    FROM catalog.schema.sales
    WHERE order_date BETWEEN start_date AND end_date
    GROUP BY region
    ORDER BY total_revenue DESC;
```

Key points:
- The **COMMENT** is critical -- the LLM reads it to decide when to use the tool
- Parameters have types -- the LLM generates arguments that match the schema
- The function runs with the caller's permissions (Unity Catalog governance)
- Results are returned to the LLM as structured data

### 2. SQL Warehouse Tools

SQL warehouse tools let agents execute arbitrary SQL against a Databricks SQL
warehouse. This is more flexible but less governed than UC function tools.

```python
# SQL warehouse tool configuration
sql_tool_config = {
    "type": "sql_warehouse",
    "warehouse_id": "abc123def456",
    "description": (
        "Execute SQL queries against the sales database. "
        "Available tables: sales, customers, products, regions. "
        "Always use fully qualified table names: catalog.schema.table"
    ),
    "max_rows": 100,          # Limit results to prevent token overflow
    "timeout_seconds": 30,    # Prevent long-running queries
}
```

### 3. Python Tools

Python tools are functions defined in Python and registered with the agent.
They can perform arbitrary computation, call external APIs, or run complex
data transformations.

```python
def calculate_growth_rate(current_value: float, previous_value: float) -> str:
    """
    Calculate the percentage growth rate between two values.
    Use this tool when you need to compute year-over-year or
    period-over-period growth rates.

    Args:
        current_value: The current period's value
        previous_value: The previous period's value

    Returns:
        A string describing the growth rate and direction
    """
    if previous_value == 0:
        return "Cannot calculate growth: previous value is zero"
    rate = (current_value - previous_value) / previous_value * 100
    direction = "increase" if rate > 0 else "decrease"
    return f"{abs(rate):.1f}% {direction} (from {previous_value:,.2f} to {current_value:,.2f})"
```

### 4. Retriever Tools (Vector Search)

Retriever tools connect agents to vector search indexes, enabling RAG
(Retrieval-Augmented Generation). The agent queries the index to find
relevant documents before generating a response.

```python
# Retriever tool configuration
retriever_config = {
    "type": "vector_search_retriever",
    "index_name": "catalog.schema.document_index",
    "description": (
        "Search the company knowledge base for relevant documents. "
        "Use this tool when the user asks about company policies, "
        "procedures, or internal documentation."
    ),
    "num_results": 5,
    "columns": ["content", "source", "title"],
    "filters": {"status": "published"},
}
```

---

## Tool Schema Design

### The Tool Schema

Every tool must have a schema that tells the LLM what the tool does and how
to call it. The schema follows a standard format:

```json
{
    "name": "get_customer_orders",
    "description": "Look up recent orders for a customer by their ID or email.
                    Use this when the user asks about order history or status.",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The customer ID (e.g., CUST-12345)"
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of orders to return",
                "default": 10
            }
        },
        "required": ["customer_id"]
    }
}
```

### Schema Design Best Practices

1. **Descriptive names** -- Use verb-noun format: `get_revenue`, `search_docs`,
   `create_ticket`. The LLM reads the name to understand the tool's purpose.

2. **Detailed descriptions** -- Tell the LLM WHEN to use the tool, not just
   what it does. "Use this when the user asks about..." is more helpful than
   just "Returns order data."

3. **Clear parameter descriptions** -- Include examples, valid ranges, and
   formats. "The customer ID (e.g., CUST-12345)" is better than "The ID."

4. **Minimal required parameters** -- Make parameters optional with defaults
   where possible. Fewer required parameters means fewer chances for errors.

5. **Bounded outputs** -- Include `max_rows` or `limit` parameters to prevent
   tools from returning more data than the LLM's context window can handle.

---

## Tool Execution Flow

### Step-by-Step Process

```
  Detailed Tool Execution Flow
  ==============================

  1. USER MESSAGE
     "What was our revenue in the North region last quarter?"
         |
         v
  2. LLM RECEIVES (message + tool schemas)
     The LLM sees available tools and their descriptions.
     It decides which tool to call based on the user's intent.
         |
         v
  3. LLM GENERATES TOOL CALL
     {
       "tool": "get_revenue_by_region",
       "arguments": {
         "start_date": "2024-10-01",
         "end_date": "2024-12-31"
       }
     }
         |
         v
  4. FRAMEWORK VALIDATES
     - Does the tool exist?
     - Are required parameters present?
     - Do parameter types match the schema?
     - Does the caller have permission to execute?
         |
         v
  5. TOOL EXECUTES
     The function runs against the actual data source.
     Returns: [{"region": "North", "total_revenue": 520000.00}, ...]
         |
         v
  6. RESULT RETURNED TO LLM
     The LLM receives the tool output as a "tool" message.
     It can now decide to:
       a) Generate a final answer using the result
       b) Call another tool for more information
       c) Call the same tool with different parameters
         |
         v
  7. LLM GENERATES RESPONSE
     "The North region generated $520,000 in revenue last quarter."
```

### Error Handling

Tools can fail. Robust agents handle errors gracefully:

```
  Error Handling Patterns
  ========================

  Tool Error Types:
  +-------------------+------------------------------+-------------------+
  | Error Type        | Example                      | Agent Behavior    |
  +-------------------+------------------------------+-------------------+
  | Invalid arguments | Wrong date format            | Retry with fix    |
  | Permission denied | No access to table           | Inform user       |
  | Timeout           | Query took too long          | Simplify query    |
  | No results        | Query returned empty         | Try broader query |
  | Runtime error     | Division by zero in function | Use fallback tool |
  +-------------------+------------------------------+-------------------+
```

---

## Tool Governance with Unity Catalog

Unity Catalog provides governance for tools, ensuring agents can only access
what they are authorized to use:

```
  Tool Governance Model
  ======================

  +------------------+
  |  Unity Catalog   |
  +------------------+
  | catalog          |
  |   schema         |
  |     function_1   |  <-- Agent A can access (EXECUTE grant)
  |     function_2   |  <-- Agent A can access (EXECUTE grant)
  |     function_3   |  <-- Agent B only (no grant for Agent A)
  +------------------+

  Each agent's service principal has specific EXECUTE grants.
  The LLM only sees tool schemas for tools the agent can access.
  Tool execution respects Unity Catalog permissions at runtime.
```

This is a critical advantage over open-source agent frameworks where tool
access is often controlled through less formal mechanisms.

---

## Key Takeaways

1. Tool calling lets LLMs interact with external systems (databases, APIs, code)
2. Databricks supports four tool types: UC functions, SQL, Python, and retrievers
3. Tool schemas are the interface between the LLM and the tool -- design them carefully
4. The LLM reads tool descriptions to decide WHICH tool to call and HOW to call it
5. Unity Catalog provides governance for tools (permissions, lineage, auditing)
6. Error handling is essential -- tools fail, and agents must recover gracefully

---

## Practice Exercises

1. Design a set of 5 tools for a "data quality agent" that monitors table health.
   Write the tool schemas with names, descriptions, and parameters.
2. For each tool above, describe a scenario where the LLM would choose it.
3. Write error handling logic for a SQL tool that might timeout, return too many
   rows, or hit a permissions error.
