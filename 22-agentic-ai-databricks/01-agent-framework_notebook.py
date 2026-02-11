# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Agent Framework
# MAGIC > Module 22 -- Topic 01 | Understand AI agents and the Databricks Mosaic AI Agent Framework
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Understand the core components of an AI agent (LLM, tools, memory, reasoning)
# MAGIC 2. Build a simulated agent class that follows the ChatAgent pattern
# MAGIC 3. Implement a ReAct-style reasoning loop with tool calling
# MAGIC 4. Explore agent configuration patterns
# MAGIC 5. See deployment templates for Model Serving endpoints
# MAGIC 6. Compare agent types through simulated execution traces
# MAGIC
# MAGIC **Note:** This notebook simulates agent behavior to demonstrate architectural
# MAGIC patterns. Actual Mosaic AI Agent Framework APIs require a full Databricks
# MAGIC workspace with AI/ML capabilities enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, ArrayType, MapType
)
from datetime import datetime, timedelta
import json
import random
import hashlib

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Sales Dataset for Agent Demonstrations
# MAGIC Our simulated agent will query this data using tool calls.

# COMMAND ----------

num_records = 1000
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headset", "Webcam"]
regions = ["North", "South", "East", "West"]
categories = {"Laptop": "Computing", "Phone": "Mobile", "Tablet": "Mobile",
              "Monitor": "Computing", "Keyboard": "Accessories", "Mouse": "Accessories",
              "Headset": "Audio", "Webcam": "Video"}

sales_data = []
for i in range(num_records):
    product = products[i % len(products)]
    sales_data.append((
        i + 1,
        product,
        categories[product],
        regions[i % len(regions)],
        random.randint(1, 50),
        round(random.uniform(15.0, 2500.0), 2),
        datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364)),
    ))

sales_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("order_date", TimestampType(), False),
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df.createOrReplaceTempView("agent_sales")
print(f"Sales dataset created: {sales_df.count()} rows")
sales_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Understanding Agent Components
# MAGIC
# MAGIC An AI agent has four core components. Let us define each one as a Python
# MAGIC class to understand their roles and interactions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component 1: Tool Definitions
# MAGIC Tools are functions that the agent can call. Each tool has a name,
# MAGIC description, input schema, and an execute method.

# COMMAND ----------

class Tool:
    """Represents a tool that an agent can invoke."""

    def __init__(self, name, description, parameters, function):
        self.name = name
        self.description = description
        self.parameters = parameters  # Dict describing expected inputs
        self.function = function      # Callable that executes the tool

    def execute(self, **kwargs):
        """Execute the tool with given parameters."""
        return self.function(**kwargs)

    def get_schema(self):
        """Return the tool schema for the LLM."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
        }


# Define a SQL query tool
def sql_query_tool_fn(query):
    """Execute a SQL query against the sales data."""
    try:
        result_df = spark.sql(query)
        rows = result_df.collect()
        if len(rows) == 0:
            return "Query returned no results."
        result_str = str([row.asDict() for row in rows[:20]])
        return f"Query returned {len(rows)} rows. First results: {result_str}"
    except Exception as e:
        return f"Query failed: {str(e)}"


def calculator_tool_fn(expression):
    """Evaluate a mathematical expression."""
    try:
        allowed_names = {"abs": abs, "round": round, "min": min, "max": max}
        result = eval(expression, {"__builtins__": {}}, allowed_names)
        return f"Result: {result}"
    except Exception as e:
        return f"Calculation failed: {str(e)}"


# Create tool instances
sql_tool = Tool(
    name="sql_query",
    description="Execute a SQL query against the agent_sales table. Columns: order_id, product, category, region, quantity, unit_price, order_date",
    parameters={"query": {"type": "string", "description": "SQL query to execute"}},
    function=lambda **kwargs: sql_query_tool_fn(kwargs["query"]),
)

calc_tool = Tool(
    name="calculator",
    description="Evaluate a mathematical expression. Supports basic arithmetic and functions like abs, round, min, max.",
    parameters={"expression": {"type": "string", "description": "Math expression to evaluate"}},
    function=lambda **kwargs: calculator_tool_fn(kwargs["expression"]),
)

print("Tools defined:")
for tool in [sql_tool, calc_tool]:
    print(f"  - {tool.name}: {tool.description[:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component 2: Memory
# MAGIC Memory allows the agent to maintain context across conversation turns.

# COMMAND ----------

class ConversationMemory:
    """Maintains conversation history for an agent."""

    def __init__(self, max_turns=20):
        self.messages = []
        self.max_turns = max_turns

    def add_message(self, role, content):
        """Add a message to the conversation history."""
        self.messages.append({
            "role": role,
            "content": content,
            "timestamp": datetime.now().isoformat(),
        })
        # Trim old messages if we exceed max_turns
        if len(self.messages) > self.max_turns * 2:
            # Keep system message + last max_turns exchanges
            system_msgs = [m for m in self.messages if m["role"] == "system"]
            other_msgs = [m for m in self.messages if m["role"] != "system"]
            self.messages = system_msgs + other_msgs[-(self.max_turns * 2):]

    def get_history(self):
        """Return the full conversation history."""
        return self.messages

    def get_last_n(self, n=5):
        """Return the last n messages."""
        return self.messages[-n:]

    def clear(self):
        """Clear all conversation history."""
        self.messages = []

    def summary(self):
        """Return a summary of the conversation state."""
        role_counts = {}
        for msg in self.messages:
            role_counts[msg["role"]] = role_counts.get(msg["role"], 0) + 1
        return f"Memory: {len(self.messages)} messages, roles: {role_counts}"


memory = ConversationMemory()
memory.add_message("system", "You are a helpful data analyst agent.")
memory.add_message("user", "What is total revenue by region?")
memory.add_message("assistant", "Let me query the sales data for you.")
print(memory.summary())
print(json.dumps(memory.get_history(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component 3: Reasoning Engine (Simulated LLM)
# MAGIC In production, the LLM decides which tools to call. Here we simulate
# MAGIC that decision-making to demonstrate the pattern.

# COMMAND ----------

class SimulatedLLM:
    """
    Simulates LLM reasoning for demonstration purposes.
    In production, this would be a call to a Model Serving endpoint.
    """

    def __init__(self, model_name="simulated-llm"):
        self.model_name = model_name
        self.call_count = 0

    def generate(self, messages, tools):
        """
        Simulate LLM response generation.
        Analyzes the user message and determines tool calls.
        """
        self.call_count += 1
        user_msg = ""
        for msg in reversed(messages):
            if msg["role"] == "user":
                user_msg = msg["content"].lower()
                break

        # Simple keyword-based routing to simulate LLM tool selection
        if any(word in user_msg for word in ["revenue", "total", "sum", "average",
                                              "count", "how many", "sales", "top"]):
            return self._generate_sql_response(user_msg, tools)
        elif any(word in user_msg for word in ["calculate", "compute", "math",
                                                "percent", "difference"]):
            return self._generate_calc_response(user_msg)
        else:
            return {
                "type": "final_answer",
                "content": f"I can help you analyze the sales data. Try asking about revenue, product counts, or regional breakdowns.",
            }

    def _generate_sql_response(self, user_msg, tools):
        """Generate a SQL tool call based on the user message."""
        if "region" in user_msg and ("revenue" in user_msg or "total" in user_msg):
            query = "SELECT region, ROUND(SUM(quantity * unit_price), 2) as total_revenue FROM agent_sales GROUP BY region ORDER BY total_revenue DESC"
        elif "top" in user_msg and "product" in user_msg:
            query = "SELECT product, SUM(quantity) as total_qty FROM agent_sales GROUP BY product ORDER BY total_qty DESC LIMIT 5"
        elif "average" in user_msg:
            query = "SELECT category, ROUND(AVG(unit_price), 2) as avg_price FROM agent_sales GROUP BY category ORDER BY avg_price DESC"
        elif "count" in user_msg or "how many" in user_msg:
            query = "SELECT COUNT(*) as total_orders FROM agent_sales"
        else:
            query = "SELECT region, product, SUM(quantity) as total_qty, ROUND(SUM(quantity * unit_price), 2) as revenue FROM agent_sales GROUP BY region, product ORDER BY revenue DESC LIMIT 10"
        return {
            "type": "tool_call",
            "tool_name": "sql_query",
            "arguments": {"query": query},
            "thought": f"The user is asking about sales data. I will query the agent_sales table to find the answer.",
        }

    def _generate_calc_response(self, user_msg):
        """Generate a calculator tool call."""
        return {
            "type": "tool_call",
            "tool_name": "calculator",
            "arguments": {"expression": "2450000 / 2100000 - 1"},
            "thought": "The user wants a calculation. Let me compute this.",
        }


llm = SimulatedLLM()
print(f"Simulated LLM initialized: {llm.model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Building a ReAct Agent
# MAGIC
# MAGIC The ReAct pattern alternates between **reasoning** (thought) and **acting**
# MAGIC (tool call). Let us build a complete agent using this pattern.

# COMMAND ----------

class ReActAgent:
    """
    A ReAct-style agent that reasons and acts iteratively.

    This follows the same architectural pattern as Databricks ChatAgent
    but is self-contained for demonstration purposes.
    """

    def __init__(self, llm, tools, memory, max_iterations=5):
        self.llm = llm
        self.tools = {tool.name: tool for tool in tools}
        self.memory = memory
        self.max_iterations = max_iterations
        self.execution_trace = []

    def predict(self, user_message):
        """
        Process a user message through the ReAct loop.
        Returns the final response after reasoning and acting.
        """
        self.memory.add_message("user", user_message)
        self.execution_trace = []

        for iteration in range(self.max_iterations):
            # Step 1: Ask the LLM what to do
            llm_response = self.llm.generate(
                messages=self.memory.get_history(),
                tools=list(self.tools.values()),
            )

            if llm_response["type"] == "final_answer":
                # Agent has decided on a final answer
                self.execution_trace.append({
                    "step": iteration + 1,
                    "type": "final_answer",
                    "content": llm_response["content"],
                })
                self.memory.add_message("assistant", llm_response["content"])
                return llm_response["content"]

            elif llm_response["type"] == "tool_call":
                # Step 2: Execute the tool
                tool_name = llm_response["tool_name"]
                arguments = llm_response["arguments"]
                thought = llm_response.get("thought", "")

                self.execution_trace.append({
                    "step": iteration + 1,
                    "type": "thought",
                    "content": thought,
                })

                if tool_name in self.tools:
                    observation = self.tools[tool_name].execute(**arguments)
                else:
                    observation = f"Error: Tool '{tool_name}' not found."

                self.execution_trace.append({
                    "step": iteration + 1,
                    "type": "tool_call",
                    "tool": tool_name,
                    "arguments": arguments,
                    "observation": observation,
                })

                # Step 3: Add observation to memory for next iteration
                self.memory.add_message("tool", f"[{tool_name}] {observation}")

                # For this simulation, generate final answer after first tool call
                final = f"Based on the data: {observation}"
                self.memory.add_message("assistant", final)
                self.execution_trace.append({
                    "step": iteration + 2,
                    "type": "final_answer",
                    "content": final,
                })
                return final

        return "Max iterations reached. Could not complete the task."

    def get_trace(self):
        """Return the execution trace for debugging."""
        return self.execution_trace


# Create the agent
memory = ConversationMemory()
memory.add_message("system", "You are a data analyst agent. Use the sql_query tool to answer questions about sales data. Use the calculator for math.")

agent = ReActAgent(
    llm=SimulatedLLM(),
    tools=[sql_tool, calc_tool],
    memory=memory,
    max_iterations=5,
)

print("ReAct Agent created successfully")
print(f"  Tools: {list(agent.tools.keys())}")
print(f"  Max iterations: {agent.max_iterations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Agent on Sample Questions

# COMMAND ----------

# Question 1: Revenue by region
print("=" * 70)
print("USER: What is the total revenue by region?")
print("=" * 70)
response = agent.predict("What is the total revenue by region?")
print(f"\nAGENT RESPONSE:\n{response}")
print(f"\nEXECUTION TRACE:")
for step in agent.get_trace():
    print(f"  Step {step['step']} [{step['type']}]: {str(step.get('content', step.get('tool', '')))[:100]}")

# COMMAND ----------

# Question 2: Top products
agent2 = ReActAgent(
    llm=SimulatedLLM(),
    tools=[sql_tool, calc_tool],
    memory=ConversationMemory(),
    max_iterations=5,
)

print("=" * 70)
print("USER: What are the top selling products?")
print("=" * 70)
response2 = agent2.predict("What are the top selling products?")
print(f"\nAGENT RESPONSE:\n{response2}")
print(f"\nEXECUTION TRACE:")
for step in agent2.get_trace():
    print(f"  Step {step['step']} [{step['type']}]: {str(step.get('content', step.get('tool', '')))[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: The ChatAgent Interface (Databricks Pattern)
# MAGIC
# MAGIC The following shows how you would define an agent using the actual
# MAGIC Databricks `ChatAgent` interface. This is a template -- it requires
# MAGIC a full Databricks workspace to execute.

# COMMAND ----------

# ============================================================
# DATABRICKS AGENT TEMPLATE (requires full workspace)
# ============================================================
# Uncomment and modify for production use.
#
# from databricks.agents import ChatAgent, ChatAgentMessage, ChatAgentResponse
# from databricks.agents.tools import UCFunctionTool
# import mlflow
#
# class SalesAnalystAgent(ChatAgent):
#     """
#     Production agent that analyzes sales data using SQL tools.
#     Deployed as a Model Serving endpoint on Databricks.
#     """
#
#     def __init__(self):
#         super().__init__()
#         self.tools = [
#             UCFunctionTool(function_name="catalog.schema.run_sql_query"),
#             UCFunctionTool(function_name="catalog.schema.get_customer_info"),
#         ]
#
#     def predict(self, messages: list[ChatAgentMessage]) -> ChatAgentResponse:
#         """Process messages and return agent response."""
#         # The framework handles:
#         # - Tool calling loop (ReAct pattern)
#         # - Memory management
#         # - Error handling and retries
#         # - Response formatting
#
#         response = self._run_agent_loop(messages)
#         return ChatAgentResponse(
#             messages=[ChatAgentMessage(role="assistant", content=response)]
#         )
#
# # Log the agent to MLflow
# agent = SalesAnalystAgent()
# with mlflow.start_run():
#     model_info = mlflow.pyfunc.log_model(
#         artifact_path="sales_agent",
#         python_model=agent,
#         registered_model_name="catalog.schema.sales_analyst_agent",
#     )

print("ChatAgent template defined (see commented code above)")
print("This template shows the production pattern for Databricks agents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Agent Configuration Patterns
# MAGIC
# MAGIC Different use cases require different agent configurations. Let us
# MAGIC explore common patterns using a configuration-driven approach.

# COMMAND ----------

# Configuration templates for different agent types
agent_configs = {
    "data_analyst": {
        "name": "Data Analyst Agent",
        "llm_endpoint": "databricks-dbrx-instruct",
        "llm_parameters": {"temperature": 0.1, "max_tokens": 4096},
        "system_prompt": (
            "You are a data analyst agent with access to the company's sales database. "
            "Use SQL queries to answer questions accurately. Always verify your results "
            "before responding. If you are unsure, say so."
        ),
        "tools": ["sql_query", "calculator"],
        "max_iterations": 8,
        "reasoning_type": "react",
    },
    "customer_support": {
        "name": "Customer Support Agent",
        "llm_endpoint": "databricks-meta-llama-3-70b-instruct",
        "llm_parameters": {"temperature": 0.3, "max_tokens": 2048},
        "system_prompt": (
            "You are a customer support agent. Be empathetic, clear, and helpful. "
            "Look up customer information before responding. Escalate complex issues."
        ),
        "tools": ["customer_lookup", "order_status", "create_ticket"],
        "max_iterations": 5,
        "reasoning_type": "react",
    },
    "research_assistant": {
        "name": "Research Assistant Agent",
        "llm_endpoint": "databricks-dbrx-instruct",
        "llm_parameters": {"temperature": 0.5, "max_tokens": 8192},
        "system_prompt": (
            "You are a research assistant. Break down complex questions into steps. "
            "Search multiple sources and synthesize findings. Cite your sources."
        ),
        "tools": ["document_search", "sql_query", "web_search", "summarizer"],
        "max_iterations": 15,
        "reasoning_type": "plan_and_execute",
    },
}

# Display configurations as a DataFrame
config_rows = []
for key, config in agent_configs.items():
    config_rows.append((
        config["name"],
        config["llm_endpoint"],
        config["llm_parameters"]["temperature"],
        len(config["tools"]),
        config["max_iterations"],
        config["reasoning_type"],
    ))

config_schema = StructType([
    StructField("agent_name", StringType()),
    StructField("llm_endpoint", StringType()),
    StructField("temperature", DoubleType()),
    StructField("num_tools", IntegerType()),
    StructField("max_iterations", IntegerType()),
    StructField("reasoning_type", StringType()),
])

config_df = spark.createDataFrame(config_rows, schema=config_schema)
config_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Agent Deployment Template
# MAGIC
# MAGIC The deployment workflow follows the MLflow model lifecycle.

# COMMAND ----------

# Simulated deployment workflow
deployment_steps = [
    {"step": 1, "action": "Define Agent",
     "detail": "Create a ChatAgent subclass with predict() method",
     "status": "completed"},
    {"step": 2, "action": "Test Locally",
     "detail": "Run agent.predict() with sample messages in a notebook",
     "status": "completed"},
    {"step": 3, "action": "Log to MLflow",
     "detail": "mlflow.pyfunc.log_model(python_model=agent, ...)",
     "status": "completed"},
    {"step": 4, "action": "Register Model",
     "detail": "Register in Unity Catalog: catalog.schema.agent_name",
     "status": "completed"},
    {"step": 5, "action": "Evaluate",
     "detail": "Run mlflow.evaluate() with agent evaluation dataset",
     "status": "completed"},
    {"step": 6, "action": "Deploy Endpoint",
     "detail": "Create Model Serving endpoint with auto-scaling config",
     "status": "pending"},
    {"step": 7, "action": "Monitor",
     "detail": "Track latency, token usage, tool call patterns, errors",
     "status": "pending"},
]

deploy_schema = StructType([
    StructField("step", IntegerType()),
    StructField("action", StringType()),
    StructField("detail", StringType()),
    StructField("status", StringType()),
])

deploy_df = spark.createDataFrame(
    [(d["step"], d["action"], d["detail"], d["status"]) for d in deployment_steps],
    schema=deploy_schema,
)
deploy_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Comparing Agent Types Through Execution Traces
# MAGIC
# MAGIC Let us simulate how different agent types would approach the same
# MAGIC question to understand their reasoning patterns.

# COMMAND ----------

question = "What was the revenue growth from Q1 to Q4, and which region grew the most?"

# Simulate ReAct trace
react_trace = [
    {"step": 1, "type": "thought", "content": "I need Q1 and Q4 revenue. Let me query Q1 first."},
    {"step": 1, "type": "action", "content": "sql_query(SELECT region, SUM(quantity*unit_price) as rev FROM agent_sales WHERE MONTH(order_date) BETWEEN 1 AND 3 GROUP BY region)"},
    {"step": 1, "type": "observation", "content": "Q1 results: North=450K, South=380K, East=410K, West=360K"},
    {"step": 2, "type": "thought", "content": "Good, I have Q1 data. Now I need Q4 revenue by region."},
    {"step": 2, "type": "action", "content": "sql_query(SELECT region, SUM(quantity*unit_price) as rev FROM agent_sales WHERE MONTH(order_date) BETWEEN 10 AND 12 GROUP BY region)"},
    {"step": 2, "type": "observation", "content": "Q4 results: North=520K, South=440K, East=500K, West=410K"},
    {"step": 3, "type": "thought", "content": "Now I can calculate growth for each region."},
    {"step": 3, "type": "action", "content": "calculator(520/450 - 1, 440/380 - 1, 500/410 - 1, 410/360 - 1)"},
    {"step": 3, "type": "observation", "content": "Growth: North=15.6%, South=15.8%, East=22.0%, West=13.9%"},
    {"step": 4, "type": "final", "content": "Overall revenue grew from $1.6M (Q1) to $1.87M (Q4), a 16.9% increase. East region grew the most at 22.0%."},
]

# Simulate Plan-and-Execute trace
plan_trace = [
    {"step": 0, "type": "plan", "content": "1. Get Q1 revenue by region\n2. Get Q4 revenue by region\n3. Calculate growth per region\n4. Identify highest growth region\n5. Summarize findings"},
    {"step": 1, "type": "execute", "content": "sql_query(... Q1 revenue ...) -> North=450K, South=380K, East=410K, West=360K"},
    {"step": 2, "type": "execute", "content": "sql_query(... Q4 revenue ...) -> North=520K, South=440K, East=500K, West=410K"},
    {"step": 3, "type": "execute", "content": "calculator(growth percentages) -> N=15.6%, S=15.8%, E=22.0%, W=13.9%"},
    {"step": 4, "type": "execute", "content": "max(growth_rates) -> East at 22.0%"},
    {"step": 5, "type": "final", "content": "Revenue grew 16.9% from Q1 to Q4. East region led with 22.0% growth."},
]

print(f"Question: {question}")
print()
print("=" * 70)
print("ReAct Agent Trace (interleaved reasoning and action)")
print("=" * 70)
for entry in react_trace:
    print(f"  Step {entry['step']} [{entry['type']:11}] {entry['content'][:80]}")
print()
print("=" * 70)
print("Plan-and-Execute Agent Trace (plan first, then execute)")
print("=" * 70)
for entry in plan_trace:
    print(f"  Step {entry['step']} [{entry['type']:7}] {entry['content'][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Agent Execution Metrics
# MAGIC
# MAGIC Let us track and visualize agent performance metrics to understand
# MAGIC how agents behave in practice.

# COMMAND ----------

# Simulate metrics from multiple agent runs
agent_runs = []
for i in range(50):
    agent_runs.append((
        f"run_{i+1:03d}",
        random.choice(["data_analyst", "customer_support", "research_assistant"]),
        random.randint(1, 10),                    # num tool calls
        random.randint(1, 8),                     # num iterations
        round(random.uniform(0.5, 15.0), 2),      # latency seconds
        random.randint(200, 5000),                 # tokens used
        random.choice(["success", "success", "success", "success", "max_iter_reached", "tool_error"]),
        datetime(2024, 6, 1) + timedelta(hours=random.randint(0, 720)),
    ))

metrics_schema = StructType([
    StructField("run_id", StringType()),
    StructField("agent_type", StringType()),
    StructField("num_tool_calls", IntegerType()),
    StructField("num_iterations", IntegerType()),
    StructField("latency_seconds", DoubleType()),
    StructField("tokens_used", IntegerType()),
    StructField("outcome", StringType()),
    StructField("run_timestamp", TimestampType()),
])

metrics_df = spark.createDataFrame(agent_runs, schema=metrics_schema)
metrics_df.createOrReplaceTempView("agent_metrics")

print("Agent execution metrics:")
metrics_df.show(10, truncate=False)

# COMMAND ----------

# Analyze agent performance by type
print("Performance by Agent Type:")
perf_by_type = spark.sql("""
    SELECT
        agent_type,
        COUNT(*) as total_runs,
        ROUND(AVG(num_tool_calls), 1) as avg_tool_calls,
        ROUND(AVG(num_iterations), 1) as avg_iterations,
        ROUND(AVG(latency_seconds), 2) as avg_latency_sec,
        ROUND(AVG(tokens_used), 0) as avg_tokens,
        ROUND(SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate_pct
    FROM agent_metrics
    GROUP BY agent_type
    ORDER BY agent_type
""")
perf_by_type.show(truncate=False)

# COMMAND ----------

# Analyze outcomes
print("Outcome Distribution:")
spark.sql("""
    SELECT outcome, COUNT(*) as count,
           ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM agent_metrics), 1) as pct
    FROM agent_metrics
    GROUP BY outcome
    ORDER BY count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Cleanup

# COMMAND ----------

spark.catalog.dropTempView("agent_sales")
spark.catalog.dropTempView("agent_metrics")
print("Temporary views cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **AI agents** are LLMs that can reason, plan, and take actions through tools
# MAGIC 2. **Four components**: LLM (reasoning), Tools (actions), Memory (state), Logic (strategy)
# MAGIC 3. **ReAct** is the most common pattern -- interleave thinking and acting
# MAGIC 4. **Plan-and-Execute** creates an upfront plan, then executes steps sequentially
# MAGIC 5. The Databricks **ChatAgent** interface provides a standardized way to build agents
# MAGIC 6. Agents are **logged to MLflow** and **deployed as Model Serving endpoints**
# MAGIC 7. **Configuration-driven** agents are easier to maintain and iterate on
# MAGIC 8. **Execution traces** are essential for debugging and understanding agent behavior
