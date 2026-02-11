# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Tool Calling
# MAGIC > Module 22 -- Topic 02 | Define tools, register them with agents, and trace the tool execution lifecycle
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Define multiple tool types (SQL query, calculator, data lookup, text processor)
# MAGIC 2. Build a tool registry with schema validation
# MAGIC 3. Simulate LLM tool selection based on user queries
# MAGIC 4. Trace the complete tool execution flow step by step
# MAGIC 5. Implement error handling and retry patterns
# MAGIC 6. See Unity Catalog function tool templates for production use
# MAGIC
# MAGIC **Note:** This notebook simulates the LLM's tool selection to demonstrate
# MAGIC the full lifecycle. Production agents use actual LLM endpoints for decisions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, ArrayType
)
from datetime import datetime, timedelta
import json
import random
import time
import traceback

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sample Datasets
# MAGIC We create sales and customer tables that our tools will query.

# COMMAND ----------

# Sales data
num_sales = 800
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"]
regions = ["North", "South", "East", "West"]

sales_data = []
for i in range(num_sales):
    product = products[i % len(products)]
    sales_data.append((
        i + 1,
        product,
        regions[i % len(regions)],
        random.randint(1, 30),
        round(random.uniform(25.0, 2000.0), 2),
        datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364)),
    ))

sales_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("region", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("order_date", TimestampType(), False),
])

sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df.createOrReplaceTempView("tool_sales")

# Customer data
customers = []
for i in range(200):
    customers.append((
        f"CUST-{i+1:04d}",
        f"Customer {i+1}",
        f"customer{i+1}@example.com",
        regions[i % len(regions)],
        random.choice(["Gold", "Silver", "Bronze", "Platinum"]),
        round(random.uniform(100.0, 50000.0), 2),
    ))

cust_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("region", StringType(), False),
    StructField("tier", StringType(), False),
    StructField("lifetime_value", DoubleType(), False),
])

cust_df = spark.createDataFrame(customers, schema=cust_schema)
cust_df.createOrReplaceTempView("tool_customers")

print(f"Sales data: {sales_df.count()} rows")
print(f"Customer data: {cust_df.count()} rows")
sales_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Defining Tool Types
# MAGIC
# MAGIC We define four different tool types that demonstrate common patterns
# MAGIC used in production agents.

# COMMAND ----------

class ToolDefinition:
    """
    Represents a tool with schema, validation, and execution logic.
    Mirrors the structure used by the Databricks Agent Framework.
    """

    def __init__(self, name, description, parameters, function, tool_type="python"):
        self.name = name
        self.description = description
        self.parameters = parameters
        self.function = function
        self.tool_type = tool_type
        self.call_count = 0
        self.total_latency_ms = 0
        self.errors = []

    def validate_arguments(self, arguments):
        """Validate that required arguments are present and correctly typed."""
        errors = []
        required = self.parameters.get("required", [])
        properties = self.parameters.get("properties", {})

        for req in required:
            if req not in arguments:
                errors.append(f"Missing required parameter: '{req}'")

        for key, value in arguments.items():
            if key in properties:
                expected_type = properties[key].get("type", "string")
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(f"Parameter '{key}' should be string, got {type(value).__name__}")
                elif expected_type == "integer" and not isinstance(value, int):
                    errors.append(f"Parameter '{key}' should be integer, got {type(value).__name__}")
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    errors.append(f"Parameter '{key}' should be number, got {type(value).__name__}")

        return errors

    def execute(self, **arguments):
        """Execute the tool with validation, timing, and error handling."""
        # Validate arguments
        validation_errors = self.validate_arguments(arguments)
        if validation_errors:
            error_msg = f"Validation failed: {'; '.join(validation_errors)}"
            self.errors.append(error_msg)
            return {"status": "error", "error": error_msg}

        # Execute with timing
        start_time = time.time()
        try:
            result = self.function(**arguments)
            elapsed_ms = (time.time() - start_time) * 1000
            self.call_count += 1
            self.total_latency_ms += elapsed_ms
            return {
                "status": "success",
                "result": result,
                "latency_ms": round(elapsed_ms, 2),
            }
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.errors.append(error_msg)
            return {"status": "error", "error": error_msg, "latency_ms": round(elapsed_ms, 2)}

    def get_schema(self):
        """Return the schema for LLM consumption."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }

    def get_stats(self):
        """Return execution statistics."""
        avg_latency = self.total_latency_ms / self.call_count if self.call_count > 0 else 0
        return {
            "name": self.name,
            "calls": self.call_count,
            "avg_latency_ms": round(avg_latency, 2),
            "errors": len(self.errors),
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 1: SQL Query Tool

# COMMAND ----------

def execute_sql_query(query, max_rows=20):
    """Execute a SQL query and return formatted results."""
    if not query.strip().upper().startswith("SELECT"):
        return "Only SELECT queries are allowed for safety."
    result_df = spark.sql(query)
    rows = result_df.limit(max_rows).collect()
    if not rows:
        return "Query returned no results."
    columns = result_df.columns
    results = [dict(zip(columns, [row[c] for c in columns])) for row in rows]
    total_count = result_df.count()
    return {
        "total_rows": total_count,
        "returned_rows": len(results),
        "columns": columns,
        "data": str(results),
    }


sql_query_tool = ToolDefinition(
    name="sql_query",
    description=(
        "Execute a SQL SELECT query against the sales database. "
        "Available tables: tool_sales (order_id, product, region, quantity, unit_price, order_date), "
        "tool_customers (customer_id, name, email, region, tier, lifetime_value). "
        "Use this when the user asks about sales data, revenue, or customer information."
    ),
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A SQL SELECT query. Use table names: tool_sales, tool_customers",
            },
            "max_rows": {
                "type": "integer",
                "description": "Maximum rows to return (default 20)",
            },
        },
        "required": ["query"],
    },
    function=execute_sql_query,
    tool_type="sql",
)

print(f"Tool defined: {sql_query_tool.name}")
print(json.dumps(sql_query_tool.get_schema(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 2: Calculator Tool

# COMMAND ----------

def calculate(expression):
    """Evaluate a mathematical expression safely."""
    allowed = {"abs": abs, "round": round, "min": min, "max": max, "sum": sum, "len": len}
    result = eval(expression, {"__builtins__": {}}, allowed)
    return {"expression": expression, "result": result}


calculator_tool = ToolDefinition(
    name="calculator",
    description=(
        "Evaluate mathematical expressions. Supports: +, -, *, /, **, %, "
        "and functions abs(), round(), min(), max(). "
        "Use this for precise calculations like growth rates, percentages, or averages."
    ),
    parameters={
        "type": "object",
        "properties": {
            "expression": {
                "type": "string",
                "description": "A mathematical expression to evaluate, e.g., '(520000 - 450000) / 450000 * 100'",
            },
        },
        "required": ["expression"],
    },
    function=calculate,
    tool_type="python",
)

print(f"Tool defined: {calculator_tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 3: Customer Lookup Tool

# COMMAND ----------

def lookup_customer(customer_id=None, email=None):
    """Look up a customer by ID or email."""
    if customer_id:
        result = spark.sql(f"SELECT * FROM tool_customers WHERE customer_id = '{customer_id}'")
    elif email:
        result = spark.sql(f"SELECT * FROM tool_customers WHERE email = '{email}'")
    else:
        return "Please provide either customer_id or email."
    rows = result.collect()
    if not rows:
        return f"No customer found."
    return {col: rows[0][col] for col in result.columns}


customer_lookup_tool = ToolDefinition(
    name="lookup_customer",
    description=(
        "Look up a customer's profile by their ID or email address. "
        "Returns name, email, region, loyalty tier, and lifetime value. "
        "Use this when the user asks about a specific customer."
    ),
    parameters={
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "Customer ID in format CUST-XXXX (e.g., CUST-0042)",
            },
            "email": {
                "type": "string",
                "description": "Customer email address",
            },
        },
        "required": [],
    },
    function=lookup_customer,
    tool_type="python",
)

print(f"Tool defined: {customer_lookup_tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool 4: Data Summary Tool

# COMMAND ----------

def summarize_table(table_name, group_by_column=None):
    """Generate a summary of a table, optionally grouped by a column."""
    try:
        df = spark.sql(f"SELECT * FROM {table_name}")
        summary = {
            "table": table_name,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
        }
        # Add numeric column stats
        numeric_cols = [f.name for f in df.schema.fields
                        if f.dataType in (IntegerType(), DoubleType())]
        if numeric_cols:
            stats_rows = df.select([
                F.mean(c).alias(f"{c}_mean") for c in numeric_cols
            ] + [
                F.stddev(c).alias(f"{c}_stddev") for c in numeric_cols
            ]).collect()
            if stats_rows:
                summary["numeric_stats"] = stats_rows[0].asDict()

        if group_by_column and group_by_column in df.columns:
            groups = df.groupBy(group_by_column).count().collect()
            summary["group_counts"] = {row[group_by_column]: row["count"] for row in groups}

        return summary
    except Exception as e:
        return f"Error summarizing table: {str(e)}"


data_summary_tool = ToolDefinition(
    name="summarize_table",
    description=(
        "Generate a statistical summary of a table including row count, columns, "
        "and basic numeric statistics. Optionally group by a column. "
        "Available tables: tool_sales, tool_customers. "
        "Use this to get an overview of the data before running specific queries."
    ),
    parameters={
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to summarize (tool_sales or tool_customers)",
            },
            "group_by_column": {
                "type": "string",
                "description": "Optional column to group the summary by",
            },
        },
        "required": ["table_name"],
    },
    function=summarize_table,
    tool_type="python",
)

print(f"Tool defined: {data_summary_tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Tool Registry
# MAGIC
# MAGIC A tool registry manages all available tools and provides schema
# MAGIC information to the LLM.

# COMMAND ----------

class ToolRegistry:
    """Registry that manages tool definitions and provides schemas to the LLM."""

    def __init__(self):
        self.tools = {}

    def register(self, tool):
        """Register a tool in the registry."""
        self.tools[tool.name] = tool
        print(f"  Registered tool: {tool.name} ({tool.tool_type})")

    def get_tool(self, name):
        """Get a tool by name."""
        return self.tools.get(name)

    def get_all_schemas(self):
        """Return all tool schemas for the LLM."""
        return [tool.get_schema() for tool in self.tools.values()]

    def execute_tool(self, name, **arguments):
        """Execute a tool by name with arguments."""
        tool = self.get_tool(name)
        if tool is None:
            return {"status": "error", "error": f"Tool '{name}' not found in registry"}
        return tool.execute(**arguments)

    def get_all_stats(self):
        """Return execution stats for all tools."""
        return [tool.get_stats() for tool in self.tools.values()]

    def list_tools(self):
        """List all registered tools."""
        return [(t.name, t.tool_type, t.description[:60]) for t in self.tools.values()]


# Create and populate the registry
registry = ToolRegistry()
print("Registering tools:")
registry.register(sql_query_tool)
registry.register(calculator_tool)
registry.register(customer_lookup_tool)
registry.register(data_summary_tool)

print(f"\nTotal tools registered: {len(registry.tools)}")

# Display as DataFrame
tool_list = registry.list_tools()
tool_list_schema = StructType([
    StructField("tool_name", StringType()),
    StructField("tool_type", StringType()),
    StructField("description_preview", StringType()),
])
spark.createDataFrame(tool_list, schema=tool_list_schema).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Simulating LLM Tool Selection
# MAGIC
# MAGIC In production, the LLM reads tool schemas and decides which tool to call.
# MAGIC Here we simulate that decision process to show the full flow.

# COMMAND ----------

def simulate_llm_tool_selection(user_query, tool_schemas):
    """
    Simulates how an LLM would select a tool based on the user query.
    In production, this is handled by the LLM's function calling capability.
    """
    query_lower = user_query.lower()

    # Simulate LLM reasoning about tool selection
    reasoning = f"Analyzing query: '{user_query}'\n"
    reasoning += f"Available tools: {[s['function']['name'] for s in tool_schemas]}\n"

    # Rule-based selection (simulating LLM intelligence)
    if any(kw in query_lower for kw in ["customer", "cust-", "lookup", "profile"]):
        tool_name = "lookup_customer"
        # Extract customer ID if present
        import re
        cust_match = re.search(r'cust-\d+', query_lower)
        if cust_match:
            arguments = {"customer_id": cust_match.group().upper()}
        else:
            email_match = re.search(r'[\w.]+@[\w.]+', query_lower)
            arguments = {"email": email_match.group()} if email_match else {"customer_id": "CUST-0042"}
        reasoning += f"Decision: User is asking about a specific customer -> {tool_name}\n"

    elif any(kw in query_lower for kw in ["summary", "overview", "describe", "shape"]):
        tool_name = "summarize_table"
        table = "tool_customers" if "customer" in query_lower else "tool_sales"
        arguments = {"table_name": table}
        if "by" in query_lower:
            for col in ["region", "product", "tier"]:
                if col in query_lower:
                    arguments["group_by_column"] = col
                    break
        reasoning += f"Decision: User wants a data overview -> {tool_name}\n"

    elif any(kw in query_lower for kw in ["calculate", "percent", "growth", "ratio", "math"]):
        tool_name = "calculator"
        arguments = {"expression": "round((520000 - 450000) / 450000 * 100, 2)"}
        reasoning += f"Decision: User needs a calculation -> {tool_name}\n"

    else:
        tool_name = "sql_query"
        if "revenue" in query_lower and "region" in query_lower:
            query = "SELECT region, ROUND(SUM(quantity * unit_price), 2) as revenue FROM tool_sales GROUP BY region ORDER BY revenue DESC"
        elif "top" in query_lower or "best" in query_lower:
            query = "SELECT product, SUM(quantity) as total_sold FROM tool_sales GROUP BY product ORDER BY total_sold DESC LIMIT 5"
        elif "average" in query_lower or "avg" in query_lower:
            query = "SELECT product, ROUND(AVG(unit_price), 2) as avg_price FROM tool_sales GROUP BY product ORDER BY avg_price DESC"
        elif "monthly" in query_lower or "month" in query_lower:
            query = "SELECT MONTH(order_date) as month, ROUND(SUM(quantity * unit_price), 2) as revenue FROM tool_sales GROUP BY MONTH(order_date) ORDER BY month"
        else:
            query = "SELECT region, product, COUNT(*) as orders, ROUND(SUM(quantity * unit_price), 2) as revenue FROM tool_sales GROUP BY region, product ORDER BY revenue DESC LIMIT 10"
        arguments = {"query": query}
        reasoning += f"Decision: User needs data from database -> {tool_name}\n"

    return {
        "tool_name": tool_name,
        "arguments": arguments,
        "reasoning": reasoning,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Tool Selection with Various Queries

# COMMAND ----------

test_queries = [
    "What is the total revenue by region?",
    "Look up customer CUST-0042",
    "Give me a summary of the sales table by product",
    "What are the top selling products?",
    "Calculate the growth rate from 450000 to 520000",
    "What is the average price per product?",
    "Show me monthly revenue trends",
]

print("Tool Selection Simulation")
print("=" * 70)
for query in test_queries:
    selection = simulate_llm_tool_selection(query, registry.get_all_schemas())
    print(f"\nQuery: \"{query}\"")
    print(f"  Selected tool: {selection['tool_name']}")
    print(f"  Arguments: {json.dumps(selection['arguments'], default=str)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Complete Tool Execution Flow
# MAGIC
# MAGIC Let us trace through the full lifecycle for each test query,
# MAGIC from user input to final result.

# COMMAND ----------

def full_tool_execution_flow(user_query, registry):
    """Execute the complete tool calling flow and return a detailed trace."""
    trace = {"query": user_query, "steps": []}

    # Step 1: Prepare tool schemas
    schemas = registry.get_all_schemas()
    trace["steps"].append({
        "phase": "schema_preparation",
        "detail": f"Prepared {len(schemas)} tool schemas for LLM",
    })

    # Step 2: LLM selects tool (simulated)
    selection = simulate_llm_tool_selection(user_query, schemas)
    trace["steps"].append({
        "phase": "tool_selection",
        "tool": selection["tool_name"],
        "arguments": selection["arguments"],
        "reasoning": selection["reasoning"],
    })

    # Step 3: Validate and execute
    result = registry.execute_tool(selection["tool_name"], **selection["arguments"])
    trace["steps"].append({
        "phase": "execution",
        "status": result["status"],
        "latency_ms": result.get("latency_ms", 0),
        "result_preview": str(result.get("result", result.get("error", "")))[:200],
    })

    # Step 4: Format response (simulated LLM synthesis)
    if result["status"] == "success":
        trace["steps"].append({
            "phase": "response_synthesis",
            "detail": "LLM synthesizes tool result into natural language response",
        })
        trace["final_response"] = f"Based on the data: {str(result['result'])[:150]}"
    else:
        trace["final_response"] = f"I encountered an error: {result.get('error', 'Unknown error')}"

    return trace


# Execute the flow for sample queries
print("Complete Tool Execution Traces")
print("=" * 70)

for query in test_queries[:4]:
    trace = full_tool_execution_flow(query, registry)
    print(f"\nQuery: \"{trace['query']}\"")
    for step in trace["steps"]:
        print(f"  [{step['phase']}]", end=" ")
        if "tool" in step:
            print(f"-> {step['tool']}({json.dumps(step['arguments'], default=str)[:60]})")
        elif "status" in step:
            print(f"-> {step['status']} ({step['latency_ms']}ms)")
        else:
            print(f"-> {step['detail'][:60]}")
    print(f"  Response: {trace['final_response'][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Error Handling and Retry Patterns

# COMMAND ----------

def execute_with_retry(registry, tool_name, arguments, max_retries=3):
    """Execute a tool with retry logic for transient failures."""
    attempts = []

    for attempt in range(max_retries):
        result = registry.execute_tool(tool_name, **arguments)
        attempts.append({
            "attempt": attempt + 1,
            "status": result["status"],
            "latency_ms": result.get("latency_ms", 0),
        })

        if result["status"] == "success":
            return {"result": result, "attempts": attempts, "final_status": "success"}

        # Wait before retry (simulated exponential backoff)
        if attempt < max_retries - 1:
            wait_ms = (2 ** attempt) * 100
            time.sleep(wait_ms / 1000)

    return {"result": result, "attempts": attempts, "final_status": "failed_after_retries"}


# Test with valid query
print("Test 1: Valid SQL query")
result = execute_with_retry(registry, "sql_query",
    {"query": "SELECT region, COUNT(*) as cnt FROM tool_sales GROUP BY region"})
print(f"  Status: {result['final_status']} after {len(result['attempts'])} attempt(s)")

# Test with invalid query (will fail validation in real scenario)
print("\nTest 2: Missing required parameter")
result = registry.execute_tool("sql_query")
print(f"  Status: {result['status']}, Error: {result.get('error', 'none')[:60]}")

# Test with non-existent tool
print("\nTest 3: Non-existent tool")
result = registry.execute_tool("non_existent_tool", query="test")
print(f"  Status: {result['status']}, Error: {result.get('error', 'none')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Tool Execution Metrics

# COMMAND ----------

# Run multiple tool executions to gather metrics
execution_queries = [
    ("sql_query", {"query": "SELECT COUNT(*) as cnt FROM tool_sales"}),
    ("sql_query", {"query": "SELECT region, SUM(quantity) as qty FROM tool_sales GROUP BY region"}),
    ("sql_query", {"query": "SELECT product, AVG(unit_price) as avg_price FROM tool_sales GROUP BY product"}),
    ("calculator", {"expression": "round(520000 / 450000 * 100 - 100, 2)"}),
    ("calculator", {"expression": "round(2500 * 0.15, 2)"}),
    ("lookup_customer", {"customer_id": "CUST-0001"}),
    ("lookup_customer", {"customer_id": "CUST-0100"}),
    ("summarize_table", {"table_name": "tool_sales", "group_by_column": "region"}),
    ("summarize_table", {"table_name": "tool_customers", "group_by_column": "tier"}),
]

for tool_name, args in execution_queries:
    registry.execute_tool(tool_name, **args)

# Display stats
stats = registry.get_all_stats()
stats_schema = StructType([
    StructField("tool_name", StringType()),
    StructField("call_count", IntegerType()),
    StructField("avg_latency_ms", DoubleType()),
    StructField("error_count", IntegerType()),
])

stats_rows = [(s["name"], s["calls"], s["avg_latency_ms"], s["errors"]) for s in stats]
stats_df = spark.createDataFrame(stats_rows, schema=stats_schema)
print("Tool Execution Statistics:")
stats_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Unity Catalog Tool Templates
# MAGIC
# MAGIC The following shows how tools are defined as Unity Catalog functions
# MAGIC in production. These are SQL-based templates.

# COMMAND ----------

# UC Function Tool templates (require full Databricks workspace)
uc_tool_templates = [
    {
        "name": "get_revenue_by_region",
        "sql": """
CREATE OR REPLACE FUNCTION catalog.schema.get_revenue_by_region(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE(region STRING, total_revenue DOUBLE)
COMMENT 'Returns total revenue by region for the given date range.
         Use when the user asks about regional revenue or sales performance.'
RETURN
    SELECT region, SUM(quantity * unit_price) as total_revenue
    FROM catalog.schema.sales
    WHERE order_date BETWEEN start_date AND end_date
    GROUP BY region ORDER BY total_revenue DESC;""",
    },
    {
        "name": "search_customers",
        "sql": """
CREATE OR REPLACE FUNCTION catalog.schema.search_customers(
    search_term STRING,
    max_results INT DEFAULT 10
)
RETURNS TABLE(customer_id STRING, name STRING, email STRING, tier STRING)
COMMENT 'Search for customers by name or email. Use when the user asks
         to find or look up a customer by partial name or email.'
RETURN
    SELECT customer_id, name, email, tier
    FROM catalog.schema.customers
    WHERE LOWER(name) LIKE CONCAT('%', LOWER(search_term), '%')
       OR LOWER(email) LIKE CONCAT('%', LOWER(search_term), '%')
    LIMIT max_results;""",
    },
    {
        "name": "get_product_stats",
        "sql": """
CREATE OR REPLACE FUNCTION catalog.schema.get_product_stats(
    product_name STRING
)
RETURNS TABLE(total_orders INT, total_quantity INT, avg_price DOUBLE, total_revenue DOUBLE)
COMMENT 'Get sales statistics for a specific product. Use when the user
         asks about how a particular product is performing.'
RETURN
    SELECT COUNT(*) as total_orders, SUM(quantity) as total_quantity,
           AVG(unit_price) as avg_price, SUM(quantity * unit_price) as total_revenue
    FROM catalog.schema.sales
    WHERE product = product_name;""",
    },
]

print("Unity Catalog Function Tool Templates")
print("=" * 70)
print("(These require a full Databricks workspace to execute)")
print()
for template in uc_tool_templates:
    print(f"Tool: {template['name']}")
    print(template["sql"])
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Tool Schema Best Practices Summary

# COMMAND ----------

best_practices = [
    ("Use verb-noun names", "get_revenue, search_docs, create_ticket", "Helps LLM understand the action"),
    ("Write WHEN descriptions", "Use this when the user asks about...", "Guides LLM on when to select this tool"),
    ("Add parameter examples", "customer_id: e.g., CUST-12345", "Helps LLM generate correct arguments"),
    ("Set sensible defaults", "max_rows: default 20", "Prevents token overflow from large results"),
    ("Mark required params", "required: [query]", "Ensures critical arguments are always provided"),
    ("Limit output size", "Truncate results to 100 rows", "Prevents context window exhaustion"),
    ("Handle errors gracefully", "Return error messages, not stack traces", "Lets LLM explain failures to users"),
    ("Use specific types", "type: integer vs type: string", "Enables argument validation"),
]

bp_schema = StructType([
    StructField("practice", StringType()),
    StructField("example", StringType()),
    StructField("why", StringType()),
])
bp_df = spark.createDataFrame(best_practices, schema=bp_schema)
print("Tool Schema Best Practices:")
bp_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Cleanup

# COMMAND ----------

spark.catalog.dropTempView("tool_sales")
spark.catalog.dropTempView("tool_customers")
print("Temporary views cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Tools** let agents interact with external systems -- databases, APIs, calculators
# MAGIC 2. **Tool schemas** describe inputs, outputs, and WHEN to use each tool
# MAGIC 3. **Four tool types** on Databricks: UC functions, SQL warehouse, Python, retrievers
# MAGIC 4. The **LLM reads tool descriptions** to decide which tool to call
# MAGIC 5. **Validation and error handling** are essential -- tools fail and agents must recover
# MAGIC 6. **Unity Catalog governance** controls which tools each agent can access
# MAGIC 7. **Metrics tracking** helps optimize tool performance and agent behavior
# MAGIC 8. **Tool design** matters as much as prompt engineering for agent quality
