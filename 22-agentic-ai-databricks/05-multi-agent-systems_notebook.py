# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 - Multi-Agent Systems
# MAGIC > Module 22 -- Topic 05 | Design and orchestrate multi-agent architectures with specialized agent roles
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Build specialized agent classes for different roles (SQL, RAG, calculator)
# MAGIC 2. Implement a supervisor agent that orchestrates worker agents
# MAGIC 3. Demonstrate the debate pattern with multiple independent agents
# MAGIC 4. Build a collaboration pipeline where agents work sequentially
# MAGIC 5. Show inter-agent communication with structured messages
# MAGIC 6. Analyze multi-agent execution traces and performance
# MAGIC
# MAGIC **Note:** This notebook simulates multi-agent behavior to demonstrate
# MAGIC architectural patterns. Production systems deploy each agent as a separate
# MAGIC Model Serving endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from datetime import datetime, timedelta
import json
import random
import time

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Business Data for Multi-Agent Demonstrations

# COMMAND ----------

# Sales data
sales_data = []
products = ["Laptop Pro X1", "Tablet Ultra S8", "Headset Pro Max", "Monitor 4K", "Keyboard Mech"]
regions = ["North", "South", "East", "West"]
for i in range(600):
    product = products[i % len(products)]
    sales_data.append((
        i + 1, product, regions[i % len(regions)],
        random.randint(1, 25),
        round(random.uniform(50.0, 2000.0), 2),
        datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364)),
    ))

sales_schema = StructType([
    StructField("order_id", IntegerType()), StructField("product", StringType()),
    StructField("region", StringType()), StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()), StructField("order_date", TimestampType()),
])
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
sales_df.createOrReplaceTempView("multi_agent_sales")

# Support tickets
ticket_data = []
severities = ["Critical", "High", "Medium", "Low"]
categories = ["Billing", "Technical", "General", "Returns"]
for i in range(200):
    ticket_data.append((
        f"TKT-{i+1:04d}",
        f"Customer {i+1}",
        categories[i % len(categories)],
        severities[i % len(severities)],
        random.choice(["Open", "In Progress", "Resolved", "Escalated"]),
        datetime(2024, 6, 1) + timedelta(hours=random.randint(0, 720)),
    ))

ticket_schema = StructType([
    StructField("ticket_id", StringType()), StructField("customer_name", StringType()),
    StructField("category", StringType()), StructField("severity", StringType()),
    StructField("status", StringType()), StructField("created_at", TimestampType()),
])
ticket_df = spark.createDataFrame(ticket_data, schema=ticket_schema)
ticket_df.createOrReplaceTempView("support_tickets")

print(f"Sales data: {sales_df.count()} rows")
print(f"Support tickets: {ticket_df.count()} rows")
sales_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Defining Specialized Agent Roles
# MAGIC
# MAGIC Each agent has a focused role, specific tools, and a clear system prompt.

# COMMAND ----------

class SpecializedAgent:
    """
    Base class for a specialized agent in a multi-agent system.
    Each agent has a role, tools, and domain expertise.
    """

    def __init__(self, name, role, system_prompt):
        self.name = name
        self.role = role
        self.system_prompt = system_prompt
        self.execution_history = []

    def process(self, task):
        """Process a task and return a result. Override in subclasses."""
        raise NotImplementedError

    def get_history(self):
        return self.execution_history

    def _log_execution(self, task, result, duration_ms):
        self.execution_history.append({
            "agent": self.name,
            "role": self.role,
            "task": task[:100],
            "result_preview": str(result)[:100],
            "duration_ms": duration_ms,
            "timestamp": datetime.now().isoformat(),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Analyst Agent

# COMMAND ----------

class SQLAnalystAgent(SpecializedAgent):
    """Agent specialized in querying and analyzing structured data via SQL."""

    def __init__(self):
        super().__init__(
            name="sql_analyst",
            role="Data Query Specialist",
            system_prompt=(
                "You are a SQL analyst agent. Your job is to query the sales and "
                "support databases to extract data requested by the team lead. "
                "Return results in a structured format."
            ),
        )

    def process(self, task):
        start = time.time()
        task_lower = task.lower()

        # Route to appropriate query based on task
        if "revenue" in task_lower and "region" in task_lower:
            query = "SELECT region, ROUND(SUM(quantity * unit_price), 2) as revenue FROM multi_agent_sales GROUP BY region ORDER BY revenue DESC"
        elif "top" in task_lower and "product" in task_lower:
            query = "SELECT product, SUM(quantity) as total_sold, ROUND(SUM(quantity * unit_price), 2) as revenue FROM multi_agent_sales GROUP BY product ORDER BY revenue DESC"
        elif "ticket" in task_lower and ("count" in task_lower or "summary" in task_lower):
            query = "SELECT category, severity, COUNT(*) as ticket_count FROM support_tickets GROUP BY category, severity ORDER BY category, severity"
        elif "monthly" in task_lower or "trend" in task_lower:
            query = "SELECT MONTH(order_date) as month, ROUND(SUM(quantity * unit_price), 2) as revenue FROM multi_agent_sales GROUP BY MONTH(order_date) ORDER BY month"
        elif "escalat" in task_lower:
            query = "SELECT category, COUNT(*) as escalated_count FROM support_tickets WHERE status = 'Escalated' GROUP BY category ORDER BY escalated_count DESC"
        else:
            query = "SELECT COUNT(*) as total_orders, ROUND(SUM(quantity * unit_price), 2) as total_revenue, ROUND(AVG(quantity * unit_price), 2) as avg_order_value FROM multi_agent_sales"

        try:
            result_df = spark.sql(query)
            rows = result_df.collect()
            result = {
                "query": query,
                "data": [row.asDict() for row in rows],
                "row_count": len(rows),
                "status": "success",
            }
        except Exception as e:
            result = {"query": query, "status": "error", "error": str(e)}

        duration_ms = round((time.time() - start) * 1000, 2)
        self._log_execution(task, result, duration_ms)
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Statistics Agent

# COMMAND ----------

class StatsAnalystAgent(SpecializedAgent):
    """Agent specialized in statistical analysis and calculations."""

    def __init__(self):
        super().__init__(
            name="stats_analyst",
            role="Statistical Analysis Specialist",
            system_prompt=(
                "You are a statistics agent. Analyze data provided by other agents "
                "and compute statistical measures, growth rates, trends, and insights."
            ),
        )

    def process(self, task):
        start = time.time()

        # Process data passed from other agents
        if isinstance(task, dict) and "data" in task:
            data = task["data"]
            analysis = self._analyze_data(data, task.get("analysis_type", "summary"))
        else:
            analysis = self._analyze_from_query(str(task))

        duration_ms = round((time.time() - start) * 1000, 2)
        self._log_execution(str(task)[:100], analysis, duration_ms)
        return analysis

    def _analyze_data(self, data, analysis_type):
        """Perform statistical analysis on provided data."""
        if not data:
            return {"status": "error", "error": "No data to analyze"}

        # Extract numeric values
        numeric_keys = []
        for key, value in data[0].items():
            if isinstance(value, (int, float)):
                numeric_keys.append(key)

        result = {"analysis_type": analysis_type, "insights": []}

        for key in numeric_keys:
            values = [row[key] for row in data if key in row and row[key] is not None]
            if values:
                total = sum(values)
                avg_val = total / len(values)
                min_val = min(values)
                max_val = max(values)
                result["insights"].append({
                    "metric": key,
                    "count": len(values),
                    "total": round(total, 2),
                    "average": round(avg_val, 2),
                    "min": round(min_val, 2),
                    "max": round(max_val, 2),
                    "range": round(max_val - min_val, 2),
                })

        # Add growth analysis if there is sequential data
        if len(data) >= 2 and any("month" in str(k).lower() for k in data[0].keys()):
            first_val = None
            last_val = None
            for key in numeric_keys:
                if key != "month":
                    first_val = data[0].get(key)
                    last_val = data[-1].get(key)
                    break
            if first_val and last_val and first_val != 0:
                growth = round((last_val - first_val) / first_val * 100, 2)
                result["insights"].append({
                    "metric": "period_growth_pct",
                    "value": growth,
                    "direction": "increasing" if growth > 0 else "decreasing",
                })

        result["status"] = "success"
        return result

    def _analyze_from_query(self, task_str):
        """Perform analysis based on task description."""
        return {
            "status": "success",
            "insights": [{"note": f"Analysis performed for: {task_str[:80]}"}],
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Report Writer Agent

# COMMAND ----------

class ReportWriterAgent(SpecializedAgent):
    """Agent specialized in formatting analysis results into readable reports."""

    def __init__(self):
        super().__init__(
            name="report_writer",
            role="Report Formatting Specialist",
            system_prompt=(
                "You are a report writer agent. Take data and analysis from other "
                "agents and format them into clear, professional reports."
            ),
        )

    def process(self, task):
        start = time.time()

        if isinstance(task, dict):
            report = self._format_report(task)
        else:
            report = f"Report: {task}"

        duration_ms = round((time.time() - start) * 1000, 2)
        self._log_execution(str(task)[:100], report, duration_ms)
        return report

    def _format_report(self, data):
        """Format data and analysis into a structured report."""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("BUSINESS ANALYSIS REPORT")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report_lines.append("=" * 60)

        # Include data summary
        if "sql_data" in data:
            report_lines.append("\n--- DATA SUMMARY ---")
            sql_result = data["sql_data"]
            if isinstance(sql_result, dict) and "data" in sql_result:
                report_lines.append(f"Records analyzed: {sql_result.get('row_count', 'N/A')}")
                for row in sql_result["data"][:5]:
                    formatted = ", ".join(f"{k}: {v}" for k, v in row.items())
                    report_lines.append(f"  {formatted}")
                if sql_result.get("row_count", 0) > 5:
                    report_lines.append(f"  ... and {sql_result['row_count'] - 5} more rows")

        # Include analysis insights
        if "analysis" in data:
            report_lines.append("\n--- STATISTICAL ANALYSIS ---")
            analysis = data["analysis"]
            if isinstance(analysis, dict) and "insights" in analysis:
                for insight in analysis["insights"]:
                    if "metric" in insight and "total" in insight:
                        report_lines.append(
                            f"  {insight['metric']}: Total={insight['total']:,.2f}, "
                            f"Avg={insight['average']:,.2f}, "
                            f"Range={insight.get('range', 'N/A')}"
                        )
                    elif "metric" in insight and "value" in insight:
                        report_lines.append(
                            f"  {insight['metric']}: {insight['value']}% ({insight.get('direction', '')})"
                        )

        report_lines.append("\n" + "=" * 60)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 60)

        return "\n".join(report_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Supervisor Pattern Implementation
# MAGIC
# MAGIC The supervisor decomposes a user request into subtasks, delegates
# MAGIC to specialist agents, and aggregates results.

# COMMAND ----------

class SupervisorAgent:
    """
    Orchestrates multiple specialized agents using the supervisor pattern.
    Decomposes tasks, delegates to workers, and aggregates results.
    """

    def __init__(self, workers):
        self.workers = {w.name: w for w in workers}
        self.execution_trace = []

    def handle_request(self, user_request):
        """
        Process a user request by:
        1. Decomposing into subtasks
        2. Delegating to appropriate workers
        3. Aggregating results
        """
        self.execution_trace = []
        start_time = time.time()

        # Step 1: Decompose (simulated task decomposition)
        subtasks = self._decompose(user_request)
        self.execution_trace.append({
            "phase": "decomposition",
            "subtasks": [{"agent": s["agent"], "task": s["task"][:60]} for s in subtasks],
        })

        # Step 2: Execute subtasks
        results = {}
        for subtask in subtasks:
            agent_name = subtask["agent"]
            if agent_name in self.workers:
                agent = self.workers[agent_name]
                result = agent.process(subtask["input"])
                results[agent_name] = result
                self.execution_trace.append({
                    "phase": "execution",
                    "agent": agent_name,
                    "status": result.get("status", "completed") if isinstance(result, dict) else "completed",
                })

        # Step 3: Aggregate results
        final_result = self._aggregate(results, user_request)
        total_ms = round((time.time() - start_time) * 1000, 2)
        self.execution_trace.append({
            "phase": "aggregation",
            "total_duration_ms": total_ms,
        })

        return final_result

    def _decompose(self, request):
        """Decompose a user request into subtasks for specialist agents."""
        request_lower = request.lower()
        subtasks = []

        # Always start with a data query
        if any(kw in request_lower for kw in ["revenue", "sales", "product", "order"]):
            if "region" in request_lower:
                subtasks.append({
                    "agent": "sql_analyst",
                    "task": "Query revenue by region",
                    "input": "Get revenue by region",
                })
            elif "product" in request_lower:
                subtasks.append({
                    "agent": "sql_analyst",
                    "task": "Query top products",
                    "input": "Get top products by revenue",
                })
            elif "trend" in request_lower or "monthly" in request_lower:
                subtasks.append({
                    "agent": "sql_analyst",
                    "task": "Query monthly trends",
                    "input": "Get monthly revenue trends",
                })
            else:
                subtasks.append({
                    "agent": "sql_analyst",
                    "task": "Query overall sales metrics",
                    "input": "Get overall sales summary",
                })

        if any(kw in request_lower for kw in ["ticket", "support", "escalat"]):
            subtasks.append({
                "agent": "sql_analyst",
                "task": "Query support ticket data",
                "input": "Get ticket summary by category and severity",
            })

        # Add analysis if report or analysis is requested
        if any(kw in request_lower for kw in ["analysis", "analyze", "report", "insight", "trend"]):
            subtasks.append({
                "agent": "stats_analyst",
                "task": "Perform statistical analysis",
                "input": {"data": [], "analysis_type": "summary"},
            })

        # Add report formatting if a report is requested
        if any(kw in request_lower for kw in ["report", "summary", "format"]):
            subtasks.append({
                "agent": "report_writer",
                "task": "Format results into report",
                "input": {},
            })

        if not subtasks:
            subtasks.append({
                "agent": "sql_analyst",
                "task": "General data query",
                "input": request,
            })

        return subtasks

    def _aggregate(self, results, original_request):
        """Aggregate results from all workers."""
        # Pass SQL data to stats agent if both ran
        if "sql_analyst" in results and "stats_analyst" in results:
            sql_data = results["sql_analyst"]
            if isinstance(sql_data, dict) and "data" in sql_data:
                stats_agent = self.workers["stats_analyst"]
                analysis = stats_agent.process({
                    "data": sql_data["data"],
                    "analysis_type": "comprehensive",
                })
                results["stats_analyst"] = analysis

        # Pass everything to report writer if it ran
        if "report_writer" in results:
            report_agent = self.workers["report_writer"]
            report = report_agent.process({
                "sql_data": results.get("sql_analyst", {}),
                "analysis": results.get("stats_analyst", {}),
            })
            return {"type": "report", "content": report, "all_results": results}

        return {"type": "data", "content": results}

    def get_trace(self):
        return self.execution_trace

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Supervisor Agent

# COMMAND ----------

# Create specialist agents
sql_agent = SQLAnalystAgent()
stats_agent = StatsAnalystAgent()
report_agent = ReportWriterAgent()

# Create supervisor
supervisor = SupervisorAgent(workers=[sql_agent, stats_agent, report_agent])

# Test: Revenue analysis report
print("REQUEST: Generate a revenue analysis report by region with insights")
print("=" * 70)
result = supervisor.handle_request("Generate a revenue analysis report by region with insights")

if result["type"] == "report":
    print(result["content"])
else:
    print(json.dumps(result["content"], indent=2, default=str)[:500])

# COMMAND ----------

# Show execution trace
print("\nSupervisor Execution Trace:")
print("-" * 50)
for step in supervisor.get_trace():
    if step["phase"] == "decomposition":
        print(f"[DECOMPOSE] Subtasks:")
        for st in step["subtasks"]:
            print(f"    -> {st['agent']}: {st['task']}")
    elif step["phase"] == "execution":
        print(f"[EXECUTE]   {step['agent']}: {step['status']}")
    elif step["phase"] == "aggregation":
        print(f"[AGGREGATE] Total time: {step['total_duration_ms']}ms")

# COMMAND ----------

# Test: Support ticket analysis
print("REQUEST: Analyze support ticket escalation patterns")
print("=" * 70)
result2 = supervisor.handle_request("Analyze support ticket escalation patterns and generate a report")

if result2["type"] == "report":
    print(result2["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Debate Pattern Implementation
# MAGIC
# MAGIC Multiple agents independently analyze the same question and present
# MAGIC their perspectives. A judge evaluates the debate.

# COMMAND ----------

class DebateAgent(SpecializedAgent):
    """An agent that argues a particular perspective in a debate."""

    def __init__(self, name, perspective, bias_factor=0.0):
        super().__init__(
            name=name,
            role=f"Debate participant ({perspective})",
            system_prompt=f"You argue from the {perspective} perspective.",
        )
        self.perspective = perspective
        self.bias_factor = bias_factor

    def process(self, task):
        """Generate an argument from this agent's perspective."""
        start = time.time()

        # Query the data
        if "region" in task.lower():
            query = "SELECT region, ROUND(SUM(quantity * unit_price), 2) as revenue FROM multi_agent_sales GROUP BY region ORDER BY revenue DESC"
        else:
            query = "SELECT product, ROUND(SUM(quantity * unit_price), 2) as revenue FROM multi_agent_sales GROUP BY product ORDER BY revenue DESC"

        result_df = spark.sql(query)
        rows = result_df.collect()
        data = [row.asDict() for row in rows]

        # Generate perspective-based analysis
        argument = self._generate_argument(task, data)

        duration_ms = round((time.time() - start) * 1000, 2)
        self._log_execution(task, argument, duration_ms)
        return argument

    def _generate_argument(self, task, data):
        """Generate an argument based on this agent's perspective."""
        if not data:
            return {"perspective": self.perspective, "argument": "Insufficient data.", "confidence": 0.0}

        # Each agent emphasizes different aspects
        if self.perspective == "optimistic":
            # Focus on highest values and growth
            best_item = data[0]
            key = [k for k in best_item.keys() if k != "revenue"][0]
            return {
                "perspective": self.perspective,
                "argument": f"The data shows strong performance. {best_item[key]} leads with ${best_item['revenue']:,.2f} in revenue. The top performers demonstrate healthy business growth.",
                "evidence": data[:3],
                "confidence": round(0.75 + random.uniform(0, 0.2), 2),
            }
        elif self.perspective == "conservative":
            # Focus on gaps and risks
            worst_item = data[-1]
            best_item = data[0]
            key = [k for k in best_item.keys() if k != "revenue"][0]
            gap = best_item["revenue"] - worst_item["revenue"]
            return {
                "perspective": self.perspective,
                "argument": f"While top performers are strong, there is a ${gap:,.2f} gap between the best ({best_item[key]}) and worst ({worst_item[key]}). This imbalance poses risks.",
                "evidence": [data[0], data[-1]],
                "confidence": round(0.70 + random.uniform(0, 0.2), 2),
            }
        else:  # balanced
            total = sum(d["revenue"] for d in data)
            avg = total / len(data)
            return {
                "perspective": self.perspective,
                "argument": f"Total revenue across all segments is ${total:,.2f} with an average of ${avg:,.2f}. Performance is distributed across {len(data)} segments.",
                "evidence": data,
                "confidence": round(0.80 + random.uniform(0, 0.15), 2),
            }


class JudgeAgent(SpecializedAgent):
    """Agent that evaluates debate arguments and produces a verdict."""

    def __init__(self):
        super().__init__(
            name="judge",
            role="Debate Judge",
            system_prompt="Evaluate arguments from all debate participants and synthesize a balanced verdict.",
        )

    def process(self, arguments):
        """Evaluate debate arguments and produce a final verdict."""
        start = time.time()

        # Score each argument
        scored = []
        for arg in arguments:
            score = arg.get("confidence", 0.5)
            evidence_count = len(arg.get("evidence", []))
            # Bonus for more evidence
            score += evidence_count * 0.02
            score = min(1.0, score)
            scored.append({
                "perspective": arg["perspective"],
                "score": round(score, 3),
                "argument_preview": arg["argument"][:80],
            })

        scored.sort(key=lambda x: x["score"], reverse=True)

        # Synthesize verdict
        winner = scored[0]
        verdict = {
            "verdict": f"The {winner['perspective']} perspective provides the most well-supported analysis (score: {winner['score']}).",
            "scores": scored,
            "synthesis": "A comprehensive view requires considering all perspectives. " + " ".join(s["argument_preview"] for s in scored[:2]),
        }

        duration_ms = round((time.time() - start) * 1000, 2)
        self._log_execution("Judge debate", verdict, duration_ms)
        return verdict

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Debate

# COMMAND ----------

# Create debate participants
optimist = DebateAgent("optimist", "optimistic")
conservative = DebateAgent("conservative", "conservative")
balanced = DebateAgent("balanced", "balanced")
judge = JudgeAgent()

debate_question = "Analyze regional revenue performance"

print(f"DEBATE QUESTION: {debate_question}")
print("=" * 70)

# Each agent independently analyzes
arguments = []
for agent in [optimist, conservative, balanced]:
    arg = agent.process(debate_question)
    arguments.append(arg)
    print(f"\n[{agent.name.upper()}] (confidence: {arg['confidence']})")
    print(f"  {arg['argument']}")

# Judge evaluates
print("\n" + "=" * 70)
print("JUDGE VERDICT:")
print("=" * 70)
verdict = judge.process(arguments)
print(f"\n{verdict['verdict']}")
print(f"\nScores:")
for s in verdict["scores"]:
    print(f"  {s['perspective']:15} -> {s['score']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Collaboration Pipeline Pattern
# MAGIC
# MAGIC Agents work sequentially, each enriching the output of the previous one.

# COMMAND ----------

class CollaborationPipeline:
    """
    Orchestrates agents in a sequential pipeline.
    Each agent's output becomes the next agent's input.
    """

    def __init__(self, agents):
        self.agents = agents
        self.pipeline_trace = []

    def run(self, initial_input):
        """Execute the pipeline sequentially."""
        self.pipeline_trace = []
        current_input = initial_input
        start_time = time.time()

        for i, agent in enumerate(self.agents):
            step_start = time.time()
            result = agent.process(current_input)
            step_ms = round((time.time() - step_start) * 1000, 2)

            self.pipeline_trace.append({
                "step": i + 1,
                "agent": agent.name,
                "role": agent.role,
                "duration_ms": step_ms,
                "output_type": type(result).__name__,
            })

            # Prepare input for next agent
            if isinstance(result, dict) and i < len(self.agents) - 1:
                next_agent = self.agents[i + 1]
                if next_agent.name == "stats_analyst" and "data" in result:
                    current_input = {"data": result["data"], "analysis_type": "comprehensive"}
                elif next_agent.name == "report_writer":
                    current_input = {"sql_data": current_input if isinstance(current_input, dict) else {},
                                     "analysis": result}
                else:
                    current_input = result
            else:
                current_input = result

        total_ms = round((time.time() - start_time) * 1000, 2)
        self.pipeline_trace.append({"step": "total", "duration_ms": total_ms})

        return current_input

    def get_trace(self):
        return self.pipeline_trace


# Create pipeline: SQL -> Stats -> Report
pipeline = CollaborationPipeline([
    SQLAnalystAgent(),
    StatsAnalystAgent(),
    ReportWriterAgent(),
])

print("COLLABORATION PIPELINE: SQL -> Stats -> Report")
print("INPUT: Get monthly revenue trends")
print("=" * 70)

pipeline_result = pipeline.run("Get monthly revenue trends")
print(pipeline_result if isinstance(pipeline_result, str) else json.dumps(pipeline_result, indent=2, default=str)[:800])

# COMMAND ----------

# Show pipeline execution trace
print("\nPipeline Execution Trace:")
trace_data = [(t.get("step", ""), t.get("agent", "total"), t.get("role", ""),
                t.get("duration_ms", 0))
               for t in pipeline.get_trace()]

trace_schema = StructType([
    StructField("step", StringType()),
    StructField("agent", StringType()),
    StructField("role", StringType()),
    StructField("duration_ms", DoubleType()),
])
spark.createDataFrame(trace_data, schema=trace_schema).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Multi-Agent Performance Analysis

# COMMAND ----------

# Collect all execution histories
all_agent_executions = []
for agent in [sql_agent, stats_agent, report_agent, optimist, conservative, balanced, judge]:
    for entry in agent.get_history():
        all_agent_executions.append((
            entry["agent"],
            entry["role"],
            entry["task"][:60],
            entry["duration_ms"],
            entry["timestamp"],
        ))

exec_schema = StructType([
    StructField("agent_name", StringType()),
    StructField("role", StringType()),
    StructField("task_preview", StringType()),
    StructField("duration_ms", DoubleType()),
    StructField("timestamp", StringType()),
])

exec_df = spark.createDataFrame(all_agent_executions, schema=exec_schema)
exec_df.createOrReplaceTempView("agent_executions")

print("All Agent Executions:")
exec_df.show(truncate=False)

# COMMAND ----------

# Performance summary by agent
print("Performance Summary by Agent:")
spark.sql("""
    SELECT
        agent_name,
        role,
        COUNT(*) as total_calls,
        ROUND(AVG(duration_ms), 2) as avg_duration_ms,
        ROUND(MIN(duration_ms), 2) as min_duration_ms,
        ROUND(MAX(duration_ms), 2) as max_duration_ms
    FROM agent_executions
    GROUP BY agent_name, role
    ORDER BY total_calls DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Production Multi-Agent Template

# COMMAND ----------

print("=" * 70)
print("TEMPLATE: Multi-Agent Deployment on Databricks")
print("=" * 70)
print("""
# Each agent is deployed as a separate Model Serving endpoint.
# The orchestrator treats worker agents as tools.

import mlflow
from databricks.agents import ChatAgent, ChatAgentMessage, ChatAgentResponse

class OrchestratorAgent(ChatAgent):
    \"\"\"
    Supervisor agent that delegates to specialist agent endpoints.
    Each specialist runs as its own Model Serving endpoint.
    \"\"\"

    def __init__(self):
        super().__init__()
        self.worker_endpoints = {
            "sql_analyst": "sql-analyst-agent-endpoint",
            "stats_analyst": "stats-analyst-agent-endpoint",
            "report_writer": "report-writer-agent-endpoint",
        }

    def predict(self, messages):
        # 1. Decompose the request
        # 2. Call worker endpoints via HTTP
        # 3. Aggregate results
        # 4. Return final response

        response = self._orchestrate(messages)
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=response)]
        )

# Deploy orchestrator
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="orchestrator",
        python_model=OrchestratorAgent(),
        registered_model_name="catalog.schema.multi_agent_orchestrator",
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Cleanup

# COMMAND ----------

spark.catalog.dropTempView("multi_agent_sales")
spark.catalog.dropTempView("support_tickets")
spark.catalog.dropTempView("agent_executions")
print("Temporary views cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Multi-agent systems** use specialized agents that collaborate on complex tasks
# MAGIC 2. **Supervisor pattern**: one orchestrator decomposes tasks and delegates to workers
# MAGIC 3. **Debate pattern**: multiple agents analyze independently, a judge synthesizes
# MAGIC 4. **Collaboration pipeline**: agents work sequentially, each enriching the previous output
# MAGIC 5. **Role design** matters -- clear boundaries, focused tools, explicit handoffs
# MAGIC 6. **Structured messages** enable reliable inter-agent communication
# MAGIC 7. On Databricks, each agent can be a **separate Model Serving endpoint**
# MAGIC 8. **Execution traces** are critical for debugging multi-agent workflows
# MAGIC 9. Start with a **single agent** and evolve to multi-agent only when complexity demands it
