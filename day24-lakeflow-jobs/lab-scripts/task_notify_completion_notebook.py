# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Task: Pipeline Completion Notification
# MAGIC
# MAGIC **Objective**: Demonstrate a notification task that runs at the end of a Lakeflow Job
# MAGIC DAG. It reads task values from upstream tasks, builds a run summary, and shows how
# MAGIC to integrate with Slack, email, and webhook alerting.
# MAGIC
# MAGIC **Key Insight**: Notification tasks consume `taskValues` set by upstream tasks. This
# MAGIC lets you build rich, data-driven alerts ("Pipeline processed 10,000 records with 3
# MAGIC DQ failures") instead of generic "job succeeded" messages.
# MAGIC
# MAGIC **Usage in a Lakeflow Job**:
# MAGIC ```
# MAGIC Task type:   Notebook
# MAGIC Depends on:  daily_revenue_report
# MAGIC Parameters:  channel, environment
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC ingest_orders ──> transform_pipeline ──> daily_report ──> [this task]
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Parameters

# COMMAND ----------

dbutils.widgets.text("channel", "#data-engineering", "Slack Channel")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("job_run_id", "interactive", "Job Run ID")

channel = dbutils.widgets.get("channel")
environment = dbutils.widgets.get("environment")
job_run_id = dbutils.widgets.get("job_run_id")

print(f"Notification Task")
print(f"  Channel:     {channel}")
print(f"  Environment: {environment}")
print(f"  Job Run ID:  {job_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read Upstream Task Values
# MAGIC
# MAGIC `dbutils.jobs.taskValues.get()` retrieves values set by upstream tasks.
# MAGIC The first argument is the task key, the second is the value key, and the
# MAGIC third is a default (used when running interactively outside a job).

# COMMAND ----------

# Read task values from the transform_pipeline task
# Defaults are used when running this notebook interactively (outside a job)
bronze_count = dbutils.jobs.taskValues.get(
    taskKey="transform_pipeline",
    key="bronze_count",
    default=200,
    debugValue=200,
)
silver_count = dbutils.jobs.taskValues.get(
    taskKey="transform_pipeline",
    key="silver_count",
    default=195,
    debugValue=195,
)
dq_failed_count = dbutils.jobs.taskValues.get(
    taskKey="transform_pipeline",
    key="dq_failed_count",
    default=3,
    debugValue=3,
)
gold_revenue_rows = dbutils.jobs.taskValues.get(
    taskKey="transform_pipeline",
    key="gold_revenue_rows",
    default=5,
    debugValue=5,
)
run_date = dbutils.jobs.taskValues.get(
    taskKey="transform_pipeline",
    key="run_date",
    default="2025-06-15",
    debugValue="2025-06-15",
)

print("Upstream task values received:")
print(f"  Bronze records ingested:  {bronze_count}")
print(f"  Silver records produced:  {silver_count}")
print(f"  DQ failures:              {dq_failed_count}")
print(f"  Gold revenue rows:        {gold_revenue_rows}")
print(f"  Run date:                 {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Build Run Summary
# MAGIC
# MAGIC Assemble a structured summary from the upstream task values.
# MAGIC This is the payload that gets sent to Slack, email, or a webhook.

# COMMAND ----------

from datetime import datetime

# Calculate data quality pass rate
dq_pass_rate = round((silver_count - dq_failed_count) / silver_count * 100, 1) if silver_count > 0 else 0.0

# Determine alert severity based on DQ results
if dq_failed_count == 0:
    severity = "INFO"
    status_emoji = "white_check_mark"
elif dq_failed_count / silver_count < 0.05:
    severity = "WARNING"
    status_emoji = "warning"
else:
    severity = "CRITICAL"
    status_emoji = "rotating_light"

# Build the summary payload
run_summary = {
    "job_name": "ecommerce_daily_pipeline",
    "job_run_id": job_run_id,
    "run_date": run_date,
    "environment": environment,
    "completed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    "severity": severity,
    "metrics": {
        "bronze_records": bronze_count,
        "silver_records": silver_count,
        "dq_failures": dq_failed_count,
        "dq_pass_rate": f"{dq_pass_rate}%",
        "gold_revenue_rows": gold_revenue_rows,
    },
}

print("Run Summary:")
for key, value in run_summary.items():
    if isinstance(value, dict):
        print(f"  {key}:")
        for k, v in value.items():
            print(f"    {k}: {v}")
    else:
        print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Format Notification Messages
# MAGIC
# MAGIC Build formatted messages for each notification channel. In production, these
# MAGIC would be sent via API calls. Here we demonstrate the message format.

# COMMAND ----------

# Slack message (Block Kit format)
slack_message = {
    "channel": channel,
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":{status_emoji}: Pipeline Run Complete — {environment.upper()}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Job:*\n{run_summary['job_name']}"},
                {"type": "mrkdwn", "text": f"*Run Date:*\n{run_date}"},
                {"type": "mrkdwn", "text": f"*Run ID:*\n{job_run_id}"},
                {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Bronze Records:*\n{bronze_count:,}"},
                {"type": "mrkdwn", "text": f"*Silver Records:*\n{silver_count:,}"},
                {"type": "mrkdwn", "text": f"*DQ Pass Rate:*\n{dq_pass_rate}%"},
                {"type": "mrkdwn", "text": f"*Gold Rows:*\n{gold_revenue_rows}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Completed at {run_summary['completed_at']}",
                },
            ],
        },
    ],
}

print("Slack Message Payload (Block Kit):")
import json
print(json.dumps(slack_message, indent=2))

# COMMAND ----------

# Email message (HTML format)
email_subject = f"[{severity}] E-Commerce Pipeline — {environment.upper()} — {run_date}"

email_body = f"""
<html>
<body style="font-family: Arial, sans-serif; color: #333;">
<h2>E-Commerce Daily Pipeline Report</h2>
<table style="border-collapse: collapse; width: 400px;">
  <tr style="background-color: #f2f2f2;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Environment</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{environment}</td>
  </tr>
  <tr>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Run Date</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{run_date}</td>
  </tr>
  <tr style="background-color: #f2f2f2;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Bronze Records</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{bronze_count:,}</td>
  </tr>
  <tr>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Silver Records</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{silver_count:,}</td>
  </tr>
  <tr style="background-color: #f2f2f2;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>DQ Pass Rate</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{dq_pass_rate}%</td>
  </tr>
  <tr>
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Gold Revenue Rows</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{gold_revenue_rows}</td>
  </tr>
  <tr style="background-color: #f2f2f2;">
    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Severity</strong></td>
    <td style="padding: 8px; border: 1px solid #ddd;">{severity}</td>
  </tr>
</table>
<p style="font-size: 12px; color: #999;">
  Job Run ID: {job_run_id} | Completed: {run_summary['completed_at']}
</p>
</body>
</html>
"""

print(f"Email Subject: {email_subject}")
print(f"Email Body Length: {len(email_body)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Send Notifications (Production Patterns)
# MAGIC
# MAGIC Below are production-ready patterns for sending notifications. Each is wrapped
# MAGIC in a function you can adapt to your environment.
# MAGIC
# MAGIC > **Note**: API calls are commented out since they require real credentials.
# MAGIC > Uncomment and configure for your workspace.

# COMMAND ----------

import json

# -- Pattern 1: Slack via Incoming Webhook --
def send_slack_notification(webhook_url: str, message: dict) -> None:
    """Send a Slack notification via an incoming webhook.

    In production, store the webhook URL in a Databricks secret scope:
        webhook_url = dbutils.secrets.get("notifications", "slack_webhook_url")
    """
    import urllib.request

    req = urllib.request.Request(
        url=webhook_url,
        data=json.dumps(message).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # urllib.request.urlopen(req)


# -- Pattern 2: Webhook (generic HTTP POST) --
def send_webhook_notification(webhook_url: str, payload: dict) -> None:
    """Send a JSON payload to a generic webhook endpoint.

    Works with PagerDuty, Opsgenie, Microsoft Teams, or any HTTP endpoint.
    """
    import urllib.request

    req = urllib.request.Request(
        url=webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # urllib.request.urlopen(req)


# -- Pattern 3: Write summary to a Delta audit table --
def write_audit_record(spark, summary: dict, table: str) -> None:
    """Persist the run summary to a Delta audit table for historical tracking.

    This is the most reliable notification method -- it survives even if
    Slack or email delivery fails.
    """
    from pyspark.sql import Row

    row = Row(**{k: json.dumps(v) if isinstance(v, dict) else str(v) for k, v in summary.items()})
    audit_df = spark.createDataFrame([row])
    # audit_df.write.format("delta").mode("append").saveAsTable(table)


print("Notification functions defined (uncomment API calls for production use)")
print()
print("Production setup checklist:")
print("  1. Store webhook URLs in Databricks secret scope")
print("  2. Create audit table: CREATE TABLE catalog.ops.pipeline_audit_log (...)")
print("  3. Grant USAGE and INSERT on the audit table to the job service principal")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Demonstrate the Notification Flow
# MAGIC
# MAGIC Simulate what happens in production by calling the functions with mock endpoints.

# COMMAND ----------

# Simulate sending notifications (no actual HTTP calls)
print("=" * 60)
print("NOTIFICATION SIMULATION")
print("=" * 60)
print()

# Slack
print(f"[1/3] Slack -> {channel}")
print(f"       Header: Pipeline Run Complete — {environment.upper()}")
print(f"       Severity: {severity}")
# send_slack_notification("https://hooks.slack.com/services/YOUR/WEBHOOK/URL", slack_message)
print("       Status: SIMULATED (uncomment for production)")
print()

# Webhook (e.g., PagerDuty for critical alerts)
if severity == "CRITICAL":
    print("[2/3] PagerDuty -> Triggering incident")
    pagerduty_payload = {
        "routing_key": "YOUR_ROUTING_KEY",
        "event_action": "trigger",
        "payload": {
            "summary": f"DQ failure rate exceeded threshold: {dq_failed_count} failures",
            "severity": "critical",
            "source": f"lakeflow/{run_summary['job_name']}",
        },
    }
    # send_webhook_notification("https://events.pagerduty.com/v2/enqueue", pagerduty_payload)
    print("       Status: SIMULATED")
else:
    print(f"[2/3] PagerDuty -> Skipped (severity={severity}, threshold=CRITICAL)")
print()

# Audit table
print("[3/3] Audit Table -> catalog.ops.pipeline_audit_log")
print(f"       Record: {json.dumps(run_summary, default=str)[:100]}...")
# write_audit_record(spark, run_summary, f"{catalog}.ops.pipeline_audit_log")
print("       Status: SIMULATED")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Conditional Alerting with If/Else Tasks
# MAGIC
# MAGIC In a Lakeflow Job, you can use **If/Else condition tasks** to branch the DAG
# MAGIC based on task values. This is more powerful than in-notebook conditionals because
# MAGIC it skips entire downstream task branches.
# MAGIC
# MAGIC ```
# MAGIC transform_pipeline ──> check_dq_threshold (If/Else)
# MAGIC                            │
# MAGIC                    ┌───────┴────────┐
# MAGIC                    │                │
# MAGIC              condition met     condition not met
# MAGIC                    │                │
# MAGIC             ┌──────▼──────┐   ┌─────▼──────┐
# MAGIC             │ alert_oncall│   │ log_success│
# MAGIC             │ (PagerDuty) │   │ (audit)    │
# MAGIC             └─────────────┘   └────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Configuring the If/Else Task
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk.service.jobs import (
# MAGIC     Task, ConditionTask, TaskDependency,
# MAGIC )
# MAGIC
# MAGIC # If/Else task that checks DQ failure count
# MAGIC Task(
# MAGIC     task_key="check_dq_threshold",
# MAGIC     condition_task=ConditionTask(
# MAGIC         # Expression evaluates to true/false
# MAGIC         # References a task value from the transform task
# MAGIC         op="GREATER_THAN",
# MAGIC         left="{{tasks.transform_pipeline.values.dq_failed_count}}",
# MAGIC         right="10",
# MAGIC     ),
# MAGIC     depends_on=[TaskDependency(task_key="transform_pipeline")],
# MAGIC ),
# MAGIC # Runs only when condition is TRUE (dq_failed_count > 10)
# MAGIC Task(
# MAGIC     task_key="alert_oncall",
# MAGIC     depends_on=[TaskDependency(
# MAGIC         task_key="check_dq_threshold",
# MAGIC         outcome="true",
# MAGIC     )],
# MAGIC     notebook_task=NotebookTask(...),
# MAGIC ),
# MAGIC # Runs only when condition is FALSE
# MAGIC Task(
# MAGIC     task_key="log_success",
# MAGIC     depends_on=[TaskDependency(
# MAGIC         task_key="check_dq_threshold",
# MAGIC         outcome="false",
# MAGIC     )],
# MAGIC     notebook_task=NotebookTask(...),
# MAGIC ),
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

dbutils.widgets.removeAll()

print("Notification task complete")
print(f"  Severity: {severity}")
print(f"  DQ Pass Rate: {dq_pass_rate}%")
print(f"  Channels notified: Slack ({channel}), Audit Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Task values** (`dbutils.jobs.taskValues`) pass data between tasks in a DAG
# MAGIC 2. **Rich notifications** use upstream metrics, not just pass/fail status
# MAGIC 3. **Multiple channels**: Slack, email, PagerDuty, webhooks, and Delta audit tables
# MAGIC 4. **Conditional DAG branches** (If/Else tasks) route alerts based on severity
# MAGIC 5. **Secret scopes** keep webhook URLs and API keys out of notebook code
# MAGIC 6. **Audit tables** provide durable, queryable history of every pipeline run
