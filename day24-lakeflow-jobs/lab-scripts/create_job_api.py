"""
create_job_api.py -- Create a multi-task job using the Databricks SDK.

This script demonstrates how to programmatically create a production-grade
e-commerce pipeline job with:
  - 4 tasks arranged in a DAG (ingest -> transform -> report + notify)
  - Per-task retry policies
  - Email notifications on failure
  - Scheduled trigger (daily at 2 AM UTC)

Prerequisites:
  - pip install databricks-sdk
  - DATABRICKS_HOST and DATABRICKS_TOKEN environment variables set
    (or run from within a Databricks notebook for automatic auth)

Usage:
  python create_job_api.py
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobEmailNotifications,
    NotebookTask,
    PauseStatus,
    Source,
    SqlTask,
    SqlTaskQuery,
    Task,
    TaskDependency,
)


def create_ecommerce_pipeline_job(
    workspace_client: WorkspaceClient,
    notebook_root: str = "/Workspace/Repos/data-engineering/ecommerce-pipeline",
    warehouse_id: str = "your_warehouse_id",
    notification_emails: list = None,
) -> int:
    """Create the e-commerce daily pipeline job.

    Args:
        workspace_client: Authenticated WorkspaceClient.
        notebook_root: Base path for task notebooks in the workspace.
        warehouse_id: SQL warehouse ID for the SQL task.
        notification_emails: List of emails for failure notifications.

    Returns:
        The created job ID.
    """
    if notification_emails is None:
        notification_emails = ["data-engineering-team@company.com"]

    job = workspace_client.jobs.create(
        name="ecommerce_daily_pipeline",
        tasks=[
            # ── Task 1: Ingest raw orders from S3 ──
            Task(
                task_key="ingest_orders",
                description="Ingest raw order data from S3 into ecommerce.bronze.orders",
                notebook_task=NotebookTask(
                    notebook_path=f"{notebook_root}/task_ingest_orders",
                    source=Source.WORKSPACE,
                    base_parameters={
                        "catalog": "ecommerce",
                        "source_path": "s3://ecommerce-lakehouse/raw/orders/",
                        "target_table": "ecommerce.bronze.orders",
                    },
                ),
                environment_key="Default",
                max_retries=3,
                min_retry_interval_millis=60000,  # 1 minute between retries
            ),
            # ── Task 2: Run SDP pipeline (Bronze -> Silver -> Gold) ──
            Task(
                task_key="transform_pipeline",
                description="Run Spark Declarative Pipeline for Bronze -> Silver -> Gold",
                depends_on=[TaskDependency(task_key="ingest_orders")],
                notebook_task=NotebookTask(
                    notebook_path=f"{notebook_root}/transform_pipeline",
                    source=Source.WORKSPACE,
                ),
                environment_key="Default",
                max_retries=2,
                min_retry_interval_millis=120000,  # 2 minutes between retries
            ),
            # ── Task 3: Generate daily revenue report (SQL) ──
            Task(
                task_key="daily_revenue_report",
                description="Aggregate daily revenue by store from the Gold layer",
                depends_on=[TaskDependency(task_key="transform_pipeline")],
                sql_task=SqlTask(
                    warehouse_id=warehouse_id,
                    query=SqlTaskQuery(query_id="your_query_id"),
                ),
                max_retries=1,
            ),
            # ── Task 4: Send completion notification ──
            Task(
                task_key="send_notification",
                description="Notify the team that the pipeline completed successfully",
                depends_on=[TaskDependency(task_key="transform_pipeline")],
                notebook_task=NotebookTask(
                    notebook_path=f"{notebook_root}/send_notification",
                    source=Source.WORKSPACE,
                    base_parameters={
                        "channel": "#data-engineering",
                        "message": "E-commerce daily pipeline completed",
                    },
                ),
                environment_key="Default",
                max_retries=1,
            ),
        ],
        # ── Schedule: daily at 2 AM UTC ──
        schedule=CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",
            timezone_id="UTC",
            pause_status=PauseStatus.PAUSED,  # Start paused; unpause when ready
        ),
        # ── Notifications ──
        email_notifications=JobEmailNotifications(
            on_failure=notification_emails,
            on_success=notification_emails,
        ),
        # ── Tags ──
        tags={
            "team": "data-engineering",
            "domain": "ecommerce",
            "env": "dev",
            "managed_by": "databricks-sdk",
        },
    )

    return job.job_id


def main():
    """Entry point: create the job and print the result."""
    w = WorkspaceClient()

    print("Creating e-commerce pipeline job...")
    job_id = create_ecommerce_pipeline_job(workspace_client=w)
    print(f"Job created successfully!")
    print(f"  Job ID:  {job_id}")
    print(f"  Job URL: {w.config.host}/#job/{job_id}")
    print()
    print("Next steps:")
    print("  1. Deploy task notebooks to the workspace")
    print("  2. Update warehouse_id and query_id in the SQL task")
    print("  3. Unpause the schedule when ready for production")


if __name__ == "__main__":
    main()
