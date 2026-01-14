# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from databricks.sdk.service.jobs import JobSettings as Job
governBot = Job.from_dict(
    {
        "name": "governBot",
        "tasks": [
            {
                "task_key": "watcher",
                "notebook_task": {
                    "notebook_path": "/Workspace/ingestion_ws/governBot/05_watcher",
                    "source": "WORKSPACE",
                },
                "min_retry_interval_millis": 900000,
                "disable_auto_optimization": True,
                "environment_key": "Default",
            },
            {
                "task_key": "SyncChanges",
                "depends_on": [
                    {
                        "task_key": "watcher",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/ingestion_ws/governBot/04a_sync_approved_changes",
                    "source": "WORKSPACE",
                },
                "environment_key": "Default",
            },
            {
                "task_key": "remediation",
                "depends_on": [
                    {
                        "task_key": "watcher",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/ingestion_ws/governBot/06_remediation",
                    "source": "WORKSPACE",
                },
                "min_retry_interval_millis": 900000,
                "disable_auto_optimization": True,
                "environment_key": "Default",
            },
            {
                "task_key": "validation",
                "depends_on": [
                    {
                        "task_key": "remediation",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/ingestion_ws/governBot/07_validation",
                    "source": "WORKSPACE",
                },
                "min_retry_interval_millis": 900000,
                "disable_auto_optimization": True,
                "environment_key": "Default",
            },
        ],
        "queue": {
            "enabled": True,
        },
        "parameters": [
            {
                "name": "dry_run",
                "default": "false",
            },
            {
                "name": "lookback_hours",
                "default": "24",
            },
            {
                "name": "catalog",
                "default": "qadl",
            },
            {
                "name": "schema",
                "default": "sch_mng_admon",
            },
            {
                "name": "show_query",
                "default": "Y",
            },
            {
                "name": "sync_creations",
                "default": "Y",
            },
            {
                "name": "sync_permissions",
                "default": "Y",
            },
        ],
        "environments": [
            {
                "environment_key": "Default",
                "spec": {
                    "environment_version": "4",
                },
            },
        ],
        "budget_policy_id": "01b92416-62be-44be-ac02-997f100bcae6",
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
#w.jobs.reset(new_settings=governBot, job_id=355417892888347)
w.jobs.create(**governBot.as_shallow_dict())
