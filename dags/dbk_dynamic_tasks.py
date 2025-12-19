from __future__ import annotations
import requests
import os
import time
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
    DatabricksTaskOperator,
)
from custom_plugin.databricks.operators.databricks import CustomDatabricksNotebookOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator

from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.utils.timezone import datetime

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "circus_maximus")
DATABRICKS_NOTIFICATION_EMAIL = os.getenv("DATABRICKS_NOTIFICATION_EMAIL", "sunilkunchoor@gmail.com")

dag = DAG(
    dag_id="databricks_dynamic_tasks",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "databricks"],
)

with dag:

    workflow = DatabricksWorkflowTaskGroup(
        group_id=f"workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"ts": "{{ ts }}"},
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
        },
    )

    with workflow:
        notebook_1 = CustomDatabricksNotebookOperator(
            task_id="notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            databricks_loop_inputs="1,2,3"
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
        )