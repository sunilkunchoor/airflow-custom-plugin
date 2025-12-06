from __future__ import annotations

import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
    DatabricksTaskOperator,
)
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.utils.timezone import datetime

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", str(6)))
DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "circus_maximus")
DATABRICKS_NOTIFICATION_EMAIL = os.getenv("DATABRICKS_NOTIFICATION_EMAIL", "sunilkunchoor@gmail.com")
GROUP_ID = os.getenv("DATABRICKS_GROUP_ID", "1234").replace(".", "_")
USER = os.environ.get("USER","sunil")

dag = DAG(
    dag_id="databricks_workflow",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "databricks"],
)

with dag:
    task_group = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID}",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"ts": "{{ ts }}"},
        notebook_packages=[
            {
                "pypi": {
                    "package": "simplejson==3.18.0",  # Pin specification version of a package like this.
                    "repo": "https://pypi.org/simple",  # You can specify your required Pypi index here.
                }
            },
        ],
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
        },
    )

    with task_group:
        notebook_1 = DatabricksNotebookOperator(
            task_id="workflow_notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            notebook_packages=[{"pypi": {"package": "Faker"}}],
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
            execution_timeout=timedelta(seconds=600),
        )

        notebook_2 = DatabricksNotebookOperator(
            task_id="workflow_notebook_2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_1 >> notebook_2