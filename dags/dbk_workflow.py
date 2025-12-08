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
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.operators.python import PythonOperator

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

def track_databricks_workflow(**context):
    hook = DatabricksHook(DATABRICKS_CONN_ID)
    job_name = "dbk_workflow"
    
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"TASK ID: {context['task'].task_id}")
    print(f"RUN ID: {context['dag_run'].run_id}")
    print(f"DAG RUN OBJECT: {context['dag_run']}")
    
    time.sleep(30)

    dag_id = context['dag'].dag_id
    dag_run_id = context['dag_run'].run_id    

    workflow = context['ti'].xcom_pull(task_ids='workflow.launch', key='return_value')
    dbk_run_id = workflow['run_id']
    print(f"DAG RUN ID FROM X_COM: {dbk_run_id}")
    print(f"RUN Status: {hook.get_run_status(dbk_run_id)}")

    repair_history_id = hook.get_latest_repair_id(dbk_run_id)
    print(f"REPAIR HISTORY ID: {repair_history_id}")


    # response = requests.post(
    #     url = f"https://192.168.0.19:8080/databricks_plugin_api/repair_run",
    #     json={
    #         "databricks_conn_id": DATABRICKS_CONN_ID,
    #         "databricks_run_id": dbk_run_id,
    #         "dag_id": dag_id,
    #         "run_id": dag_run_id,
    #         "tasks_to_repair": "",
    #     },
    #     auth=requests.auth.HTTPBasicAuth("airflow", "airflow"),
    # )
    # print(f"RUN Status: {response.json()}")


with dag:

    tracker = PythonOperator(
        task_id="track_databricks_workflow",
        python_callable=track_databricks_workflow,
        provide_context=True,
    )


    task_group = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID}",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"ts": "{{ ts }}"},
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
            source="WORKSPACE",
            execution_timeout=timedelta(seconds=600),
        )

        notebook_2 = DatabricksNotebookOperator(
            task_id="workflow_notebook_2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_3 = DatabricksNotebookOperator(
            task_id="workflow_notebook_3",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

        notebook_4 = DatabricksNotebookOperator(
            task_id="workflow_notebook_4",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Users/sunilkunchoor@gmail.com/simple_notebook",
            source="WORKSPACE",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )
        
        notebook_1 >> notebook_2 >> [notebook_3, notebook_4]