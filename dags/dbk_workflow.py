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
USER = os.environ.get("USER","sunil")

GROUP_ID_1 = "1234"
GROUP_ID_2 = "5678"
GROUP_ID_3 = "9101"

dag = DAG(
    dag_id="databricks_workflow",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "databricks"],
)

def track_databricks_workflow(**context):

    ti = context['ti']  # TaskInstance object
    
    if ti.xcom_pull(key='status', task_ids='track_workflow') is None:
        
        # Sleep untill the workflow started.
        time.sleep(30)
        
        print(f"DAG ID: {context['dag'].dag_id}")
        print(f"TASK ID: {context['task'].task_id}")
        print(f"RUN ID: {context['dag_run'].run_id}")
        print(f"DAG RUN OBJECT: {context['dag_run']}")

        context['ti'].xcom_push(key='status', value="Done")

    else:

        print(f"DAG ID: {context['dag'].dag_id}")
        print(f"TASK ID: {context['task'].task_id}")
        print(f"RUN ID: {context['dag_run'].run_id}")
        print(f"DAG RUN OBJECT: {context['dag_run']}")

        workflows = [
            "test_workflow_sunil_1234",
            "test_workflow_sunil_5678",
            "test_workflow_sunil_9101",
        ]

        clear_tasks = []

        for workflow in workflows:
            
            print(f"Repairing {workflow}")

            response_1 = requests.post(
                url = f"http://192.168.0.19:8080/databricks_plugin_api/repair_all_failed",
                json={
                    "dag_id": context['dag'].dag_id,
                    "run_id": context['dag_run'].run_id,
                    "task_group_id": "test_workflow_sunil_1234",
                },
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                auth=requests.auth.HTTPBasicAuth("airflow", "airflow"),
            )
            print(f"Repairing {workflow} from Databricks: {response_1.json()}")
            if response_1.status_code != 200:
                clear_tasks.append(f"{workflow}.launch")


        time.sleep(30)

        response_2 = requests.post(
            url = f"http://192.168.0.19:8080/api/v1/dags/{context['dag'].dag_id}/clearTaskInstances",
            json={
                "dag_run_id": context['dag_run'].run_id,
                "dry_run": False,
                "only_failed": True,
                "include_downstream": True,
                "task_ids": clear_tasks,
            },
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            auth=requests.auth.HTTPBasicAuth("airflow", "airflow"),
        )
        print(f"RUN Status: {response_2.json()}")
        


with dag:

    tracker = PythonOperator(
        task_id="track_workflow",
        python_callable=track_databricks_workflow,
        provide_context=True,
    )


    task_group_1 = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID_1}",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params={"ts": "{{ ts }}"},
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
        },
    )

    with task_group_1:
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
    
    task_group_2 = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID_2}",
        databricks_conn_id=DATABRICKS_CONN_ID,
    )

    with task_group_2:
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

        notebook_1 >> notebook_2
    
    task_group_3 = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID_3}",
        databricks_conn_id=DATABRICKS_CONN_ID,
    )

    with task_group_3:
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

    task_group_1 >> task_group_2 >> task_group_3