from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import logging
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from urllib.parse import quote
from custom_plugins.databricks.utils import (
    get_task_group_children,
    get_all_downstream_ids,
    get_failed_databricks_tasks,
    clear_downstream_tasks
)

log = logging.getLogger(__name__)

databricks_app = FastAPI(title="Databricks Related API", version="1.0.0")

@databricks_app.get("/cancel")
async def cancel(
    dag_id: str = None,
    dag_run_id: str = None,
    dbk_run_id: str = None, 
    conn_id: str = None
):
    log.info("## CANCEL API ## Started ## dag_id: %s", dag_id)
    log.info("## CANCEL API ## Started ## dag_run_id: %s", dag_run_id)
    log.info("## CANCEL API ## Started ## dbk_run_id: %s", dbk_run_id)
    log.info("## CANCEL API ## Started ## conn_id: %s", conn_id)

    if not (dag_id and dbk_run_id and conn_id):
        return {"error": "Something went wrong in the cancel api"}
        
    hook = DatabricksHook(databricks_conn_id=conn_id)
    cancel = hook.cancel_run(run_id=dbk_run_id)
    log.info("## CANCEL API ## Cancelled the job ## cancel: %s", cancel)
    
    dag_id = quote(dag_id)
    dag_run_id = quote(dag_run_id)
    redirect_url = f"/dags/{dag_id}/runs/{dag_run_id}"
    log.info("## CANCEL API ## Cancelled the job ## cancel: %s", cancel)
    return RedirectResponse(url=redirect_url)

@databricks_app.get("/repair")
def repair(
    dag_id: str = None,
    dag_run_id: str = None,
    dbk_run_id: str = None, 
    conn_id: str = None,
    task_id: str = None
):
    if not (dag_id and dbk_run_id and conn_id and task_id):
        return {"error": "Something went wrong"}

    from airflow.models.dagbag import DagBag
    from airflow.utils.session import create_session

    with create_session() as session:
        dagbag = DagBag()
        dag = dagbag.get_dag(dag_id)
        if not dag or not dag.has_task(task_id):
            return {"error": "DAG or task not found."}

        task = dag.get_task(task_id)
        task_group = task.task_group
        
        if not task_group:
            return {"error": "Task is not part of a task group."}

        tasks_to_repair, failed_tasks_ids = get_failed_databricks_tasks(session, dag_id, dag_run_id, task_group)

        if tasks_to_repair:
            hook = DatabricksHook(databricks_conn_id=conn_id)
            try:
                repair_history_id = hook.get_latest_repair_id(int(dbk_run_id))
                repair_json = {
                    "run_id": int(dbk_run_id),
                    "latest_repair_id": repair_history_id,
                    "rerun_tasks": tasks_to_repair,
                }
                hook.repair_run(repair_json)
                log.info("## REPAIR API ## Sent repair query for run %s: %s", dbk_run_id, tasks_to_repair)
            except Exception as e:
                log.error("## REPAIR API ## Failed to repair run: %s", e)
        
        clear_downstream_tasks(session, dag, dag_id, dag_run_id, failed_tasks_ids)

    encoded_dag_id = quote(dag_id)
    encoded_dag_run_id = quote(dag_run_id)
    redirect_url = f"/dags/{encoded_dag_id}/runs/{encoded_dag_run_id}"
    return RedirectResponse(url=redirect_url)