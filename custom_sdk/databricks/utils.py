from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.dagbag import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import BaseOperator
from typing import Dict

def get_task_group_children(group: TaskGroup) -> Dict[str, BaseOperator]:
    children = {}
    for child_id, child in group.children.items():
        if isinstance(child, TaskGroup):
            children.update(get_task_group_children(child))
        else:
            children[child_id] = child
    return children

def get_all_downstream_ids(curr_task: BaseOperator):
    ids = set()
    for child in curr_task.downstream_list:
        if hasattr(child, 'task_id'):
            ids.add(child.task_id)
        ids.update(get_all_downstream_ids(child))
    return ids

def get_failed_databricks_tasks(session, dag_id: str, dag_run_id: str, task_group: TaskGroup):
    task_group_sub_tasks = get_task_group_children(task_group)

    repair_states = {
        TaskInstanceState.FAILED,
        TaskInstanceState.SKIPPED,
        TaskInstanceState.UP_FOR_RETRY,
        TaskInstanceState.UPSTREAM_FAILED,
        None
    }
    
    tis = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
        TaskInstance.task_id.in_(task_group_sub_tasks.keys())
    ).all()
    
    failed_tasks_ids = [ti.task_id for ti in tis if ti.state in repair_states]
    
    tasks_to_repair = []
    for tid, t in task_group_sub_tasks.items():
        if tid in failed_tasks_ids:
            if tid == f"{task_group.group_id}.launch":
                continue
            if getattr(t, 'databricks_task_key', None):
                tasks_to_repair.append(t.databricks_task_key)
                
    return tasks_to_repair, failed_tasks_ids

def clear_downstream_tasks(session, dag, dag_id: str, dag_run_id: str, failed_tasks_ids: list):
    task_ids_to_clear = set(failed_tasks_ids)
    for tid in failed_tasks_ids:
        task_ids_to_clear.update(get_all_downstream_ids(dag.get_task(tid)))

    if task_ids_to_clear:
        tis_to_clear = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == dag_run_id,
            TaskInstance.task_id.in_(task_ids_to_clear)
        ).all()
        if tis_to_clear:
            clear_task_instances(tis_to_clear, session=session)

