from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
)
from airflow.providers.databricks.operators.databricks_workflow import (
    _CreateDatabricksWorkflowOperator,
    DatabricksWorkflowTaskGroup,
    _flatten_node
)

from custom_plugins.databricks.workflow_plugins import JobRunLink, RepairJobLink, CancelJobLink

from types import TracebackType


class DatabricksWorkflow(DatabricksWorkflowTaskGroup):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def __exit__(
        self, _type: type[BaseException] | None, _value: BaseException | None, _tb: TracebackType | None
    ) -> None:
        """Exit the context manager and add tasks to a single ``_CreateDatabricksWorkflowOperator``."""
        roots = list(self.get_roots())
        tasks = _flatten_node(self)

        launch = _WorkflowOperator(
            dag=self.dag,
            task_group=self,
            task_id="launch",
            databricks_conn_id=self.databricks_conn_id,
            extra_job_params=self.extra_job_params,
            job_clusters=self.job_clusters,
            max_concurrent_runs=self.max_concurrent_runs,
            notebook_params=self.notebook_params,
        )

        for task in tasks:
            if not (
                hasattr(task, "_convert_to_databricks_workflow_task")
                and callable(task._convert_to_databricks_workflow_task)
            ):
                raise AirflowException(
                    f"Task {task.task_id} does not support conversion to databricks workflow task."
                )

            task.workflow_run_metadata = launch.output
            task.databricks_conn_id = self.databricks_conn_id
            launch.relevant_upstreams.append(task.task_id)
            launch.add_task(task.task_id, task)

        for root_task in roots:
            root_task.set_upstream(launch)

        super(DatabricksWorkflowTaskGroup, self).__exit__(_type, _value, _tb)


class _WorkflowOperator(_CreateDatabricksWorkflowOperator):

    operator_extra_links = (
        JobRunLink(),
        RepairJobLink(),
        CancelJobLink(),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class DatabricksTask(DatabricksNotebookOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.databricks_task_key = f"{self.dag_id}__{self.task_id.replace(".", "__")}"