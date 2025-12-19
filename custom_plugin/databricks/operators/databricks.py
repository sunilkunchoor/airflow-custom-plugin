from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.context import Context
from custom_plugin.databricks.hooks.databricks import DatabricksHook
from typing import Any, Optional

class DatabricksJobRunLink(BaseOperatorLink):
    name = "Databricks Job Run"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        return "https://databricks.com/"

class DatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Databricks job.
    """
    operator_extra_links = (DatabricksJobRunLink(),)
    template_fields = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        databricks_conn_id: str = "databricks_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.databricks_conn_id = databricks_conn_id


    def execute(self, context: Context) -> Any:
        hook = DatabricksHook(databricks_conn_id=self.databricks_conn_id)
        self.log.info(f"Triggering run for job: {self.job_id}")
        response = hook.run(endpoint="jobs/run-now", json={"job_id": self.job_id})
        return response

from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator

class CustomDatabricksNotebookOperator(DatabricksNotebookOperator):
    """
    Custom operator that runs a Databricks notebook.
    Supports Databricks 'For Each' tasks via `databricks_loop_inputs`.
    """
    template_fields = DatabricksNotebookOperator.template_fields + ("databricks_loop_inputs",)

    def __init__(self, *args, databricks_loop_inputs: Optional[list] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.databricks_loop_inputs = databricks_loop_inputs

    def execute(self, context: Context):
        if self.databricks_loop_inputs:
            self.log.info("Executing with Databricks For Each Task")
            hook = self.get_hook()
            payload = self._build_loop_payload()
            self.log.info(f"Payload: {payload}")
            
            # Using official DatabricksHook submit_run
            # It typically returns run_id (int or str)
            response = hook.submit_run(payload)
            
            if isinstance(response, (str, int)):
                self.databricks_run_id = response
            elif isinstance(response, dict):
                self.databricks_run_id = response.get("run_id")
            
            if self.wait_for_termination:
                self.monitor_databricks_job()
                
        else:
            super().execute(context)

    def _build_loop_payload(self):
        """Builds the payload for a For Each task run."""
        task_spec = {
            "notebook_task": {
                "notebook_path": self.notebook_path,
            }
        }
        if self.notebook_params:
            task_spec["notebook_task"]["base_parameters"] = self.notebook_params
        
        # Handle cluster config (simplified)
        if self.new_cluster:
            task_spec["new_cluster"] = self.new_cluster
        elif self.existing_cluster_id:
            task_spec["existing_cluster_id"] = self.existing_cluster_id

        # Wrap in for_each_task
        loop_task = {
            "task_key": f"{self.task_id}_loop",
            "for_each_task": {
                "inputs": self.databricks_loop_inputs,
                "task": task_spec
            }
        }
        
        payload = {
            "run_name": self.task_id,
            "tasks": [loop_task]
        }
        return payload
