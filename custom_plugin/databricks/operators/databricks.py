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
