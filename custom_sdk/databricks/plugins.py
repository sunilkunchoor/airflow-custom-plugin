from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.bases.operatorlink import BaseOperatorLink
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.models import XCom
from airflow.utils.log.logging_mixin import LoggingMixin

from typing import Optional, Dict, Any

from airflow.models import BaseOperator
from airflow.models.taskinstance import TaskInstanceKey

from custom_plugins.databricks.api import databricks_app
from urllib.parse import quote
import logging

log = logging.getLogger(__name__)

class JobRunLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "🔍Job Link"

    @property
    def xcom_key(self) -> str:
        """XCom key where the link is stored during task execution."""
        return "databricks_job_run_link"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey | None = None,
        **context
    ) -> str:
        try:
            link = XCom.get_value(
                ti_key=ti_key,
                key=self.xcom_key,
            )
            return link if link else ""
        except Exception as e:
            self.log.warning("Failed to retrieve Databricks job run link from XCom: %s", e)
            return ""

class RepairJobLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "🛠️Repair"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey | None = None,
        **context
    ) -> str:
        log.info("## REPAIR PLUGIN ## Started ## ti_key: %s", ti_key)
        if ti_key:
            dag_id = quote(ti_key.dag_id)
            dag_run_id = quote(ti_key.run_id)
            job = XCom.get_value(
                ti_key=ti_key,
                key="return_value",
            )
            log.info("## REPAIR PLUGIN ## Job Details: %s", job)
            conn_id = job.get("conn_id","unknown")
            run_id = job.get("run_id","unknown")
            task_id = quote(ti_key.task_id)
            return f"/databricks/repair?dag_id={dag_id}&dag_run_id={dag_run_id}&dbk_run_id={run_id}&conn_id={conn_id}&task_id={task_id}"
        return "/databricks/repair"

class CancelJobLink(BaseOperatorLink, LoggingMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "❌Cancel"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey | None = None,
        **context
    ) -> str:
        log.info("## CANCEL PLUGIN ## Started ## ti_key: %s", ti_key)
        if ti_key:
            dag_id = quote(ti_key.dag_id)
            dag_run_id = quote(ti_key.run_id)
            job = XCom.get_value(
                ti_key=ti_key,
                key="return_value",
            )
            log.info("## CANCEL PLUGIN ## Job Details: %s", job)
            conn_id = job.get("conn_id","unknown")
            run_id = job.get("run_id","unknown")
            return f"/databricks/cancel?dag_id={dag_id}&dag_run_id={dag_run_id}&dbk_run_id={run_id}&conn_id={conn_id}"
        return "/databricks/cancel"


class DatabricksCustomPlugin(AirflowPlugin):
    name = "databricks_custom_plugin"
    
    operator_extra_links = [
        JobRunLink(),
        RepairJobLink(),
        CancelJobLink(),
    ]

    fastapi_apps = [
        {"app": databricks_app, "url_prefix": "/databricks", "name": "Databricks"}
    ]