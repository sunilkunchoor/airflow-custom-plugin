from airflow.plugins_manager import AirflowPlugin
from custom_plugin.databricks.hooks.databricks import DatabricksHook
from custom_plugin.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksJobRunLink

class DatabricksWorkflowPlugin(AirflowPlugin):
    name = "databricks_workflow"
    hooks = [DatabricksHook]
    operators = [DatabricksRunNowOperator]
    operator_extra_links = [DatabricksJobRunLink()]
