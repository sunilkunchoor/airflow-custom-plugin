
def get_provider_info():
    return {
        "package-name": "custom_plugin",
        "name": "Databricks",
        "description": "Databricks Provider",
        "versions": ["1.0.0"],
        "integrations": [
            {
                "integration-name": "Databricks",
                "external-doc-url": "https://databricks.com/",
                "tags": ["service"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Databricks",
                "python-modules": ["custom_plugin.databricks.hooks.databricks"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "custom_plugin.databricks.hooks.databricks.DatabricksHook",
                "connection-type": "databricks",
            }
        ],
        "plugins": [
            {
                "name": "databricks_workflow",
                "plugin-class": "custom_plugin.databricks.plugins.databricks_workflow.DatabricksWorkflowPlugin",
            }
        ],
        "extra-links": [
            "custom_plugin.databricks.operators.databricks.DatabricksJobRunLink",
        ],
    }
