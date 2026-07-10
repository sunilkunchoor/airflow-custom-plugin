# Databricks Custom SDK for Airflow

This module provides custom extensions, hooks, and operators for integrating Apache Airflow with Databricks.

## Overview
This package is part of the `custom_sdk` for Airflow. It registers a custom Airflow Plugin (`DatabricksCustomPlugin`) to enhance out-of-the-box Databricks functionality.

## Features
- **Databricks Workflow Task Groups**: Group Databricks tasks efficiently (`DatabricksWorkflowTaskGroup`).
- **Custom Operators**: Wrappers and enhancements for `DatabricksNotebookOperator` and `DatabricksTaskOperator`.
- **API and Hook Enhancements**: Custom logic for interacting with the Databricks API via `custom_sdk.databricks.api` and `custom_sdk.databricks.hooks`.

## Usage
The plugin is automatically registered via the `pyproject.toml` entry points. You can use the custom task groups and operators directly in your DAGs:

```python
from custom_sdk.databricks.operators import (
    DatabricksNotebookOperator,
    DatabricksTaskOperator
)
from custom_sdk.databricks.plugins import DatabricksWorkflowTaskGroup
```

See the `dags/` directory for full examples like `dbk_v3_workflow.py`.
