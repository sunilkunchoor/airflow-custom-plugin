# Databricks Repair Endpoint Walkthrough

I have implemented a new REST API endpoint `/databricks_plugin_api/repair_run` that allows you to trigger a repair (rerun) of specific tasks in a Databricks run.

## Changes
- **File**: [custom_plugin\databricks\plugins\databricks_workflow.py](file:///c:/Users/sunil/GitHub/airflow-custom-plugin/custom_plugin/databricks/plugins/databricks_workflow.py)
- **New Feature**: Added [repair_run_endpoint](file:///c:/Users/sunil/GitHub/airflow-custom-plugin/custom_plugin/databricks/plugins/databricks_workflow.py#586-657) to the `databricks_plugin_bp` blueprint.
- **Functionality**:
    - Accepts POST requests.
    - Validates required inputs: `databricks_conn_id`, `databricks_run_id`, `tasks_to_repair`.
    - Calls [_repair_task](file:///c:/Users/sunil/GitHub/airflow-custom-plugin/custom_plugin/databricks/plugins/databricks_workflow.py#228-267) to handle the Databricks API interaction.
    - Optionally calls [_clear_task_instances](file:///c:/Users/sunil/GitHub/airflow-custom-plugin/custom_plugin/databricks/plugins/databricks_workflow.py#164-207) if `dag_id` and `run_id` are provided, to sync the Airflow state.

## Verification Steps

Since I cannot connect to your Databricks instance, please perform the following manual verification:

### 1. Verify Endpoint Registration
Ensure the plugin is loaded by Airflow. You can check the Airflow logs during startup for any import errors.

### 2. Test with cURL
You can test the endpoint using `curl` from a terminal that has access to your Airflow webserver.

**Basic Repair (Databricks only):**
```bash
curl -X POST http://<YOUR_AIRFLOW_HOST>/databricks_plugin_api/repair_run \
  -H "Content-Type: application/json" \
  -d '{
    "databricks_conn_id": "databricks_default",
    "databricks_run_id": <YOUR_DATABRICKS_RUN_ID>,
    "tasks_to_repair": ["task_key_1", "task_key_2"]
  }'
```

**Repair and Clear Airflow Tasks:**
```bash
curl -X POST http://<YOUR_AIRFLOW_HOST>/databricks_plugin_api/repair_run \
  -H "Content-Type: application/json" \
  -d '{
    "databricks_conn_id": "databricks_default",
    "databricks_run_id": <YOUR_DATABRICKS_RUN_ID>,
    "tasks_to_repair": ["task_key_1"],
    "dag_id": "<YOUR_DAG_ID>",
    "run_id": "<YOUR_AIRFLOW_RUN_ID>"
  }'
```

### 3. Check Responses
- **Success (200)**: Should return JSON with `repair_id` and status.
- **Error (400/500)**: Should return a JSON error message if parameters are missing or the repair fails.

### 4. Test Repair All Failed (New)
This endpoint automatically detects failed or skipped tasks in a task group and triggers a repair.

```bash
curl -X POST http://<YOUR_AIRFLOW_HOST>/databricks_plugin_api/repair_all_failed \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "<YOUR_DAG_ID>",
    "run_id": "<YOUR_AIRFLOW_RUN_ID>",
    "task_group_id": "<YOUR_TASK_GROUP_ID>"
  }'
```
