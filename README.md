# airflow-custom-plugin
**Goal:** Adding a custom plugin to apache Airflow for Databricks.

**Description:**


# Bash Commands

## Run Docker Compose and Build
```bash
docker-compose up --build
```

## Stop Docker Compose
```bash
docker-compose down
```

## Remove Docker Volumes, Images and Networks
```bash
docker-compose down --volumes --rmi all --remove-orphans
```



# cURL Commands

## Simple cURL command using Basic Auth
```bash
curl -X GET 'http://localhost:8080/api/v1/dags' --user "airflow:airflow"
```

## Get Airflow Webserver Health
```bash
curl http://localhost:8080/health
```

## Custom API Endpoint Test

```bash
curl -X GET 'http://localhost:8080/databricks_plugin_api/test' --user "airflow:airflow"
```

## Repair Airflow DAG from Databricks Plugin

```bash
curl -X POST 'http://localhost:8080/databricks_plugin_api/repair_run' -H "Content-Type: application/json" --user "airflow:airflow" -d '{ "databricks_conn_id": "<DATABRICKS_CONN_ID>", "databricks_run_id": "<DATABRICKS_RUN_ID>", "tasks_to_repair": ["<TASK_KEY_1>", "<TASK_KEY_2>"],"dag_id": "<DAG_ID>", "run_id": "<RUN_ID>" }'
```

```bash
curl -X POST 'http://192.168.0.19:8080/databricks_plugin_api/repair_all_failed' -H "Content-Type: application/json" --user "airflow:airflow" -d '{ "dag_id": "<DAG_ID>", "run_id": "<RUN_ID>", "task_group_id": "<TASK_GROUP_ID>"}'
```
