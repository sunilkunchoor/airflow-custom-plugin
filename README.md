# airflow-custom-plugin
Adding a custom plugin to apache Airflow for Databricks.



# Bash Commands

## Run Docker Compose
```bash
docker-compose up -d
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

## Get Airflow Webserver Health
```bash
curl http://localhost:8080/health
```


## Repair Airflow DAG from Databricks Plugin
```bash
curl -X POST 'http://localhost:8080/databricks_plugin_api/repair_all_failed' -H "Content-Type: application/json" --user "airflow:airflow" -d '{ "dag_id": "databricks_workflow", "run_id": "manual__2025-12-08T20:55:16.991445+00:00", "task_group_id": "test_workflow_sunil_5678"}'
```