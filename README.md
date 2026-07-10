# Airflow Custom SDK

A comprehensive custom SDK and plugin package for Apache Airflow, designed to support both Airflow v2 and Airflow v3. This repository includes custom operators, hooks, and plugins to extend Airflow's functionality, with a primary focus on Databricks, Azure, and Bash integrations.

## Repository Structure

The project has been restructured to cleanly separate Airflow v2 and v3 codebases:

- **`custom_sdk/v2/`**: Contains the original Airflow v2 compatible code (formerly `airflow_custom_sdk`).
- **`custom_sdk/` (Root of the module)**: Contains the main Airflow v3 compatible plugins and SDK logic.
  - `custom_sdk/databricks/`: Custom Databricks operators and workflow task groups.
  - `custom_sdk/azure/`: Azure-related extensions and integrations.
  - `custom_sdk/bash/`: Custom bash plugins (e.g., custom extra links).
- **`dags/`**: Sample DAGs demonstrating the usage of the custom SDK (both v2 and v3 workflows).
- **`docs/`**: Additional documentation and guides.
  - [`mlops_guide.md`](file:///c:/Users/sunil/GitHub/airflow-custom-sdk/docs/mlops_guide.md): Guide on building custom ML operators and OpenTelemetry integrations.
- **`docker/`**: Docker Compose setup and configurations to run Airflow locally with these plugins installed.

## Getting Started

### Running Locally with Docker

You can spin up an Airflow environment with the custom SDK installed using Docker Compose.

1. **Build and start the services:**
   ```bash
   docker-compose -f docker/docker-compose.yaml up --build
   ```

2. **Access Airflow:**
   Navigate to `http://localhost:8080` and log in (default credentials: `airflow`/`airflow`).

3. **Stop the services:**
   ```bash
   docker-compose -f docker/docker-compose.yaml down
   ```

4. **Clean up (remove volumes and orphans):**
   ```bash
   docker-compose -f docker/docker-compose.yaml down --volumes --rmi all --remove-orphans
   ```

### API Testing (cURL Commands)

The plugin may also expose custom API endpoints (e.g., for Databricks plugin tests).

**Simple cURL command using Basic Auth:**
```bash
curl -X GET 'http://localhost:8080/api/v1/dags' --user "airflow:airflow"
```

**Get Airflow Webserver Health:**
```bash
curl http://localhost:8080/health
```

**Custom API Endpoint Test:**
```bash
curl -X GET 'http://localhost:8080/databricks_plugin_api/test' --user "airflow:airflow"
```

## Installation (Development)

To install the SDK in editable mode for local development:
```bash
pip install -e .
```

This uses the `pyproject.toml` configuration and will automatically register the entry points for the Airflow plugins (e.g., `DatabricksCustomPlugin` and `OperatorExtraLinkPlugin`).
