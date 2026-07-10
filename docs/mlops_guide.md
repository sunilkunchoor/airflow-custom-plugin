# Scaling Airflow: Custom Operators & Telemetry

Apache Airflow is the industry standard for workflow orchestration. However, as organizations scale their MLOps architectures, default operators (like `PythonOperator` or `BashOperator`) often result in duplicated code, fragmented logging, and untraceable pipeline executions.

This guide details how we built **custom reusable Airflow operators** and integrated them with **OpenTelemetry** to build a highly scalable, observable data orchestration hub.

---

## Building a Custom Operator

When multiple data pipelines perform similar tasks—such as launching a Spark job, executing a SQL query, or validating a model output—relying on standard Python operators results in severe code duplication.

Custom operators inherit from Airflow's `BaseOperator` and override the `execute` method. Here is an example of an `MLModelValidationOperator`:

```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MLModelValidationOperator(BaseOperator):
    @apply_defaults
    def __init__(self, model_uri: str, test_data_path: str, threshold: float = 0.85, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model_uri = model_uri
        self.test_data_path = test_data_path
        self.threshold = threshold

    def execute(self, context):
        self.log.info(f"Loading model from {self.model_uri}...")
        self.log.info(f"Running validation tests on {self.test_data_path}...")
        
        # Mock validation execution
        accuracy = 0.89
        
        if accuracy < self.threshold:
            raise ValueError(f"Model validation failed! Accuracy {accuracy} is below threshold {self.threshold}")
            
        self.log.info("Model validation passed successfully!")
        return {"accuracy": accuracy}
```

By abstracting model loading and scoring logic inside the operator, developers can orchestrate model validation in one line of clean, declarative DAG code.

---

## OpenTelemetry Integration

Without centralized telemetry, debugging a failed task in a complex pipeline requires manually loading logs for each task run. Integrating OpenTelemetry into custom operators resolves this by linking task runs with global trace IDs.

### Telemetry Architecture

Using a custom Airflow Listener, task executions emit traces to a centralized collector (like Dynatrace or Datadog):

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

tracer = trace.get_tracer("airflow.orchestration")

def on_task_instance_success(previous_state, task_instance, session):
    with tracer.start_as_current_span(
        name=f"task_{task_instance.task_id}",
        attributes={
            "dag_id": task_instance.dag_id,
            "run_id": task_instance.run_id,
            "operator": task_instance.operator
        }
    ):
        pass
```

With traces flowing from Airflow, operators, and the downstream API containers, you can inspect a single transaction graph mapping exactly how a model was validated and served!
