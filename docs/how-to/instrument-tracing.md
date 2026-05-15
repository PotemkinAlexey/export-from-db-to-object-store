# How-to: Instrument with OpenTelemetry tracing

Wire up distributed tracing so that operator spans appear in your
observability backend alongside Airflow scheduler and task runner spans.

## Install

```bash
pip install opentelemetry-sdk opentelemetry-exporter-otlp
```

The operator imports `opentelemetry-api` lazily. If the package is not
installed, all tracing calls are no-ops — the operator runs normally with
zero overhead from the tracing module.

Verify the package is seen by Python before going further:

```python
from airflow_export_to_object_store import tracing
print(tracing.is_available())   # True if opentelemetry-api is importable
```

## Spans the operator emits

| Span name | When | Key attributes |
|---|---|---|
| `export.execute` | Entire `execute()` call | `task_id`, `db_hook_id`, `storage_hook_id`, `mode` (`stream`/`unload`), `shards` (count) |
| `export.unload` | Native unload path only | `unload.strategy` (e.g. `snowflake`) |
| `export.shard.fetch` | Per shard, fetch phase | `shard_index`, `chunk_rows` |
| `export.shard.write` | Per shard, Parquet write phase | `shard_index` |
| `export.shard.validate` | Per shard, validation | `shard_index` |
| `export.shard.upload` | Per shard, upload | `shard_index`, `remote_path` |

`export.execute` is the root span. The shard sub-spans are children of
it. If you have multiple shards running in parallel, their sub-spans
overlap in the timeline.

## Airflow 2.10+ — built-in OTel integration

Airflow 2.10 ships a built-in OpenTelemetry integration. Configure it in
`airflow.cfg`:

```ini
[metrics]
otel_on = True
otel_host = otel-collector.internal
otel_port = 4318
otel_prefix = airflow
```

When this is active, Airflow initialises a `TracerProvider` and
configures an OTLP exporter automatically. The operator's spans appear
in the same trace as Airflow's own task-runner and scheduler spans —
no additional code needed in the DAG.

## Older Airflow — bootstrap in the DAG

For Airflow < 2.10, initialise the SDK in the DAG file before defining
the operator. This runs once when the DAG is parsed by the scheduler and
once when the task executes on the worker — only the worker execution
matters for task-level spans.

```python
# dags/export_with_tracing.py
from __future__ import annotations

from datetime import datetime

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# Bootstrap once per Python process. Guard with a flag to avoid
# re-registering if the DAG file is parsed multiple times.
_TRACING_INITIALISED = False

def _init_tracing() -> None:
    global _TRACING_INITIALISED
    if _TRACING_INITIALISED:
        return
    provider = TracerProvider()
    exporter = OTLPSpanExporter(endpoint="http://otel-collector.internal:4318/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACING_INITIALISED = True

_init_tracing()

from airflow import DAG
from airflow_export_to_object_store import StreamingExportOperator

with DAG(
    dag_id="export_orders_traced",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    StreamingExportOperator(
        task_id="orders_to_s3",
        db_hook_id="pg_default",
        storage_hook_id="aws_default",
        bucket="my-data-lake",
        sql_template="SELECT * FROM orders WHERE order_date = '{{ ds }}'",
        remote_path_template="orders/{{ ds }}/data.parquet",
    )
```

For Jaeger instead of OTLP:

```bash
pip install opentelemetry-exporter-jaeger
```

```python
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

exporter = JaegerExporter(
    agent_host_name="jaeger.internal",
    agent_port=6831,
)
provider.add_span_processor(BatchSpanProcessor(exporter))
```

## Adding custom attributes to spans

Use `tracing.set_attribute` to annotate an already-open span from
`transform_fn` or downstream code. Note that `transform_fn` runs inside
the shard sub-span context, so the current span at call time is the
`export.shard.fetch` span:

```python
from airflow_export_to_object_store import tracing
import pyarrow as pa

def my_transform(tbl: pa.Table) -> pa.Table:
    # The current span is export.shard.fetch when this runs.
    span_obj = tracing.span.__self__ if hasattr(tracing.span, "__self__") else None
    # Simpler: get the current span via opentelemetry directly.
    from opentelemetry import trace as otel_trace
    current = otel_trace.get_current_span()
    tracing.set_attribute(current, "custom.row_count_before", len(tbl))
    tbl = tbl.filter(pa.compute.greater(tbl.column("amount"), 0))
    tracing.set_attribute(current, "custom.row_count_after", len(tbl))
    return tbl
```

`set_attribute` is a no-op if the span object is `None` or if OTel is
not installed, so this code is safe to ship even without the SDK.

## Verifying traces appear

1. Check `tracing.is_available()` returns `True` in your worker
   environment.
2. Trigger the DAG once.
3. In your observability UI (Jaeger, Grafana Tempo, Honeycomb, Datadog
   APM), search for traces with service name `airflow_export_to_object_store`
   or filter on `task_id=<your task id>`.

You should see `export.execute` as the root with shard sub-spans inside
it. Each `export.shard.upload` span includes `remote_path` as an
attribute so you can correlate a slow upload with its destination object.

## See also

- [OpenTelemetry Python SDK docs](https://opentelemetry-python.readthedocs.io/)
- [Airflow 2.10 OTel integration](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)
- Source: `src/airflow_export_to_object_store/tracing.py`
