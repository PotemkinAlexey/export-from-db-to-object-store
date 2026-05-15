# RetryOptions

Controls upload retry behaviour for transient object-store errors (network timeouts, 5xx responses, throttling).

Pass an instance to `StreamingExportOperator(retry_options=...)`.

```python
from airflow_export_to_object_store.options import RetryOptions

retry_options = RetryOptions(
    upload_retries=5,
    backoff_base=2.0,
    backoff_cap=30.0,
)
```

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `upload_retries` | `int` | `3` | Number of retry attempts after the first failure. `0` means no retries; the upload fails immediately on the first error. |
| `backoff_base` | `float` | `1.5` | Base of the exponential backoff formula. Controls how steeply wait time grows between attempts. |
| `backoff_cap` | `float` | `20.0` | Maximum wait time in seconds between attempts. Backoff is clamped to this value regardless of the attempt number. |

## Backoff formula

```
wait = min(backoff_base ^ attempt, backoff_cap)
```

`attempt` is 1-indexed: the wait before the first retry is `backoff_base^1`, before the second retry is `backoff_base^2`, and so on.

## Default schedule

With `upload_retries=3`, `backoff_base=1.5`, `backoff_cap=20.0`:

| Attempt | Formula | Wait (s) |
|---------|---------|---------|
| 1 | `min(1.5^1, 20.0)` | 1.5 |
| 2 | `min(1.5^2, 20.0)` | 2.25 |
| 3 | `min(1.5^3, 20.0)` | 3.38 |

After all retries are exhausted the original exception is re-raised and the Airflow task fails.

## Dataclass signature

```python
@dataclass(frozen=True)
class RetryOptions:
    upload_retries: int = 3
    backoff_base: float = 1.5
    backoff_cap: float = 20.0
```

`RetryOptions` is frozen; all fields must be set at construction time.
