# Task types

One folder per source-to-sink type. Each folder contains:

| File | Purpose | Imports Airflow? |
|---|---|---|
| `spec.py` | Required settings and validation rules (`SPEC`). Used by the sync job and the generator. | No |
| `build.py` | `build(task_id, source, sink)` returns the operator(s) to create. | Only inside `build()` |
| `README.md` | Settings reference and an example row. | |

Folders in this package are discovered automatically, so adding a type means adding a folder.

| Type | Flow |
|---|---|
| [`gcs_to_bq`](gcs_to_bq/README.md) | GCS → BigQuery |
| [`bq_to_bq`](bq_to_bq/README.md) | BigQuery → BigQuery |
| [`pg_to_bq`](pg_to_bq/README.md) | Postgres → GCS → BigQuery (two steps) |
| [`bq_to_files`](bq_to_files/README.md) | BigQuery → files in GCS |

## Add your own type

1. Create `task_types/<your_type>/` with an empty-ish `__init__.py`, plus `spec.py` and `build.py`.
2. In `spec.py` export `SPEC = TaskTypeSpec(...)`: name, source and sink type, `required` keys such as `"source.bucket"`, and an optional `check(source, sink)` that returns a list of error messages.
3. In `build.py` define `build(task_id, source, sink) -> list[Step]`. Return one `Step(None, OperatorClass, kwargs)` for a plain task. Return several named steps and the generator wraps them in a task group, chained in order. Import Airflow provider classes **inside** `build()`.
4. Insert a row into `task_type_catalog` so a UI can offer the type, and add a `README.md` in your folder.

The type name is the folder name and is what you put in `task_config.task_type`.
