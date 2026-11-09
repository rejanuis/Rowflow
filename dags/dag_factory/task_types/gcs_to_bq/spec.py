from ..base import TaskTypeSpec, bq_table_error, choice_error

WRITE = ("WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY")
CREATE = ("CREATE_IF_NEEDED", "CREATE_NEVER")


def _check(source: dict, sink: dict) -> list[str]:
    errors = bq_table_error("sink.table", sink["table"])
    objects = source["objects"]
    if not isinstance(objects, list) or not all(isinstance(o, str) for o in objects):
        errors.append("source.objects must be a list of strings")
    errors += choice_error("sink.write_disposition", sink.get("write_disposition"), WRITE)
    errors += choice_error("sink.create_disposition", sink.get("create_disposition"), CREATE)
    return errors


SPEC = TaskTypeSpec(
    name="gcs_to_bq", source_type="gcs", sink_type="bigquery",
    description="Load files from Google Cloud Storage into a BigQuery table.",
    required=("source.bucket", "source.objects", "sink.table"), check=_check)
