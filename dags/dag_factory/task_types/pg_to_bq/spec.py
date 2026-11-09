from ..base import TaskTypeSpec, bq_table_error, choice_error, text_error
from ..gcs_to_bq.spec import CREATE, WRITE


def _check(source: dict, sink: dict) -> list[str]:
    return (text_error("source.sql", source["sql"]) + bq_table_error("sink.table", sink["table"])
            + choice_error("sink.write_disposition", sink.get("write_disposition"), WRITE)
            + choice_error("sink.create_disposition", sink.get("create_disposition"), CREATE))


SPEC = TaskTypeSpec(
    name="pg_to_bq", source_type="postgres", sink_type="bigquery",
    description="Extract a Postgres query to GCS, then load it into BigQuery (two steps in one task group).",
    required=("source.conn_id", "source.sql", "source.staging_bucket", "sink.table"),
    supports_mapping=False, check=_check)
