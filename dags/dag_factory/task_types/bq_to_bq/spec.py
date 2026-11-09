from ..base import TaskTypeSpec, bq_table_error, choice_error, text_error

WRITE = ("WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY")
CREATE = ("CREATE_IF_NEEDED", "CREATE_NEVER")


def _check(source: dict, sink: dict) -> list[str]:
    return (text_error("source.sql", source["sql"]) + bq_table_error("sink.table", sink["table"])
            + choice_error("sink.write_disposition", sink.get("write_disposition"), WRITE)
            + choice_error("sink.create_disposition", sink.get("create_disposition"), CREATE))


SPEC = TaskTypeSpec(
    name="bq_to_bq", source_type="bigquery", sink_type="bigquery",
    description="Run a SQL query in BigQuery and write the result to a table.",
    required=("source.sql", "sink.table"), check=_check)
