from ..base import TaskTypeSpec, bq_table_error, choice_error

FORMATS = ("CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET")


def _check(source: dict, sink: dict) -> list[str]:
    errors = bq_table_error("source.table", source["table"])
    if not (isinstance(sink["uri"], str) and sink["uri"].startswith("gs://")):
        errors.append(f"sink.uri must start with gs://, got {sink['uri']!r}")
    return errors + choice_error("sink.format", sink.get("format"), FORMATS)


SPEC = TaskTypeSpec(
    name="bq_to_files", source_type="bigquery", sink_type="gcs_files",
    description="Export a BigQuery table to files in Google Cloud Storage.",
    required=("source.table", "sink.uri"), check=_check)
