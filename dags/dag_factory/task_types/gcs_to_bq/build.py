from ..base import Step


def operator_kwargs(source: dict, sink: dict) -> dict:
    """Pure function (no Airflow import): also reused by pg_to_bq for its load step."""
    fmt = source.get("format", "CSV")
    kwargs = dict(
        bucket=source["bucket"], source_objects=source["objects"],
        destination_project_dataset_table=sink["table"], source_format=fmt,
        write_disposition=sink.get("write_disposition", "WRITE_APPEND"),
        create_disposition=sink.get("create_disposition", "CREATE_IF_NEEDED"),
        autodetect=sink.get("autodetect", True),
        gcp_conn_id=sink.get("conn_id", "google_cloud_default"))
    if fmt == "CSV":
        kwargs["skip_leading_rows"] = source.get("skip_leading_rows", 1)
    return kwargs


def build(task_id: str, source: dict, sink: dict) -> list[Step]:
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    return [Step(None, GCSToBigQueryOperator, operator_kwargs(source, sink))]
