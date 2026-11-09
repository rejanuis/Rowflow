from ..base import Step


def build(task_id: str, source: dict, sink: dict) -> list[Step]:
    from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
    return [Step(None, BigQueryToGCSOperator, dict(
        source_project_dataset_table=source["table"], destination_cloud_storage_uris=[sink["uri"]],
        export_format=sink.get("format", "CSV"), print_header=True,
        gcp_conn_id=source.get("conn_id", "google_cloud_default")))]
