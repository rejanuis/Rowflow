from ..base import Step
from ..gcs_to_bq.build import operator_kwargs as gcs_to_bq_kwargs


def build(task_id: str, source: dict, sink: dict) -> list[Step]:
    """Two steps (wrapped in a TaskGroup by the generator): Postgres -> GCS (JSON), then GCS -> BigQuery."""
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
    prefix = source.get("staging_prefix", "staging/airflow") + "/{{ ds_nodash }}/" + task_id
    extract = dict(
        postgres_conn_id=source["conn_id"], sql=source["sql"], bucket=source["staging_bucket"],
        filename=prefix + "_{}.json", export_format="json",
        gcp_conn_id=sink.get("conn_id", "google_cloud_default"))
    load = gcs_to_bq_kwargs({"bucket": source["staging_bucket"], "objects": [prefix + "_*.json"],
                             "format": "NEWLINE_DELIMITED_JSON"}, sink)
    return [Step("extract", PostgresToGCSOperator, extract), Step("load", GCSToBigQueryOperator, load)]
