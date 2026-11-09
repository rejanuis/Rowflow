from ..base import Step


def build(task_id: str, source: dict, sink: dict) -> list[Step]:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
    project, dataset, table = sink["table"].split(".")
    return [Step(None, BigQueryInsertJobOperator, dict(
        gcp_conn_id=sink.get("conn_id", "google_cloud_default"),
        configuration={"query": {
            "query": source["sql"], "useLegacySql": False,
            "destinationTable": {"projectId": project, "datasetId": dataset, "tableId": table},
            "writeDisposition": sink.get("write_disposition", "WRITE_TRUNCATE"),
            "createDisposition": sink.get("create_disposition", "CREATE_IF_NEEDED")}}))]
