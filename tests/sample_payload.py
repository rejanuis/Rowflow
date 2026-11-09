"""A payload shaped like one row of v_dag_metadata: nested groups, all four task types, mapping."""
import copy


def _task(task_id, task_type, source, sink, group_id=None, **extra):
    return {"task_id": task_id, "group_id": group_id, "task_type": task_type, "source_config": source,
            "sink_config": sink, "mapping": None, "params": {}, **extra}


PAYLOAD = {
    "dag_id": "sales_daily", "team_name": "data-eng", "project_name": "sales", "description": None,
    "schedule": "0 2 * * *", "start_date": "2026-01-01", "max_active_runs": 1, "default_args": {}, "tags": [],
    "groups": [{"group_id": "ingest", "parent_group": None}, {"group_id": "ingest_pg", "parent_group": "ingest"},
               {"group_id": "mart", "parent_group": None}],
    "tasks": [
        _task("orders_gcs", "gcs_to_bq", {"bucket": "raw", "objects": ["orders/*.csv"]},
              {"table": "proj.raw.orders"}, "ingest", params={"retries": 2}),
        _task("customers_pg", "pg_to_bq", {"conn_id": "pg", "sql": "select 1", "staging_bucket": "stg"},
              {"table": "proj.raw.customers"}, "ingest_pg"),
        _task("multi_tables", "gcs_to_bq", {"bucket": "raw", "format": "PARQUET"}, {}, "ingest", mapping=[
            {"source": {"objects": ["a/*.parquet"]}, "sink": {"table": "proj.raw.a"}},
            {"source": {"objects": ["b/*.parquet"]}, "sink": {"table": "proj.raw.b"}}]),
        _task("daily_mart", "bq_to_bq", {"sql": "select 1"}, {"table": "proj.mart.daily"}, "mart"),
        _task("export_csv", "bq_to_files", {"table": "proj.mart.daily"}, {"uri": "gs://exports/daily/*.csv"}),
    ],
    "dependencies": [{"upstream": "orders_gcs", "downstream": "daily_mart"},
                     {"upstream": "customers_pg", "downstream": "daily_mart"},
                     {"upstream": "multi_tables", "downstream": "daily_mart"},
                     {"upstream": "daily_mart", "downstream": "export_csv"}],
}


def fresh():
    return copy.deepcopy(PAYLOAD)
