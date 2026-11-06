# airflow dag generator -- SINGLE FILE for dynamic DAG + task group + task generation.
# Reads ONLY the metadata-cache (Redis). It never connects to the config database.
import json
import logging
import os
import re
from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup
from dag_factory.validation import validate, sort_groups     # shared with the sync job

log = logging.getLogger(__name__)
N_SHARDS = int(os.environ.get("DAG_SHARDS", "1"))             # keep equal to the sync job's value


# ---- 1. task type registry: task_type -> (operator class, kwargs). Lazy imports = cheap parse ----
def _split(table):
    p, d, t = table.split(".")
    return {"projectId": p, "datasetId": d, "tableId": t}


def gcs_to_bq(tid, src, snk):
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    fmt = src.get("format", "CSV")
    kw = dict(bucket=src["bucket"], source_objects=src["objects"],
              destination_project_dataset_table=snk["table"], source_format=fmt,
              write_disposition=snk.get("write_disposition", "WRITE_APPEND"),
              create_disposition=snk.get("create_disposition", "CREATE_IF_NEEDED"),
              autodetect=snk.get("autodetect", True),
              gcp_conn_id=snk.get("conn_id", "google_cloud_default"))
    if fmt == "CSV":
        kw["skip_leading_rows"] = src.get("skip_leading_rows", 1)
    return GCSToBigQueryOperator, kw


def bq_to_bq(tid, src, snk):
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
    return BigQueryInsertJobOperator, dict(
        gcp_conn_id=snk.get("conn_id", "google_cloud_default"),
        configuration={"query": {
            "query": src["sql"], "useLegacySql": False,
            "destinationTable": _split(snk["table"]),
            "writeDisposition": snk.get("write_disposition", "WRITE_TRUNCATE"),
            "createDisposition": snk.get("create_disposition", "CREATE_IF_NEEDED")}})


def bq_to_files(tid, src, snk):
    from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
    return BigQueryToGCSOperator, dict(
        source_project_dataset_table=src["table"], destination_cloud_storage_uris=[snk["uri"]],
        export_format=snk.get("format", "CSV"), print_header=True,
        gcp_conn_id=src.get("conn_id", "google_cloud_default"))


def pg_to_bq(tid, src, snk):
    """Composite: Postgres -> GCS staging -> BigQuery (built as a small TaskGroup)."""
    from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
    prefix = src.get("staging_prefix", "staging/airflow") + "/{{ ds_nodash }}/" + tid
    extract = ("extract", PostgresToGCSOperator, dict(
        postgres_conn_id=src["conn_id"], sql=src["sql"], bucket=src["staging_bucket"],
        filename=prefix + "_{}.json", export_format="json",
        gcp_conn_id=snk.get("conn_id", "google_cloud_default")))
    load_cls, load_kw = gcs_to_bq(tid, {"bucket": src["staging_bucket"], "objects": [prefix + "_*.json"],
                                        "format": "NEWLINE_DELIMITED_JSON"}, snk)
    return [extract, ("load", load_cls, load_kw)]


BUILDERS = {"gcs_to_bq": gcs_to_bq, "bq_to_bq": bq_to_bq, "bq_to_files": bq_to_files, "pg_to_bq": pg_to_bq}


# ---- 2. task + DAG generation ----
def _common(t):
    p = dict(t.get("params", {}))
    if "retry_delay_seconds" in p:
        p["retry_delay"] = timedelta(seconds=p.pop("retry_delay_seconds"))
    return p


def _build_task(t):
    """Returns the node to wire dependencies on (operator, mapped operator, or TaskGroup)."""
    tid, common = t["task_id"], _common(t)
    build = BUILDERS[t["task_type"]]
    src, snk = t["source_config"], t["sink_config"]
    if t["task_type"] == "pg_to_bq":
        with TaskGroup(group_id=tid) as g:
            prev = None
            for name, cls, kw in build(tid, src, snk):
                op = cls(task_id=name, **kw, **common)
                if prev:
                    prev >> op
                prev = op
        return g
    if t.get("mapping"):                                       # dynamic task mapping
        merged = [({**src, **m.get("source", {})}, {**snk, **m.get("sink", {})}) for m in t["mapping"]]
        cls = build(tid, *merged[0])[0]
        return cls.partial(task_id=tid, **common).expand_kwargs([build(tid, s, k)[1] for s, k in merged])
    cls, kw = build(tid, src, snk)
    return cls(task_id=tid, **kw, **common)


def build_dag(cfg):
    validate(cfg)                                              # cycles, unknown refs, required config
    owner = cfg["team_name"]
    with DAG(dag_id=cfg["dag_id"], schedule=cfg["schedule"], catchup=False,
             start_date=datetime.fromisoformat(cfg["start_date"]),
             max_active_runs=cfg["max_active_runs"],
             default_args={"owner": owner, **cfg.get("default_args", {})},
             description=cfg.get("description"),
             tags=[*cfg.get("tags", []), f"team:{owner}", f"project:{cfg['project_name']}"]) as dag:
        groups = {}
        for g in sort_groups(cfg["groups"]):                   # parents first
            groups[g["group_id"]] = TaskGroup(g["group_id"], parent_group=groups.get(g.get("parent_group")))
        nodes = {}
        for t in cfg["tasks"]:
            grp = groups.get(t.get("group_id"))
            if grp:
                with grp:
                    nodes[t["task_id"]] = _build_task(t)
            else:
                nodes[t["task_id"]] = _build_task(t)
        for d in cfg["dependencies"]:                          # upstream -> downstream
            nodes[d["upstream"]] >> nodes[d["downstream"]]
    return dag


# ---- 3. read the metadata-cache ----
def load_configs():
    import redis
    m = re.search(r"_(\d+)\.py$", __file__)                    # optional shard copy: dynamic_dag_generator_07.py
    if N_SHARDS > 1 and not m:
        raise RuntimeError("DAG_SHARDS > 1: copy this file as dynamic_dag_generator_00..NN.py")
    shard = int(m.group(1)) if m else 0
    r = redis.Redis.from_url(os.environ["CONFIG_CACHE_URL"], socket_timeout=5)
    raw = r.hgetall(f"metadata:dags:{shard}")
    if not raw:
        raise RuntimeError(f"metadata-cache empty for shard {shard}: trigger the metadata_sync DAG in the Airflow UI")
    return [json.loads(v) for _, v in sorted(raw.items())]    # sorted = deterministic = no phantom DAG versions


# ---- 4. generate ----
for _cfg in load_configs():
    try:
        globals()[_cfg["dag_id"]] = build_dag(_cfg)            # module-level global => Airflow discovers it
    except Exception:
        log.exception("skipping DAG %s: invalid config", _cfg.get("dag_id"))
