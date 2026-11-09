import pytest
from sample_payload import fresh

from dag_factory.task_types import TASK_TYPES, get_spec
from dag_factory.validation import ConfigError, validate


def task(cfg, task_id):
    return next(t for t in cfg["tasks"] if t["task_id"] == task_id)


def test_sample_payload_is_valid():
    validate(fresh())


def test_all_task_types_are_discovered():
    assert TASK_TYPES == ("bq_to_bq", "bq_to_files", "gcs_to_bq", "pg_to_bq")
    assert all(get_spec(name).name == name for name in TASK_TYPES)


def break_table(c): task(c, "daily_mart")["sink_config"]["table"] = "oops"
def break_write(c): task(c, "daily_mart")["sink_config"]["write_disposition"] = "APPEND"
def break_type(c): task(c, "daily_mart")["task_type"] = "nope"
def break_traversal(c): task(c, "daily_mart")["task_type"] = "../../os"
def break_objects(c): task(c, "orders_gcs")["source_config"]["objects"] = "a.csv"
def break_uri(c): task(c, "export_csv")["sink_config"]["uri"] = "s3://x"
def break_mapping_pg(c): task(c, "customers_pg")["mapping"] = [{}]
def break_mapping_entry(c): task(c, "multi_tables")["mapping"][1]["sink"] = {}
def break_params(c): task(c, "orders_gcs")["params"] = {"queue": "x"}
def break_cycle(c): c["dependencies"].append({"upstream": "export_csv", "downstream": "orders_gcs"})
def break_ref(c): c["dependencies"].append({"upstream": "orders_gcs", "downstream": "ghost"})
def break_self(c): c["dependencies"].append({"upstream": "orders_gcs", "downstream": "orders_gcs"})
def break_group_cycle(c): c["groups"][0]["parent_group"] = "ingest_pg"
def break_group_ref(c): task(c, "daily_mart")["group_id"] = "ghost"
def break_empty(c): c["tasks"], c["dependencies"] = [], []


@pytest.mark.parametrize("breaker, message", [
    (break_table, "project.dataset.table"), (break_write, "write_disposition"), (break_type, "unknown task_type"),
    (break_traversal, "unknown task_type"), (break_objects, "list of strings"), (break_uri, "gs://"),
    (break_mapping_pg, "mapping not supported"), (break_mapping_entry, "missing sink.table"),
    (break_params, "disallowed params"), (break_cycle, "dependency cycle"), (break_ref, "unknown/inactive"),
    (break_self, "self dependency"), (break_group_cycle, "cycle in task group"),
    (break_group_ref, "unknown group"), (break_empty, "no active tasks")])
def test_invalid_configs_are_rejected(breaker, message):
    cfg = fresh()
    breaker(cfg)
    with pytest.raises(ConfigError, match=message):
        validate(cfg)
