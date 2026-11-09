"""Builds the sample DAG with real Airflow 3 and the Google provider. Skipped when they are not installed."""
import importlib.util
import json
import pathlib

import pytest
from sample_payload import PAYLOAD, fresh

pytest.importorskip("airflow.sdk")
pytest.importorskip("airflow.providers.google")
fakeredis = pytest.importorskip("fakeredis")
import redis  # noqa: E402

from airflow.sdk import DAG  # noqa: E402
from dag_factory import metadata_sync  # noqa: E402

GENERATOR = pathlib.Path(__file__).resolve().parents[1] / "dags" / "dynamic_dag_generator.py"


@pytest.fixture
def dags(monkeypatch):
    server = fakeredis.FakeServer()
    monkeypatch.setenv("CONFIG_CACHE_URL", "redis://fake/0")
    monkeypatch.setattr(redis.Redis, "from_url", staticmethod(lambda url, **kw: fakeredis.FakeRedis(server=server)))
    client = fakeredis.FakeRedis(server=server)
    metadata_sync.publish(client, {"sales_daily": fresh()}, {"sales_daily"}, 1)        # the real sync code path
    broken = fresh()
    broken.update(dag_id="broken_dag", dependencies=[*PAYLOAD["dependencies"],
                  {"upstream": "export_csv", "downstream": "orders_gcs"}])             # cycle, bypassing the sync
    client.hset("metadata:dags:0", "broken_dag", json.dumps(broken))
    spec = importlib.util.spec_from_file_location("dynamic_dag_generator", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {k: v for k, v in vars(module).items() if isinstance(v, DAG)}


def test_broken_dag_is_skipped_and_others_load(dags):
    assert set(dags) == {"sales_daily"}


def test_groups_nested_and_tasks_prefixed(dags):
    dag = dags["sales_daily"]
    assert dag.task_group_dict["ingest.ingest_pg"].parent_group.group_id == "ingest"
    assert {"mart.daily_mart", "ingest.orders_gcs", "export_csv"} <= set(dag.task_dict)


def test_multi_step_type_becomes_extract_then_load(dags):
    dag = dags["sales_daily"]
    load = dag.task_dict["ingest.ingest_pg.customers_pg.load"]
    assert load.upstream_task_ids == {"ingest.ingest_pg.customers_pg.extract"}


def test_dependencies_follow_upstream_downstream(dags):
    dag = dags["sales_daily"]
    assert dag.task_dict["mart.daily_mart"].upstream_task_ids == {
        "ingest.orders_gcs", "ingest.multi_tables", "ingest.ingest_pg.customers_pg.load"}
    assert dag.task_dict["export_csv"].upstream_task_ids == {"mart.daily_mart"}


def test_mapping_and_params(dags):
    dag = dags["sales_daily"]
    assert type(dag.task_dict["ingest.multi_tables"]).__name__ == "MappedOperator"
    assert dag.task_dict["ingest.orders_gcs"].retries == 2
