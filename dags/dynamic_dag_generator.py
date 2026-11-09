# airflow dag generator -- the ONE file that turns cached metadata into Airflow DAGs.
# It reads ONLY the metadata-cache (Redis) and never connects to the config database.
#
# Flow:  cache -> load_configs() -> build_dag() -> _build_task() -> task_types/<type>/build.py
# Every source-to-sink type lives in its own folder: dag_factory/task_types/<type>/{spec.py,build.py}.
import json
import logging
import os
import re
from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup
from dag_factory.task_types import get_builder
from dag_factory.validation import sort_groups, validate

log = logging.getLogger(__name__)
N_SHARDS = int(os.environ.get("DAG_SHARDS", "1"))             # keep equal to the sync job's value


# ---- tasks ----
def _common_args(task):
    """Operator arguments shared by every task type (from task_config.params)."""
    args = dict(task.get("params", {}))
    if "retry_delay_seconds" in args:
        args["retry_delay"] = timedelta(seconds=args.pop("retry_delay_seconds"))
    return args


def _build_task(task):
    """Create the Airflow object for one task_config row; returns the node to wire dependencies on."""
    task_id, common = task["task_id"], _common_args(task)
    build = get_builder(task["task_type"])
    source, sink = task["source_config"], task["sink_config"]

    if task.get("mapping"):                                    # dynamic task mapping: one instance per entry
        steps = [build(task_id, {**source, **m.get("source", {})}, {**sink, **m.get("sink", {})})[0]
                 for m in task["mapping"]]
        return steps[0].operator.partial(task_id=task_id, **common).expand_kwargs([s.kwargs for s in steps])

    steps = build(task_id, source, sink)
    if len(steps) == 1:                                        # the usual case: a single operator
        return steps[0].operator(task_id=task_id, **steps[0].kwargs, **common)
    with TaskGroup(group_id=task_id) as group:                 # multi-step type: steps run in order
        previous = None
        for step in steps:
            operator = step.operator(task_id=step.name, **step.kwargs, **common)
            if previous:
                previous >> operator
            previous = operator
    return group


# ---- dags ----
def build_dag(cfg):
    """Create one DAG (groups, tasks, dependencies) from a validated metadata payload."""
    validate(cfg)                                              # cycles, unknown refs, per-type required config
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
        for task in cfg["tasks"]:
            group = groups.get(task.get("group_id"))
            if group:
                with group:
                    nodes[task["task_id"]] = _build_task(task)
            else:
                nodes[task["task_id"]] = _build_task(task)
        for dep in cfg["dependencies"]:                        # upstream -> downstream
            nodes[dep["upstream"]] >> nodes[dep["downstream"]]
    return dag


# ---- metadata-cache ----
def load_configs():
    """Read this shard's DAG payloads from Redis, sorted so output is deterministic."""
    import redis
    match = re.search(r"_(\d+)\.py$", __file__)                # optional shard copy: dynamic_dag_generator_07.py
    if N_SHARDS > 1 and not match:
        raise RuntimeError("DAG_SHARDS > 1: copy this file as dynamic_dag_generator_00..NN.py")
    shard = int(match.group(1)) if match else 0
    client = redis.Redis.from_url(os.environ["CONFIG_CACHE_URL"], socket_timeout=5)
    raw = client.hgetall(f"metadata:dags:{shard}")
    if not raw:
        raise RuntimeError(f"metadata-cache empty for shard {shard}: trigger the metadata_sync DAG in the Airflow UI")
    return [json.loads(value) for _, value in sorted(raw.items())]


def generate(module_globals):
    """Build every DAG of this shard. A bad DAG is logged and skipped; the others still load."""
    for cfg in load_configs():
        try:
            module_globals[cfg["dag_id"]] = build_dag(cfg)     # module-level global => Airflow discovers it
        except Exception:
            log.exception("skipping DAG %s: invalid config", cfg.get("dag_id"))


generate(globals())
