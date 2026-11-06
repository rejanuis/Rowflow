"""Pure-python validation (no Airflow imports) shared by the sync job and the DAG factory."""
import re
from graphlib import TopologicalSorter, CycleError

ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,250}$")
REQUIRED = {   # task_type -> required keys as "source.x" / "sink.x"
    "gcs_to_bq":   ["source.bucket", "source.objects", "sink.table"],
    "bq_to_bq":    ["source.sql", "sink.table"],
    "pg_to_bq":    ["source.conn_id", "source.sql", "source.staging_bucket", "sink.table"],
    "bq_to_files": ["source.table", "sink.uri"],
}
ALLOWED_PARAMS = {"retries", "pool", "retry_delay_seconds"}


class ConfigError(ValueError):
    pass


def sort_groups(groups):
    """Parents first; raises on unknown parent or hierarchy cycle."""
    by_id = {g["group_id"]: g for g in groups}
    graph = {}
    for gid, g in by_id.items():
        p = g.get("parent_group")
        if p and p not in by_id:
            raise ConfigError(f"group '{gid}' has unknown parent '{p}'")
        graph[gid] = {p} if p else set()
    try:
        return [by_id[i] for i in TopologicalSorter(graph).static_order()]
    except CycleError:
        raise ConfigError("cycle in task group hierarchy")


def validate(cfg):
    errs = []
    if not ID_RE.match(cfg.get("dag_id", "")):
        errs.append("invalid dag_id")
    tasks = cfg.get("tasks", [])
    if not tasks:
        errs.append("DAG has no active tasks")
    ids = [t["task_id"] for t in tasks]
    if len(ids) != len(set(ids)):
        errs.append("duplicate task_id")
    errs += [f"invalid task_id '{i}'" for i in ids if not ID_RE.match(i)]
    try:
        sort_groups(cfg.get("groups", []))
    except ConfigError as e:
        errs.append(str(e))
    gids = {g["group_id"] for g in cfg.get("groups", [])}
    for t in tasks:
        tid, tt = t["task_id"], t["task_type"]
        if t.get("group_id") and t["group_id"] not in gids:
            errs.append(f"{tid}: unknown group '{t['group_id']}'")
        if tt not in REQUIRED:
            errs.append(f"{tid}: unknown task_type '{tt}'")
            continue
        for e in (t.get("mapping") or [{}]):                   # with mapping, each entry must complete the config
            for key in REQUIRED[tt]:
                side, k = key.split(".")
                v = {**t.get(f"{side}_config", {}), **e.get(side, {})}
                if v.get(k) in (None, "", []):
                    errs.append(f"{tid}: missing {key}")
        if t.get("mapping") and tt == "pg_to_bq":
            errs.append(f"{tid}: mapping not supported for pg_to_bq (composite task)")
        bad = set(t.get("params", {})) - ALLOWED_PARAMS
        if bad:
            errs.append(f"{tid}: disallowed params {sorted(bad)}")
    # upstream / downstream checks
    graph = {i: set() for i in ids}
    for d in cfg.get("dependencies", []):
        u, v = d["upstream"], d["downstream"]
        if u not in graph or v not in graph:
            errs.append(f"dependency {u}->{v} references unknown/inactive task")
        elif u == v:
            errs.append(f"self dependency on {u}")
        else:
            graph[v].add(u)
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as e:
        errs.append(f"dependency cycle: {e.args[1]}")
    if errs:
        raise ConfigError("; ".join(errs))
