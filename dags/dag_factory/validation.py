"""Pure-Python validation (no Airflow imports), shared by the sync job and the DAG generator.

Generic rules live here (ids, groups, dependency graph). Rules for each task type live in
task_types/<type>/spec.py.
"""
import re
from graphlib import CycleError, TopologicalSorter

from .task_types import TASK_TYPES, get_spec

ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,250}$")
ALLOWED_PARAMS = {"retries", "pool", "retry_delay_seconds"}


class ConfigError(ValueError):
    pass


def sort_groups(groups):
    """Parents first; raises on an unknown parent or a cycle in the group hierarchy."""
    by_id = {g["group_id"]: g for g in groups}
    graph = {}
    for gid, g in by_id.items():
        parent = g.get("parent_group")
        if parent and parent not in by_id:
            raise ConfigError(f"group '{gid}' has unknown parent '{parent}'")
        graph[gid] = {parent} if parent else set()
    try:
        return [by_id[i] for i in TopologicalSorter(graph).static_order()]
    except CycleError:
        raise ConfigError("cycle in task group hierarchy")


def _task_errors(task, group_ids):
    tid, task_type = task["task_id"], task["task_type"]
    errors = []
    if not ID_RE.match(tid):
        errors.append(f"invalid task_id '{tid}'")
    if task.get("group_id") and task["group_id"] not in group_ids:
        errors.append(f"unknown group '{task['group_id']}'")
    bad = set(task.get("params", {})) - ALLOWED_PARAMS
    if bad:
        errors.append(f"disallowed params {sorted(bad)}")
    if task_type not in TASK_TYPES:
        return [*errors, f"unknown task_type '{task_type}'"]

    spec = get_spec(task_type)
    mapping = task.get("mapping")
    if mapping is not None and not (isinstance(mapping, list) and all(isinstance(m, dict) for m in mapping)):
        return [*errors, "mapping must be a list of objects"]
    if mapping and not spec.supports_mapping:
        errors.append(f"mapping not supported for {task_type}")
    for entry in mapping or [{}]:          # with mapping, every entry together with the base must be complete
        source = {**task.get("source_config", {}), **entry.get("source", {})}
        sink = {**task.get("sink_config", {}), **entry.get("sink", {})}
        errors += spec.validate(source, sink)
    return list(dict.fromkeys(errors))


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
    try:
        sort_groups(cfg.get("groups", []))
    except ConfigError as e:
        errs.append(str(e))
    group_ids = {g["group_id"] for g in cfg.get("groups", [])}
    for task in tasks:
        errs += [f"{task['task_id']}: {m}" for m in _task_errors(task, group_ids)]

    graph = {i: set() for i in ids}                            # upstream / downstream checks
    for dep in cfg.get("dependencies", []):
        up, down = dep["upstream"], dep["downstream"]
        if up not in graph or down not in graph:
            errs.append(f"dependency {up}->{down} references unknown/inactive task")
        elif up == down:
            errs.append(f"self dependency on {up}")
        else:
            graph[down].add(up)
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as e:
        errs.append(f"dependency cycle: {e.args[1]}")
    if errs:
        raise ConfigError("; ".join(errs))
