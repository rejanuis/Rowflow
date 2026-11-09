"""Shared building blocks for task types. Pure Python: no Airflow imports, so the sync job can use it.

One folder per task type:
    task_types/<name>/spec.py    what config the type needs and how to validate it   (pure Python)
    task_types/<name>/build.py   how to create the Airflow operator(s)               (Airflow imported lazily)
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, NamedTuple

BQ_TABLE_RE = re.compile(r"^[\w-]+\.[\w-]+\.[\w$-]+$")        # project.dataset.table


class Step(NamedTuple):
    """One operator to create. A task type returns one Step (plain task) or several (wrapped in a TaskGroup)."""
    name: str | None          # task id suffix, only used when there are several steps
    operator: type            # the Airflow operator class
    kwargs: dict              # arguments for it (without task_id)


@dataclass(frozen=True)
class TaskTypeSpec:
    name: str
    source_type: str
    sink_type: str
    description: str
    required: tuple[str, ...]                                  # e.g. ("source.bucket", "sink.table")
    supports_mapping: bool = True                              # False for multi-step types
    check: Callable[[dict, dict], list[str]] | None = None     # extra checks, run only when nothing is missing

    def validate(self, source: dict, sink: dict) -> list[str]:
        errors = [f"missing {key}" for key in self.required
                  if (source if key.startswith("source.") else sink).get(key.split(".", 1)[1]) in (None, "", [])]
        if not errors and self.check:
            errors += self.check(source, sink)
        return errors


# ---- small validators shared by the specs ----
def bq_table_error(key: str, value) -> list[str]:
    ok = isinstance(value, str) and BQ_TABLE_RE.match(value)
    return [] if ok else [f"{key} must look like project.dataset.table, got {value!r}"]


def choice_error(key: str, value, allowed: tuple[str, ...]) -> list[str]:
    return [] if value is None or value in allowed else [f"{key} must be one of {', '.join(allowed)}, got {value!r}"]


def text_error(key: str, value) -> list[str]:
    return [] if isinstance(value, str) and value.strip() else [f"{key} must be a non-empty string"]
