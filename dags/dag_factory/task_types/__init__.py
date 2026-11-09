"""Task type registry. Drop in a new folder with spec.py + build.py and it is registered automatically.

Only folders that exist in this package can be loaded, so a task_type value coming from the config
database can never import arbitrary code.
"""
import importlib
import pkgutil

from .base import Step, TaskTypeSpec  # noqa: F401  (re-exported)

TASK_TYPES: tuple[str, ...] = tuple(sorted(
    m.name for m in pkgutil.iter_modules(__path__) if m.ispkg and not m.name.startswith("_")))


def _known(name: str) -> str:
    if name not in TASK_TYPES:
        raise KeyError(f"unknown task_type '{name}' (available: {', '.join(TASK_TYPES)})")
    return name


def get_spec(name: str) -> TaskTypeSpec:
    """Pure-Python description and validation rules of a task type."""
    return importlib.import_module(f"{__name__}.{_known(name)}.spec").SPEC


def get_builder(name: str):
    """build(task_id, source, sink) -> list[Step]. Imports Airflow providers lazily when called."""
    return importlib.import_module(f"{__name__}.{_known(name)}.build").build
