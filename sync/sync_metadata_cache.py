#!/usr/bin/env python3
"""CLI fallback for the Airflow 'metadata_sync' DAG (use it if Airflow itself is down).
  python sync_metadata_cache.py [--dag-id X] [--dry-run] [--rebuild]
Env: CONFIG_DB_DSN, CONFIG_CACHE_URL, DAG_SHARDS, GENERATOR_FILE. --rebuild is needed after changing DAG_SHARDS."""
import argparse, json, pathlib, sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "dags"))
from dag_factory.metadata_sync import run_sync  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--dag-id")
ap.add_argument("--dry-run", action="store_true")
ap.add_argument("--rebuild", action="store_true")
a = ap.parse_args()
summary = run_sync(a.dag_id, a.dry_run, a.rebuild)
print(json.dumps(summary, indent=2))
sys.exit(1 if summary["failed"] else 0)
