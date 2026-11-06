# airflow dag: metadata_sync -- hand-written (not generated).
# Config DB (Postgres) -> validate -> metadata-cache (Redis). Runs every 3 hours; trigger manually from the UI
# ("Trigger DAG w/ config": optional dag_id to sync one DAG, dry_run to validate without writing).
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, get_current_context, task

with DAG(
    dag_id="metadata_sync",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"owner": "data-platform"},
    tags=["platform", "metadata"],
    description="Sync DAG/task/group config from Postgres into the metadata-cache.",
    params={
        "dag_id": Param(None, type=["null", "string"],
                        description="Optional: sync only this dag_id. Empty = sync everything."),
        "dry_run": Param(False, type="boolean", description="Validate only; do not write the cache."),
    },
) as dag:

    @task(retries=2, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=15))
    def sync_cache():
        from dag_factory.metadata_sync import run_sync       # lazy: keeps DAG parsing cheap
        p = get_current_context()["params"]
        summary = run_sync(dag_id=p.get("dag_id") or None, dry_run=bool(p.get("dry_run")))
        print(summary)
        return summary                                       # stored as XCom, visible in the UI

    @task(retries=0)
    def report(summary: dict):
        """Fail the run (so alerting fires) when any DAG config was invalid; valid DAGs were still synced."""
        if summary["failed"]:
            lines = [f"{f['dag_id']}: {f['error']}" for f in summary["failed"][:20]]
            raise RuntimeError(f"{len(summary['failed'])} DAG config(s) invalid, last good version kept:\n"
                               + "\n".join(lines))

    report(sync_cache())
