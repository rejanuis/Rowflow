# Operations guide

Everyday tasks once the system is running. Commands run from the project folder.

## 1. Add a new pipeline

Pipelines are rows in the config DB. Open a SQL shell:

```bash
docker compose exec configdb psql -U postgres -d config
```

Example: a DAG with two tasks, where the second runs after the first.

```sql
INSERT INTO dag_config(dag_id, team_name, project_name, schedule, start_date)
VALUES ('marketing_daily', 'marketing', 'campaigns', '0 3 * * *', '2026-11-01');

INSERT INTO task_config(dag_id, task_id, task_type, source_config, sink_config) VALUES
 ('marketing_daily', 'load_clicks',   'gcs_to_bq',
  '{"bucket":"mkt-raw","objects":["clicks/*.csv"]}', '{"table":"my-proj.mkt.clicks"}'),
 ('marketing_daily', 'build_summary', 'bq_to_bq',
  '{"sql":"select channel, count(*) n from `my-proj.mkt.clicks` group by 1"}', '{"table":"my-proj.mkt.summary"}');

INSERT INTO task_dependency VALUES ('marketing_daily', 'load_clicks', 'build_summary');
```

Then trigger `metadata_sync` in the Airflow UI. After a minute the DAG appears (paused by default; unpause it when ready).

**Task groups:** insert into `task_group_config(dag_id, group_id, parent_group)` first, then set `group_id` on the tasks.
**Rules:** `dag_id` and `task_id` may contain letters, digits, `_`, `.`, `-`. Keep IDs stable: renaming creates a new DAG or task and orphans the history.

## 2. Change or remove things

```sql
UPDATE dag_config  SET schedule = '0 4 * * *' WHERE dag_id = 'marketing_daily';   -- change a schedule
UPDATE task_config SET is_active = false WHERE dag_id = 'marketing_daily' AND task_id = 'build_summary';  -- remove a task (soft)
UPDATE dag_config  SET is_active = false WHERE dag_id = 'marketing_daily';        -- remove the whole DAG (soft)
```

Prefer `is_active = false` to deleting rows. Dependencies that touch an inactive task are dropped automatically. A deactivated DAG disappears from the active DAG list after the next sync and parse.

Look at what the sync will read for one DAG:

```sql
SELECT * FROM v_dag_metadata WHERE dag_id = 'marketing_daily';
```

## 3. Run the sync

**Automatic:** `metadata_sync` runs at 00:00, 03:00, 06:00 ... UTC (`0 */3 * * *` in `dags/metadata_sync_dag.py`; edit that line to change it). The DAG must be unpaused.

**Manual, in the UI:** DAGs -> `metadata_sync` -> **Trigger** (play icon) -> **Trigger DAG w/ config**:

| Option | Meaning |
|---|---|
| `dag_id` | Sync only this DAG. Empty = everything. |
| `dry_run` | Validate and report, write nothing. |

**Manual, from a terminal:**

```bash
docker compose exec airflow-apiserver airflow dags trigger metadata_sync --conf '{"dag_id": "marketing_daily"}'
```

**If Airflow itself is down** (runs the same code in a throwaway container):

```bash
docker compose run --rm -v "$PWD/sync:/opt/airflow/sync" airflow-scheduler \
  python /opt/airflow/sync/sync_metadata_cache.py --dry-run
```

Drop `--dry-run` to really sync. The sync tool also has `--dag-id X` and `--rebuild` (see Scaling).

## 4. Read the sync result

Open the latest `metadata_sync` run:

- `sync_cache` -> **XCom**: `written` (changed DAGs), `removed`, `failed`.
- `report` is **red** when any DAG config was invalid. Valid DAGs are still synced, and the invalid DAG keeps its last good version. The log names each problem. Fix the config and sync again.
- Database history: `SELECT * FROM sync_log ORDER BY id DESC LIMIT 20;` and `dag_config_history` (every published version, useful for audit and diffs; automatic rollback is not implemented).

Common validation messages:

| Message | Fix |
|---|---|
| `dependency cycle: [...]` | Remove one dependency in the cycle |
| `dependency a->b references unknown/inactive task` | Fix the task ID or reactivate the task |
| `missing source.bucket` (or similar) | Add the key to `source_config` / `sink_config` |
| `unknown task_type` | Use one of the types below |
| `group 'x' has unknown parent` | Create the parent group first |
| `DAG has no active tasks` | Add at least one active task |
| `disallowed params [...]` | `params` may only contain `retries`, `pool`, `retry_delay_seconds` |

## 5. Task types and their settings

`source_config` and `sink_config` are JSON. **Bold** keys are required.

| task_type | source_config | sink_config |
|---|---|---|
| `gcs_to_bq` | **bucket**, **objects** (list), format (`CSV` default), skip_leading_rows | **table** (`project.dataset.table`), write_disposition, create_disposition, autodetect, conn_id |
| `bq_to_bq` | **sql** | **table**, write_disposition, create_disposition, conn_id |
| `pg_to_bq` | **conn_id** (Airflow connection), **sql**, **staging_bucket**, staging_prefix | **table**, write_disposition, conn_id |
| `bq_to_files` | **table**, conn_id | **uri** (`gs://bucket/path/*.csv`), format |

`params` (optional): `{"retries": 2, "pool": "etl", "retry_delay_seconds": 300}`.

**Dynamic task mapping:** `mapping` is a list. Each entry can override keys of `source` and `sink`, and one task instance is created per entry:

```sql
UPDATE task_config SET mapping = '[
  {"source": {"objects": ["a/*.parquet"]}, "sink": {"table": "proj.raw.a"}},
  {"source": {"objects": ["b/*.parquet"]}, "sink": {"table": "proj.raw.b"}}]'
WHERE dag_id = 'sales_daily' AND task_id = 'multi_tables';
```

With mapping, each entry together with the base config must contain every required key. Mapping is not supported for `pg_to_bq`.

## 6. Connections for real runs

Tasks use Airflow connection IDs, never passwords in the config tables. Create them in the UI (Admin -> Connections): `google_cloud_default` for BigQuery and GCS, and one per Postgres source such as `pg_crm`. Passwords are encrypted with the Fernet key from `.env`.

## 7. Everyday commands

```bash
docker compose up -d                        # start
docker compose ps                           # status
docker compose logs -f airflow-dag-processor   # parse errors show here
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose down                         # stop, keep data
docker compose down -v                      # stop and DELETE all data (config DB, Airflow DB)
```

Back up the config DB regularly: `docker compose exec -T configdb pg_dump -U postgres config > config_backup.sql`.

## 8. Scaling to thousands of DAGs

The single generator file builds every DAG in its shard on each parse, and Airflow workers parse it again at every task start. Test before relying on it:

1. Load a few thousand synthetic DAGs into the config DB and run the sync.
2. Watch the DAG processor CPU, parse time, and task start latency.
3. If it is slow, split into shards: set `DAG_SHARDS=N` in `.env`, copy `dags/dynamic_dag_generator.py` to `dynamic_dag_generator_00.py` ... `_NN-1.py`, delete the unnumbered file, and run the sync tool once with `--rebuild` (see section 3 for the command). Keep `DAG_SHARDS` identical on every Airflow service and the sync.

Redis holds the cache that every DAG depends on. Enable persistence for it in production, and consider an HA setup. If the cache is empty, the generator shows "metadata-cache empty" until the sync runs.

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| New DAG not in the UI | Did the sync write it? (XCom `written`). Then wait one parse cycle or check `list-import-errors` |
| Change not visible | Same as above; the change appears after sync plus one parse of the generator |
| Generator import error "metadata-cache empty" | Run `metadata_sync` once; check `CONFIG_CACHE_URL` |
| `metadata_sync` never runs on schedule | It is paused: unpause it |
| Invalid DAG vanished after an edit | It did not vanish; the sync kept the last good version. Look at the `report` task log |
| Everything slow | See Scaling; check Redis and Postgres health |
