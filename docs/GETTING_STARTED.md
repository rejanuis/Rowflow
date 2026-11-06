# Getting started

Goal: a secured Airflow 3 on one machine (Docker Compose) where your pipelines are built from Postgres config, with the sync running every 3 hours or on demand from the UI.
Time: about 30 minutes. Run every command from the project folder.

## 0. Prerequisites

- Docker Engine with Docker Compose **v2.24.4 or newer** (`docker compose version`)
- `bash` and `openssl` (Linux, macOS, or WSL2 on Windows)
- At least 4 GB RAM given to Docker (8 GB is more comfortable)
- Free host port **8443** (Airflow itself no longer uses 8080)
- Google Cloud credentials are only needed later, when you run real tasks

## 1. Put the files together

Download the official Airflow 3 compose file into the project folder:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
mkdir -p logs plugins
```

Your folder should now contain `docker-compose.yaml` (official) next to `docker-compose.security.yaml` (ours), plus `dags/`, `config/`, `sql/`, `scripts/`, `security/`, `logs/`, `plugins/`.

Two quick checks on the official file:

```bash
grep -n "airflow-apiserver" docker-compose.yaml | head -3     # service must exist (Airflow 3)
grep -n "ALL_ADMINS" docker-compose.yaml                      # must print nothing
```

If `ALL_ADMINS` appears, delete that line. It makes every visitor an admin.

## 2. Generate secrets and a TLS certificate

```bash
bash scripts/setup_security.sh
```

This creates `.env` (random JWT secret, API secret key, Fernet key, config DB password, permissions 600) and a self-signed certificate in `security/certs/`. It never overwrites existing files. Never commit `.env`.

> Never use `AIRFLOW_UID=0`. If you run the script as root, it uses 50000 and fixes folder ownership for you.

> Windows (not WSL): the separator in `COMPOSE_FILE` must be `;`. Add `COMPOSE_PATH_SEPARATOR=;` to `.env` and use `;` in `COMPOSE_FILE`.

## 3. Start the config database and load the schema

```bash
docker compose up -d configdb
docker compose exec -T configdb psql -U postgres -d config < sql/001_schema.sql
docker compose exec -T configdb psql -U postgres -d config < sql/002_sample_data.sql
docker compose exec -T configdb psql -U postgres -d config < sql/003_views.sql
```

Check that the sample pipeline is there:

```bash
docker compose exec configdb psql -U postgres -d config -c "select dag_id, team_name, jsonb_array_length(tasks) as tasks from v_dag_metadata"
```

Expected: one row, `sales_daily`, with 5 tasks.

> If `configdb` already existed from an earlier try with another password, Postgres keeps the old password. Either remove that volume (`docker compose down -v`, which deletes data) or run `ALTER USER postgres PASSWORD '<value of CONFIG_DB_PASSWORD>'`.

## 4. Initialize Airflow

```bash
docker compose up airflow-init
```

Wait for `airflow-init ... exited with code 0`. If the output says it created a default user such as `airflow`, delete it later in step 6.

## 5. Create the login tables and start everything

```bash
docker compose run --rm airflow-scheduler airflow fab-db migrate
docker compose up -d
docker compose ps
```

All services should become `running` or `healthy`: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`, `postgres`, `redis`, `configdb`, `nginx`. The first start installs Python packages and can take a few minutes.

## 6. Create your admin user

```bash
docker compose exec airflow-apiserver airflow users create \
  --username admin --firstname Admin --lastname User --role Admin --email you@example.com
```

It asks for a password, so the password never appears in your shell history. Use a strong one.
If step 4 created a default user, remove it: `docker compose exec airflow-apiserver airflow users delete --username airflow`.

## 7. Log in

Open **https://localhost:8443**. Your browser warns about the self-signed certificate, which is expected in dev: continue anyway. Log in with the admin user.

Nothing else listens on your machine: `http://localhost:8080` should not respond.

## 8. Run the sync for the first time

1. In the DAG list find **`metadata_sync`** and switch on the toggle on the left (unpause). New DAGs start paused, and a paused DAG will not run on schedule.
2. Click the **Trigger** button (play icon). Use **Trigger DAG w/ config** if you want the options: `dag_id` (sync one DAG only) and `dry_run` (validate without writing).
3. Open the run. Both tasks, `sync_cache` and `report`, should be green. In `sync_cache`, open **XCom** to see `written: ['sales_daily']`.

Before this first run, the generator file shows an import error "metadata-cache empty". That is expected.

Check the cache directly if you like:

```bash
docker compose exec redis redis-cli -n 5 hkeys metadata:hashes
```

## 9. See your DAG

After about a minute, `sales_daily` appears in the DAG list. In its Graph view you should see:

- group `ingest` with a nested group `ingest_pg` (the Postgres extract and load pair)
- `orders_gcs`, the mapped task `multi_tables`, then `daily_mart`, then `export_csv`

If it does not appear:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail=50 airflow-dag-processor
```

The sample tasks use fake buckets and projects, so running `sales_daily` for real will fail. That is fine for the check. See OPERATIONS to wire in real connections.

## 10. From now on

The sync runs every 3 hours (00:00, 03:00, 06:00 UTC and so on). After editing config in Postgres, trigger `metadata_sync` to see the change within a minute or two instead of waiting. More in [OPERATIONS.md](OPERATIONS.md).

## Start / stop cheat sheet

```bash
docker compose up -d          # start everything
docker compose ps             # status
docker compose logs -f airflow-apiserver nginx
docker compose down           # stop, keep all data
docker compose down -v        # stop and DELETE all data, including the config DB
```

## If a step fails

| Symptom | Likely cause and fix |
|---|---|
| Warning `The "FERNET_KEY" variable is not set` | Harmless, but silence it: `echo "FERNET_KEY=$(grep ^AIRFLOW_FERNET_KEY= .env \| cut -d= -f2-)" >> .env`. New `.env` files from `setup_security.sh` already include it. Check the real key is used: `docker compose config \| grep FERNET_KEY` must show a long value, not an empty string |
| `You are running pip as root. Please use 'airflow' user` | `AIRFLOW_UID` in `.env` is `0` (the setup script was run as root). Fix: `sed -i 's/^AIRFLOW_UID=.*/AIRFLOW_UID=50000/' .env` (macOS: `sed -i '' ...`), `mkdir -p logs plugins && chown -R 50000:0 logs plugins dags config` (add `sudo` if not root), then `docker compose down` and `docker compose up airflow-init` |
| `Permission denied` writing to `logs/` | Same ownership fix: `chown -R <AIRFLOW_UID>:0 logs plugins` |
| `400 Bad Request: The plain HTTP request was sent to HTTPS port` even with `https://` | Airflow redirected to `http://` because the API server ignores proxy headers without `--proxy-headers`. The override must contain `command: api-server --proxy-headers` under `airflow-apiserver`, and nginx must have `proxy_redirect off;`. Then `docker compose up -d --force-recreate airflow-apiserver nginx`. Check: `curl -k -i https://localhost:8443/ \| grep -i ^location` must not show `http://` |
| DAG import error `attempted relative import with no known parent package` for `dag_factory/metadata_sync.py` | Airflow scanned the helper package as a DAG file. `dags/.airflowignore` must contain the line `dag_factory/`. Then `docker compose restart airflow-dag-processor` |
| DAG import error `metadata-cache empty for shard 0` | Expected before the first sync. Unpause and trigger `metadata_sync` (step 8) |
| `required variable AIRFLOW_JWT_SECRET is missing` | Step 2 not done, or you are in the wrong folder (no `.env`) |
| `docker compose` says `!reset` unknown | Compose is older than 2.24.4: upgrade it |
| Login page does not load / browser shows an error | `docker compose logs nginx airflow-apiserver`; check `docker compose ps` for `airflow-apiserver` |
| Login works but pages loop or links point to the wrong address | `AIRFLOW_BASE_URL` in `.env` must equal the address in your browser |
| `no such table` for users or roles | Run the `fab-db migrate` command from step 5 again |
| `ModuleNotFoundError: redis / psycopg / fab` | Packages not installed: check `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`, then `docker compose up -d --force-recreate` |
| `sync_cache` fails with a connection error | `docker compose ps`: `configdb` and `redis` must be up; check `CONFIG_DB_PASSWORD` matches the database (see step 3 note) |
| `report` task is red | A DAG config is invalid. The task log names the DAG and the problem (see OPERATIONS) |
