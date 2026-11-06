-- Config DB (separate from the Airflow metastore). PostgreSQL 14+
CREATE TABLE task_type_catalog (          -- drives the Phase 2 UI (source/sink pickers)
  task_type    TEXT PRIMARY KEY,          -- gcs_to_bq, bq_to_bq, pg_to_bq, bq_to_files
  source_type  TEXT NOT NULL,
  sink_type    TEXT NOT NULL,
  config_schema JSONB NOT NULL DEFAULT '{}'
);
INSERT INTO task_type_catalog(task_type, source_type, sink_type) VALUES
 ('gcs_to_bq','gcs','bigquery'),('bq_to_bq','bigquery','bigquery'),
 ('pg_to_bq','postgres','bigquery'),('bq_to_files','bigquery','gcs_files');

CREATE TABLE dag_config (                 -- UI: project_name/team_name -> dag
  dag_id        TEXT PRIMARY KEY CHECK (dag_id ~ '^[A-Za-z0-9_.-]{1,250}$'),
  team_name     TEXT NOT NULL,
  project_name  TEXT NOT NULL,
  description   TEXT,
  schedule      TEXT,                     -- cron / preset / NULL
  start_date    DATE NOT NULL DEFAULT CURRENT_DATE,
  max_active_runs INT NOT NULL DEFAULT 1,
  default_args  JSONB NOT NULL DEFAULT '{}',
  tags          TEXT[] NOT NULL DEFAULT '{}',
  owner_email   TEXT,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  content_hash  TEXT,                     -- written by sync job
  config_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (team_name, project_name)
);

CREATE TABLE task_group_config (
  dag_id       TEXT NOT NULL REFERENCES dag_config ON DELETE CASCADE,
  group_id     TEXT NOT NULL,
  parent_group TEXT,
  PRIMARY KEY (dag_id, group_id),
  FOREIGN KEY (dag_id, parent_group) REFERENCES task_group_config (dag_id, group_id)
);

CREATE TABLE task_config (                -- UI: etl_name -> task
  dag_id        TEXT NOT NULL REFERENCES dag_config ON DELETE CASCADE,
  task_id       TEXT NOT NULL,            -- etl_name
  group_id      TEXT,
  task_type     TEXT NOT NULL REFERENCES task_type_catalog,
  source_config JSONB NOT NULL DEFAULT '{}',
  sink_config   JSONB NOT NULL DEFAULT '{}',
  mapping       JSONB,                    -- list of overrides -> dynamic task mapping
  params        JSONB NOT NULL DEFAULT '{}', -- retries, pool, retry_delay_seconds
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (dag_id, task_id),
  FOREIGN KEY (dag_id, group_id) REFERENCES task_group_config (dag_id, group_id)
);

CREATE TABLE task_dependency (            -- UI: upstream/downstream
  dag_id     TEXT NOT NULL,
  upstream   TEXT NOT NULL,
  downstream TEXT NOT NULL,
  PRIMARY KEY (dag_id, upstream, downstream),
  CHECK (upstream <> downstream),
  FOREIGN KEY (dag_id, upstream)   REFERENCES task_config ON DELETE CASCADE,
  FOREIGN KEY (dag_id, downstream) REFERENCES task_config ON DELETE CASCADE
);

CREATE TABLE dag_config_history (         -- audit + rollback
  id BIGSERIAL PRIMARY KEY, dag_id TEXT NOT NULL, content_hash TEXT NOT NULL,
  payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE sync_log (
  id BIGSERIAL PRIMARY KEY, run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  dag_id TEXT, status TEXT NOT NULL, message TEXT   -- written | failed | removed
);

-- any child change marks the parent DAG dirty
CREATE FUNCTION touch_dag() RETURNS trigger AS $$
BEGIN
  UPDATE dag_config SET config_updated_at = now()
   WHERE dag_id = COALESCE(NEW.dag_id, OLD.dag_id);
  RETURN NULL;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER t_grp  AFTER INSERT OR UPDATE OR DELETE ON task_group_config FOR EACH ROW EXECUTE FUNCTION touch_dag();
CREATE TRIGGER t_task AFTER INSERT OR UPDATE OR DELETE ON task_config       FOR EACH ROW EXECUTE FUNCTION touch_dag();
CREATE TRIGGER t_dep  AFTER INSERT OR UPDATE OR DELETE ON task_dependency   FOR EACH ROW EXECUTE FUNCTION touch_dag();
CREATE INDEX ON task_config (dag_id);
CREATE INDEX ON task_dependency (dag_id);
CREATE INDEX ON dag_config (team_name);
