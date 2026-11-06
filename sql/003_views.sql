-- Read-model for the sync job: all joins/aggregation live here, the sync does a plain SELECT.
-- Column names of v_dag_metadata == keys of the DAG payload (what the generator and validator expect).
-- Safe to re-run (CREATE OR REPLACE).

CREATE OR REPLACE VIEW v_active_task AS
SELECT t.dag_id, t.task_id, t.group_id, t.task_type,
       t.source_config, t.sink_config, t.mapping, t.params
FROM task_config t
WHERE t.is_active;

-- dependencies whose upstream AND downstream tasks are both active (soft-delete safe)
CREATE OR REPLACE VIEW v_active_dependency AS
SELECT d.dag_id, d.upstream, d.downstream
FROM task_dependency d
JOIN v_active_task u ON u.dag_id = d.dag_id AND u.task_id = d.upstream
JOIN v_active_task v ON v.dag_id = d.dag_id AND v.task_id = d.downstream;

-- one row per active DAG, with groups / tasks / dependencies already nested as JSON
CREATE OR REPLACE VIEW v_dag_metadata AS
SELECT
  d.dag_id,
  d.team_name,
  d.project_name,
  d.description,
  d.schedule,
  to_char(d.start_date, 'YYYY-MM-DD') AS start_date,
  d.max_active_runs,
  d.default_args,
  d.tags,
  COALESCE((SELECT jsonb_agg(jsonb_build_object('group_id', g.group_id, 'parent_group', g.parent_group)
                             ORDER BY g.group_id)
            FROM task_group_config g WHERE g.dag_id = d.dag_id), '[]'::jsonb) AS groups,
  COALESCE((SELECT jsonb_agg(jsonb_build_object(
                      'task_id', t.task_id, 'group_id', t.group_id, 'task_type', t.task_type,
                      'source_config', t.source_config, 'sink_config', t.sink_config,
                      'mapping', t.mapping, 'params', t.params) ORDER BY t.task_id)
            FROM v_active_task t WHERE t.dag_id = d.dag_id), '[]'::jsonb) AS tasks,
  COALESCE((SELECT jsonb_agg(jsonb_build_object('upstream', x.upstream, 'downstream', x.downstream)
                             ORDER BY x.upstream, x.downstream)
            FROM v_active_dependency x WHERE x.dag_id = d.dag_id), '[]'::jsonb) AS dependencies
FROM dag_config d
WHERE d.is_active;
