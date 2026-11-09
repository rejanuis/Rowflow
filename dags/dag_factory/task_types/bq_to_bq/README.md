# bq_to_bq: BigQuery → BigQuery

Runs a SQL query and writes the result to a table (`BigQueryInsertJobOperator`).

| Setting | Required | Notes |
|---|---|---|
| `source.sql` | yes | Standard SQL (not legacy) |
| `sink.table` | yes | `project.dataset.table` |
| `sink.write_disposition` | no | `WRITE_TRUNCATE` (default), `WRITE_APPEND`, `WRITE_EMPTY` |
| `sink.create_disposition` | no | `CREATE_IF_NEEDED` (default), `CREATE_NEVER` |
| `sink.conn_id` | no | Default `google_cloud_default` |

Supports dynamic task mapping.

```sql
INSERT INTO task_config(dag_id, task_id, task_type, source_config, sink_config) VALUES
 ('sales_daily', 'daily_mart', 'bq_to_bq',
  '{"sql":"select * from proj.raw.orders"}', '{"table":"proj.mart.daily"}');
```
