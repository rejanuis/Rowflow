# bq_to_files: BigQuery → files in GCS

Exports a table to Cloud Storage (`BigQueryToGCSOperator`). Use it as the first half of a BigQuery → Hive/files flow (a Hive external table can read the files).

| Setting | Required | Notes |
|---|---|---|
| `source.table` | yes | `project.dataset.table` |
| `source.conn_id` | no | Default `google_cloud_default` |
| `sink.uri` | yes | Must start with `gs://`, e.g. `gs://exports/daily/*.csv` |
| `sink.format` | no | `CSV` (default), `NEWLINE_DELIMITED_JSON`, `AVRO`, `PARQUET` |

Supports dynamic task mapping.

```sql
INSERT INTO task_config(dag_id, task_id, task_type, source_config, sink_config) VALUES
 ('sales_daily', 'export_csv', 'bq_to_files',
  '{"table":"proj.mart.daily"}', '{"uri":"gs://exports/daily/*.csv"}');
```
