# gcs_to_bq: GCS → BigQuery

Loads files from Cloud Storage into a BigQuery table (`GCSToBigQueryOperator`).

| Setting | Required | Notes |
|---|---|---|
| `source.bucket` | yes | Bucket name |
| `source.objects` | yes | List of object paths or patterns, e.g. `["orders/*.csv"]` |
| `source.format` | no | `CSV` (default), `NEWLINE_DELIMITED_JSON`, `PARQUET`, ... |
| `source.skip_leading_rows` | no | CSV header rows to skip (default 1) |
| `sink.table` | yes | `project.dataset.table` |
| `sink.write_disposition` | no | `WRITE_APPEND` (default), `WRITE_TRUNCATE`, `WRITE_EMPTY` |
| `sink.create_disposition` | no | `CREATE_IF_NEEDED` (default), `CREATE_NEVER` |
| `sink.autodetect` | no | Let BigQuery infer the schema (default `true`) |
| `sink.conn_id` | no | Airflow connection, default `google_cloud_default` |

Supports dynamic task mapping.

```sql
INSERT INTO task_config(dag_id, task_id, task_type, source_config, sink_config) VALUES
 ('sales_daily', 'orders_gcs', 'gcs_to_bq',
  '{"bucket":"raw","objects":["orders/*.csv"]}', '{"table":"proj.raw.orders"}');
```
