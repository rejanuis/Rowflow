# pg_to_bq: Postgres → GCS → BigQuery

Two steps in one task group: `extract` (`PostgresToGCSOperator`, JSON files in a staging bucket), then `load` (`GCSToBigQueryOperator`). BigQuery autodetect infers column types; there is no explicit Postgres-to-BigQuery type mapping yet.

| Setting | Required | Notes |
|---|---|---|
| `source.conn_id` | yes | Airflow connection to Postgres |
| `source.sql` | yes | Query to extract |
| `source.staging_bucket` | yes | GCS bucket for the intermediate files |
| `source.staging_prefix` | no | Default `staging/airflow` |
| `sink.table` | yes | `project.dataset.table` |
| `sink.write_disposition`, `sink.create_disposition`, `sink.conn_id` | no | As in `gcs_to_bq` |

Dynamic task mapping is not supported (multi-step type).

```sql
INSERT INTO task_config(dag_id, task_id, task_type, source_config, sink_config) VALUES
 ('sales_daily', 'customers_pg', 'pg_to_bq',
  '{"conn_id":"pg_crm","sql":"select * from customers","staging_bucket":"stg"}',
  '{"table":"proj.raw.customers"}');
```
