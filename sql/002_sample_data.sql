INSERT INTO dag_config(dag_id,team_name,project_name,schedule,start_date)
VALUES ('sales_daily','data-eng','sales','0 2 * * *','2026-01-01');
INSERT INTO task_group_config VALUES ('sales_daily','ingest',NULL),('sales_daily','ingest_pg','ingest'),('sales_daily','mart',NULL);
INSERT INTO task_config(dag_id,task_id,group_id,task_type,source_config,sink_config,mapping,params) VALUES
('sales_daily','orders_gcs','ingest','gcs_to_bq','{"bucket":"raw","objects":["orders/*.csv"]}','{"table":"proj.raw.orders"}',NULL,'{"retries":2}'),
('sales_daily','customers_pg','ingest_pg','pg_to_bq','{"conn_id":"pg_crm","sql":"select * from customers","staging_bucket":"stg"}','{"table":"proj.raw.customers"}',NULL,'{}'),
('sales_daily','multi_tables','ingest','gcs_to_bq','{"bucket":"raw","format":"PARQUET"}','{}',
 '[{"source":{"objects":["a/*.parquet"]},"sink":{"table":"proj.raw.a"}},{"source":{"objects":["b/*.parquet"]},"sink":{"table":"proj.raw.b"}}]','{}'),
('sales_daily','daily_mart','mart','bq_to_bq','{"sql":"select o.* from proj.raw.orders o join proj.raw.customers c using(customer_id)"}','{"table":"proj.mart.daily"}',NULL,'{}'),
('sales_daily','export_csv',NULL,'bq_to_files','{"table":"proj.mart.daily"}','{"uri":"gs://exports/daily/*.csv"}',NULL,'{}');
INSERT INTO task_dependency VALUES
('sales_daily','orders_gcs','daily_mart'),('sales_daily','customers_pg','daily_mart'),
('sales_daily','multi_tables','daily_mart'),('sales_daily','daily_mart','export_csv');
