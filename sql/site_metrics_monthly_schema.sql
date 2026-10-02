-- Seven-column serving contract v1. Local Parquet has exactly these columns.
-- LOCATION is deliberately a deployment placeholder, to be set by R2 on the VM.
-- This DDL has not been executed in Hive/Impala here.
CREATE EXTERNAL TABLE IF NOT EXISTS site_metrics_monthly (
  site STRING,
  month STRING,
  domain_group STRING,
  questions_count BIGINT,
  new_users_count BIGINT,
  answered_rate DOUBLE,
  downvote_ratio DOUBLE
)
STORED AS PARQUET
LOCATION '/REPLACE_WITH_R2_SERVING_PATH/site_metrics_monthly';
