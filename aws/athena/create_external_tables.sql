CREATE EXTERNAL TABLE IF NOT EXISTS tech_job_lake.bronze_jobs (
  id string,
  title string,
  location string,
  published string,
  description string,
  application_url string,
  has_remote boolean,
  salary_min double,
  salary_max double,
  salary_currency string,
  company struct<
    name:string
  >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://${S3_BUCKET}/bronze/jobs/'
TBLPROPERTIES (
  'classification' = 'json'
);

CREATE EXTERNAL TABLE IF NOT EXISTS tech_job_lake.silver_jobs (
  id string,
  title string,
  location string,
  published string,
  description string,
  application_url string,
  has_remote boolean,
  salary_min double,
  salary_max double,
  salary_currency string,
  company struct<
    name:string
  >
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/silver/jobs/'
TBLPROPERTIES (
  'classification' = 'parquet'
);
