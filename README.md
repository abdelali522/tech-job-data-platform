# Tech Job Data Platform

An end-to-end data engineering project that ingests job-market data from a REST API, streams it through Kafka, processes it with Spark Structured Streaming, stores medallion layers in AWS S3, catalogs the data with Glue/Athena, and serves curated Gold data from PostgreSQL for analytics and BI.

This project is designed as a production-style portfolio pipeline for modern data engineering workflows.

## Architecture

```text
CleanJobData API
        |
        v
Airflow-orchestrated Python ingestion
        |
        v
Kafka topic: jobs.raw
        |
        v
Spark Structured Streaming
        |
        +--> AWS S3 Bronze: raw JSON
        |
        +--> AWS S3 Silver: typed Parquet
        |
        +--> PostgreSQL Gold: analytics-ready serving table
        |
        v
Power BI / SQL consumers

AWS S3 Silver
        |
        v
AWS Glue Data Catalog
        |
        v
AWS Athena SQL
```

## What This Project Demonstrates

- REST API ingestion with retries and pagination
- Kafka-based event streaming
- Spark Structured Streaming processing
- Medallion architecture: Bronze, Silver, Gold
- AWS S3 data lake storage
- AWS Glue/Athena external querying over S3
- PostgreSQL Gold serving layer with upsert logic
- Airflow orchestration and scheduling
- Docker Compose local infrastructure
- Environment-based configuration
- Basic ingestion tests with `pytest`

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Ingestion | Python, Requests |
| Streaming broker | Apache Kafka |
| Stream processing | Apache Spark Structured Streaming |
| Data lake | AWS S3 |
| Catalog/query | AWS Glue Data Catalog, Amazon Athena |
| Serving database | PostgreSQL |
| BI-ready output | PostgreSQL Gold table |
| Infrastructure | Docker Compose |
| Testing | Pytest |

## Repository Structure

```text
airflow/dags/                 Airflow DAG for scheduled ingestion
aws/athena/                   Athena SQL templates for Glue tables and views
config/                       Environment and API settings
docs/                         Additional architecture and AWS setup notes
ingestion/                    CleanJobData API ingestion and Kafka producer
scripts/                      PowerShell helper scripts
storage/gold/                 PostgreSQL initialization SQL
streaming/                    Spark Structured Streaming job
tests/                        Ingestion unit tests
transformation/               Older local batch transformation experiment
docker-compose.yml            Local platform services
documentation.md              Full detailed project documentation
requirements.txt              Python dependencies
```

## Data Flow

1. Airflow runs the ingestion DAG on an hourly schedule.
2. The Python ingestion script calls the CleanJobData API.
3. API results are published to Kafka topic `jobs.raw`.
4. Spark Structured Streaming reads from Kafka.
5. Raw JSON payloads are written to the S3 Bronze layer.
6. Parsed job records are written to the S3 Silver layer as Parquet.
7. Curated records are upserted into PostgreSQL Gold.
8. Glue/Athena external tables expose S3 data for SQL analytics.

## S3 Layout

By default, the Spark job writes to:

```text
s3://<bucket>/bronze/jobs/
s3://<bucket>/silver/jobs/
s3://<bucket>/checkpoints/jobs/
```

Athena query results can be stored under:

```text
s3://<bucket>/athena-results/
```

## Prerequisites

- Docker Desktop
- Python virtual environment for local development
- AWS account
- S3 bucket
- AWS access key with permissions for S3, Glue, and Athena
- CleanJobData API key

## Environment Configuration

Create a `.env` file from `.env.example`:

```powershell
Copy-Item .env.example .env
```

Then fill in the required values:

```env
CLEANJOBDATA_API_KEY=your_cleanjobdata_key
CLEANJOBDATA_COUNTRIES=za
CLEANJOBDATA_TITLE=data engineer
CLEANJOBDATA_MAX_PAGES=5
CLEANJOBDATA_PAGE_LIMIT=20

POSTGRES_PASSWORD=your_local_password
SPARK_MAX_OFFSETS_PER_TRIGGER=1000

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_WEBSERVER_SECRET_KEY=replace_with_a_secret

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_SESSION_TOKEN=
AWS_DEFAULT_REGION=your_bucket_region
S3_BUCKET=your_s3_bucket_name
ATHENA_DATABASE=tech_job_lake
ATHENA_QUERY_RESULTS=s3://your_s3_bucket_name/athena-results/
```

The `.env` file is ignored by Git and should never be committed.

## Running The Platform

Start all services:

```powershell
docker compose up -d
```

Check service status:

```powershell
docker compose ps
```

Open Airflow:

```text
http://localhost:8081
```

Default local login:

```text
username: admin
password: admin
```

Enable or manually trigger the DAG:

```text
tech_job_ingestion
```

## Key Local URLs

| Service | URL |
|---|---|
| Airflow UI | `http://localhost:8081` |
| Spark UI | `http://localhost:8080` |
| PostgreSQL | `127.0.0.1:5433` |
| Kafka external listener | `localhost:9094` |

## Verifying The Pipeline

Check Spark logs:

```powershell
docker compose logs --tail=200 spark-streaming
```

Check PostgreSQL Gold row count:

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT COUNT(*) FROM jobs_gold;"
```

Inspect recent Gold rows:

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT id, job_title, company_name, location_name, published FROM jobs_gold ORDER BY updated_at DESC LIMIT 10;"
```

Confirm files exist in S3:

```text
bronze/jobs/
silver/jobs/
checkpoints/jobs/
```

## AWS Glue and Athena Setup

Render Athena SQL using the bucket configured in `.env`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\render_athena_sql.ps1
```

Run these generated files in Athena, in order:

```text
build/athena/create_database.sql
build/athena/create_external_tables.sql
build/athena/create_views.sql
```

Example Athena query:

```sql
SELECT company_name, job_count
FROM tech_job_lake.jobs_by_company
ORDER BY job_count DESC
LIMIT 10;
```

## Running Tests

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe -m pytest
```

The current tests cover:

- API cursor pagination
- invalid API response shape handling
- HTTP retry configuration

## Main Components

### Airflow

Airflow schedules the ingestion workflow. The DAG is defined in:

```text
airflow/dags/tech_job_ingestion.py
```

It runs:

```text
python ingestion/api_ingestion.py
```

### Kafka

Kafka decouples ingestion from processing. Jobs are published to:

```text
jobs.raw
```

Malformed records are sent by Spark to:

```text
jobs.dlq
```

### Spark Structured Streaming

Spark reads Kafka continuously and writes:

- raw JSON to S3 Bronze
- structured Parquet to S3 Silver
- curated rows to PostgreSQL Gold

The streaming job is:

```text
streaming/spark_streaming.py
```

### PostgreSQL

PostgreSQL stores the Gold serving layer.

Tables:

```text
jobs_gold
jobs_gold_stage
```

`jobs_gold` uses `id` as the primary key and is updated through an upsert.

### AWS S3

S3 stores the data lake and Spark checkpoints.

### Glue/Athena

Glue stores table metadata and Athena queries S3 data directly with SQL.

## Troubleshooting

### Docker cannot connect

Make sure Docker Desktop is running. If needed:

```powershell
wsl --shutdown
```

Then restart Docker Desktop.

### S3 403 Forbidden

The AWS key does not have enough permissions. Check S3 bucket permissions for:

```text
bronze/*
silver/*
checkpoints/*
athena-results/*
```

### NoSuchBucket

Check that `S3_BUCKET` exactly matches the AWS bucket name.

### Region or signature errors

Set `AWS_DEFAULT_REGION` to the actual bucket region.

### Airflow DAG does not appear

Check scheduler logs:

```powershell
docker compose logs --tail=200 airflow-scheduler
```

### PostgreSQL row count is zero

Check Airflow task logs, Spark logs, S3 outputs, and PostgreSQL status in that order.

## Detailed Documentation

For a full step-by-step explanation of every component, see:

```text
documentation.md
```

That file explains the project in more detail, including small implementation decisions and troubleshooting notes.

## Future Improvements

- Partition Silver data by country and ingestion date
- Add data quality checks for missing IDs, duplicate jobs, dates, and salaries
- Add CI with GitHub Actions
- Add Terraform for AWS infrastructure
- Add Power BI dashboard screenshots
- Add monitoring for Kafka lag, Spark failures, and Airflow runs
- Move secrets to AWS Secrets Manager or Parameter Store

## Resume Summary

This project can be summarized as:

```text
Designed and containerized an end-to-end data engineering platform using Airflow, Kafka, Spark Structured Streaming, AWS S3, Glue/Athena, and PostgreSQL to ingest, process, catalog, and serve job-market data for analytics.
```
