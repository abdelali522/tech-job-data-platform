# Tech Job Data Platform

## 1. Purpose

This project collects job listings from CleanJobData, publishes them to Kafka, processes them with Spark Structured Streaming, stores data in a medallion architecture, and exposes the Gold data to PostgreSQL and Power BI.

The current implementation is a local development version of a scalable streaming platform.

```text
CleanJobData API
      |
      v
Python ingestion producer
      |
      v
Kafka topic: jobs.raw
      |
      v
Spark Structured Streaming
      |
      +--> Bronze: raw JSON in AWS S3
      +--> Silver: Parquet in AWS S3
      +--> Gold: PostgreSQL jobs_gold
                              |
                              v
                         Power BI
```

The current dataset is small, but the architecture supports larger volumes and continuous ingestion.

This is a production-style local baseline. CleanJobData remains a polled REST source; the internal pipeline is event-driven after each poll.

## 2. Repository Structure

```text
config/
  settings.py              Loads .env values and API configuration.

ingestion/
  api_ingestion.py         Fetches CleanJobData and publishes Kafka messages.

streaming/
  spark_streaming.py       Reads Kafka and writes Bronze, Silver, and Gold.

airflow/
  dags/
    tech_job_ingestion.py  Airflow DAG that schedules the CleanJobData producer.

storage/
  bronze/                  Existing local raw JSON files from earlier ingestion.
  gold/
    postgres_init.sql      Creates PostgreSQL Gold and staging tables.
  checkpoints/             Legacy local Spark checkpoints from earlier runs.
  logs/                    Scheduled ingestion logs.

scripts/
  run_ingestion.ps1        Manual Windows ingestion fallback.

docker-compose.yml         Local Airflow, Kafka, Spark, and PostgreSQL services.
requirements.txt           Python dependencies for the producer.
.env                       Local API key; never commit this file.
.gitignore                 Protects secrets, logs, and Python cache files.
```

## 3. Configuration

### `config/settings.py`

This module loads the project-root `.env` file with `python-dotenv`:

```python
load_dotenv(project_root / ".env")
```

It exposes:

- `CLEANJOBDATA_API_KEY`: Bearer token for CleanJobData.
- `CLEANJOBDATA_API_URL`: `https://api.cleanjobdata.com/jobs`.

The API key is not hard-coded in Python. Keep `.env` local and excluded by `.gitignore`.

Example `.env`:

```text
CLEANJOBDATA_API_KEY=your_key_here
POSTGRES_PASSWORD=your_local_postgres_password
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=your-tech-job-bucket
```

The configured S3 bucket must already exist in AWS. The streaming job writes `bronze/`, `silver/`, and `checkpoints/` prefixes inside that bucket by default.

## 4. Ingestion Layer

### `ingestion/api_ingestion.py`

This file is the source adapter and Kafka producer.

### 4.1 Import path handling

When launched directly with:

```powershell
python ingestion\api_ingestion.py
```

Python starts with the `ingestion` directory on its import path. The script adds the project root to `sys.path` so the sibling `config` package can be imported.

### 4.2 `fetch_tech_jobs(country)`

This function:

1. Checks that `CLEANJOBDATA_API_KEY` exists.
2. Requests CleanJobData's `/jobs` endpoint.
3. Searches for `data engineer` jobs.
4. Filters by an ISO country code, such as `ZA`.
5. Requests up to 20 jobs per API page.
6. Requests the full description with `extra_fields=description`.
7. Follows CleanJobData cursor pagination through `pagination.next_page`.
8. Stops after five pages or when no next cursor exists.
9. Returns a Python list of job dictionaries.

CleanJobData uses cursor pagination, not numbered pages. The cursor is sent back as `next_page` on the next request.

The HTTP request uses:

```text
Authorization: Bearer <CLEANJOBDATA_API_KEY>
```

A 30-second request timeout prevents the producer from hanging indefinitely. The HTTP session is configured with retries for temporary API failures and rate-limit responses.

### 4.3 `publish_jobs_to_kafka(jobs)`

This function first tries to use `confluent-kafka`, which is suitable for a production Kafka producer. If that native package cannot be imported, the code falls back to `kafka-python`, which avoids Windows DLL policy issues seen on some machines.

Each job is serialized as UTF-8 JSON and sent as one message to:

```text
Topic: jobs.raw
```

The producer uses these bootstrap servers:

- From the host: `localhost:9094`.
- Inside Docker: `kafka:9092`.

The two Kafka listeners exist because the host Python process and Docker containers use different network addresses.

For `confluent-kafka`, `producer.flush(30)` waits for messages to be delivered before the process exits. For the `kafka-python` fallback, each send waits for delivery with a timeout and then flushes before closing.

The primary producer enables Kafka idempotence, retries buffer-full conditions, and fails the run when a delivery callback reports an error.

### 4.4 Script entry point

The default entry point loops over `CLEANJOBDATA_COUNTRIES`:

```python
jobs = []
for country in CLEANJOBDATA_COUNTRIES:
    jobs.extend(fetch_tech_jobs(country))
publish_jobs_to_kafka(jobs)
```

By default, `CLEANJOBDATA_COUNTRIES` is `za`, but `.env` can set multiple countries such as `za,us,gb`.

## 5. Kafka Layer

Kafka provides the durable buffer between ingestion and processing.

### Topic

```text
jobs.raw
```

Each Kafka message contains one complete CleanJobData job object.

### Listeners

The Kafka service has:

```text
Internal Docker address: kafka:9092
Host address:             localhost:9094
```

The Spark container uses `kafka:9092`. The Windows producer uses `localhost:9094`.

### Why Kafka is present

Kafka allows the system to:

- Buffer data when Spark is temporarily unavailable.
- Replay messages from an offset.
- Add more downstream consumers later.
- Separate API ingestion from transformation.
- Scale producers and consumers independently.

CleanJobData itself is still polled. Kafka makes your internal pipeline event-driven after ingestion; it does not turn the external API into a push API.

## 6. Spark Streaming Layer

### `streaming/spark_streaming.py`

This is the active transformation and storage pipeline.

The Docker command runs:

```text
spark-submit --master spark://spark-master:7077 streaming/spark_streaming.py
```

Spark loads these packages at startup:

- Spark Kafka connector.
- PostgreSQL JDBC driver.
- Hadoop AWS/S3A connector for AWS S3.

The Ivy cache is redirected to `/tmp/.ivy2` because the Spark container needs a writable dependency cache.

### 6.1 `JOB_SCHEMA`

The schema defines the fields Spark extracts from each Kafka JSON message:

- `id`
- `title`
- `location`
- `published`
- `description`
- `application_url`
- `has_remote`
- `salary_min`
- `salary_max`
- `salary_currency`
- `company.name`

The job ID is treated as a string because external APIs can use numeric-looking IDs but identifiers should not be used for arithmetic.

### 6.2 Kafka input

Spark reads the Kafka value as binary, converts it to a string, parses it as JSON, and expands the `job` struct:

```text
Kafka value -> string -> from_json -> structured job columns
```

The stream subscribes to `jobs.raw` and uses `startingOffsets=earliest` for initial replay. Checkpoints control what is processed after the first run.

`SPARK_MAX_OFFSETS_PER_TRIGGER` limits backlog processing per micro-batch. Malformed JSON and records without a job ID are sent to `jobs.dlq` with the original payload and an error description.

### 6.3 Checkpoints

The checkpoint root defaults to:

```text
s3a://$S3_BUCKET/checkpoints/jobs
```

Separate checkpoint directories are used for:

```text
s3a://$S3_BUCKET/checkpoints/jobs/bronze
s3a://$S3_BUCKET/checkpoints/jobs/silver
s3a://$S3_BUCKET/checkpoints/jobs/gold
```

Checkpoints preserve streaming progress across restarts. Do not delete them casually, because deleting them can cause old Kafka messages to be replayed.

## 7. Bronze Layer

The Bronze query serializes each parsed job back to JSON and writes it to:

```text
s3a://$S3_BUCKET/bronze/jobs
```

The path can be overridden with `S3_BRONZE_PATH`.

Bronze should retain source-level information with minimal transformation. It is useful for:

- Auditing the source data.
- Reprocessing after a transformation bug.
- Debugging malformed records.
- Rebuilding Silver and Gold.

Older manual API runs also produced files under `storage/bronze/*.json`. Those files are separate from the active streaming S3 Bronze path.

## 8. Silver Layer

The Silver query writes the structured Spark rows as Parquet:

```text
s3a://$S3_BUCKET/silver/jobs
```

The path can be overridden with `S3_SILVER_PATH`.

Parquet is useful because it is:

- Columnar.
- Compressed.
- Efficient for analytical reads.
- Typed compared with raw JSON.

The current streaming Silver path writes the parsed CleanJobData schema. It does not currently flatten every nested company or location field.

## 9. Gold Layer

## 9A. Glue and Athena

Athena can query the S3 lake directly through Glue Data Catalog external tables.

SQL templates live in:

```text
aws/athena/
```

Render them with:

```powershell
.\scripts\render_athena_sql.ps1
```

Then run the generated SQL files from `build/athena/` in Athena. See `docs/aws_glue_athena.md` for the full setup.

### PostgreSQL tables

`storage/gold/postgres_init.sql` creates:

### `jobs_gold`

This is the Power BI-facing table:

- `id`: primary key.
- `job_title`: job title.
- `location_name`: human-readable location.
- `published`: publication timestamp.
- `job_description`: full description.
- `application_url`: application link.
- `company_name`: company name.
- `salary_min`: lower salary bound.
- `salary_max`: upper salary bound.
- `salary_currency`: currency code.
- `updated_at`: time of the latest upsert.

### `jobs_gold_stage`

This is an internal staging table used for each Spark micro-batch. It should not be used as the dashboard source.

### 9.1 Gold write process

For each non-empty micro-batch, `write_gold`:

1. Selects and renames normalized columns.
2. Converts `published` from ISO text to a Spark timestamp.
3. Removes duplicate IDs within the micro-batch.
4. Writes the batch to `jobs_gold_stage`.
5. Executes PostgreSQL `INSERT ... ON CONFLICT (id) DO UPDATE`.
6. Updates existing jobs or inserts new jobs.
7. Closes the JDBC connection.

This makes ingestion idempotent. Publishing the same job multiple times does not create duplicate rows in `jobs_gold`.

## 10. Docker Compose Services

### Kafka

Runs the Kafka broker and KRaft controller without ZooKeeper.

### AWS S3

Spark writes Bronze, Silver, and checkpoint data to the S3 bucket configured by `S3_BUCKET`.

By default:

```text
s3a://$S3_BUCKET/bronze/jobs
s3a://$S3_BUCKET/silver/jobs
s3a://$S3_BUCKET/checkpoints/jobs
```

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, and `AWS_DEFAULT_REGION` are passed to the Spark container from `.env`.

Kafka and PostgreSQL expose healthchecks. Production deployments should provide AWS and database credentials through a secret manager rather than plain `.env` files.

### PostgreSQL

Runs the Gold database:

```text
Host:     127.0.0.1
Port:     5433
Database: tech_jobs
User:     tech_jobs
```

Inside Docker, Spark uses:

```text
jdbc:postgresql://postgres:5432/tech_jobs
```

Port `5433` is used on Windows because another PostgreSQL process was already listening on host port `5432`.

### Spark master and worker

The master coordinates Spark jobs. The worker executes tasks. The streaming container submits the application to the master.

Spark UI:

```text
http://localhost:8080
```

### Volumes

Named Docker volumes preserve service data:

- `kafka_data`
- `postgres_data`

Removing volumes deletes persisted local data. Do not use `docker compose down -v` unless you intentionally want a clean reset.

## 11. Scheduled Ingestion

### Airflow

Airflow orchestrates the hourly ingestion run through:

```text
airflow/dags/tech_job_ingestion.py
```

The `tech_job_ingestion` DAG runs one task:

```text
publish_jobs_to_kafka
```

That task executes the existing producer from inside Docker:

```bash
cd /opt/project && python ingestion/api_ingestion.py
```

Inside the Airflow container, `KAFKA_BOOTSTRAP_SERVERS` is set to `kafka:9092`.

Airflow UI:

```text
http://localhost:8081
```

Default local login:

```text
Username: admin
Password: admin
```

Override these in `.env` with `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, and `AIRFLOW_ADMIN_EMAIL`.

### `scripts/run_ingestion.ps1`

This script remains available as a manual Windows fallback.

It:

1. Finds the project root.
2. Uses `venv\Scripts\python.exe` explicitly.
3. Creates `storage/logs`.
4. Runs `ingestion/api_ingestion.py` from the project root.
5. Appends output to a daily log file.
6. Throws an error when Python returns a non-zero exit code.

The Docker stack must remain running for scheduled messages to be consumed:

```powershell
docker compose up -d
```

## 12. Power BI

Power BI connects to the final Gold table, not the staging table.

Use:

```text
Server:   127.0.0.1:5433
Database: tech_jobs
Table:    public.jobs_gold
```

Choose **Import** for a beginner-friendly report. Refresh the dataset to load new rows.

Do not select:

```text
public.jobs_gold_stage
```

That table is an internal Spark staging table.

Useful report fields:

```text
job_title
company_name
location_name
published
salary_min
salary_max
salary_currency
application_url
```

## 13. Common Commands

### Start the platform

```powershell
docker compose up -d
```

### Check services

```powershell
docker compose ps
```

### Follow Spark logs

```powershell
docker compose logs -f spark-streaming
```

### Run ingestion manually

```powershell
.\scripts\run_ingestion.ps1
```

or:

```powershell
venv\Scripts\python.exe ingestion\api_ingestion.py
```

### Check Gold row count

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT COUNT(*) FROM jobs_gold;"
```

### Inspect recent Gold records

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT id, job_title, company_name, location_name, published FROM jobs_gold ORDER BY updated_at DESC LIMIT 10;"
```

### Stop services without deleting data

```powershell
docker compose down
```

### Reset all local service data

```powershell
docker compose down -v
```

Use the reset command only when you intentionally want to delete Kafka and PostgreSQL volumes. It does not delete data already written to AWS S3.

## 14. Older Batch Transformation

`transformation/silver_cleaning.py` is an older Spark batch script. It reads local JSON files from:

```text
storage/bronze/*.json
```

It creates a temporary Spark SQL view, deduplicates by job ID, and displays cleaned records.

The active Docker streaming pipeline does not call this script. The streaming application performs the current Bronze, Silver, and Gold writes directly.

The batch script remains useful for local experimentation, but it should not be confused with the active Kafka pipeline.

## 15. Data Freshness

CleanJobData is a REST API and must be polled. It is not a push-based real-time source. The Airflow DAG currently polls hourly.

The internal flow is event-driven after polling:

```text
Hourly poll -> Kafka message -> Spark micro-batch -> database update
```

This gives near-real-time internal processing after a job is fetched, but the total freshness depends on:

- CleanJobData indexing delay.
- The scheduled polling interval.
- API rate limits.
- Kafka and Spark availability.

## 16. Scaling Considerations

The current 11-job dataset is small, so Spark has more overhead than a simple Python or pandas script. PySpark is still appropriate as an architecture and learning project because the same processing model can handle larger streams.

For real scale, consider:

- More countries and job categories.
- A configurable country list.
- API retry and exponential backoff.
- Dead-letter Kafka topic for malformed messages.
- Schema versioning.
- Kafka keys based on job ID.
- Lifecycle policies and IAM least-privilege access for the S3 bucket.
- PostgreSQL indexes on `published`, `location_name`, and `company_name`.
- Monitoring for Kafka lag, Spark query failures, and API errors.
- Secure credentials instead of local development passwords.
- TLS and password authentication for PostgreSQL.

## 17. End-to-End Runbook

1. Start Docker Desktop.
2. Start the stack:

   ```powershell
   docker compose up -d
   ```

3. Confirm Spark is running:

   ```powershell
   docker compose ps
   ```

4. Open Airflow:

   ```text
   http://localhost:8081
   ```

5. Enable or manually trigger the `tech_job_ingestion` DAG.
6. Optionally run the producer manually:

   ```powershell
   .\scripts\run_ingestion.ps1
   ```

7. Confirm Kafka publishing in the Airflow task logs.
8. Check Spark logs for micro-batch processing.
9. Check PostgreSQL:

   ```powershell
   docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT COUNT(*) FROM jobs_gold;"
   ```

10. Refresh Power BI using `127.0.0.1:5433`.
11. Build visuals from `public.jobs_gold`.

## 18. Troubleshooting

### Power BI authentication failure

Make sure Power BI uses the Docker port:

```text
127.0.0.1:5433
```

Host port `5432` belongs to another Windows PostgreSQL process on this machine.

Clear Power BI data-source permissions before reconnecting:

```text
File -> Options and settings -> Data source settings -> Clear Permissions
```

### Spark is not running

Inspect logs:

```powershell
docker compose logs --tail=100 spark-streaming
```

Common causes include connector download failures, malformed Compose commands, or a failed database sink.

### Gold count does not increase

Check all three stages:

1. Ingestion log confirms Kafka publishing.
2. Spark logs show the micro-batch was processed.
3. PostgreSQL Gold query shows the expected rows.

### Duplicate jobs

The Gold layer uses `id` as the primary key and upserts with `ON CONFLICT`. Repeated ingestion of the same job should keep the row count stable.

### Fresh start

To delete all local platform data and rebuild:

```powershell
docker compose down -v
docker compose up -d
```

This removes Kafka offsets, PostgreSQL data, and the Gold tables. It does not remove S3 objects. Use it only for a deliberate reset.
