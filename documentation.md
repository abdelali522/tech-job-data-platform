# Tech Job Data Platform Documentation

This document explains the whole project step by step, including the small details that are easy to miss when you are still learning the stack.

The project is an end-to-end data engineering pipeline. It collects job data from an external API, sends each job into Kafka, processes the stream with Spark Structured Streaming, stores raw and cleaned data in AWS S3, serves curated data in PostgreSQL, schedules ingestion with Airflow, and exposes S3 data to Athena through Glue Data Catalog tables.

## 1. Big Picture

The project answers this question:

```text
Can we build a production-style data platform that collects job-market data and makes it queryable for analytics?
```

The pipeline follows this path:

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
    +--> Bronze layer in AWS S3
    |
    +--> Silver layer in AWS S3
    |
    +--> Gold layer in PostgreSQL
    |
    v
Power BI / SQL / analytics

AWS S3 Silver data
    |
    v
AWS Glue Data Catalog
    |
    v
AWS Athena SQL queries
```

Airflow sits above the ingestion process:

```text
Airflow scheduler
    |
    v
Runs Python ingestion every hour
```

Docker Compose starts the local platform:

```text
Airflow
Kafka
Spark master
Spark worker
Spark streaming job
PostgreSQL
```

AWS provides the cloud storage and query layer:

```text
S3
Glue Data Catalog
Athena
```

## 2. Why This Project Is Structured This Way

This project uses a medallion architecture.

Medallion architecture means data is stored in layers:

```text
Bronze -> Silver -> Gold
```

Each layer has a different purpose.

### Bronze

Bronze is the raw layer.

In this project, Bronze keeps the original job payloads from the API as JSON text.

Bronze is useful because:

- it preserves the source data
- it lets you reprocess data later
- it helps debug parsing or transformation issues
- it gives you an audit trail

Current path:

```text
s3://$S3_BUCKET/bronze/jobs/
```

### Silver

Silver is the cleaned and structured layer.

In this project, Silver stores parsed jobs as Parquet files.

Parquet is better for analytics than JSON because:

- it is columnar
- it is compressed
- it keeps data types
- it is faster for Spark and Athena queries

Current path:

```text
s3://$S3_BUCKET/silver/jobs/
```

### Gold

Gold is the serving layer.

In this project, Gold is stored in PostgreSQL.

Gold is useful because:

- Power BI can connect to it easily
- SQL queries are simple
- the table is deduplicated by job ID
- users do not need to understand Kafka or S3 to consume the final data

Current PostgreSQL table:

```text
jobs_gold
```

## 3. Repository Structure

The project contains these important folders and files:

```text
airflow/
  dags/
    tech_job_ingestion.py

aws/
  athena/
    create_database.sql
    create_external_tables.sql
    create_views.sql

config/
  settings.py

data/
  jobs.csv
  companies.csv

docs/
  architecture.md
  aws_glue_athena.md

ingestion/
  __init__.py
  api_ingestion.py

scripts/
  render_athena_sql.ps1
  run_ingestion.ps1

storage/
  bronze/
    raw_jobs_*.json
  gold/
    postgres_init.sql

streaming/
  spark_streaming.py

tests/
  test_ingestion.py

transformation/
  silver_cleaning.py

.env.example
.gitignore
docker-compose.yml
requirements.txt
```

Each part is explained below.

## 4. Environment Variables

The project uses environment variables for secrets and settings.

The example file is:

```text
.env.example
```

Your real file should be:

```text
.env
```

The `.env` file is ignored by Git, because it contains secrets.

The `.gitignore` file includes:

```text
.env
```

That means `.env` should not be committed to GitHub.

### API Variables

```text
CLEANJOBDATA_API_KEY=replace_me
CLEANJOBDATA_COUNTRIES=za
CLEANJOBDATA_TITLE=data engineer
CLEANJOBDATA_MAX_PAGES=5
CLEANJOBDATA_PAGE_LIMIT=20
```

These control the external API ingestion.

`CLEANJOBDATA_API_KEY` is the API token for CleanJobData.

`CLEANJOBDATA_COUNTRIES` is a comma-separated list of country codes. Example:

```text
za,us,gb
```

`CLEANJOBDATA_TITLE` is the job title search term. The default is:

```text
data engineer
```

`CLEANJOBDATA_MAX_PAGES` limits how many API pages are fetched per country.

`CLEANJOBDATA_PAGE_LIMIT` controls how many jobs are requested per API page.

### Local Service Variables

```text
POSTGRES_PASSWORD=replace_me
SPARK_MAX_OFFSETS_PER_TRIGGER=1000
```

`POSTGRES_PASSWORD` is the password used by the local PostgreSQL container.

`SPARK_MAX_OFFSETS_PER_TRIGGER` controls how many Kafka messages Spark reads per micro-batch.

### Airflow Variables

```text
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_WEBSERVER_SECRET_KEY=replace_me
```

These configure the local Airflow web UI.

The UI runs at:

```text
http://localhost:8081
```

### AWS Variables

```text
AWS_ACCESS_KEY_ID=replace_me
AWS_SECRET_ACCESS_KEY=replace_me
AWS_SESSION_TOKEN=
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=your-tech-job-bucket
ATHENA_DATABASE=tech_job_lake
ATHENA_QUERY_RESULTS=s3://your-tech-job-bucket/athena-results/
```

`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are used by Spark to write to S3.

`AWS_SESSION_TOKEN` is optional. It is used only if your AWS credentials are temporary.

`AWS_DEFAULT_REGION` must match the region of your S3 bucket.

`S3_BUCKET` is the bucket where Bronze, Silver, and checkpoints are stored.

`ATHENA_DATABASE` is the Glue/Athena database name.

`ATHENA_QUERY_RESULTS` is where Athena writes query results.

### Optional S3 Path Overrides

```text
S3_BRONZE_PATH=
S3_SILVER_PATH=
S3_CHECKPOINT_PATH=
```

If these are empty, the Spark job uses default paths:

```text
s3a://$S3_BUCKET/bronze/jobs
s3a://$S3_BUCKET/silver/jobs
s3a://$S3_BUCKET/checkpoints/jobs
```

You only need to set these if you want a custom layout.

## 5. Python Dependencies

Dependencies are listed in:

```text
requirements.txt
```

Current dependencies:

```text
confluent-kafka>=2.8,<3
kafka-python>=2.2,<3
pyspark==3.5.3
python-dotenv==1.2.2
requests==2.32.3
pytest>=8.3,<9
```

### confluent-kafka

This is a Kafka producer library.

It is fast, but it uses native compiled code.

On some Windows systems, application security policies can block its DLL files.

The project handles this by falling back to `kafka-python`.

### kafka-python

This is a pure-Python Kafka client.

It is used as a fallback if `confluent-kafka` cannot load.

### pyspark

PySpark lets Python code run Apache Spark jobs.

This project uses Spark Structured Streaming to read Kafka continuously.

### python-dotenv

This loads environment variables from the `.env` file.

### requests

This calls the CleanJobData REST API.

### pytest

This runs automated tests.

## 6. Docker Compose

The main infrastructure file is:

```text
docker-compose.yml
```

Docker Compose starts multiple containers at once.

The services are:

```text
airflow-init
airflow-webserver
airflow-scheduler
kafka
postgres
spark-master
spark-worker
spark-streaming
```

It also defines volumes:

```text
airflow_data
kafka_data
postgres_data
```

Volumes preserve service data across restarts.

## 7. Airflow Services

Airflow is used to orchestrate ingestion.

In this project, Airflow does not run Spark directly.

Airflow runs the Python ingestion script, which sends jobs to Kafka.

Spark is already running separately and continuously consuming Kafka messages.

### airflow-init

`airflow-init` prepares Airflow.

It runs:

```text
airflow db migrate
```

This creates or updates the Airflow metadata database.

Then it creates an admin user:

```text
airflow users create
```

The username, password, and email come from `.env`.

If they are not set, the defaults are:

```text
username: admin
password: admin
email: admin@example.com
```

`airflow-init` depends on Kafka being healthy.

That means it waits until Kafka is ready before running.

### airflow-webserver

`airflow-webserver` starts the Airflow web UI.

The host port is:

```text
8081
```

The container port is:

```text
8080
```

That means your browser uses:

```text
http://localhost:8081
```

The Airflow webserver mounts:

```text
./airflow/dags:/opt/airflow/dags
.:/opt/project
```

The first mount makes DAG files visible to Airflow.

The second mount makes the whole project available inside the container at:

```text
/opt/project
```

### airflow-scheduler

`airflow-scheduler` is the part of Airflow that decides when DAGs should run.

It also sees the DAG folder:

```text
/opt/airflow/dags
```

It sets:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

This is important because inside Docker, containers talk to Kafka using the service name:

```text
kafka
```

They do not use:

```text
localhost
```

## 8. Kafka Service

Kafka is the event broker.

In simple words, Kafka is a message queue for streams of data.

The ingestion script publishes each job to Kafka.

Spark reads jobs from Kafka.

The topic is:

```text
jobs.raw
```

### Kafka Ports

Kafka exposes:

```text
9094:9094
```

This means:

- from your Windows host, use `localhost:9094`
- from another Docker container, use `kafka:9092`

This split exists because Docker networking and host networking are different.

### Kafka Listeners

The Compose file defines:

```text
INTERNAL://kafka:9092
EXTERNAL://localhost:9094
```

Internal listener:

```text
kafka:9092
```

External listener:

```text
localhost:9094
```

### Kafka Healthcheck

Kafka is considered healthy when this command works:

```text
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Other services can wait for Kafka to be healthy before starting.

## 9. PostgreSQL Service

PostgreSQL stores the Gold table.

In Compose:

```text
POSTGRES_DB=tech_jobs
POSTGRES_USER=tech_jobs
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-change-me-local}
```

The host port is:

```text
5433
```

The container port is:

```text
5432
```

That means from your computer:

```text
127.0.0.1:5433
```

Inside Docker:

```text
postgres:5432
```

The database schema is initialized by:

```text
storage/gold/postgres_init.sql
```

Compose mounts that file into the PostgreSQL container:

```text
./storage/gold/postgres_init.sql:/docker-entrypoint-initdb.d/001_init.sql:ro
```

The `:ro` means read-only.

PostgreSQL automatically runs SQL files from:

```text
/docker-entrypoint-initdb.d/
```

when the database volume is first created.

## 10. PostgreSQL Tables

The file:

```text
storage/gold/postgres_init.sql
```

creates two tables:

```text
jobs_gold
jobs_gold_stage
```

### jobs_gold

This is the final table.

It has:

```sql
id TEXT PRIMARY KEY
```

The primary key prevents duplicate jobs.

Columns:

```text
id
job_title
location_name
published
job_description
application_url
company_name
salary_min
salary_max
salary_currency
updated_at
```

`updated_at` defaults to:

```sql
CURRENT_TIMESTAMP
```

That records when a row was inserted or updated.

### jobs_gold_stage

This is a temporary staging table used by Spark.

Spark writes each micro-batch here first.

Then Spark merges the staged rows into `jobs_gold`.

The staging table does not have a primary key.

It is overwritten for each batch.

## 11. Spark Services

Spark is the distributed processing engine.

This project uses three Spark-related containers:

```text
spark-master
spark-worker
spark-streaming
```

### spark-master

The master coordinates Spark jobs.

It listens on:

```text
7077
```

The Spark UI is exposed on:

```text
http://localhost:8080
```

### spark-worker

The worker runs Spark tasks.

It connects to:

```text
spark://spark-master:7077
```

It is configured with:

```text
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G
```

### spark-streaming

This container submits the streaming job.

It runs:

```text
/opt/spark/bin/spark-submit
```

The master is:

```text
spark://spark-master:7077
```

The script is:

```text
/opt/project/streaming/spark_streaming.py
```

It includes packages:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
org.postgresql:postgresql:42.7.4
org.apache.hadoop:hadoop-aws:3.3.4
```

These packages are needed for:

- reading Kafka
- writing PostgreSQL
- writing AWS S3 through S3A

The Ivy cache is set to:

```text
/tmp/.ivy2
```

This gives Spark a writable place to download dependency jars.

## 12. Ingestion Configuration

The file:

```text
config/settings.py
```

loads values from `.env`.

It does this:

```python
project_root = Path(__file__).resolve().parent.parent
load_dotenv(project_root / ".env")
```

That means:

- find the folder containing the project
- load `.env` from that folder

It exposes:

```text
CLEANJOBDATA_API_KEY
CLEANJOBDATA_API_URL
CLEANJOBDATA_TITLE
CLEANJOBDATA_COUNTRIES
CLEANJOBDATA_MAX_PAGES
CLEANJOBDATA_PAGE_LIMIT
```

The API URL is hard-coded:

```text
https://api.cleanjobdata.com/jobs
```

The default title is:

```text
data engineer
```

The default country is:

```text
za
```

The country setting is parsed like this:

```python
tuple(
    country.strip().lower()
    for country in os.getenv("CLEANJOBDATA_COUNTRIES", "za").split(",")
    if country.strip()
)
```

That means:

- read the environment variable
- split it by commas
- remove spaces
- convert each country to lowercase
- ignore empty values

Example:

```text
CLEANJOBDATA_COUNTRIES=za, us, gb
```

becomes:

```python
("za", "us", "gb")
```

## 13. Ingestion Script

The main ingestion file is:

```text
ingestion/api_ingestion.py
```

It has three main jobs:

1. call the CleanJobData API
2. validate the response shape
3. publish each job to Kafka

### Import Path Setup

At the top, the script calculates the project root:

```python
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
```

Then it adds the project root to `sys.path` if needed:

```python
if project_root not in sys.path:
    sys.path.insert(0, project_root)
```

This makes imports like this work:

```python
from config.settings import ...
```

This is useful when you run:

```powershell
python ingestion\api_ingestion.py
```

because Python would otherwise start with `ingestion/` as the script directory.

### HTTP Session With Retries

The function:

```python
create_http_session()
```

creates a `requests.Session`.

It attaches retry behavior:

```text
total retries: 4
connect retries: 4
read retries: 4
backoff factor: 1
retry HTTP statuses: 429, 500, 502, 503, 504
allowed method: GET
```

This matters because APIs can fail temporarily.

For example:

- `429` means rate limited
- `500` means server error
- `502`, `503`, `504` often mean gateway or temporary service problems

The retry strategy makes ingestion more robust.

### Fetching Jobs

The function:

```python
fetch_tech_jobs(country, session=None)
```

fetches jobs for one country.

First, it checks that the API key exists:

```python
if not CLEANJOBDATA_API_KEY:
    raise RuntimeError(...)
```

If the key is missing, the script stops immediately.

It starts with:

```python
all_jobs = []
next_page = None
```

`all_jobs` stores every job collected.

`next_page` stores the pagination cursor.

For each page, it builds query parameters:

```python
params = {
    "title": CLEANJOBDATA_TITLE,
    "location": country.upper(),
    "limit": CLEANJOBDATA_PAGE_LIMIT,
    "extra_fields": "description",
}
```

Important details:

- `country.upper()` turns `za` into `ZA`
- `extra_fields=description` requests full job descriptions
- `limit` controls page size

If there is a next-page cursor, it adds:

```python
params["next_page"] = next_page
```

Then it makes the GET request:

```python
response = session.get(
    CLEANJOBDATA_API_URL,
    headers={"Authorization": f"Bearer {CLEANJOBDATA_API_KEY}"},
    params=params,
    timeout=30,
)
```

The API key is sent as:

```text
Authorization: Bearer <key>
```

The timeout is:

```text
30 seconds
```

Then:

```python
response.raise_for_status()
```

raises an error if the API returns a bad HTTP status.

The JSON response is parsed:

```python
data = response.json()
```

The script expects:

```python
data["data"]
```

to be a list.

If it is not a list, it raises:

```python
ValueError("CleanJobData response field 'data' must be a list")
```

This prevents bad API shapes from silently entering Kafka.

Pagination is handled through:

```python
next_page = data.get("pagination", {}).get("next_page")
```

If there is no `next_page`, the loop stops.

### Publishing Jobs To Kafka

The function:

```python
publish_jobs_to_kafka(jobs)
```

sends collected jobs to Kafka.

If the list is empty:

```python
if not jobs:
    print("No jobs were collected.")
    return
```

No empty Kafka messages are created.

The Kafka bootstrap server is:

```python
os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
```

That means:

- if Airflow sets `KAFKA_BOOTSTRAP_SERVERS`, use that
- otherwise default to `localhost:9094`

When running from Windows manually, use:

```text
localhost:9094
```

When running from Airflow in Docker, use:

```text
kafka:9092
```

### Kafka Producer Fallback

The script first tries:

```python
from confluent_kafka import Producer
```

If that fails with `ImportError`, it calls:

```python
publish_jobs_with_kafka_python(jobs, bootstrap_servers)
```

This fallback exists because Windows can block the native DLL used by `confluent-kafka`.

The fallback uses:

```python
from kafka import KafkaProducer
```

The fallback serializes each job with:

```python
json.dumps(job, separators=(",", ":")).encode("utf-8")
```

That creates compact JSON bytes.

Both producer paths publish to:

```text
jobs.raw
```

### Main Block

At the bottom:

```python
if __name__ == "__main__":
    jobs = []
    for country in CLEANJOBDATA_COUNTRIES:
        jobs.extend(fetch_tech_jobs(country))
    publish_jobs_to_kafka(jobs)
```

This means:

1. start with an empty job list
2. fetch jobs for every configured country
3. combine all jobs into one list
4. publish them to Kafka

## 14. Airflow DAG

The Airflow DAG is:

```text
airflow/dags/tech_job_ingestion.py
```

It defines:

```python
dag_id="tech_job_ingestion"
```

This is the name you see in Airflow UI.

The DAG description is:

```text
Fetch CleanJobData jobs and publish them to Kafka for Spark processing.
```

### Schedule

The schedule is:

```python
schedule_interval="@hourly"
```

That means Airflow runs ingestion once per hour.

### Start Date

The start date is:

```python
start_date=days_ago(1)
```

This tells Airflow the DAG is valid starting from yesterday.

### Catchup

The DAG has:

```python
catchup=False
```

This is important.

Without it, Airflow might try to run old missed hourly runs.

With `catchup=False`, it only runs current and future schedules.

### Max Active Runs

The DAG has:

```python
max_active_runs=1
```

This prevents two ingestion runs from overlapping.

### Retries

Default args include:

```python
"retries": 2
"retry_delay": timedelta(minutes=5)
```

If ingestion fails, Airflow retries twice.

It waits five minutes between retries.

### BashOperator

The DAG has one task:

```python
publish_jobs_to_kafka = BashOperator(...)
```

The command is:

```bash
cd /opt/project && python ingestion/api_ingestion.py
```

This means:

1. go to the project folder inside the container
2. run the ingestion script

The task sets:

```python
"KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"
```

This makes the ingestion script publish to Kafka inside Docker.

`append_env=True` means Airflow keeps the existing environment variables and adds the Kafka variable.

## 15. Spark Streaming Script

The streaming job is:

```text
streaming/spark_streaming.py
```

This is the core transformation and storage pipeline.

It reads:

```text
Kafka topic jobs.raw
```

It writes:

```text
Bronze JSON text to S3
Silver Parquet to S3
Gold rows to PostgreSQL
Invalid rows to Kafka topic jobs.dlq
```

### Job Schema

The variable:

```python
JOB_SCHEMA
```

defines the fields Spark expects from each job JSON.

Fields:

```text
id
title
location
published
description
application_url
has_remote
salary_min
salary_max
salary_currency
company.name
```

Types:

- IDs and text are `StringType`
- remote flag is `BooleanType`
- salary values are `DoubleType`
- company is a nested struct

This schema lets Spark parse raw JSON strings into columns.

### required_env

The helper:

```python
required_env(name)
```

reads an environment variable.

If it is empty or missing, it raises:

```python
RuntimeError
```

The script uses it for:

```text
S3_BUCKET
```

because the bucket is required.

### s3_path

The helper:

```python
s3_path(bucket, *parts)
```

builds paths like:

```text
s3a://bucket/bronze/jobs
```

S3A is Hadoop's S3 connector scheme.

Spark uses `s3a://`, not `s3://`, for S3 file operations.

### optional_env

The helper:

```python
optional_env(name, default)
```

returns the environment variable if it has a value.

If the variable is empty, it returns the default.

This matters because Docker Compose can pass an empty string.

Without this helper, an empty string could accidentally replace the default path.

### configure_s3

The helper:

```python
configure_s3(builder)
```

adds S3 settings to Spark.

It sets:

```text
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint.region=<AWS_DEFAULT_REGION>
```

If `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` exist, it configures explicit credentials.

If `AWS_SESSION_TOKEN` exists, it uses temporary credentials.

If `S3_ENDPOINT_URL` exists, it also configures a custom endpoint.

That custom endpoint option is useful for S3-compatible systems, but the current project uses AWS S3.

### Main S3 Paths

In `main()`, the script reads:

```python
s3_bucket = required_env("S3_BUCKET")
```

Then it builds:

```python
bronze_path = optional_env("S3_BRONZE_PATH", s3_path(s3_bucket, "bronze", "jobs"))
silver_path = optional_env("S3_SILVER_PATH", s3_path(s3_bucket, "silver", "jobs"))
checkpoint = optional_env("S3_CHECKPOINT_PATH", s3_path(s3_bucket, "checkpoints", "jobs"))
```

So the default paths are:

```text
s3a://$S3_BUCKET/bronze/jobs
s3a://$S3_BUCKET/silver/jobs
s3a://$S3_BUCKET/checkpoints/jobs
```

### Spark Session

Spark is created with:

```python
SparkSession.builder.appName("TechJobMarketStreaming")
```

Then `configure_s3` adds S3 config.

Finally:

```python
.getOrCreate()
```

creates the session or reuses an existing one.

The app name appears in Spark UI and logs.

### Reading Kafka

The stream reads Kafka like this:

```python
spark.readStream.format("kafka")
```

Important options:

```text
kafka.bootstrap.servers = KAFKA_BOOTSTRAP_SERVERS
subscribe = jobs.raw
startingOffsets = earliest
failOnDataLoss = false
maxOffsetsPerTrigger = SPARK_MAX_OFFSETS_PER_TRIGGER
```

`startingOffsets=earliest` means when there is no checkpoint, Spark starts from the earliest available Kafka messages.

Checkpoints prevent Spark from rereading old messages after restart.

`failOnDataLoss=false` makes Spark more tolerant if Kafka has removed old offsets.

`maxOffsetsPerTrigger` limits how many messages are processed per micro-batch.

### Parsing Kafka Messages

Kafka stores the message value as binary.

Spark casts it to a string:

```python
col("value").cast("string")
```

Then it parses JSON:

```python
from_json(col("value").cast("string"), JOB_SCHEMA)
```

The result has:

```text
raw
job
```

`raw` is the original JSON string.

`job` is the parsed structured object.

### Invalid Jobs

Invalid jobs are detected with:

```python
parsed_stream.where(col("job.id").isNull())
```

If a payload cannot be parsed or does not contain an ID, it is considered invalid.

Invalid jobs are written to Kafka topic:

```text
jobs.dlq
```

DLQ means dead-letter queue.

The DLQ message contains:

```text
raw payload
error message
```

### Valid Jobs

Valid jobs are:

```python
jobs = parsed_stream.where(col("job.id").isNotNull()).select("job.*")
```

This filters out bad records and expands the job fields into columns.

### Bronze Write

Bronze uses:

```python
bronze.writeStream.format("text")
```

The output path is:

```text
s3a://$S3_BUCKET/bronze/jobs
```

The checkpoint path is:

```text
s3a://$S3_BUCKET/checkpoints/jobs/bronze
```

Bronze writes the original raw JSON string as text.

### Silver Write

Silver uses:

```python
jobs.writeStream.format("parquet")
```

The output path is:

```text
s3a://$S3_BUCKET/silver/jobs
```

The checkpoint path is:

```text
s3a://$S3_BUCKET/checkpoints/jobs/silver
```

Silver stores typed columns as Parquet.

### Gold Write

Gold uses:

```python
jobs.writeStream.foreachBatch(write_gold)
```

`foreachBatch` runs custom logic for each micro-batch.

The checkpoint path is:

```text
s3a://$S3_BUCKET/checkpoints/jobs/gold
```

The function:

```python
write_gold(batch_df, batch_id)
```

does the PostgreSQL write.

### write_gold Details

First:

```python
if batch_df.rdd.isEmpty():
    return
```

If the batch has no rows, do nothing.

Then it selects and renames columns:

```text
id -> id
title -> job_title
location -> location_name
published -> published timestamp
description -> job_description
application_url -> application_url
company.name -> company_name
salary_min -> salary_min
salary_max -> salary_max
salary_currency -> salary_currency
```

It also deduplicates:

```python
.dropDuplicates(["id"])
```

Then it writes the batch to:

```text
jobs_gold_stage
```

using JDBC.

After staging, it opens a JDBC connection and runs an upsert:

```sql
INSERT INTO jobs_gold (...)
SELECT ...
FROM jobs_gold_stage
ON CONFLICT (id) DO UPDATE SET ...
```

This means:

- if the job ID is new, insert it
- if the job ID already exists, update the row

This is what makes the Gold table idempotent.

Idempotent means running the same ingestion again should not create duplicate Gold rows.

## 16. AWS S3 Layout

The S3 bucket contains:

```text
bronze/
silver/
checkpoints/
athena-results/
```

### bronze/

Stores raw JSON text files created by Spark.

### silver/

Stores Parquet files created by Spark.

### checkpoints/

Stores Spark checkpoint metadata.

Do not casually delete checkpoints.

If you delete checkpoints, Spark may reread Kafka messages from the beginning and reprocess old data.

### athena-results/

Stores Athena query results.

Athena requires an output location for query results.

## 17. AWS IAM Permissions

The AWS access key used by Spark needs permissions for S3.

At minimum, Spark needs to:

- list the bucket
- get bucket location
- read objects
- write objects
- delete objects when needed by commit/checkpoint logic
- manage multipart uploads

Typical actions:

```text
s3:ListBucket
s3:GetBucketLocation
s3:GetObject
s3:PutObject
s3:DeleteObject
s3:AbortMultipartUpload
s3:ListMultipartUploadParts
```

Athena and Glue need additional permissions:

```text
glue:CreateDatabase
glue:GetDatabase
glue:GetDatabases
glue:CreateTable
glue:GetTable
glue:GetTables
glue:UpdateTable
athena:StartQueryExecution
athena:GetQueryExecution
athena:GetQueryResults
```

The safest setup is least privilege.

That means permissions should target only your project bucket and prefixes.

## 18. AWS Glue and Athena

Athena lets you query S3 files with SQL.

Glue Data Catalog stores table metadata.

The Athena files are in:

```text
aws/athena/
```

There are three SQL files:

```text
create_database.sql
create_external_tables.sql
create_views.sql
```

### create_database.sql

This creates a database:

```sql
CREATE DATABASE IF NOT EXISTS tech_job_lake
COMMENT 'External tables over the Tech Job S3 data lake';
```

This database is a logical container for Athena tables.

### create_external_tables.sql

This creates:

```text
tech_job_lake.bronze_jobs
tech_job_lake.silver_jobs
```

`bronze_jobs` reads JSON from:

```text
s3://$S3_BUCKET/bronze/jobs/
```

`silver_jobs` reads Parquet from:

```text
s3://$S3_BUCKET/silver/jobs/
```

These are external tables.

External table means Athena does not own the data.

It only points to data already stored in S3.

### create_views.sql

This creates views:

```text
silver_jobs_flat
jobs_by_company
```

`silver_jobs_flat` flattens fields into analytics-friendly names.

For example:

```text
title -> job_title
location -> location_name
company.name -> company_name
```

`jobs_by_company` counts jobs per company.

### Rendering Athena SQL

The SQL templates contain:

```text
${S3_BUCKET}
```

The helper script:

```text
scripts/render_athena_sql.ps1
```

replaces that placeholder with your real bucket name from `.env`.

It writes generated SQL to:

```text
build/athena/
```

Run it with:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\render_athena_sql.ps1
```

The `ExecutionPolicy Bypass` part is useful if Windows blocks local PowerShell scripts.

Generated files are ignored by Git because `.gitignore` includes:

```text
build/
```

## 19. Manual Ingestion Script

The file:

```text
scripts/run_ingestion.ps1
```

is a Windows helper script.

It is useful if you want to run ingestion manually without Airflow.

It does these steps:

1. stops on errors
2. finds the project root
3. finds the virtual environment Python executable
4. creates a log directory
5. runs `ingestion/api_ingestion.py`
6. appends output to a daily log file
7. throws an error if Python exits with a non-zero exit code

The log directory is:

```text
storage/logs/
```

The log file format is:

```text
ingestion_yyyyMMdd.log
```

Example:

```text
storage/logs/ingestion_20260915.log
```

`storage/logs/` is ignored by Git.

## 20. Older Batch Transformation

The file:

```text
transformation/silver_cleaning.py
```

is an older local batch Spark script.

It is not part of the active streaming pipeline.

It reads local JSON files from:

```text
storage/bronze/*.json
```

It creates a Spark SQL temp view:

```text
raw_jobs
```

Then it runs SQL to group by job ID:

```sql
SELECT
    id,
    FIRST(title) AS job_title,
    FIRST(location) AS location_name,
    FIRST(published) AS created_date,
    FIRST(description) AS job_description
FROM raw_jobs
GROUP BY id
```

This demonstrates deduplication and flattening.

The active streaming pipeline now does the main processing instead.

Keep this file as a learning or experimentation script.

## 21. Tests

Tests are in:

```text
tests/test_ingestion.py
```

They focus on ingestion logic.

### FakeResponse

`FakeResponse` imitates a `requests` response.

It has:

```python
raise_for_status()
json()
```

This lets tests run without calling the real API.

### FakeSession

`FakeSession` imitates a `requests.Session`.

It records calls in:

```python
self.calls
```

It returns two fake pages:

Page 1:

```python
{"data": [{"id": "first"}], "pagination": {"next_page": "cursor-2"}}
```

Page 2:

```python
{"data": [{"id": "second"}], "pagination": {"next_page": None}}
```

### test_fetch_tech_jobs_follows_cursor_pagination

This verifies:

- the function follows cursor pagination
- the country is sent uppercase
- the first request does not include `next_page`
- the second request includes `next_page`
- timeout is 30 seconds

### test_fetch_tech_jobs_rejects_invalid_data_shape

This verifies that the API response must contain a list under `data`.

If `data` is not a list, the function raises `ValueError`.

### test_create_http_session_configures_retries

This verifies:

- retry total is 4
- HTTP 429 is in the retry status list

To run tests:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe -m pytest
```

## 22. Data Files

The folder:

```text
data/
```

contains CSV files:

```text
jobs.csv
companies.csv
```

These appear to be local datasets or earlier data assets.

They are not used by the active Docker streaming pipeline.

The active pipeline uses:

```text
CleanJobData API -> Kafka -> Spark -> S3/PostgreSQL
```

## 23. Local Bronze JSON Files

The folder:

```text
storage/bronze/
```

contains older JSON files:

```text
raw_jobs_*.json
```

These are local files from earlier ingestion runs.

They are separate from the active S3 Bronze layer.

Active Bronze is:

```text
s3://$S3_BUCKET/bronze/jobs/
```

Local Bronze is:

```text
storage/bronze/
```

## 24. How To Start The Project

### Step 1: Start Docker Desktop

Open Docker Desktop.

Wait until Docker says it is running.

### Step 2: Confirm Docker Works

Run:

```powershell
docker version
```

If Docker is not running, you may see an error about:

```text
dockerDesktopLinuxEngine
```

In that case, restart Docker Desktop.

### Step 3: Create `.env`

Copy:

```text
.env.example
```

to:

```text
.env
```

Fill in real values.

Important values:

```text
CLEANJOBDATA_API_KEY=...
POSTGRES_PASSWORD=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=...
S3_BUCKET=...
```

### Step 4: Start The Stack

Run:

```powershell
docker compose up -d
```

The `-d` means detached mode.

Detached mode starts containers in the background.

### Step 5: Check Containers

Run:

```powershell
docker compose ps
```

You should see services like:

```text
kafka
postgres
spark-master
spark-worker
spark-streaming
airflow-webserver
airflow-scheduler
```

### Step 6: Open Airflow

Open:

```text
http://localhost:8081
```

Log in with your Airflow username and password.

Default:

```text
admin / admin
```

### Step 7: Trigger The DAG

Find:

```text
tech_job_ingestion
```

Enable it or trigger it manually.

This runs:

```text
python ingestion/api_ingestion.py
```

inside the Airflow container.

### Step 8: Watch Airflow Logs

Open the task logs in Airflow.

You should see messages like:

```text
Starting ingestion for za...
Fetching Page 1...
Published 11 jobs to Kafka topic jobs.raw
```

The number may change depending on API results.

### Step 9: Watch Spark Logs

Run:

```powershell
docker compose logs --tail=200 spark-streaming
```

Look for errors.

If there are no errors, Spark is processing.

### Step 10: Check S3

In AWS S3, check your bucket.

You should see:

```text
bronze/
silver/
checkpoints/
```

Inside Bronze and Silver, Spark creates files.

### Step 11: Check PostgreSQL

Run:

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT COUNT(*) FROM jobs_gold;"
```

If the count is above zero, Gold has data.

## 25. How The Full Run Works Internally

This is the sequence when the pipeline runs correctly:

1. Docker starts Kafka.
2. Docker starts PostgreSQL.
3. Docker starts Spark master.
4. Docker starts Spark worker.
5. Docker starts Spark streaming job.
6. Docker starts Airflow services.
7. Airflow scheduler sees `tech_job_ingestion`.
8. The DAG runs the ingestion script.
9. The ingestion script reads `.env`.
10. The ingestion script calls CleanJobData.
11. The API returns job JSON.
12. The ingestion script publishes each job to Kafka topic `jobs.raw`.
13. Spark reads messages from `jobs.raw`.
14. Spark parses each JSON message.
15. Spark writes raw JSON to S3 Bronze.
16. Spark writes parsed Parquet to S3 Silver.
17. Spark writes malformed messages to `jobs.dlq`.
18. Spark writes cleaned Gold rows to PostgreSQL staging.
19. Spark upserts staging rows into `jobs_gold`.
20. Athena can query S3 through Glue tables.
21. Power BI can query PostgreSQL Gold.

## 26. Common Commands

Start everything:

```powershell
docker compose up -d
```

Stop everything:

```powershell
docker compose down
```

See service status:

```powershell
docker compose ps
```

See Spark logs:

```powershell
docker compose logs --tail=200 spark-streaming
```

See Airflow scheduler logs:

```powershell
docker compose logs --tail=200 airflow-scheduler
```

See Airflow webserver logs:

```powershell
docker compose logs --tail=200 airflow-webserver
```

Run ingestion manually from Windows:

```powershell
.\scripts\run_ingestion.ps1
```

Run Python ingestion manually:

```powershell
.\venv\Scripts\python.exe ingestion\api_ingestion.py
```

Run tests:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe -m pytest
```

Render Athena SQL:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\render_athena_sql.ps1
```

Query Gold count:

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT COUNT(*) FROM jobs_gold;"
```

Inspect recent Gold records:

```powershell
docker compose exec -T postgres psql -U tech_jobs -d tech_jobs -c "SELECT id, job_title, company_name, location_name, published FROM jobs_gold ORDER BY updated_at DESC LIMIT 10;"
```

## 27. Troubleshooting

### Docker Cannot Connect To The Engine

Error example:

```text
failed to connect to the docker API
```

Common cause:

Docker Desktop is not running.

Fix:

1. Open Docker Desktop.
2. Wait until it is running.
3. Try `docker compose ps`.

If needed:

```powershell
wsl --shutdown
```

Then restart Docker Desktop.

### Docker Permission Denied

Error example:

```text
permission denied while trying to connect to the docker API
```

Possible fixes:

- run PowerShell as Administrator
- add your Windows user to the `docker-users` group
- sign out and sign back in

### CleanJobData API Key Missing

Error:

```text
Set the CLEANJOBDATA_API_KEY environment variable before running ingestion.
```

Fix:

Add this to `.env`:

```text
CLEANJOBDATA_API_KEY=your_key
```

### API Network Error

Possible symptoms:

```text
Failed to establish a new connection
```

Possible causes:

- no internet connection
- firewall
- API temporarily unavailable
- local execution sandbox/network restrictions

### Kafka Connection Error

If running ingestion from Windows, use:

```text
localhost:9094
```

If running inside Docker, use:

```text
kafka:9092
```

Airflow sets this automatically.

### confluent_kafka DLL Blocked

Error example:

```text
DLL load failed while importing cimpl
```

The project has a fallback to `kafka-python`.

Make sure dependencies are installed:

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

### S3 403 Forbidden

Error example:

```text
AmazonS3Exception: Forbidden
Status Code: 403
```

This means AWS credentials are valid enough to reach S3, but do not have permission for the action.

Fix IAM permissions for the bucket.

Make sure the key can access:

```text
bronze/*
silver/*
checkpoints/*
athena-results/*
```

### S3 NoSuchBucket

Error:

```text
NoSuchBucket
```

Fix:

Check:

```text
S3_BUCKET
```

in `.env`.

It must exactly match the bucket name.

### S3 Region Problem

Symptoms may include redirect or signature errors.

Fix:

Set:

```text
AWS_DEFAULT_REGION
```

to the actual S3 bucket region.

### PostgreSQL Gold Count Is Zero

Check each step:

1. Did Airflow ingestion publish jobs?
2. Did Kafka receive jobs?
3. Is Spark streaming running?
4. Are there Spark errors?
5. Did S3 Bronze/Silver files appear?
6. Did PostgreSQL start correctly?

Useful command:

```powershell
docker compose logs --tail=200 spark-streaming
```

### Airflow DAG Does Not Appear

Check:

```powershell
docker compose logs --tail=200 airflow-scheduler
```

Also confirm the DAG file exists:

```text
airflow/dags/tech_job_ingestion.py
```

### PowerShell Script Execution Disabled

Error:

```text
execution of scripts is disabled on this system
```

Use:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\render_athena_sql.ps1
```

or for ingestion:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_ingestion.ps1
```

## 28. What To Explain In An Interview

If asked to explain the project, start with the business purpose:

```text
I built a data engineering pipeline that collects job-market data and turns it into analytics-ready data.
```

Then explain the architecture:

```text
Airflow schedules ingestion. Python calls the API and publishes jobs to Kafka. Spark Structured Streaming reads Kafka, writes raw data to S3 Bronze, cleaned Parquet to S3 Silver, and curated data to PostgreSQL Gold. Glue and Athena allow SQL queries directly on S3.
```

Then explain reliability:

- API retries
- Kafka decoupling
- Spark checkpoints
- dead-letter queue
- PostgreSQL upsert
- S3 medallion layers

Then explain cloud:

- AWS S3 stores the data lake
- Glue catalogs the S3 data
- Athena queries files directly
- IAM controls access

Then explain orchestration:

- Airflow runs ingestion hourly
- retries are configured
- logs are visible in the UI

## 29. Current Strengths

This project demonstrates:

- REST API ingestion
- environment-based configuration
- Kafka event streaming
- Spark Structured Streaming
- AWS S3 data lake
- Bronze/Silver/Gold architecture
- PostgreSQL serving layer
- Airflow orchestration
- Glue/Athena external tables
- Docker Compose infrastructure
- automated tests for ingestion behavior
- Power BI-ready Gold table

## 30. Current Limitations

The project is good, but these are limitations to know:

- Silver data is not partitioned yet
- data quality checks are basic
- Airflow uses local SequentialExecutor
- PostgreSQL is local, not managed cloud PostgreSQL
- AWS credentials are passed through environment variables
- monitoring is mostly logs
- no CI/CD pipeline yet
- no Terraform/IaC for AWS resources yet

These are normal for a portfolio project.

They are also good next improvement topics.

## 31. Recommended Next Improvements

### Add Partitioning

Write Silver like:

```text
silver/jobs/country=za/ingestion_date=2026-09-15/
```

This helps Athena and Spark scan less data.

### Add Data Quality Checks

Check for:

- missing IDs
- duplicate IDs
- missing company names
- invalid dates
- salary minimum greater than salary maximum

### Add Dashboard Screenshots

Power BI visuals can show:

- jobs by company
- jobs by country
- remote vs onsite
- salary ranges
- job posting trends

### Add GitHub Actions

Run tests automatically:

```text
pytest
python -m py_compile
```

### Add Terraform

Use infrastructure as code for:

- S3 bucket
- IAM policy
- Glue database
- Athena result location

### Add Better Monitoring

Track:

- number of jobs ingested
- Kafka lag
- Spark failures
- last successful Airflow run
- S3 files written
- Gold table row count

## 32. Glossary

### API

An interface for requesting data from another system.

This project uses the CleanJobData API.

### Kafka

A streaming platform used to store and move event messages.

This project sends one job per Kafka message.

### Topic

A named stream in Kafka.

This project uses:

```text
jobs.raw
jobs.dlq
```

### Spark

A distributed data processing engine.

This project uses Spark Structured Streaming.

### Structured Streaming

Spark's streaming API.

It processes data in small continuous micro-batches.

### S3

AWS object storage.

This project uses S3 as the data lake.

### S3A

The Hadoop connector used by Spark to read and write S3.

Spark paths use:

```text
s3a://
```

### Parquet

A columnar file format optimized for analytics.

Silver data is stored as Parquet.

### PostgreSQL

A relational database.

This project uses it for the Gold serving table.

### Airflow

An orchestration tool for scheduling and monitoring workflows.

This project uses Airflow to run ingestion hourly.

### DAG

Directed Acyclic Graph.

In Airflow, a DAG defines a workflow.

This project has:

```text
tech_job_ingestion
```

### Glue Data Catalog

AWS metadata catalog.

Athena uses it to know table names, schemas, and S3 locations.

### Athena

AWS service that queries S3 data using SQL.

### Checkpoint

Metadata that lets Spark remember what it has already processed.

### DLQ

Dead-letter queue.

A place for bad records that could not be processed normally.

### Upsert

Insert or update.

If a row does not exist, insert it.

If it exists, update it.

## 33. Final Mental Model

Think of the project like a factory.

The API is the supplier.

The ingestion script is the truck that brings raw material.

Kafka is the loading dock.

Spark is the factory line.

S3 Bronze is the raw storage room.

S3 Silver is the cleaned storage room.

PostgreSQL Gold is the showroom for business users.

Airflow is the shift manager.

Glue is the inventory catalog.

Athena is the analyst asking questions directly against the warehouse.

Power BI is the final dashboard.

The most important technical flow is:

```text
Airflow -> Python ingestion -> Kafka -> Spark -> S3 + PostgreSQL -> Athena/BI
```

If you understand that flow, you understand the project.
