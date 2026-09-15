# AWS Glue and Athena Setup

This project writes lake data to S3:

```text
s3://$S3_BUCKET/bronze/jobs/
s3://$S3_BUCKET/silver/jobs/
s3://$S3_BUCKET/checkpoints/jobs/
```

Athena uses the AWS Glue Data Catalog for table metadata. The SQL files in `aws/athena/` create:

- `tech_job_lake.bronze_jobs`
- `tech_job_lake.silver_jobs`
- `tech_job_lake.silver_jobs_flat`
- `tech_job_lake.jobs_by_company`

## 1. Configure Athena Query Results

In the AWS Console:

1. Open Athena.
2. Choose the same region as your S3 bucket.
3. Open Settings.
4. Set a query result location, for example:

```text
s3://$S3_BUCKET/athena-results/
```

## 2. Render SQL With Your Bucket

From the project root:

```powershell
.\scripts\render_athena_sql.ps1
```

If PowerShell blocks local scripts on your machine, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\render_athena_sql.ps1
```

This creates ready-to-run files under:

```text
build/athena/
```

## 3. Run The SQL In Athena

Run these files in Athena, in order:

1. `build/athena/create_database.sql`
2. `build/athena/create_external_tables.sql`
3. `build/athena/create_views.sql`

The external tables point directly at S3. They do not copy data into Athena.

## 4. Test Queries

```sql
SELECT count(*) AS bronze_count
FROM tech_job_lake.bronze_jobs;
```

```sql
SELECT count(*) AS silver_count
FROM tech_job_lake.silver_jobs;
```

```sql
SELECT company_name, job_count
FROM tech_job_lake.jobs_by_company
ORDER BY job_count DESC
LIMIT 10;
```

## IAM Permissions

The IAM identity running Athena needs Glue permissions plus access to the S3 data and query result prefixes.

Minimum useful actions:

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
s3:GetBucketLocation
s3:ListBucket
s3:GetObject
s3:PutObject
```

Scope S3 object permissions to:

```text
arn:aws:s3:::$S3_BUCKET/bronze/*
arn:aws:s3:::$S3_BUCKET/silver/*
arn:aws:s3:::$S3_BUCKET/athena-results/*
```

## Notes

The current Silver path is not partitioned yet, so `MSCK REPAIR TABLE` is not needed. Once the project writes partitioned paths such as `country=za/ingestion_date=...`, add partitions or run partition repair after new data lands.
